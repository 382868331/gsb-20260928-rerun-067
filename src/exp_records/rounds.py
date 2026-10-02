from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import canonical_json
from .pipeline import Pipeline, StageContext, StageDef


CONFIRMED = "confirmed"
RERUN = "rerun"
UNPROVEN = "unproven"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def select_parameters(
    baseline: dict[str, Any],
    adjustments: dict[str, Any],
    adopted: set[str] | None = None,
) -> dict[str, Any]:
    """Build a candidate parameter set by adopting only part of a revision.

    Parameters not adopted keep the baseline value; the candidate is then
    re-planned from scratch, so rejected adjustments can never contribute.
    """

    merged = dict(baseline)
    for name, value in adjustments.items():
        if adopted is None or name in adopted:
            merged[name] = value
    return merged


class StageStore:
    """Content-addressed, per-stage store with reuse plans and evidence.

    Directory layout::

        stages/<pipeline>/<stage>/<stage_hash>/record.json + artifacts/
        staging/<pipeline>/<stage>/<attempt_id>/
        evidence/<evidence_id>/{manifest.json,artifacts/}
        locks/<stage_hash>.lock
        attempts/<pipeline>/<stage>/<stage_hash>.jsonl
        rounds/<round_id>.json

    A stage hash never depends on file names: input content digests, the
    declared parameter subset and the exact upstream stage hashes and
    artifact digests are all folded into the identity.
    """

    def __init__(self, root: str | Path, pipeline: Pipeline):
        self.root = Path(root)
        self.pipeline = pipeline
        self.executor_id = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        for name in ("stages", "staging", "evidence", "locks", "attempts", "rounds"):
            (self.root / name).mkdir(parents=True, exist_ok=True)

    # ---- identity -------------------------------------------------------

    @staticmethod
    def _artifact_digests(artifact_dir: Path) -> dict[str, str]:
        digests: dict[str, str] = {}
        for path in sorted(artifact_dir.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(artifact_dir).as_posix()
            digests[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        return digests

    def _stage_basis(
        self,
        stage: StageDef,
        inputs: dict[str, bytes],
        parameters: dict[str, Any],
        environment: dict[str, Any],
        resolved: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        input_entries = [
            {
                "name": name,
                "sha256": hashlib.sha256(inputs[name]).hexdigest(),
                "size": len(inputs[name]),
            }
            for name in sorted(stage.inputs)
        ]
        upstream_entries = []
        for upstream_name in stage.upstreams:
            upstream = resolved[upstream_name]
            upstream_entries.append(
                {
                    "stage": upstream_name,
                    "stage_hash": upstream["stage_hash"],
                    "artifacts": [
                        {"path": path, "sha256": digest}
                        for path, digest in sorted(upstream["artifacts"].items())
                    ],
                }
            )
        return {
            "pipeline": self.pipeline.name,
            "stage": stage.name,
            "inputs": input_entries,
            "parameters": {name: parameters.get(name) for name in sorted(stage.parameters)},
            "environment": environment,
            "upstreams": upstream_entries,
        }

    @staticmethod
    def stage_hash_for(basis: dict[str, Any]) -> str:
        return hashlib.sha256(canonical_json(basis)).hexdigest()

    def _stage_dir(self, stage_name: str, stage_hash: str) -> Path:
        return self.root / "stages" / self.pipeline.name / stage_name / stage_hash

    # ---- verification ---------------------------------------------------

    def _read_record_unchecked(self, stage_name: str, stage_hash: str) -> dict[str, Any] | None:
        record_path = self._stage_dir(stage_name, stage_hash) / "record.json"
        if not record_path.is_file():
            return None
        try:
            return json.loads(record_path.read_bytes())
        except (OSError, json.JSONDecodeError):
            return None

    def _load_confirmed(
        self, stage_name: str, stage_hash: str
    ) -> tuple[dict[str, Any], dict[str, str]] | None:
        """Return a record only when every artifact still hashes correctly."""
        stage_dir = self._stage_dir(stage_name, stage_hash)
        record = self._read_record_unchecked(stage_name, stage_hash)
        if record is None or record.get("stage_hash") != stage_hash:
            return None
        artifacts = record.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            return None
        digests: dict[str, str] = {}
        artifact_root = stage_dir / "artifacts"
        for item in artifacts:
            path = artifact_root / item["path"]
            if not path.is_file():
                return None
            if path.stat().st_size != item["size"]:
                return None
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != item["sha256"]:
                return None
            digests[item["path"]] = digest
        return record, digests

    # ---- planning -------------------------------------------------------

    def plan(
        self,
        inputs: dict[str, bytes],
        parameters: dict[str, Any],
        environment: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve every stage identity and classify confirmed/rerun/unproven.

        ``confirmed`` means a verified published record with the exact same
        basis exists. ``rerun`` means the basis is sound but nothing usable
        exists (or it changed). ``unproven`` marks stages whose upstream is
        missing, failed or damaged: their own hash cannot be trusted yet.
        Same-named files never count as reuse; only content digests do.
        """

        self.pipeline.validate(inputs, parameters)
        resolved: dict[str, dict[str, Any]] = {}
        stages_report: dict[str, dict[str, Any]] = {}
        for stage_name in self.pipeline.ordered_stage_names():
            stage = self.pipeline.stages[stage_name]
            blocked_by: list[str] = []
            for upstream_name in stage.upstreams:
                if resolved[upstream_name]["status"] != CONFIRMED:
                    blocked_by.append(upstream_name)
            if blocked_by:
                stages_report[stage_name] = {
                    "status": UNPROVEN,
                    "reasons": [f"upstream_not_confirmed:{name}" for name in blocked_by],
                    "basis": None,
                    "stage_hash": None,
                    "upstreams": [
                        {
                            "stage": name,
                            "stage_hash": resolved[name]["stage_hash"],
                            "artifacts": resolved[name]["artifacts"],
                        }
                        for name in stage.upstreams
                    ],
                }
                resolved[stage_name] = {
                    "status": UNPROVEN,
                    "stage_hash": None,
                    "artifacts": {},
                }
                continue

            basis = self._stage_basis(stage, inputs, parameters, environment, resolved)
            stage_hash = self.stage_hash_for(basis)
            record = self._matching_record(stage, basis)
            if record is not None and record["stage_hash"] == stage_hash:
                status = CONFIRMED
                reasons: list[str] = []
                artifacts = record["digests"]
                origin = record["record"].get("origin")
            else:
                status = RERUN
                reasons = self._rerun_reasons(stage, basis, resolved, record)
                artifacts = {}
                origin = None
            stages_report[stage_name] = {
                "status": status,
                "reasons": reasons,
                "basis": basis,
                "stage_hash": stage_hash,
                "origin": origin,
                "upstreams": [
                    {
                        "stage": name,
                        "stage_hash": resolved[name]["stage_hash"],
                        "artifacts": resolved[name]["artifacts"],
                    }
                    for name in stage.upstreams
                ],
            }
            resolved[stage_name] = {
                "status": status,
                "stage_hash": stage_hash,
                "artifacts": artifacts,
            }
        return {
            "pipeline": self.pipeline.name,
            "environment": environment,
            "parameters": parameters,
            "stages": stages_report,
        }

    def _matching_record(
        self, stage: StageDef, basis: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Find a verified record whose stored basis is identical.

        All stage versions are scanned: reuse is decided by content identity,
        never by directory or file names.
        """

        stage_root = self.root / "stages" / self.pipeline.name / stage.name
        if not stage_root.is_dir():
            return None
        for candidate in sorted(stage_root.iterdir()):
            if not candidate.is_dir():
                continue
            loaded = self._load_confirmed(stage.name, candidate.name)
            if loaded is None:
                continue
            record, digests = loaded
            if self._basis_equal(record.get("basis"), basis):
                return {"record": record, "digests": digests, "stage_hash": candidate.name}
        return None

    @staticmethod
    def _basis_equal(stored: Any, wanted: dict[str, Any]) -> bool:
        if not isinstance(stored, dict):
            return False
        return canonical_json(stored) == canonical_json(wanted)

    def _rerun_reasons(
        self,
        stage: StageDef,
        basis: dict[str, Any],
        resolved: dict[str, dict[str, Any]],
        record: dict[str, Any] | None,
    ) -> list[str]:
        if record is None:
            stage_root = self.root / "stages" / self.pipeline.name / stage.name
            return ["no_record"] if not stage_root.is_dir() or not any(stage_root.iterdir()) else ["no_matching_basis"]
        reasons: list[str] = []
        stored = record["record"].get("basis", {})
        if stored.get("parameters") != basis["parameters"]:
            for name in sorted(stage.parameters):
                if stored.get("parameters", {}).get(name) != basis["parameters"][name]:
                    reasons.append(f"parameter_changed:{name}")
        if stored.get("inputs") != basis["inputs"]:
            reasons.append("input_changed")
        if stored.get("environment") != basis["environment"]:
            reasons.append("environment_changed")
        if stored.get("upstreams") != basis["upstreams"]:
            for upstream_name in stage.upstreams:
                if stored.get("upstreams"):
                    stored_up = next(
                        (item for item in stored["upstreams"] if item.get("stage") == upstream_name),
                        None,
                    )
                else:
                    stored_up = None
                wanted_up = next(item for item in basis["upstreams"] if item["stage"] == upstream_name)
                if stored_up != wanted_up:
                    if stored_up is None or stored_up.get("stage_hash") != wanted_up["stage_hash"]:
                        reasons.append(f"upstream_changed:{upstream_name}")
                    elif stored_up.get("artifacts") != wanted_up["artifacts"]:
                        reasons.append(f"upstream_artifacts_changed:{upstream_name}")
        return reasons or ["basis_changed"]

    # ---- locking, attempts and evidence ---------------------------------

    def _thread_lock(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def _append_event(self, stage_name: str, stage_hash: str, event: dict[str, Any]) -> None:
        path = self.root / "attempts" / self.pipeline.name / stage_name / f"{stage_hash}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(event)
        payload.setdefault("executor_id", self.executor_id)
        payload.setdefault("pid", os.getpid())
        payload.setdefault("at", _utcnow())
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def _new_evidence_id(self, kind: str) -> str:
        return f"{kind}-{_utcnow().replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}"

    def _quarantine_dir(
        self, source: Path, kind: str, details: dict[str, Any]
    ) -> str | None:
        if not source.exists():
            return None
        evidence_id = self._new_evidence_id(kind)
        target = self.root / "evidence" / evidence_id
        target.mkdir(parents=True)
        shutil.move(str(source), str(target / "artifacts"))
        manifest = {"evidence_id": evidence_id, "kind": kind, "quarantined_at": _utcnow()}
        manifest.update(details)
        (target / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        return evidence_id

    def _quarantine_corrupted(self, stage_name: str, stage_hash: str) -> str | None:
        """Move a damaged published stage (missing/tampered artifacts) aside.

        The bad bytes and the old record are preserved as evidence instead
        of being deleted, and the damaged record is unlinked from the
        trusted stage directory.
        """

        stage_dir = self._stage_dir(stage_name, stage_hash)
        if not stage_dir.exists():
            return None
        saved_root = self.root / "stages" / self.pipeline.name / stage_name
        temporary = saved_root / f".damaged-{uuid.uuid4().hex[:8]}"
        os.replace(stage_dir, temporary)
        try:
            return self._quarantine_dir(
                temporary,
                "corrupted_stage",
                {
                    "pipeline": self.pipeline.name,
                    "stage": stage_name,
                    "claimed_stage_hash": stage_hash,
                },
            )
        except BaseException:
            if temporary.exists() and not stage_dir.exists():
                os.replace(temporary, stage_dir)
            raise

    def _materialize_upstreams(
        self, stage: StageDef, resolved: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, bytes]]:
        upstream_data: dict[str, dict[str, bytes]] = {}
        for upstream_name in stage.upstreams:
            info = resolved[upstream_name]
            if info["status"] != CONFIRMED:
                raise RuntimeError(f"upstream {upstream_name!r} is not confirmed")
            artifact_root = self._stage_dir(upstream_name, info["stage_hash"]) / "artifacts"
            files: dict[str, bytes] = {}
            for relative, expected_digest in info["artifacts"].items():
                data = (artifact_root / relative).read_bytes()
                if hashlib.sha256(data).hexdigest() != expected_digest:
                    raise RuntimeError(f"upstream artifact {upstream_name}/{relative} failed verification")
                files[relative] = data
            upstream_data[upstream_name] = files
        return upstream_data

    # ---- execution ------------------------------------------------------

    def execute_stage(
        self,
        stage_name: str,
        basis: dict[str, Any],
        inputs: dict[str, bytes],
        parameters: dict[str, Any],
        environment: dict[str, Any],
        resolved: dict[str, dict[str, Any]],
        *,
        origin: dict[str, Any],
        expected_stage_hash: str | None = None,
    ) -> dict[str, Any]:
        stage = self.pipeline.stages[stage_name]
        stage_hash = expected_stage_hash or self.stage_hash_for(basis)
        key = f"{self.pipeline.name}:{stage_name}:{stage_hash}"
        lock_path = self.root / "locks" / f"{stage_hash}.lock"
        with self._thread_lock(key):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    already = self._load_confirmed(stage_name, stage_hash)
                    if already is not None:
                        self._append_event(
                            stage_name,
                            stage_hash,
                            {"event": "reused", "origin": origin},
                        )
                        return {
                            "status": CONFIRMED,
                            "stage_hash": stage_hash,
                            "artifacts": already[1],
                            "reused": True,
                        }

                    damaged = self._read_record_unchecked(stage_name, stage_hash)
                    evidence_id: str | None = None
                    if damaged is not None:
                        evidence_id = self._quarantine_corrupted(stage_name, stage_hash)

                    attempt_id = uuid.uuid4().hex
                    staging_dir = (
                        self.root / "staging" / self.pipeline.name / stage_name / attempt_id
                    )
                    artifact_stage = staging_dir / "artifacts"
                    artifact_stage.mkdir(parents=True)
                    self._append_event(
                        stage_name,
                        stage_hash,
                        {
                            "event": "execute_start",
                            "attempt_id": attempt_id,
                            "origin": origin,
                            "replaced_evidence": evidence_id,
                        },
                    )

                    stage_inputs = {name: inputs[name] for name in stage.inputs}
                    stage_params = {name: parameters.get(name) for name in stage.parameters}
                    try:
                        upstream_data = self._materialize_upstreams(stage, resolved)
                        context = StageContext(
                            pipeline=self.pipeline.name,
                            stage=stage_name,
                            inputs=stage_inputs,
                            parameters=stage_params,
                            environment=environment,
                            upstream=upstream_data,
                        )
                        backend_metadata = self.pipeline.backend(context, artifact_stage)
                        artifact_digests = self._artifact_digests(artifact_stage)
                        if not artifact_digests:
                            raise RuntimeError("backend produced no artifacts")
                        record = {
                            "pipeline": self.pipeline.name,
                            "stage": stage_name,
                            "stage_hash": stage_hash,
                            "created_at": _utcnow(),
                            "basis": basis,
                            "artifacts": [
                                {
                                    "path": path,
                                    "size": (artifact_stage / path).stat().st_size,
                                    "sha256": digest,
                                }
                                for path, digest in sorted(artifact_digests.items())
                            ],
                            "backend_metadata": backend_metadata,
                            "origin": origin,
                        }
                        self._publish_record(stage_name, stage_hash, record, artifact_stage)
                        shutil.rmtree(staging_dir, ignore_errors=True)
                        self._append_event(
                            stage_name,
                            stage_hash,
                            {"event": "published", "attempt_id": attempt_id, "origin": origin},
                        )
                        return {
                            "status": CONFIRMED,
                            "stage_hash": stage_hash,
                            "artifacts": artifact_digests,
                            "reused": False,
                        }
                    except BaseException as exc:
                        failure = {
                            "event": "failed",
                            "attempt_id": attempt_id,
                            "origin": origin,
                            "error": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc(),
                        }
                        self._append_event(stage_name, stage_hash, failure)
                        failed_evidence = self._quarantine_dir(
                            staging_dir,
                            "failed_stage",
                            {
                                "pipeline": self.pipeline.name,
                                "stage": stage_name,
                                "stage_hash": stage_hash,
                                "basis": basis,
                                "origin": origin,
                                "error": failure["error"],
                            },
                        )
                        self._append_event(
                            stage_name,
                            stage_hash,
                            {"event": "evidence_quarantined", "evidence_id": failed_evidence},
                        )
                        return {
                            "status": RERUN,
                            "stage_hash": stage_hash,
                            "artifacts": {},
                            "reused": False,
                            "error": failure["error"],
                            "evidence_id": failed_evidence,
                        }
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _publish_record(
        self, stage_name: str, stage_hash: str, record: dict[str, Any], artifact_stage: Path
    ) -> None:
        stage_dir = self._stage_dir(stage_name, stage_hash)
        temporary_root = stage_dir.parent / f".publish-{uuid.uuid4().hex[:8]}"
        (temporary_root / "artifacts").mkdir(parents=True)
        for item in record["artifacts"]:
            source = artifact_stage / item["path"]
            target = temporary_root / "artifacts" / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        record_path = temporary_root / "record.json"
        record_path.write_text(
            json.dumps(record, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        if stage_dir.exists():
            replaced = self._quarantine_dir(
                stage_dir,
                "superseded_stage",
                {
                    "pipeline": self.pipeline.name,
                    "stage": stage_name,
                    "stage_hash": stage_hash,
                },
            )
            self._append_event(
                stage_name,
                stage_hash,
                {"event": "superseded_evidence", "evidence_id": replaced},
            )
        os.replace(temporary_root, stage_dir)
        self._fsync_directory(stage_dir.parent)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class RoundRunner:
    """Runs one comparison round: a baseline plus several candidate revisions.

    Each candidate is planned independently. A candidate may reuse stages
    produced for any earlier experiment or candidate as long as the verified
    content basis matches; results that only exist inside another candidate's
    failed/unproven path are never borrowed.
    """

    def __init__(self, store: StageStore):
        self.store = store

    def plan_candidate(
        self,
        inputs: dict[str, bytes],
        parameters: dict[str, Any],
        environment: dict[str, Any],
    ) -> dict[str, Any]:
        return self.store.plan(inputs, parameters, environment)

    def run_round(
        self,
        *,
        round_id: str,
        inputs: dict[str, bytes],
        environment: dict[str, Any],
        baseline: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ordered_names = self.store.pipeline.ordered_stage_names()
        final_stage = ordered_names[-1]
        request_snapshot = {
            "round_id": round_id,
            "pipeline": self.store.pipeline.name,
            "created_at": _utcnow(),
            "environment": environment,
            "inputs": [
                {
                    "name": name,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size": len(data),
                }
                for name, data in sorted(inputs.items())
            ],
            "baseline_parameters": baseline,
            "candidates": candidates,
        }
        round_dir = self.store.root / "rounds"
        round_dir.mkdir(parents=True, exist_ok=True)
        (round_dir / f"{round_id}.request.json").write_text(
            json.dumps(request_snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

        candidate_reports: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            candidate_id = candidate["id"]
            adjustments = candidate.get("adjustments", {})
            adopted = candidate.get("adopted")
            adopted_set = set(adopted) if adopted is not None else None
            parameters = select_parameters(baseline, adjustments, adopted_set)
            origin_round = candidate.get("origin_round", round_id)
            plan_report = self.store.plan(inputs, parameters, environment)
            resolved: dict[str, dict[str, Any]] = {}
            for stage_name in ordered_names:
                stage_def = self.store.pipeline.stages[stage_name]
                if any(
                    resolved[upstream_name]["status"] != CONFIRMED
                    for upstream_name in self.store.pipeline.stages[stage_name].upstreams
                ):
                    resolved[stage_name] = {
                        "status": UNPROVEN,
                        "stage_hash": None,
                        "artifacts": {},
                    }
                    continue
                basis = self.store._stage_basis(
                    stage_def, inputs, parameters, environment, resolved
                )
                expected_hash = self.store.stage_hash_for(basis)
                result = self.store.execute_stage(
                    stage_name,
                    basis,
                    inputs,
                    parameters,
                    environment,
                    {**resolved},
                    origin={
                        "experiment": "original"
                        if candidate_id == "original"
                        else f"candidate:{candidate_id}",
                        "round_id": origin_round,
                        "candidate_id": candidate_id,
                    },
                    expected_stage_hash=expected_hash,
                )
                if result.get("error"):
                    resolved[stage_name] = {
                        "status": RERUN,
                        "stage_hash": result["stage_hash"],
                        "artifacts": {},
                        "error": result["error"],
                        "evidence_id": result.get("evidence_id"),
                        "reused": False,
                    }
                    continue
                record = self.store._read_record_unchecked(stage_name, result["stage_hash"])
                artifacts = {item["path"]: item["sha256"] for item in record["artifacts"]}
                resolved[stage_name] = {
                    "status": CONFIRMED,
                    "stage_hash": result["stage_hash"],
                    "artifacts": artifacts,
                    "error": None,
                    "evidence_id": None,
                    "reused": bool(result.get("reused")),
                }

            # Re-plan after execution so confirmed/rerun/unproven and the
            # exact reuse origins reflect what this candidate stands on.
            final_plan = self.store.plan(inputs, parameters, environment)
            stages_report: dict[str, Any] = {}
            for stage_name in ordered_names:
                planned = final_plan["stages"][stage_name]
                executed = resolved[stage_name]
                if executed["status"] == RERUN and executed.get("error"):
                    status = RERUN
                    stage_hash = executed["stage_hash"]
                else:
                    status = planned["status"]
                    stage_hash = planned["stage_hash"]
                record = None
                actual_origin = None
                if status == CONFIRMED and stage_hash is not None:
                    loaded = self.store._load_confirmed(stage_name, stage_hash)
                    if loaded is not None:
                        record = loaded[0]
                        actual_origin = record.get("origin")
                stages_report[stage_name] = {
                    "status": status,
                    "reasons": planned["reasons"],
                    "stage_hash": stage_hash,
                    "upstreams": planned["upstreams"],
                    "executed": executed,
                    "origin": actual_origin,
                    "artifacts": planned["basis"] and {
                        item["path"]: {
                            "sha256": item["sha256"],
                            "size": item["size"],
                        }
                        for item in (record or {}).get("artifacts", [])
                    } or {},
                }
            final = stages_report[final_stage]
            candidate_reports[candidate_id] = {
                "candidate_id": candidate_id,
                "parameters": parameters,
                "adjustments": adjustments,
                "adopted": sorted(adopted_set) if adopted_set is not None else None,
                "status": final["status"],
                "stages": stages_report,
            }

        summary = {
            "round_id": round_id,
            "pipeline": self.store.pipeline.name,
            "completed_at": _utcnow(),
            "environment": environment,
            "inputs": request_snapshot["inputs"],
            "baseline_parameters": baseline,
            "candidates": candidate_reports,
        }
        (round_dir / f"{round_id}.json").write_text(
            json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        return summary


def load_round(store_root: str | Path, round_id: str) -> dict[str, Any]:
    path = Path(store_root) / "rounds" / f"{round_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def trace_final_result(store_root: str | Path, round_id: str) -> dict[str, Any]:
    """Reopen a saved round and trace every final artifact back to its origin.

    Each artifact is verified against the stored record digest before its
    chain is reported. Artifacts that cannot be verified are reported as
    unproven instead of being presented as results.
    """

    store_root = Path(store_root)
    summary = load_round(store_root, round_id)
    pipeline_name = summary["pipeline"]
    traces: dict[str, Any] = {}
    for candidate_id, report in summary["candidates"].items():
        stages = report["stages"]
        ordered = list(stages)
        final_name = ordered[-1]
        final = stages[final_name]
        if final["status"] != CONFIRMED or not final["stage_hash"]:
            traces[candidate_id] = {
                "status": final["status"],
                "final_stage": final_name,
                "artifacts": [],
                "chain": [],
            }
            continue
        chain: list[dict[str, Any]] = []
        for stage_name in ordered:
            entry = stages[stage_name]
            verified: list[dict[str, Any]] = []
            if entry["stage_hash"]:
                artifact_root = (
                    store_root / "stages" / pipeline_name / stage_name / entry["stage_hash"] / "artifacts"
                )
                for path, description in sorted(entry.get("artifacts", {}).items()):
                    artifact_path = artifact_root / path
                    digest = (
                        hashlib.sha256(artifact_path.read_bytes()).hexdigest()
                        if artifact_path.is_file()
                        else None
                    )
                    verified.append(
                        {
                            "path": path,
                            "sha256": digest,
                            "verified": digest == description["sha256"],
                        }
                    )
            chain.append(
                {
                    "stage": stage_name,
                    "status": entry["status"],
                    "stage_hash": entry["stage_hash"],
                    "origin": entry["origin"],
                    "artifacts": verified,
                    "upstreams": [item["stage"] for item in entry["upstreams"]],
                }
            )
        traces[candidate_id] = {
            "status": CONFIRMED,
            "final_stage": final_name,
            "artifacts": [item for entry in chain for item in entry["artifacts"] if entry["stage"] == final_name],
            "chain": chain,
        }

    abandoned: list[dict[str, Any]] = []
    evidence_root = store_root / "evidence"
    if evidence_root.is_dir():
        for item in sorted(evidence_root.iterdir()):
            manifest_path = item / "manifest.json"
            if not manifest_path.is_file():
                continue
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            origin = manifest.get("origin") or {}
            if origin.get("round_id") == round_id:
                abandoned.append(
                    {
                        "evidence_id": manifest["evidence_id"],
                        "kind": manifest["kind"],
                        "stage": manifest.get("stage"),
                        "stage_hash": manifest.get("stage_hash"),
                        "error": manifest.get("error"),
                        "origin": origin,
                    }
                )
    return {"round_id": round_id, "candidates": traces, "abandoned_evidence": abandoned}


def compare_candidates(round_summary: dict[str, Any]) -> dict[str, Any]:
    """Side-by-side view grouped by confirmed / rerun / unproven."""

    grouped: dict[str, list[dict[str, Any]]] = {CONFIRMED: [], RERUN: [], UNPROVEN: []}
    for candidate_id, report in round_summary["candidates"].items():
        stage_entries = report["stages"]
        per_status = {
            status: [name for name, item in stage_entries.items() if item["status"] == status]
            for status in (CONFIRMED, RERUN, UNPROVEN)
        }
        reuse = [
            {
                "stage": name,
                "origin": item["origin"],
                "stage_hash": item["stage_hash"],
            }
            for name, item in stage_entries.items()
            if item["status"] == CONFIRMED and item.get("executed", {}).get("reused")
        ]
        entry = {
            "candidate_id": candidate_id,
            "parameters": report["parameters"],
            "status": report["status"],
            CONFIRMED: per_status[CONFIRMED],
            RERUN: [
                {
                    "stage": name,
                    "reasons": stage_entries[name]["reasons"],
                    "error": stage_entries[name].get("executed", {}).get("error"),
                    "evidence_id": stage_entries[name].get("executed", {}).get("evidence_id"),
                }
                for name in per_status[RERUN]
            ],
            UNPROVEN: [
                {"stage": name, "reasons": stage_entries[name]["reasons"]}
                for name in per_status[UNPROVEN]
            ],
            "reused_from": reuse,
        }
        grouped[report["status"]].append(entry)
    return {"round_id": round_summary["round_id"], "candidates": grouped}
