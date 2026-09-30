from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backends import ComputeBackend
from .models import ExperimentRequest, RecordSummary, Stage, canonical_json


class RunStore:
    def __init__(self, root: str | Path, backend: ComputeBackend):
        self.root = Path(root)
        self.backend = backend
        self.executor_id = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._locks_guard = threading.Lock()
        self._thread_locks: dict[str, threading.Lock] = {}
        self.root.mkdir(parents=True, exist_ok=True)
        for name in (
            "requests",
            "artifacts",
            "staging",
            "stage_staging",
            "stage_artifacts",
            "stages",
            "locks",
            "attempts",
            "evidence",
        ):
            (self.root / name).mkdir(exist_ok=True)

    def _thread_lock_for(self, key: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._thread_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._thread_locks[key] = lock
            return lock

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _append_attempt_event(self, request_hash: str, event: dict[str, Any]) -> None:
        attempts_path = self.root / "attempts" / f"{request_hash}.jsonl"
        payload = dict(event)
        payload.setdefault("executor_id", self.executor_id)
        payload.setdefault("pid", os.getpid())
        payload.setdefault("at", self._utcnow())
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        with attempts_path.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()
            os.fsync(stream.fileno())

    def _quarantine(
        self,
        path: Path,
        *,
        request_hash: str,
        attempt_id: str | None,
        reason: str,
    ) -> str | None:
        if not path.exists():
            return None
        evidence_root = self.root / "evidence" / request_hash
        evidence_attempt = evidence_root / (attempt_id or uuid.uuid4().hex)
        destination_dir = evidence_attempt / reason
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / path.name
        counter = 1
        while destination.exists():
            destination = destination_dir / f"{path.name}.{counter}"
            counter += 1
        shutil.move(str(path), str(destination))
        self._fsync_directory(destination_dir)
        self._fsync_directory(evidence_attempt)
        self._fsync_directory(evidence_root)
        return str(destination.relative_to(self.root))

    def _load_attempt_events(self, request_hash: str) -> list[dict[str, Any]]:
        attempts_path = self.root / "attempts" / f"{request_hash}.jsonl"
        events: list[dict[str, Any]] = []
        try:
            lines = attempts_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return events
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                events.append({"event": "unreadable_log_line", "raw": line})
        return events

    def _takeover_basis(self, request_hash: str) -> dict[str, Any] | None:
        """Collect evidence left by earlier incomplete attempts, if any."""
        basis: dict[str, Any] = {}
        events = self._load_attempt_events(request_hash)
        finished: set[str] = set()
        started: dict[str, dict[str, Any]] = {}
        for event in events:
            attempt_id = event.get("attempt_id")
            if not attempt_id:
                continue
            if event.get("event") == "execute_start":
                started[attempt_id] = event
            elif event.get("event") in ("published", "failed"):
                finished.add(attempt_id)
        orphaned = [
            {
                "attempt_id": attempt_id,
                "executor_id": event.get("executor_id"),
                "pid": event.get("pid"),
                "started_at": event.get("at"),
            }
            for attempt_id, event in started.items()
            if attempt_id not in finished
        ]
        if orphaned:
            basis["orphaned_attempts"] = orphaned

        staging_root = self.root / "staging" / request_hash
        stale_staging: list[str] = []
        if staging_root.is_dir():
            stale_staging = sorted(entry.name for entry in staging_root.iterdir())
        if stale_staging:
            basis["stale_staging_dirs"] = stale_staging

        artifact_dir = self.root / "artifacts" / request_hash
        record_path = self.root / "requests" / f"{request_hash}.json"
        if artifact_dir.exists() and not record_path.is_file():
            basis["artifacts_without_record"] = True
        if record_path.is_file() and self._load_complete(request_hash) is None:
            basis["record_failed_verification"] = True

        return basis or None

    def run(self, request: ExperimentRequest) -> RecordSummary:
        identity = request.identity()
        request_hash = identity["request_hash"]
        lock_path = self.root / "locks" / f"{request_hash}.lock"

        with self._thread_lock_for(request_hash):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_complete(request_hash)
                    if existing is not None:
                        self._append_attempt_event(
                            request_hash,
                            {"event": "reused", "record_id": existing.record_id},
                        )
                        return existing

                    attempt_id = uuid.uuid4().hex
                    basis = self._takeover_basis(request_hash)
                    start_event: dict[str, Any] = {
                        "event": "execute_start",
                        "attempt_id": attempt_id,
                    }
                    if basis is not None:
                        start_event["takeover_basis"] = basis
                    self._append_attempt_event(request_hash, start_event)

                    self._quarantine_unpublished(request_hash, attempt_id)

                    inputs = request.resolved_inputs()
                    staging_dir = self.root / "staging" / request_hash / attempt_id
                    artifact_stage = staging_dir / "artifacts"
                    artifact_stage.mkdir(parents=True)
                    try:
                        backend_metadata = self.backend(
                            inputs,
                            dict(request.parameters),
                            artifact_stage,
                        )
                        manifest = self._build_manifest(
                            identity=identity,
                            inputs=inputs,
                            artifact_stage=artifact_stage,
                            backend_metadata=backend_metadata,
                        )
                        artifact_target = self.root / "artifacts" / request_hash
                        os.replace(artifact_stage, artifact_target)
                        self._fsync_directory(artifact_target.parent)
                        record_path = self._publish_record(request_hash, manifest)
                        shutil.rmtree(self.root / "staging" / request_hash, ignore_errors=True)
                    except BaseException as exc:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "failed",
                                "attempt_id": attempt_id,
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        )
                        self._quarantine_unpublished(request_hash, attempt_id)
                        raise

                    self._append_attempt_event(
                        request_hash,
                        {
                            "event": "published",
                            "attempt_id": attempt_id,
                            "record_id": request_hash,
                        },
                    )
                    return RecordSummary(
                        request_hash=request_hash,
                        record_id=request_hash,
                        reused=False,
                        record_path=record_path,
                        artifact_dir=artifact_target,
                    )
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def run_pipeline(
        self, request: ExperimentRequest, stages: tuple[Stage, ...] | list[Stage]
    ) -> RecordSummary:
        stages = tuple(stages)
        if not stages:
            raise ValueError("pipeline must contain at least one stage")
        names = [stage.name for stage in stages]
        if len(names) != len(set(names)):
            raise ValueError("stage names must be unique")

        identity = request.identity()
        request_hash = identity["request_hash"]
        existing = self._load_complete(request_hash)
        if existing is not None:
            self._append_attempt_event(
                request_hash, {"event": "reused", "record_id": existing.record_id}
            )
            return existing

        inputs = request.resolved_inputs()
        stage_by_name = {stage.name: stage for stage in stages}
        stage_manifests: dict[str, dict[str, Any]] = {}
        reused_stages: dict[str, bool] = {}
        for stage in stages:
            unknown = [name for name in stage.depends_on if name not in stage_by_name]
            if unknown:
                raise ValueError(f"stage {stage.name!r} depends on unknown stages {unknown}")
            order_index = names.index(stage.name)
            if any(names.index(name) > order_index for name in stage.depends_on):
                raise ValueError(f"stage {stage.name!r} has a backwards dependency")

            manifest, reused = self._run_stage(
                request=request,
                identity=identity,
                inputs=inputs,
                stage=stage,
                dependencies=stage_manifests,
            )
            stage_manifests[stage.name] = manifest
            reused_stages[stage.name] = reused

        return self._assemble_pipeline(
            request=request,
            identity=identity,
            stages=stages,
            stage_manifests=stage_manifests,
            reused_stages=reused_stages,
        )

    def _stage_identity(
        self,
        *,
        identity: dict[str, Any],
        inputs: dict[str, bytes],
        stage: Stage,
        dependencies: dict[str, dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        selected_input_names = stage.inputs if stage.inputs is not None else tuple(sorted(inputs))
        selected_inputs = []
        for name in selected_input_names:
            if name not in inputs:
                raise ValueError(f"stage {stage.name!r} requested missing input {name!r}")
            selected_inputs.append(
                {
                    "name": name,
                    "sha256": hashlib.sha256(inputs[name]).hexdigest(),
                    "size": len(inputs[name]),
                }
            )
        parameter_names = (
            tuple(identity["payload"]["parameters"].keys())
            if stage.parameters is None
            else stage.parameters
        )
        selected_parameters = {
            name: identity["payload"]["parameters"].get(name)
            for name in sorted(parameter_names)
        }
        dependency_payload = {
            name: {
                "stage_hash": dependencies[name]["stage_hash"],
                "artifacts": dependencies[name]["artifacts"],
            }
            for name in sorted(stage.depends_on)
        }
        payload = {
            "schema_version": "stage-record/v1",
            "environment": identity["payload"]["environment"],
            "stage": {
                "name": stage.name,
                "version": stage.version,
                "inputs": selected_inputs,
                "parameters": selected_parameters,
                "depends_on": list(stage.depends_on),
            },
            "dependencies": dependency_payload,
        }
        return hashlib.sha256(canonical_json(payload)).hexdigest(), payload

    def _run_stage(
        self,
        *,
        request: ExperimentRequest,
        identity: dict[str, Any],
        inputs: dict[str, bytes],
        stage: Stage,
        dependencies: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], bool]:
        request_hash = identity["request_hash"]
        stage_hash, stage_payload = self._stage_identity(
            identity=identity, inputs=inputs, stage=stage, dependencies=dependencies
        )
        lock_path = self.root / "locks" / f"stage-{stage_hash}.lock"
        with self._thread_lock_for(f"stage:{stage_hash}"):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_stage_manifest(request_hash, stage_hash)
                    if existing is not None:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "stage_reused",
                                "stage_name": stage.name,
                                "stage_hash": stage_hash,
                            },
                        )
                        return existing, True

                    attempt_id = uuid.uuid4().hex
                    self._append_attempt_event(
                        request_hash,
                        {
                            "event": "stage_start",
                            "attempt_id": attempt_id,
                            "stage_name": stage.name,
                            "stage_hash": stage_hash,
                        },
                    )
                    stage_basis = self._stage_takeover_basis(stage_hash)
                    self._quarantine_stage_leftovers(stage_hash, attempt_id)

                    stage_root = self.root / "stage_staging" / stage_hash / attempt_id
                    artifact_stage = stage_root / "artifacts"
                    artifact_stage.mkdir(parents=True)
                    try:
                        selected_inputs = self._stage_inputs(stage, inputs)
                        selected_parameters = self._stage_parameters(
                            stage, identity["payload"]["parameters"]
                        )
                        dependency_dirs = {
                            name: self.root
                            / "stage_artifacts"
                            / dependencies[name]["stage_hash"]
                            for name in stage.depends_on
                        }
                        backend_metadata = stage.compute(
                            selected_inputs,
                            selected_parameters,
                            dependency_dirs,
                            artifact_stage,
                        )
                        manifest = self._build_stage_manifest(
                            request_hash=request_hash,
                            stage=stage,
                            stage_hash=stage_hash,
                            stage_payload=stage_payload,
                            dependencies=dependencies,
                            artifact_stage=artifact_stage,
                            attempt_id=attempt_id,
                            backend_metadata=backend_metadata,
                        )
                        artifact_target = self.root / "stage_artifacts" / stage_hash
                        os.replace(artifact_stage, artifact_target)
                        self._fsync_directory(artifact_target.parent)
                        self._publish_stage_manifest(stage_hash, manifest)
                    except BaseException as exc:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "stage_failed",
                                "attempt_id": attempt_id,
                                "stage_name": stage.name,
                                "stage_hash": stage_hash,
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        )
                        self._quarantine_stage_leftovers(stage_hash, attempt_id)
                        raise
                    finally:
                        shutil.rmtree(
                            self.root / "stage_staging" / stage_hash, ignore_errors=True
                        )

                    self._append_attempt_event(
                        request_hash,
                        {
                            "event": "stage_published",
                            "attempt_id": attempt_id,
                            "stage_name": stage.name,
                            "stage_hash": stage_hash,
                        },
                    )
                    if stage_basis:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "stage_takeover_basis",
                                "attempt_id": attempt_id,
                                "stage_name": stage.name,
                                "stage_hash": stage_hash,
                                "basis": stage_basis,
                            },
                        )
                    return manifest, False
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _stage_inputs(stage: Stage, inputs: dict[str, bytes]) -> dict[str, bytes]:
        if stage.inputs is None:
            return dict(inputs)
        return {name: inputs[name] for name in stage.inputs}

    @staticmethod
    def _stage_parameters(
        stage: Stage, parameters: dict[str, Any]
    ) -> dict[str, Any]:
        if stage.parameters is None:
            return dict(parameters)
        return {name: parameters.get(name) for name in stage.parameters}

    def _assemble_pipeline(
        self,
        *,
        request: ExperimentRequest,
        identity: dict[str, Any],
        stages: tuple[Stage, ...],
        stage_manifests: dict[str, dict[str, Any]],
        reused_stages: dict[str, bool],
    ) -> RecordSummary:
        request_hash = identity["request_hash"]
        lock_path = self.root / "locks" / f"{request_hash}.lock"
        with self._thread_lock_for(request_hash):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_complete(request_hash)
                    if existing is not None:
                        self._append_attempt_event(
                            request_hash,
                            {"event": "reused", "record_id": existing.record_id},
                        )
                        return existing

                    attempt_id = uuid.uuid4().hex
                    basis = self._takeover_basis(request_hash)
                    start_event: dict[str, Any] = {
                        "event": "execute_start",
                        "attempt_id": attempt_id,
                        "pipeline": True,
                    }
                    if basis is not None:
                        start_event["takeover_basis"] = basis
                    self._append_attempt_event(request_hash, start_event)
                    self._quarantine_unpublished(request_hash, attempt_id)

                    staging_dir = self.root / "staging" / request_hash / attempt_id
                    artifact_stage = staging_dir / "artifacts"
                    artifact_stage.mkdir(parents=True)
                    provenance = []
                    try:
                        for stage in stages:
                            manifest = stage_manifests[stage.name]
                            self._validate_stage_sources(
                                stage_hash=manifest["stage_hash"],
                                manifest=manifest,
                                dependency_manifests=stage_manifests,
                            )
                            source_dir = self.root / "stage_artifacts" / manifest["stage_hash"]
                            target_dir = artifact_stage / "stages" / stage.name
                            source_record_path = (
                                self.root / "stages" / f"{manifest['stage_hash']}.json"
                            )
                            shutil.copytree(source_dir, target_dir)
                            manifest_path = target_dir / "stage-manifest.json"
                            shutil.copyfile(source_record_path, manifest_path)
                            provenance.append(
                                {
                                    "name": stage.name,
                                    "version": stage.version,
                                    "reused": reused_stages[stage.name],
                                    "stage_hash": manifest["stage_hash"],
                                    "source_attempt_id": manifest["attempt_id"],
                                    "source_stage_record": str(source_record_path),
                                    "source_stage_record_size": source_record_path.stat().st_size,
                                    "source_stage_record_sha256": hashlib.sha256(
                                        source_record_path.read_bytes()
                                    ).hexdigest(),
                                    "artifacts": manifest["artifacts"],
                                }
                            )

                        final_manifest = self._build_manifest(
                            identity=identity,
                            inputs=request.resolved_inputs(),
                            artifact_stage=artifact_stage,
                            backend_metadata={"pipeline": provenance},
                        )
                        artifact_target = self.root / "artifacts" / request_hash
                        os.replace(artifact_stage, artifact_target)
                        self._fsync_directory(artifact_target.parent)
                        record_path = self._publish_record(request_hash, final_manifest)
                        shutil.rmtree(self.root / "staging" / request_hash, ignore_errors=True)
                    except BaseException as exc:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "failed",
                                "attempt_id": attempt_id,
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        )
                        self._quarantine_unpublished(request_hash, attempt_id)
                        raise

                    self._append_attempt_event(
                        request_hash,
                        {
                            "event": "published",
                            "attempt_id": attempt_id,
                            "record_id": request_hash,
                        },
                    )
                    return RecordSummary(
                        request_hash=request_hash,
                        record_id=request_hash,
                        reused=False,
                        record_path=record_path,
                        artifact_dir=artifact_target,
                    )
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _validate_stage_sources(
        self,
        *,
        stage_hash: str,
        manifest: dict[str, Any],
        dependency_manifests: dict[str, dict[str, Any]],
    ) -> None:
        source = self._load_stage_manifest(manifest["request_hash"], stage_hash)
        if source is None:
            raise RuntimeError(f"stage {manifest.get('stage_name')!r} source is not valid")
        for name, dependency in manifest.get("depends_on", {}).items():
            expected = dependency_manifests[name]
            if dependency["stage_hash"] != expected["stage_hash"]:
                raise RuntimeError(f"stage source for {name!r} is from a different attempt")

    def _build_stage_manifest(
        self,
        *,
        request_hash: str,
        stage: Stage,
        stage_hash: str,
        stage_payload: dict[str, Any],
        dependencies: dict[str, dict[str, Any]],
        artifact_stage: Path,
        attempt_id: str,
        backend_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        artifacts = self._artifact_entries(artifact_stage)
        return {
            "record_id": stage_hash,
            "stage_hash": stage_hash,
            "request_hash": request_hash,
            "created_at": self._utcnow(),
            "attempt_id": attempt_id,
            "stage_name": stage.name,
            "stage_version": stage.version,
            "request": stage_payload,
            "depends_on": {
                name: {
                    "stage_hash": dependencies[name]["stage_hash"],
                    "record_path": str(
                        (self.root / "stages" / f"{dependencies[name]['stage_hash']}.json").resolve()
                    ),
                    "artifacts": dependencies[name]["artifacts"],
                }
                for name in sorted(stage.depends_on)
            },
            "artifacts": artifacts,
            "backend_metadata": backend_metadata,
        }

    def _artifact_entries(self, artifact_stage: Path) -> list[dict[str, Any]]:
        artifacts = []
        for path in sorted(path for path in artifact_stage.rglob("*") if path.is_file()):
            relative = path.relative_to(artifact_stage)
            if any(part in ("", ".", "..") for part in relative.parts):
                raise ValueError("backend produced an unsafe artifact path")
            data = path.read_bytes()
            artifacts.append(
                {
                    "path": relative.as_posix(),
                    "size": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        if not artifacts:
            raise RuntimeError("stage did not produce any artifacts")
        return artifacts

    def _publish_stage_manifest(self, stage_hash: str, manifest: dict[str, Any]) -> Path:
        record_path = self.root / "stages" / f"{stage_hash}.json"
        temporary_path = record_path.with_name(f".{stage_hash}.{uuid.uuid4().hex}.tmp")
        payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        with temporary_path.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, record_path)
        self._fsync_directory(record_path.parent)
        return record_path

    def _load_stage_manifest(
        self, request_hash: str, stage_hash: str
    ) -> dict[str, Any] | None:
        record_path = self.root / "stages" / f"{stage_hash}.json"
        artifact_dir = self.root / "stage_artifacts" / stage_hash
        if not record_path.is_file() or not artifact_dir.is_dir():
            return None
        try:
            manifest = json.loads(record_path.read_bytes())
            if manifest.get("record_id") != stage_hash:
                return None
            self._validate_artifacts(artifact_dir, manifest, self.root / "stage_artifacts")
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        return manifest

    def _quarantine_stage_leftovers(self, stage_hash: str, attempt_id: str) -> None:
        record_path = self.root / "stages" / f"{stage_hash}.json"
        artifact_dir = self.root / "stage_artifacts" / stage_hash
        if record_path.is_file() and self._load_stage_manifest_root(stage_hash) is None:
            self._quarantine(
                record_path,
                request_hash="stages",
                attempt_id=attempt_id,
                reason="invalid_records",
            )
        staging = self.root / "stage_staging" / stage_hash
        if staging.exists():
            for path in sorted(staging.iterdir()):
                if path.is_dir():
                    self._quarantine(
                        path,
                        request_hash="stages",
                        attempt_id=path.name,
                        reason="failed_stage_staging",
                    )
            shutil.rmtree(staging, ignore_errors=True)
        if artifact_dir.exists() and not record_path.is_file():
            self._quarantine(
                artifact_dir,
                request_hash="stages",
                attempt_id=attempt_id,
                reason="unpublished_stage_artifacts",
            )

    def _load_stage_manifest_root(self, stage_hash: str) -> dict[str, Any] | None:
        record_path = self.root / "stages" / f"{stage_hash}.json"
        artifact_dir = self.root / "stage_artifacts" / stage_hash
        if not record_path.is_file() or not artifact_dir.is_dir():
            return None
        try:
            manifest = json.loads(record_path.read_bytes())
            self._validate_artifacts(artifact_dir, manifest, self.root / "stage_artifacts")
            return manifest
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _stage_takeover_basis(self, stage_hash: str) -> dict[str, Any]:
        basis: dict[str, Any] = {}
        record_path = self.root / "stages" / f"{stage_hash}.json"
        artifact_dir = self.root / "stage_artifacts" / stage_hash
        staging = self.root / "stage_staging" / stage_hash
        if staging.is_dir():
            basis["stale_stage_staging_dirs"] = sorted(
                entry.name for entry in staging.iterdir() if entry.is_dir()
            )
        if artifact_dir.exists() and not record_path.is_file():
            basis["stage_artifacts_without_record"] = True
        if record_path.is_file() and self._load_stage_manifest_root(stage_hash) is None:
            basis["stage_record_failed_verification"] = True
        return {
            key: value for key, value in basis.items() if value not in (False, [], None)
        }

    def _publish_record(self, request_hash: str, manifest: dict[str, Any]) -> Path:
        record_path = self.root / "requests" / f"{request_hash}.json"
        temporary_path = record_path.with_name(f".{request_hash}.{uuid.uuid4().hex}.tmp")
        payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        with temporary_path.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, record_path)
        self._fsync_directory(record_path.parent)
        return record_path

    def _quarantine_unpublished(
        self, request_hash: str, attempt_id: str
    ) -> dict[str, list[str]]:
        preserved: dict[str, list[str]] = {}
        staging = self.root / "staging" / request_hash
        if staging.exists():
            for path in sorted(staging.iterdir()):
                destination = self._quarantine(
                    path,
                    request_hash=request_hash,
                    attempt_id=path.name if (path / "").is_dir() else attempt_id,
                    reason="failed_staging",
                )
                if destination:
                    preserved.setdefault("failed_staging", []).append(destination)
            try:
                staging.rmdir()
            except OSError:
                shutil.rmtree(staging, ignore_errors=True)

        record_path = self.root / "requests" / f"{request_hash}.json"
        artifact_dir = self.root / "artifacts" / request_hash
        if record_path.is_file() and self._load_complete(request_hash) is None:
            destination = self._quarantine(
                record_path,
                request_hash=request_hash,
                attempt_id=attempt_id,
                reason="invalid_records",
            )
            if destination:
                preserved["invalid_records"] = [destination]
        if artifact_dir.exists() and not record_path.is_file():
            destination = self._quarantine(
                artifact_dir,
                request_hash=request_hash,
                attempt_id=attempt_id,
                reason="unpublished_artifacts",
            )
            if destination:
                preserved["unpublished_artifacts"] = [destination]

        if preserved:
            self._append_attempt_event(
                request_hash,
                {
                    "event": "evidence_preserved",
                    "attempt_id": attempt_id,
                    "paths": preserved,
                },
            )
        return preserved

    def _load_complete(self, request_hash: str) -> RecordSummary | None:
        record_path = self.root / "requests" / f"{request_hash}.json"
        artifact_dir = self.root / "artifacts" / request_hash
        if not record_path.is_file() or not artifact_dir.is_dir():
            return None
        try:
            manifest = json.loads(record_path.read_bytes())
            self._validate_artifacts(artifact_dir, manifest, self.root / "artifacts")
            self._validate_pipeline_provenance(artifact_dir, manifest)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        return RecordSummary(
            request_hash=request_hash,
            record_id=request_hash,
            reused=True,
            record_path=record_path,
            artifact_dir=artifact_dir,
        )

    def _validate_artifacts(
        self,
        artifact_dir: Path,
        manifest: dict[str, Any],
        artifact_root: Path,
    ) -> None:
        request_hash = manifest.get("record_id")
        artifact_dir_expected = artifact_root / str(request_hash)
        if request_hash is None or artifact_dir != artifact_dir_expected:
            raise ValueError("artifact directory does not match record")
        artifacts = manifest.get("artifacts", [])
        if not isinstance(artifacts, list) or not artifacts:
            raise ValueError("record has no artifacts")
        for item in artifacts:
            path = artifact_dir / item["path"]
            relative = path.relative_to(artifact_dir)
            if any(part in ("", ".", "..") for part in relative.parts):
                raise ValueError("unsafe artifact path")
            if not path.is_file():
                raise FileNotFoundError(item["path"])
            data = path.read_bytes()
            if len(data) != item["size"]:
                raise ValueError(f"size mismatch for {item['path']}")
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise ValueError(f"checksum mismatch for {item['path']}")

    def _validate_pipeline_provenance(
        self, artifact_dir: Path, manifest: dict[str, Any]
    ) -> None:
        pipeline = manifest.get("backend_metadata", {}).get("pipeline")
        if pipeline is None:
            return
        if not isinstance(pipeline, list) or not pipeline:
            raise ValueError("invalid pipeline provenance")
        for item in pipeline:
            stage_dir = artifact_dir / "stages" / item["name"]
            stage_manifest_path = stage_dir / "stage-manifest.json"
            data = stage_manifest_path.read_bytes()
            if len(data) != item["source_stage_record_size"]:
                raise ValueError("stage manifest size mismatch")
            if hashlib.sha256(data).hexdigest() != item["source_stage_record_sha256"]:
                raise ValueError("stage manifest checksum mismatch")
            stage_manifest = json.loads(data)
            if stage_manifest.get("stage_hash") != item["stage_hash"]:
                raise ValueError("stage manifest belongs to another attempt")
            for artifact in item["artifacts"]:
                path = stage_dir / artifact["path"]
                artifact_data = path.read_bytes()
                if len(artifact_data) != artifact["size"]:
                    raise ValueError("stage artifact size mismatch")
                if hashlib.sha256(artifact_data).hexdigest() != artifact["sha256"]:
                    raise ValueError("stage artifact checksum mismatch")

    def _build_manifest(
        self,
        *,
        identity: dict[str, Any],
        inputs: dict[str, bytes],
        artifact_stage: Path,
        backend_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        request_hash = identity["request_hash"]
        artifacts = self._artifact_entries(artifact_stage)
        return {
            "record_id": request_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "request": identity["payload"],
            "artifacts": artifacts,
            "backend_metadata": backend_metadata,
        }

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
