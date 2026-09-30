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
from .models import ExperimentRequest, RecordSummary, canonical_json


class RunStore:
    _thread_locks: dict[str, threading.Lock] = {}
    _thread_locks_guard = threading.Lock()

    def __init__(self, root: str | Path, backend: ComputeBackend):
        self.root = Path(root)
        self.backend = backend
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("requests", "artifacts", "staging", "locks", "attempts"):
            (self.root / name).mkdir(exist_ok=True)

    def run(self, request: ExperimentRequest) -> RecordSummary:
        identity = request.identity()
        request_hash = identity["request_hash"]
        lock_path = self.root / "locks" / f"request-{request_hash}.lock"

        with self._thread_lock_for(self.root.resolve(), request_hash):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_complete(request_hash)
                    if existing is not None:
                        self._reconcile_completed(request_hash, existing)
                        return existing

                    inputs = request.resolved_inputs()
                    attempt_id = uuid.uuid4().hex
                    attempt, staging_dir = self._begin_attempt(request_hash, attempt_id)
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
                        self._update_attempt(attempt, state="publishing")
                        artifact_target = self._publish_artifacts(request_hash, artifact_stage)
                        record_path = self._publish_record(request_hash, manifest)
                        self._update_attempt(
                            attempt,
                            state="succeeded",
                            record_path=str(record_path),
                            artifact_dir=str(artifact_target),
                        )
                        staging_dir.rmdir()
                    except BaseException as exc:
                        self._fail_attempt(attempt, staging_dir, exc)
                        raise

                    return RecordSummary(
                        request_hash=request_hash,
                        record_id=request_hash,
                        reused=False,
                        record_path=record_path,
                        artifact_dir=artifact_target,
                    )
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @classmethod
    def _thread_lock_for(cls, root: Path, request_hash: str) -> threading.Lock:
        key = (str(root), request_hash)
        with cls._thread_locks_guard:
            if key not in cls._thread_locks:
                cls._thread_locks[key] = threading.Lock()
            return cls._thread_locks[key]

    def _begin_attempt(self, request_hash: str, attempt_id: str) -> tuple[dict[str, Any], Path]:
        staging_dir = self.root / "staging" / attempt_id
        evidence_dir = self.root / "attempts" / attempt_id
        evidence_dir.mkdir(parents=True)
        staging_dir.mkdir(parents=True)

        predecessors = []
        for prior in self._attempts_for(request_hash):
            if prior.get("state") not in {"running", "publishing"}:
                continue
            prior_id = prior.get("attempt_id")
            if not isinstance(prior_id, str):
                continue
            prior["state"] = "abandoned"
            prior["takeover_reason"] = "request lock was released before a complete record was published"
            prior["taken_over_by"] = attempt_id
            self._preserve_staging(prior)
            self._write_attempt(prior)
            predecessors.append(prior_id)

        orphaned_paths = self._quarantine_unpublished(request_hash, evidence_dir)
        sequence = max((int(prior.get("sequence", 0)) for prior in self._attempts_for(request_hash)), default=0) + 1
        attempt = {
            "attempt_id": attempt_id,
            "request_hash": request_hash,
            "sequence": sequence,
            "state": "running",
            "pid": os.getpid(),
            "started_at": self._timestamp(),
            "updated_at": self._timestamp(),
            "staging_dir": str(staging_dir),
            "predecessor_attempt_ids": predecessors,
            "takeover_basis": {
                "abandoned_attempt_ids": predecessors,
                "quarantined_paths": orphaned_paths,
            },
        }
        self._write_attempt(attempt)
        return attempt, staging_dir

    def _publish_artifacts(self, request_hash: str, artifact_stage: Path) -> Path:
        artifact_target = self.root / "artifacts" / request_hash
        if artifact_target.exists():
            raise RuntimeError(f"unverified artifact directory already exists: {artifact_target}")
        os.replace(artifact_stage, artifact_target)
        self._fsync_directory(artifact_target.parent)
        return artifact_target

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

    def _fail_attempt(self, attempt: dict[str, Any], staging_dir: Path, exc: BaseException) -> None:
        attempt["state"] = "failed"
        attempt["failed_at"] = self._timestamp()
        attempt["error"] = f"{type(exc).__name__}: {exc}"
        self._preserve_staging(attempt)
        self._write_attempt(attempt)
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _preserve_staging(self, attempt: dict[str, Any]) -> None:
        attempt_id = attempt["attempt_id"]
        staging_dir = Path(attempt["staging_dir"])
        if not staging_dir.is_dir():
            return
        evidence_dir = self.root / "attempts" / attempt_id
        evidence_dir.mkdir(parents=True, exist_ok=True)
        preserved_dir = evidence_dir / "staging"
        if preserved_dir.exists():
            shutil.rmtree(preserved_dir)
        try:
            os.replace(staging_dir, preserved_dir)
        except OSError:
            shutil.move(str(staging_dir), str(preserved_dir))
        attempt["preserved_staging"] = str(preserved_dir)

    def _quarantine_unpublished(self, request_hash: str, evidence_dir: Path) -> list[str]:
        quarantined = []
        artifact_dir = self.root / "artifacts" / request_hash
        record_path = self.root / "requests" / f"{request_hash}.json"

        record_error = None
        if record_path.is_file():
            try:
                self._read_manifest(request_hash, record_path, artifact_dir)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                record_error = str(exc)

        if artifact_dir.is_dir() and (not record_path.is_file() or record_error is not None):
            target = evidence_dir / ("invalid-artifacts" if record_error else "unpublished-artifacts")
            os.replace(artifact_dir, target)
            quarantined.append(str(target))

        if record_path.is_file() and record_error is not None:
            target = evidence_dir / "invalid-record.json"
            os.replace(record_path, target)
            quarantined.append(str(target))

        for temporary_record in record_path.parent.glob(f".{request_hash}.*.tmp"):
            temporary_dir = evidence_dir / "unpublished-records"
            temporary_dir.mkdir(exist_ok=True)
            target = temporary_dir / temporary_record.name
            os.replace(temporary_record, target)
            quarantined.append(str(target))

        if quarantined:
            (evidence_dir / "takeover-reason.txt").write_text(
                "\n".join([record_error or "record was not atomically published", *quarantined]) + "\n",
                encoding="utf-8",
            )
        return quarantined

    def _attempts_for(self, request_hash: str) -> list[dict[str, Any]]:
        attempts = []
        for path in sorted((self.root / "attempts").glob("*/attempt.json")):
            try:
                attempt = json.loads(path.read_bytes())
            except (OSError, json.JSONDecodeError):
                continue
            if attempt.get("request_hash") == request_hash:
                attempts.append(attempt)
        return attempts

    def _write_attempt(self, attempt: dict[str, Any]) -> None:
        attempt["updated_at"] = self._timestamp()
        path = self.root / "attempts" / attempt["attempt_id"] / "attempt.json"
        temporary = path.with_name(f".attempt.{uuid.uuid4().hex}.tmp")
        payload = json.dumps(attempt, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        temporary.write_bytes(payload)
        os.replace(temporary, path)
        self._fsync_directory(path.parent)

    def _update_attempt(self, attempt: dict[str, Any], **updates: Any) -> None:
        attempt.update(updates)
        self._write_attempt(attempt)

    def _reconcile_completed(self, request_hash: str, summary: RecordSummary) -> None:
        for attempt in self._attempts_for(request_hash):
            if attempt.get("state") not in {"running", "publishing"}:
                continue
            staging_dir = Path(attempt.get("staging_dir", ""))
            if staging_dir.is_dir() and not any(staging_dir.iterdir()):
                staging_dir.rmdir()
            attempt["state"] = "succeeded"
            attempt["completion_basis"] = "verified complete record found while acquiring the request lock"
            attempt["record_path"] = str(summary.record_path)
            attempt["artifact_dir"] = str(summary.artifact_dir)
            self._write_attempt(attempt)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _load_complete(self, request_hash: str) -> RecordSummary | None:
        record_path = self.root / "requests" / f"{request_hash}.json"
        artifact_dir = self.root / "artifacts" / request_hash
        if not record_path.is_file() or not artifact_dir.is_dir():
            return None
        try:
            return self._read_manifest(request_hash, record_path, artifact_dir)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return None

    def _read_manifest(self, request_hash: str, record_path: Path, artifact_dir: Path) -> RecordSummary:
        manifest = json.loads(record_path.read_bytes())
        if not isinstance(manifest, dict):
            raise ValueError("record manifest must be an object")
        if manifest.get("record_id") != request_hash or manifest.get("request_hash") != request_hash:
            raise ValueError("record identity does not match its request")

        request_payload = manifest.get("request")
        if not isinstance(request_payload, dict):
            raise ValueError("record request payload is missing")
        if hashlib.sha256(canonical_json(request_payload)).hexdigest() != request_hash:
            raise ValueError("record request payload does not match its request hash")

        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise ValueError("record must list at least one artifact")
        declared_paths = set()
        for item in artifacts:
            if not isinstance(item, dict):
                raise ValueError("artifact entry must be an object")
            relative_text = item.get("path")
            size = item.get("size")
            digest = item.get("sha256")
            if not isinstance(relative_text, str) or not isinstance(size, int) or not isinstance(digest, str):
                raise ValueError("artifact metadata has an invalid type")
            relative = Path(relative_text)
            if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
                raise ValueError("artifact path is unsafe")
            if relative_text in declared_paths:
                raise ValueError(f"duplicate artifact path: {relative_text}")
            declared_paths.add(relative_text)
            path = artifact_dir / relative
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"artifact is missing or not a regular file: {relative_text}")
            if size < 0 or path.stat().st_size != size:
                raise ValueError(f"artifact size mismatch: {relative_text}")
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError(f"artifact digest mismatch: {relative_text}")

        actual_paths = {
            path.relative_to(artifact_dir).as_posix()
            for path in artifact_dir.rglob("*")
            if path.is_file() and not path.is_symlink()
        }
        if actual_paths != declared_paths:
            raise ValueError("artifact directory contents do not match the record manifest")
        return RecordSummary(
            request_hash=request_hash,
            record_id=request_hash,
            reused=True,
            record_path=record_path,
            artifact_dir=artifact_dir,
        )

    def _build_manifest(
        self,
        *,
        identity: dict[str, Any],
        inputs: dict[str, bytes],
        artifact_stage: Path,
        backend_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        request_hash = identity["request_hash"]
        artifacts = []
        for path in sorted(path for path in artifact_stage.rglob("*") if path.is_file()):
            relative = path.relative_to(artifact_stage)
            if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts) or path.is_symlink():
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
            raise RuntimeError("backend did not produce any artifacts")
        if not isinstance(backend_metadata, dict):
            raise ValueError("backend metadata must be an object")
        return {
            "record_id": request_hash,
            "request_hash": request_hash,
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
