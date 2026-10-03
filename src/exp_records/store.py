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
    def __init__(self, root: str | Path, backend: ComputeBackend):
        self.root = Path(root)
        self.backend = backend
        self.executor_id = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._locks_guard = threading.Lock()
        self._thread_locks: dict[str, threading.Lock] = {}
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("requests", "artifacts", "staging", "locks", "attempts"):
            (self.root / name).mkdir(exist_ok=True)

    def _thread_lock_for(self, request_hash: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._thread_locks.get(request_hash)
            if lock is None:
                lock = threading.Lock()
                self._thread_locks[request_hash] = lock
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
                        if artifact_target.exists():
                            shutil.rmtree(artifact_target)
                        os.replace(artifact_stage, artifact_target)
                        self._fsync_directory(artifact_target.parent)
                        record_path = self._publish_record(request_hash, manifest)
                        shutil.rmtree(
                            self.root / "staging" / request_hash, ignore_errors=True
                        )
                    except BaseException as exc:
                        self._append_attempt_event(
                            request_hash,
                            {
                                "event": "failed",
                                "attempt_id": attempt_id,
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        )
                        shutil.rmtree(
                            self.root / "staging" / request_hash, ignore_errors=True
                        )
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

    def load_manifest(self, request_hash: str) -> dict[str, Any] | None:
        """Return the stored manifest only if the record is complete and verifies."""
        summary = self._load_complete(request_hash)
        if summary is None:
            return None
        try:
            manifest = json.loads(summary.record_path.read_bytes())
        except (OSError, json.JSONDecodeError):
            return None
        if manifest.get("record_id") != request_hash:
            return None
        return manifest

    def record_state(self, request_hash: str) -> dict[str, Any]:
        """Describe whether a record is complete, in progress, or untraceable."""
        state: dict[str, Any] = {"request_hash": request_hash, "complete": False}
        if self._load_complete(request_hash) is not None:
            state["complete"] = True
            return state

        record_path = self.root / "requests" / f"{request_hash}.json"
        artifact_dir = self.root / "artifacts" / request_hash
        if record_path.is_file():
            state["record_failed_verification"] = True
        if artifact_dir.exists() and not record_path.is_file():
            state["artifacts_without_record"] = True

        events = self._load_attempt_events(request_hash)
        finished = {
            event.get("attempt_id")
            for event in events
            if event.get("event") in ("published", "failed")
        }
        orphaned = [
            event.get("attempt_id")
            for event in events
            if event.get("event") == "execute_start"
            and event.get("attempt_id") not in finished
        ]
        staging_root = self.root / "staging" / request_hash
        stale_staging = (
            sorted(entry.name for entry in staging_root.iterdir())
            if staging_root.is_dir()
            else []
        )
        if orphaned or stale_staging:
            state["in_progress"] = True
            if orphaned:
                state["orphaned_attempts"] = orphaned
            if stale_staging:
                state["stale_staging_dirs"] = stale_staging
        return state

    def install_record(
        self, manifest: dict[str, Any], artifact_source: str | Path
    ) -> RecordSummary:
        """Install a verified record received from a delivery bundle.

        The record identity is recomputed from the embedded request payload, so
        importing the same delivery twice reuses the existing record instead of
        creating a second provenance entry.
        """
        payload = manifest.get("request")
        if not isinstance(payload, dict):
            raise ValueError("manifest is missing the request payload")
        record_id = hashlib.sha256(canonical_json(payload)).hexdigest()
        if manifest.get("record_id") != record_id:
            raise ValueError("manifest record_id does not match its request payload")

        artifact_source = Path(artifact_source)
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise ValueError("manifest has no artifacts")
        for item in artifacts:
            path = artifact_source / item["path"]
            if not path.is_file():
                raise ValueError(f"missing artifact {item['path']!r}")
            data = path.read_bytes()
            if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise ValueError(f"artifact {item['path']!r} failed verification")

        lock_path = self.root / "locks" / f"{record_id}.lock"
        with self._thread_lock_for(record_id):
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_complete(record_id)
                    if existing is not None:
                        self._append_attempt_event(
                            record_id,
                            {"event": "reused", "record_id": record_id, "via": "import"},
                        )
                        return existing

                    staging_dir = (
                        self.root / "staging" / record_id / f"import-{uuid.uuid4().hex}"
                    )
                    artifact_stage = staging_dir / "artifacts"
                    artifact_stage.mkdir(parents=True)
                    try:
                        for item in artifacts:
                            target = artifact_stage / item["path"]
                            target.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(artifact_source / item["path"], target)
                        artifact_target = self.root / "artifacts" / record_id
                        if artifact_target.exists():
                            shutil.rmtree(artifact_target)
                        os.replace(artifact_stage, artifact_target)
                        self._fsync_directory(artifact_target.parent)
                        record_path = self._publish_record(record_id, manifest)
                        shutil.rmtree(
                            self.root / "staging" / record_id, ignore_errors=True
                        )
                    except BaseException:
                        shutil.rmtree(
                            self.root / "staging" / record_id, ignore_errors=True
                        )
                        raise

                    self._append_attempt_event(
                        record_id,
                        {"event": "published", "record_id": record_id, "via": "import"},
                    )
                    return RecordSummary(
                        request_hash=record_id,
                        record_id=record_id,
                        reused=False,
                        record_path=record_path,
                        artifact_dir=artifact_target,
                    )
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

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

    def _load_complete(self, request_hash: str) -> RecordSummary | None:
        record_path = self.root / "requests" / f"{request_hash}.json"
        artifact_dir = self.root / "artifacts" / request_hash
        if not record_path.is_file() or not artifact_dir.is_dir():
            return None
        try:
            manifest = json.loads(record_path.read_bytes())
            if manifest.get("record_id") != request_hash:
                return None
            artifacts = manifest.get("artifacts", [])
            if not isinstance(artifacts, list) or not artifacts:
                return None
            for item in artifacts:
                path = artifact_dir / item["path"]
                if not path.is_file():
                    return None
                if path.stat().st_size != item["size"]:
                    return None
                if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
                    return None
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            return None
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
            raise RuntimeError("backend did not produce any artifacts")
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
