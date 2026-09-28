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
        self._thread_lock = threading.Lock()
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("requests", "artifacts", "staging", "locks"):
            (self.root / name).mkdir(exist_ok=True)

    def run(self, request: ExperimentRequest) -> RecordSummary:
        identity = request.identity()
        request_hash = identity["request_hash"]
        lock_path = self.root / "locks" / "requests.lock"

        with self._thread_lock:
            with lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    existing = self._load_complete(request_hash)
                    if existing is not None:
                        return existing

                    inputs = request.resolved_inputs()
                    staging_dir = self.root / "staging" / uuid.uuid4().hex
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
                        record_path = self._publish_record(request_hash, manifest)
                        shutil.rmtree(staging_dir, ignore_errors=True)
                    except BaseException:
                        shutil.rmtree(staging_dir, ignore_errors=True)
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
