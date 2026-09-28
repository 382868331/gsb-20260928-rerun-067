from __future__ import annotations

import errno
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

from .records import stable_json


class Storage:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.runs_dir = self.root / "runs"
        self.artifacts_dir = self.root / "artifacts"
        self.tmp_dir = self.root / "tmp"
        self.locks_dir = self.root / "locks"
        self.index_path = self.root / "index.json"
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        for path in (self.root, self.runs_dir, self.artifacts_dir, self.tmp_dir, self.locks_dir):
            path.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def request_lock(self, fingerprint: str) -> Iterator[None]:
        lock_path = self.locks_dir / f"{fingerprint}.lock"
        handle = lock_path.open("a+")
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def read_index(self) -> dict[str, str]:
        if not self.index_path.exists():
            return {}
        with self.index_path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise ValueError("Experiment index must be a JSON object")
        return {str(key): str(item) for key, item in value.items()}

    def write_index(self, index: Mapping[str, str]) -> None:
        atomic_write_text(
            self.index_path,
            stable_json(dict(sorted(index.items()))) + "\n",
        )

    def successful_run_id(self, fingerprint: str) -> str | None:
        run_id = self.read_index().get(fingerprint)
        if run_id is not None and self._is_complete_success(fingerprint, run_id):
            return run_id

        recovered = self._find_complete_success(fingerprint)
        if recovered is not None:
            index = self.read_index()
            index[fingerprint] = recovered
            self.write_index(index)
        return recovered

    def _find_complete_success(self, fingerprint: str) -> str | None:
        prefix = f"{fingerprint}-"
        candidates = []
        for path in self.runs_dir.glob(f"{prefix}*.json"):
            run_id = path.stem
            suffix = run_id[len(prefix) :]
            if suffix.isdigit() and self._is_complete_success(fingerprint, run_id):
                candidates.append((int(suffix), run_id))
        return min(candidates, default=(0, None))[1]

    def _is_complete_success(self, fingerprint: str, run_id: str) -> bool:
        try:
            record = self.read_record(run_id)
        except (OSError, ValueError, KeyError):
            return False
        if record.get("status") != "succeeded" or record.get("request_fingerprint") != fingerprint:
            return False
        artifacts = record.get("artifacts", {})
        if not isinstance(artifacts, Mapping):
            return False
        for name, metadata in artifacts.items():
            path = self.artifact_path(run_id, str(name))
            if not path.is_file() or path.stat().st_size != metadata.get("bytes"):
                return False
            if hash_file(path) != metadata.get("sha256"):
                return False
        return True

    def publish_success(
        self,
        *,
        fingerprint: str,
        record_id: str,
        record: dict[str, Any],
        staging_dir: Path,
        artifact_files: Mapping[str, Path],
    ) -> Path:
        artifact_root = self.artifacts_dir / record_id
        artifact_manifest: dict[str, dict[str, Any]] = {}
        if artifact_files:
            artifact_root.mkdir(parents=True, exist_ok=False)
            for logical_name, source in artifact_files.items():
                safe_name = validate_artifact_name(logical_name)
                destination = artifact_root / safe_name
                destination.parent.mkdir(parents=True, exist_ok=True)
                atomic_copy(source, destination)
                artifact_manifest[safe_name] = {
                    "path": f"{record_id}/{safe_name}",
                    "sha256": hash_file(destination),
                    "bytes": destination.stat().st_size,
                }

        record = dict(record)
        record["artifacts"] = dict(sorted(artifact_manifest.items()))
        staging_record = staging_dir / "record.json"
        atomic_write_text(staging_record, json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True))

        target = self.record_path(record_id)
        atomic_move(staging_record, target)
        return target

    def commit_index(self, fingerprint: str, record_id: str) -> None:
        index = self.read_index()
        index[fingerprint] = record_id
        self.write_index(index)

    def publish_failure(self, record_id: str, record: dict[str, Any]) -> Path:
        path = self.record_path(record_id)
        atomic_write_text(path, json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True))
        return path

    def record_path(self, record_id: str) -> Path:
        validate_record_id(record_id)
        return self.runs_dir / f"{record_id}.json"

    def read_record(self, record_id: str) -> dict[str, Any]:
        with self.record_path(record_id).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise ValueError("Experiment record must be a JSON object")
        return value

    def artifact_path(self, record_id: str, artifact_name: str) -> Path:
        safe_name = validate_artifact_name(artifact_name)
        return self.artifacts_dir / record_id / safe_name

    @contextmanager
    def staging_area(self) -> Iterator[Path]:
        path = Path(tempfile.mkdtemp(prefix="run-", dir=self.tmp_dir))
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)


def validate_record_id(record_id: str) -> None:
    if not record_id or any(char in record_id for char in ("/", "\\", os.sep)) or record_id in (".", ".."):
        raise ValueError("Invalid record id")


def validate_artifact_name(name: str) -> str:
    path = Path(name)
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError("Invalid artifact name")
    return name.replace(os.altsep or "＀", os.sep) if os.altsep and os.altsep in name else name


def hash_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as incoming:
            shutil.copyfileobj(incoming, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def atomic_move(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(source, destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        atomic_copy(source, destination)+        source.unlink()
