from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .backends import ComputeBackend, LocalBackend, Task
from .environment import local_environment
from .records import (
    SCHEMA_VERSION,
    make_record_id,
    make_request_fingerprint,
    normalize_inputs,
    utc_now,
)
from .storage import Storage


@dataclass(frozen=True)
class RunResult:
    reused: bool
    record_id: str
    record_path: Path
    record: dict[str, Any]


class ExperimentRunner:
    def __init__(self, store: str | Path | Storage, backend: ComputeBackend | None = None):
        self.storage = store if isinstance(store, Storage) else Storage(store)
        self.backend = backend or LocalBackend()

    def run(
        self,
        experiment: str,
        inputs: Any,
        parameters: Mapping[str, Any] | None = None,
        *,
        environment: Mapping[str, Any] | None = None,
        force: bool = False,
    ) -> RunResult:
        params = dict(parameters or {})
        env = local_environment(environment)
        backend_identity = self.backend.identity()
        fingerprint = make_request_fingerprint(
            experiment=experiment,
            inputs=inputs,
            parameters=params,
            environment=env,
            backend=backend_identity,
        )

        with self.storage.request_lock(fingerprint):
            if not force:
                existing_id = self.storage.successful_run_id(fingerprint)
                if existing_id is not None:
                    return RunResult(
                        reused=True,
                        record_id=existing_id,
                        record_path=self.storage.record_path(existing_id),
                        record=self.storage.read_record(existing_id),
                    )
            return self._execute(experiment, inputs, params, env, backend_identity, fingerprint)

    def _execute(
        self,
        experiment: str,
        inputs: Any,
        parameters: dict[str, Any],
        environment: dict[str, Any],
        backend_identity: dict[str, Any],
        fingerprint: str,
    ) -> RunResult:
        attempt = self._next_attempt(fingerprint)
        record_id = make_record_id(fingerprint, attempt)
        started_at = utc_now()

        with self.storage.staging_area() as work_dir:
            try:
                computed = self.backend.execute(Task(experiment, inputs, parameters, work_dir))
                record = {
                    "schema_version": SCHEMA_VERSION,
                    "record_id": record_id,
                    "status": "succeeded",
                    "request_fingerprint": fingerprint,
                    "created_at": started_at,
                    "completed_at": utc_now(),
                    "experiment": {
                        "name": experiment,
                        "inputs": normalize_inputs(inputs),
                        "parameters": parameters,
                    },
                    "environment": environment,
                    "backend": backend_identity,
                    "outputs": computed.outputs,
                }
                record_path = self.storage.publish_success(
                    fingerprint=fingerprint,
                    record_id=record_id,
                    record=record,
                    staging_dir=work_dir,
                    artifact_files=computed.artifacts,
                )
                self.storage.commit_index(fingerprint, record_id)
            except Exception as exc:
                failure = {
                    "schema_version": SCHEMA_VERSION,
                    "record_id": record_id,
                    "status": "failed",
                    "request_fingerprint": fingerprint,
                    "created_at": started_at,
                    "completed_at": utc_now(),
                    "experiment": {
                        "name": experiment,
                        "inputs": normalize_inputs(inputs),
                        "parameters": parameters,
                    },
                    "environment": environment,
                    "backend": backend_identity,
                    "error": {"type": type(exc).__name__, "message": str(exc)},
                }
                record_path = self.storage.publish_failure(record_id, failure)
                return RunResult(False, record_id, record_path, failure)

        return RunResult(False, record_id, record_path, self.storage.read_record(record_id))

    def _next_attempt(self, fingerprint: str) -> int:
        prefix = f"{fingerprint}-"
        attempts = []
        for path in self.storage.runs_dir.glob(f"{prefix}*.json"):
            suffix = path.stem[len(prefix) :]
            if suffix.isdigit():
                attempts.append(int(suffix))
        return max(attempts, default=0) + 1
