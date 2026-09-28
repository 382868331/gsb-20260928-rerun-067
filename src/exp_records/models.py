from __future__ import annotations

import hashlib
import json
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "experiment-record/v1"


@dataclass(frozen=True)
class InputSpec:
    logical_name: str
    source_path: Path | None = None
    data: bytes | None = None

    def materialize(self) -> bytes:
        if self.data is not None:
            return self.data
        if self.source_path is None:
            raise ValueError(f"input {self.logical_name!r} has no content")
        return self.source_path.read_bytes()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def default_environment(backend_id: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    environment: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "backend": backend_id,
        "python": sys.version,
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
    }
    if extra:
        environment["provided"] = extra
    return environment


@dataclass(frozen=True)
class ExperimentRequest:
    inputs: tuple[InputSpec, ...]
    parameters: dict[str, Any] = field(default_factory=dict)
    environment: dict[str, Any] = field(default_factory=dict)

    def resolved_inputs(self) -> dict[str, bytes]:
        names = [item.logical_name for item in self.inputs]
        if len(names) != len(set(names)):
            raise ValueError("input logical names must be unique")
        return {item.logical_name: item.materialize() for item in self.inputs}

    def identity(self) -> dict[str, Any]:
        resolved = self.resolved_inputs()
        input_entries = []
        for name in sorted(resolved):
            data = resolved[name]
            digest = hashlib.sha256(data).hexdigest()
            input_entries.append(
                {
                    "name": name,
                    "sha256": digest,
                    "size": len(data),
                }
            )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "inputs": input_entries,
            "parameters": self.parameters,
            "environment": self.environment,
        }
        request_hash = hashlib.sha256(canonical_json(payload)).hexdigest()
        return {"request_hash": request_hash, "payload": payload}


@dataclass(frozen=True)
class RecordSummary:
    request_hash: str
    record_id: str
    reused: bool
    record_path: Path
    artifact_dir: Path
