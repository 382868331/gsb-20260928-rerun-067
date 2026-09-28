from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_inputs(inputs: Any) -> Any:
    from .inputs import FileInput

    if isinstance(inputs, FileInput):
        return inputs.fingerprint_value()
    if isinstance(inputs, Mapping):
        return {str(key): normalize_inputs(value) for key, value in sorted(inputs.items())}
    if isinstance(inputs, (list, tuple)):
        return [normalize_inputs(value) for value in inputs]
    return inputs


def make_request_fingerprint(
    *,
    experiment: str,
    inputs: Any,
    parameters: Mapping[str, Any],
    environment: Mapping[str, Any],
    backend: Mapping[str, Any],
) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "experiment": experiment,
        "inputs": normalize_inputs(inputs),
        "parameters": normalize_inputs(dict(parameters)),
        "environment": normalize_inputs(dict(environment)),
        "backend": normalize_inputs(dict(backend)),
    }
    return sha256_text(stable_json(payload))


def make_record_id(fingerprint: str, attempt_number: int) -> str:
    return f"{fingerprint}-{attempt_number:04d}"
