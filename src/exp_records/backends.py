from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable


ComputeBackend = Callable[[dict[str, bytes], dict[str, Any], Path], dict[str, Any]]


def local_short_compute(
    inputs: dict[str, bytes],
    parameters: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    if parameters.get("force_failure"):
        raise RuntimeError("forced local computation failure")

    multiplier = parameters.get("multiplier", 1)
    if not isinstance(multiplier, int):
        raise ValueError("parameter 'multiplier' must be an integer")

    terms = []
    input_digests = {}
    for name in sorted(inputs):
        data = inputs[name]
        digest = hashlib.sha256(data).hexdigest()
        input_digests[name] = digest
        terms.extend(digest.encode("ascii"))
        terms.append(multiplier & 0xFF)

    result_digest = hashlib.sha256(bytes(terms)).hexdigest()
    result = {
        "backend": "local-short-compute/v1",
        "input_digests": input_digests,
        "multiplier": multiplier,
        "result_digest": result_digest,
    }

    (output_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    lines = [f"{name} {input_digests[name]} x{multiplier} => {result_digest}" for name in sorted(inputs)]
    (output_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"result_digest": result_digest}
