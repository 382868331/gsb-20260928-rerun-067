from __future__ import annotations

import platform
import sys
from typing import Any, Mapping


def local_environment(extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    value: dict[str, Any] = {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "executable": sys.executable,
    }
    if extra:
        value.update(dict(extra))
    return value
