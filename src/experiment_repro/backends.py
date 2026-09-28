from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .records import SCHEMA_VERSION, sha256_file, sha256_text, stable_json


@dataclass(frozen=True)
class Task:
    experiment: str
    inputs: Any
    parameters: dict[str, Any]
    work_dir: Path


@dataclass(frozen=True)
class BackendResult:
    outputs: dict[str, Any]
    artifacts: dict[str, Path]


ComputeFunction = Callable[[Task], BackendResult]


class ComputeBackend:
    name = "abstract"
    version = 1

    def identity(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version, "schema_version": SCHEMA_VERSION}

    def execute(self, task: Task) -> BackendResult:
        raise NotImplementedError


class LocalBackend(ComputeBackend):
    name = "local"

    def __init__(self) -> None:
        self._handlers: dict[str, ComputeFunction] = {
            "sum": _sum_handler,
            "checksum": _checksum_handler,
            "file-checksum": _file_checksum_handler,
        }

    def register(self, experiment: str, handler: ComputeFunction) -> None:
        self._handlers[experiment] = handler

    def execute(self, task: Task) -> BackendResult:
        try:
            handler = self._handlers[task.experiment]
        except KeyError:
            raise ValueError(f"No local handler registered for experiment {task.experiment!r}") from None
        result = handler(task)
        if not isinstance(result, BackendResult):
            raise TypeError("Compute handler must return BackendResult")
        return result


def _sum_handler(task: Task) -> BackendResult:
    values = task.inputs
    if not isinstance(values, list) or not all(isinstance(value, (int, float)) for value in values):
        raise ValueError("sum experiment requires a list of numbers")
    scale = float(task.parameters.get("scale", 1))
    total = sum(values) * scale
    output_path = task.work_dir / "result.txt"
    output_path.write_text(f"{total}\n", encoding="utf-8")
    return BackendResult(
        outputs={"sum": total, "count": len(values)},
        artifacts={"result.txt": output_path},
    )


def _checksum_handler(task: Task) -> BackendResult:
    text_value = task.inputs
    if not isinstance(text_value, str):
        raise ValueError("checksum experiment requires a string")
    digest = sha256_text(text_value + stable_json(task.parameters))
    output_path = task.work_dir / "checksum.json"
    output_path.write_text(json.dumps({"sha256": digest}, ensure_ascii=False), encoding="utf-8")
    return BackendResult(outputs={"sha256": digest}, artifacts={"checksum.json": output_path})


def _file_checksum_handler(task: Task) -> BackendResult:
    from .inputs import FileInput

    if not isinstance(task.inputs, FileInput):
        raise ValueError("file-checksum experiment requires FileInput")
    digest = sha256_file(task.inputs.path)
    output_path = task.work_dir / "file-checksum.txt"
    output_path.write_text(f"{digest}  {task.inputs.sha256}\n", encoding="utf-8")
    return BackendResult(
        outputs={"sha256": digest, "bytes": task.inputs.bytes},
        artifacts={"file-checksum.txt": output_path},
    )
