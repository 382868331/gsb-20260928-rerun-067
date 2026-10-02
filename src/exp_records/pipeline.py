from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .models import default_environment


DEMO_PIPELINE = "demo-pipeline/v1"


@dataclass(frozen=True)
class StageDef:
    """Declaration of one pipeline stage.

    Identity of a stage is derived only from:
    - the content of the declared input files,
    - the declared subset of parameters,
    - the declared environment description,
    - the exact identities and artifact digests of declared upstream stages.

    A parameter that is not declared has no effect on the stage, so changing
    an unrelated parameter leaves the stage reusable.
    """

    name: str
    parameters: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    upstreams: tuple[str, ...] = ()


@dataclass(frozen=True)
class StageContext:
    pipeline: str
    stage: str
    inputs: dict[str, bytes]
    parameters: dict[str, Any]
    environment: dict[str, Any]
    upstream: dict[str, dict[str, bytes]] = field(default_factory=dict)

    def upstream_bytes(self, upstream: str, artifact: str) -> bytes:
        return self.upstream[upstream][artifact]


StageBackend = Callable[[StageContext, Path], dict[str, Any]]


@dataclass(frozen=True)
class Pipeline:
    name: str
    stages: dict[str, StageDef]
    backend: StageBackend

    def ordered_stage_names(self) -> list[str]:
        ordered: list[str] = []
        permanent: set[str] = set()
        temporary: set[str] = set()

        def visit(name: str) -> None:
            if name in permanent:
                return
            if name in temporary:
                raise ValueError(f"pipeline {self.name!r} has a dependency cycle at {name!r}")
            if name not in self.stages:
                raise ValueError(f"stage {name!r} is not defined in pipeline {self.name!r}")
            temporary.add(name)
            for upstream in self.stages[name].upstreams:
                visit(upstream)
            temporary.remove(name)
            permanent.add(name)
            ordered.append(name)

        for stage_name in self.stages:
            visit(stage_name)
        return ordered

    def stage_environment(self, provided: dict[str, Any] | None = None) -> dict[str, Any]:
        return default_environment(self.name, provided)

    def validate(self, inputs: dict[str, bytes], parameters: dict[str, Any]) -> None:
        for stage in self.stages.values():
            missing_inputs = [name for name in stage.inputs if name not in inputs]
            if missing_inputs:
                raise ValueError(f"stage {stage.name!r} misses inputs {missing_inputs}")
            missing_upstreams = [name for name in stage.upstreams if name not in self.stages]
            if missing_upstreams:
                raise ValueError(f"stage {stage.name!r} misses upstream stages {missing_upstreams}")
        known = {name for stage in self.stages.values() for name in stage.parameters}
        unknown = sorted(set(parameters) - known)
        if unknown:
            raise ValueError(f"pipeline {self.name!r} has no parameters {unknown}")


def _digest_chain(label: str, data: bytes) -> bytes:
    return hashlib.sha256(label.encode("utf-8") + b"\x00" + data).digest()


def demo_stage_backend(context: StageContext, output_dir: Path) -> dict[str, Any]:
    """Deterministic five-stage backend used for experiments and tests.

    The stages share products on purpose:
    prepare -> transform -> combine -> report
    prepare ----------------^   (combine reads two upstream paths)
    combine -> validate

    The ``fail_stage`` parameter forces a specific stage to fail, which lets
    tests exercise partial-stage failures and abandoned evidence.
    """

    fail_stage = context.parameters.get("fail_stage")
    if fail_stage == context.stage:
        (output_dir / "partial.txt").write_bytes(
            f"partial output of failed stage {context.stage}\n".encode("utf-8")
        )
        raise RuntimeError(f"forced failure at stage {context.stage!r}")

    if context.stage == "prepare":
        raw = context.inputs["raw"]
        seed = int(context.parameters["seed"])
        digest = hashlib.sha256(raw + seed.to_bytes(4, "little")).hexdigest()
        prepared = f"prepared:{digest}\n".encode("utf-8")
        (output_dir / "prepared.txt").write_bytes(prepared)
        (output_dir / "prepared.meta").write_text(
            json.dumps({"seed": seed, "input_digest": digest}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {"prepared_digest": digest}

    if context.stage == "transform":
        prepared = context.upstream_bytes("prepare", "prepared.txt")
        multiplier = int(context.parameters["multiplier"])
        digest = hashlib.sha256(
            _digest_chain("transform", prepared) + multiplier.to_bytes(4, "little")
        ).hexdigest()
        (output_dir / "transformed.txt").write_bytes(
            f"transformed:{digest}\n".encode("utf-8")
        )
        return {"transformed_digest": digest}

    if context.stage == "combine":
        transformed = context.upstream_bytes("transform", "transformed.txt")
        prepared = context.upstream_bytes("prepare", "prepared.txt")
        offset = int(context.parameters["offset"])
        digest = hashlib.sha256(
            _digest_chain("combine", transformed + prepared) + offset.to_bytes(4, "little")
        ).hexdigest()
        (output_dir / "combined.txt").write_bytes(f"combined:{digest}\n".encode("utf-8"))
        return {"combined_digest": digest}

    if context.stage == "validate":
        combined = context.upstream_bytes("combine", "combined.txt")
        token = combined.decode("utf-8").strip()
        (output_dir / "report.txt").write_text(f"validated {token}\n", encoding="utf-8")
        return {"valid": True}

    raise RuntimeError(f"demo backend cannot run unknown stage {context.stage!r}")


DEMO_STAGES: dict[str, StageDef] = {
    "prepare": StageDef("prepare", parameters=("seed", "fail_stage"), inputs=("raw",)),
    "transform": StageDef(
        "transform", parameters=("multiplier", "fail_stage"), upstreams=("prepare",)
    ),
    "combine": StageDef(
        "combine",
        parameters=("multiplier", "offset", "fail_stage"),
        upstreams=("transform", "prepare"),
    ),
    "validate": StageDef(
        "validate", parameters=("fail_stage",), upstreams=("combine",)
    ),
}


def demo_pipeline() -> Pipeline:
    return Pipeline(name=DEMO_PIPELINE, stages=dict(DEMO_STAGES), backend=demo_stage_backend)
