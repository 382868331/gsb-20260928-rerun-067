from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .backends import local_short_compute
from .models import ExperimentRequest, InputSpec, default_environment
from .store import RunStore


def parse_assignment(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected NAME=VALUE")
    name, raw_value = value.split("=", 1)
    if not name:
        raise argparse.ArgumentTypeError("NAME must not be empty")
    return name, raw_value


def parse_parameter(value: str) -> tuple[str, Any]:
    name, raw_value = parse_assignment(value)
    try:
        return name, json.loads(raw_value)
    except json.JSONDecodeError:
        return name, raw_value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run and cache content-addressed experiments")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run or reuse an experiment")
    run.add_argument("--store", required=True, type=Path)
    run.add_argument("--input", dest="inputs", action="append", default=[], metavar="NAME=PATH")
    run.add_argument("--param", dest="params", action="append", default=[], metavar="NAME=JSON")
    run.add_argument("--env", dest="env", action="append", default=[], metavar="NAME=JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    inputs = tuple(
        InputSpec(logical_name=name, source_path=Path(path))
        for name, path in (parse_assignment(item) for item in args.inputs)
    )
    parameters = dict(parse_parameter(item) for item in args.params)
    provided_environment = dict(parse_parameter(item) for item in args.env)
    environment = default_environment("local-short-compute/v1", provided_environment)
    request = ExperimentRequest(inputs=inputs, parameters=parameters, environment=environment)
    summary = RunStore(args.store, local_short_compute).run(request)
    print(
        json.dumps(
            {
                "reused": summary.reused,
                "request_hash": summary.request_hash,
                "record": str(summary.record_path),
                "artifact_dir": str(summary.artifact_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
