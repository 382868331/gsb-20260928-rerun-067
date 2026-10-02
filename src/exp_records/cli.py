from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .backends import local_short_compute
from .models import ExperimentRequest, InputSpec, default_environment
from .pipeline import demo_pipeline
from .rounds import RoundRunner, StageStore, compare_candidates, load_round, trace_final_result
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
    compare = subparsers.add_parser("compare-round", help="run a baseline plus candidate revisions")
    compare.add_argument("--store", required=True, type=Path)
    compare.add_argument("--round", required=True)
    compare.add_argument("--input", dest="inputs", action="append", default=[], metavar="NAME=PATH")
    compare.add_argument("--param", dest="params", action="append", default=[], metavar="NAME=JSON")
    compare.add_argument("--candidate", dest="candidates", action="append", default=[], metavar="JSON")
    trace = subparsers.add_parser("trace-round", help="verify and trace a saved round")
    trace.add_argument("--store", required=True, type=Path)
    trace.add_argument("--round", required=True)
    return parser


def _json_output(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)


def _command_compare_round(args: argparse.Namespace) -> int:
    inputs = {
        name: Path(path).read_bytes() for name, path in (parse_assignment(item) for item in args.inputs)
    }
    baseline = dict(parse_parameter(item) for item in args.params)
    candidates = []
    for raw in args.candidates:
        candidate = json.loads(raw)
        if "id" not in candidate:
            raise argparse.ArgumentTypeError("each --candidate needs an 'id'")
        candidate.setdefault("adjustments", {})
        candidates.append(candidate)
    store = StageStore(args.store, demo_pipeline())
    environment = store.pipeline.stage_environment({"source": "cli"})
    summary = RoundRunner(store).run_round(
        round_id=args.round,
        inputs=inputs,
        environment=environment,
        baseline=baseline,
        candidates=candidates,
    )
    print(_json_output({"round": summary, "comparison": compare_candidates(summary)}))
    return 0


def _command_trace_round(args: argparse.Namespace) -> int:
    print(_json_output(trace_final_result(args.store, args.round)))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "compare-round":
        return _command_compare_round(args)
    if args.command == "trace-round":
        return _command_trace_round(args)
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
