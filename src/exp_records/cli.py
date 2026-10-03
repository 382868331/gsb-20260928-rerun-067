from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .backends import local_short_compute
from .freeze import FreezeError, FreezeSelection, freeze, import_package, plan_freeze
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

    plan = subparsers.add_parser("freeze-plan", help="preview which selections can be frozen")
    plan.add_argument("--store", required=True, type=Path)
    plan.add_argument("--select", dest="selects", action="append", default=[], metavar="HASH")
    plan.add_argument("--selections", type=Path, help="JSON file with selection objects")

    freeze_cmd = subparsers.add_parser("freeze", help="publish an immutable freeze package")
    freeze_cmd.add_argument("--store", required=True, type=Path)
    freeze_cmd.add_argument("--output", required=True, type=Path)
    freeze_cmd.add_argument("--select", dest="selects", action="append", default=[], metavar="HASH")
    freeze_cmd.add_argument("--selections", type=Path, help="JSON file with selection objects")
    freeze_cmd.add_argument("--supersedes", type=Path, help="previous package directory")

    import_cmd = subparsers.add_parser("import-package", help="verify and import a freeze package")
    import_cmd.add_argument("--store", required=True, type=Path)
    import_cmd.add_argument("--package", required=True, type=Path)
    return parser


def load_selections(args) -> list[FreezeSelection]:
    selections = [FreezeSelection(request_hash=value) for value in args.selects]
    selections_file = getattr(args, "selections", None)
    if selections_file:
        raw = json.loads(Path(selections_file).read_text(encoding="utf-8"))
        for entry in raw:
            adopted = entry.get("adopted_parameters")
            selections.append(
                FreezeSelection(
                    request_hash=entry["request_hash"],
                    adopted_parameters=tuple(adopted) if adopted is not None else None,
                    reason=entry.get("reason", ""),
                    expected_inputs=dict(entry.get("expected_inputs", {})),
                )
            )
    if not selections:
        raise SystemExit("at least one --select or --selections entry is required")
    return selections


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
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
    if args.command == "freeze-plan":
        store = RunStore(args.store, local_short_compute)
        report = plan_freeze(store, load_selections(args))
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if report["freezable"] else 1
    if args.command == "freeze":
        store = RunStore(args.store, local_short_compute)
        try:
            report = freeze(
                store,
                load_selections(args),
                args.output,
                supersedes=args.supersedes,
            )
        except FreezeError as exc:
            print(json.dumps(exc.report, ensure_ascii=False, sort_keys=True, indent=2))
            return 1
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    if args.command == "import-package":
        store = RunStore(args.store, local_short_compute)
        report = import_package(store, args.package)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if report["verified_complete"] else 1
    raise SystemExit("unknown command: " + args.command)


if __name__ == "__main__":
    raise SystemExit(main())
