from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .backends import local_short_compute
from . import freeze
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


def add_freeze_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument(
        "--select",
        dest="selections",
        action="append",
        default=[],
        metavar="KEY=REQUEST_HASH",
        help="candidate to freeze; KEY labels it across delivery versions",
    )
    parser.add_argument(
        "--reason",
        dest="reasons",
        action="append",
        default=[],
        metavar="KEY=TEXT",
        help="why this candidate (or these parameter decisions) were adopted",
    )
    parser.add_argument(
        "--dep",
        dest="deps",
        action="append",
        default=[],
        metavar="KEY=REQUEST_HASH",
        help="shared intermediate record the candidate depends on",
    )
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        default=[],
        metavar="KEY:NAME=PATH",
        help="snapshot an input file into the bundle after re-checking its hash",
    )
    parser.add_argument(
        "--derive",
        dest="derives",
        action="append",
        default=[],
        metavar="KEY=BASE_HASH",
        help="adopt only part of a base candidate's parameters (see --set)",
    )
    parser.add_argument(
        "--set",
        dest="overrides",
        action="append",
        default=[],
        metavar="KEY:NAME=JSON",
        help="parameter override applied to a --derive candidate",
    )


def build_selections(store: RunStore, args: argparse.Namespace) -> list[freeze.Selection]:
    reasons = dict(parse_assignment(item) for item in args.reasons)
    deps: dict[str, list[str]] = {}
    for key, dep in (parse_assignment(item) for item in args.deps):
        deps.setdefault(key, []).append(dep)
    inputs: dict[str, list[tuple[str, Path]]] = {}
    for item in args.inputs:
        key_name, path = parse_assignment(item)
        if ":" not in key_name:
            raise SystemExit(f"--input expects KEY:NAME=PATH, got {item!r}")
        key, name = key_name.split(":", 1)
        inputs.setdefault(key, []).append((name, Path(path)))
    overrides: dict[str, dict[str, Any]] = {}
    for item in args.overrides:
        key_name, raw_value = parse_assignment(item)
        if ":" not in key_name:
            raise SystemExit(f"--set expects KEY:NAME=JSON, got {item!r}")
        key, name = key_name.split(":", 1)
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        overrides.setdefault(key, {})[name] = value

    request_hashes: dict[str, str] = {}
    for key, request_hash in (parse_assignment(item) for item in args.selections):
        request_hashes[key] = request_hash
    for key, base_hash in (parse_assignment(item) for item in args.derives):
        request_hashes[key] = freeze.derive_request_hash(
            store, base_hash, overrides.get(key, {})
        )
    if not request_hashes:
        raise SystemExit("at least one --select or --derive is required")
    return [
        freeze.Selection(
            key=key,
            request_hash=request_hash,
            reason=reasons.get(key, ""),
            depends_on=tuple(deps.get(key, ())),
            inputs=tuple(inputs.get(key, ())),
        )
        for key, request_hash in request_hashes.items()
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run and cache content-addressed experiments")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run or reuse an experiment")
    run.add_argument("--store", required=True, type=Path)
    run.add_argument("--input", dest="inputs", action="append", default=[], metavar="NAME=PATH")
    run.add_argument("--param", dest="params", action="append", default=[], metavar="NAME=JSON")
    run.add_argument("--env", dest="env", action="append", default=[], metavar="NAME=JSON")
    plan = subparsers.add_parser("freeze-plan", help="preview deliverable selections")
    add_freeze_arguments(plan)
    export = subparsers.add_parser("freeze-export", help="freeze selections into a bundle")
    add_freeze_arguments(export)
    export.add_argument("--out", required=True, type=Path)
    export.add_argument("--note", default="")
    verify = subparsers.add_parser("freeze-verify", help="verify a delivery bundle")
    verify.add_argument("--bundle", required=True, type=Path)
    importer = subparsers.add_parser("freeze-import", help="import a delivery bundle")
    importer.add_argument("--store", required=True, type=Path)
    importer.add_argument("--bundle", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "freeze-plan":
        store = RunStore(args.store, local_short_compute)
        print(json.dumps(freeze.plan_freeze(store, build_selections(store, args)),
                         ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    if args.command == "freeze-export":
        store = RunStore(args.store, local_short_compute)
        summary = freeze.export_delivery(
            store, build_selections(store, args), args.out, note=args.note
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    if args.command == "freeze-verify":
        report = freeze.verify_bundle(args.bundle)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if report.get("complete") else 1
    if args.command == "freeze-import":
        store = RunStore(args.store, local_short_compute)
        report = freeze.import_delivery(store, args.bundle)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
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
