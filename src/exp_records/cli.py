from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .backends import local_short_compute
from .freeze import FreezeError, FreezeSelection, freeze, import_package, plan_freeze
from .history import ImportHistory
from .models import ExperimentRequest, InputSpec, default_environment
from .replenish import ReplenishmentPlanner, load_wants
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
    freeze_cmd.add_argument(
        "--allow-partial",
        action="store_true",
        help="deliver healthy candidates even when others are missing or damaged",
    )

    import_cmd = subparsers.add_parser("import-package", help="verify and import a freeze package")
    import_cmd.add_argument("--store", required=True, type=Path)
    import_cmd.add_argument("--package", required=True, type=Path)

    history_cmd = subparsers.add_parser(
        "import-history", help="query persistent import history of the receiver store"
    )
    history_cmd.add_argument("--store", required=True, type=Path)
    history_cmd.add_argument("--package", help="filter by package_id")
    history_cmd.add_argument("--request", help="filter by request hash")

    replenish_cmd = subparsers.add_parser(
        "replenish-plan",
        help="plan batched re-delivery of missing content on the receiver",
    )
    replenish_cmd.add_argument("--store", required=True, type=Path)
    replenish_cmd.add_argument(
        "--wants",
        type=Path,
        help="JSON file of wanted requests; omit to resume from persisted state",
    )
    replenish_cmd.add_argument(
        "--budget", type=int, default=None, help="transfer budget in bytes per batch"
    )
    replenish_cmd.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="maximum batches this round; the rest is deferred",
    )
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
                allow_partial=args.allow_partial,
            )
        except FreezeError as exc:
            print(json.dumps(exc.report, ensure_ascii=False, sort_keys=True, indent=2))
            return 1
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    if args.command == "import-package":
        store = RunStore(args.store, local_short_compute)
        try:
            report = import_package(store, args.package)
        except Exception as exc:  # the failed attempt is already persisted
            entry = getattr(exc, "entry", None)
            print(
                json.dumps(
                    {"import_result": "failed", "reason": str(exc), "entry": entry},
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
            return 1
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if report["verified_complete"] else 1
    if args.command == "import-history":
        store = RunStore(args.store, local_short_compute)
        entries = ImportHistory(store).query(
            package_id=args.package,
            request_hash=args.request,
        )
        print(json.dumps(entries, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    if args.command == "replenish-plan":
        store = RunStore(args.store, local_short_compute)
        planner = ReplenishmentPlanner(store)
        if args.wants is not None:
            plan = planner.build_plan(
                load_wants(args.wants),
                budget_bytes=args.budget,
                max_batches=args.max_batches,
            )
        else:
            try:
                plan = planner.resume(
                    budget_bytes=args.budget, max_batches=args.max_batches
                )
            except FileNotFoundError as exc:
                print(str(exc), file=sys.stderr)
                return 1
        print(json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    raise SystemExit("unknown command: " + args.command)


if __name__ == "__main__":
    raise SystemExit(main())
