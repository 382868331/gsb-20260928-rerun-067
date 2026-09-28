from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .api import ExperimentRunner
from .inputs import FileInput


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="experiment-repro")
    parser.add_argument("--store", type=Path, default=Path(".experiment_store"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("experiment")
    run_parser.add_argument("inputs", nargs="?", type=_load_json)
    run_parser.add_argument("--input-file", type=Path)
    run_parser.add_argument("--parameters", type=_load_json, default={})
    run_parser.add_argument("--force", action="store_true")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("record_id")

    args = parser.parse_args(argv)
    runner = ExperimentRunner(args.store)

    if args.command == "run":
        if args.input_file is not None:
            inputs: Any = FileInput.from_path(args.input_file)
        elif args.inputs is not None:
            inputs = args.inputs
        else:
            parser.error("run requires inputs or --input-file")
        result = runner.run(
            args.experiment,
            inputs,
            args.parameters,
            force=args.force,
        )
        print(json.dumps({
            "reused": result.reused,
            "record_id": result.record_id,
            "record_path": str(result.record_path),
            "status": result.record["status"],
        }, ensure_ascii=False, indent=2))
        return 0 if result.record["status"] == "succeeded" else 1

    record = runner.storage.read_record(args.record_id)
    print(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _load_json(value: str) -> Any:
    path = Path(value)
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(value)


if __name__ == "__main__":
    raise SystemExit(main())
