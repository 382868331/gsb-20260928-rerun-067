from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from exp_records.backends import local_short_compute
from exp_records.freeze import FreezeSelection, freeze, import_package
from exp_records.history import IMPORT_HISTORY_VERSION, ImportHistory
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore


def make_request(data: bytes, parameters: dict | None = None) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data=data),),
        parameters=parameters or {},
        environment=default_environment("test/v1"),
    )


class ImportHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.origin = RunStore(self.root / "origin", local_short_compute)
        self.summary = self.origin.run(make_request(b"1 2 3", {"multiplier": 2}))
        self.out = self.root / "packages"
        freeze(self.origin, [FreezeSelection(self.summary.request_hash, reason="deliver")], self.out)
        self.package_dir = self.out / next(p.name for p in self.out.iterdir() if p.name.startswith("freeze-"))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def receiver(self) -> RunStore:
        return RunStore(self.root / "receiver", local_short_compute)

    def test_import_logs_identity_times_and_per_request_results(self) -> None:
        store = self.receiver()
        report = import_package(store, self.package_dir)
        self.assertTrue(report["verified_complete"])

        entries = ImportHistory(store).load()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["schema_version"], IMPORT_HISTORY_VERSION)
        self.assertEqual(entry["package_id"], report["package_id"])
        self.assertEqual(entry["version"], report["version"])
        self.assertEqual(entry["import_result"], "verified_complete")
        self.assertTrue(entry["package_manifest_digest"])
        self.assertTrue(entry["started_at"] <= entry["ended_at"])
        self.assertEqual(
            entry["requests"],
            [
                {
                    "request_hash": self.summary.request_hash,
                    "result": "installed",
                    "reason": "",
                    "selection_reason": "deliver",
                }
            ],
        )

    def test_duplicate_import_is_traced_but_not_installed_twice(self) -> None:
        store = self.receiver()
        import_package(store, self.package_dir)
        second = import_package(store, self.package_dir)

        entries = ImportHistory(store).load()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["requests"][0]["result"], "installed")
        self.assertEqual(entries[1]["requests"][0]["result"], "already_present")
        self.assertEqual(second["records"][0]["status"], "already_present")

        record_files = list((store.root / "requests").iterdir())
        self.assertEqual(len(record_files), 1)
        self.assertEqual(entries[0]["entry_id"] != entries[1]["entry_id"], True)
        self.assertEqual(entries[0]["package_id"], entries[1]["package_id"])

    def test_history_survives_restart_without_source_directory(self) -> None:
        store = self.receiver()
        import_package(store, self.package_dir)
        expected_id = ImportHistory(store).load()[0]["package_id"]

        shutil.rmtree(self.package_dir)
        reopened = RunStore(self.root / "receiver", local_short_compute)

        by_package = ImportHistory(reopened).query(package_id=expected_id)
        self.assertEqual(len(by_package), 1)
        by_request = ImportHistory(reopened).query(request_hash=self.summary.request_hash)
        self.assertEqual(len(by_request), 1)
        self.assertEqual(by_request[0]["package_id"], expected_id)
        self.assertEqual(ImportHistory(reopened).query(package_id="freeze-does-not-exist"), [])

    def test_failed_import_is_recorded(self) -> None:
        bad = self.root / "bad-package"
        shutil.copytree(self.package_dir, bad)
        (bad / "freeze-manifest.json").write_text("not json", encoding="utf-8")
        store = self.receiver()

        with self.assertRaises(Exception):
            import_package(store, bad)

        entries = ImportHistory(store).load()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["import_result"], "failed")
        self.assertIn("JSONDecodeError", entries[0]["reason"])

    def test_partial_package_records_undelivered_candidate(self) -> None:
        missing_hash = make_request(b"9 9 9", {"multiplier": 3}).identity()["request_hash"]
        partial_report = freeze(
            self.origin,
            [
                FreezeSelection(self.summary.request_hash, reason="healthy"),
                FreezeSelection(missing_hash, reason="not runnable"),
            ],
            self.root / "partial",
            allow_partial=True,
        )
        partial_dir = self.root / "partial" / partial_report["package_id"]
        store = self.receiver()
        report = import_package(store, partial_dir)
        self.assertFalse(report["verified_complete"])

        entry = ImportHistory(store).load()[0]
        results = {item["request_hash"]: item for item in entry["requests"]}
        self.assertEqual(results[self.summary.request_hash]["result"], "installed")
        self.assertIn(results[missing_hash]["result"], ("missing", "damaged"))
        self.assertTrue(results[missing_hash]["reason"])
        self.assertEqual(entry["import_result"], "completed")
        self.assertEqual(entry["result_counts"].get("installed"), 1)

    def test_supplement_redelivery_is_separately_traceable(self) -> None:
        store = self.receiver()
        import_package(store, self.package_dir)

        next_out = self.root / "packages-v2"
        v2 = freeze(
            self.origin,
            [FreezeSelection(self.summary.request_hash, reason="again")],
            next_out,
            supersedes=self.package_dir,
        )
        v2_dir = next_out / v2["package_id"]
        report = import_package(store, v2_dir)

        entries = ImportHistory(store).load()
        self.assertEqual(len(entries), 2)
        self.assertNotEqual(entries[0]["package_id"], entries[1]["package_id"])
        self.assertEqual(entries[1]["supersedes"], entries[0]["package_id"])
        self.assertEqual(report["records"][0]["status"], "already_present")
        by_request = ImportHistory(store).query(request_hash=self.summary.request_hash)
        self.assertEqual(len(by_request), 2)
