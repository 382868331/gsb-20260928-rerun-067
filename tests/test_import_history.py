from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from exp_records.freeze import FreezeSelection, freeze, import_package, load_package_manifest
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore
from tests.test_partial_delivery import partial_backend, make_request, package_blob_dir


class ImportHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.origin = RunStore(self.root / "origin-store", partial_backend)
        self.req_a = make_request("a", "shared")
        self.req_b = make_request("b", "shared")
        self.sum_a = self.origin.run(self.req_a)
        self.sum_b = self.origin.run(self.req_b)
        self.out = self.root / "packages"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def selections(self):
        return [
            FreezeSelection(self.sum_a.request_hash, reason="candidate a"),
            FreezeSelection(self.sum_b.request_hash, reason="candidate b"),
        ]

    def test_import_history_records_identity_times_and_request_outcomes(self) -> None:
        package = freeze(self.origin, self.selections(), self.out)
        receiver = RunStore(self.root / "receiver", partial_backend)
        report = import_package(receiver, package["path"])

        history = receiver.import_history()
        entries = history.entries()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["package_id"], package["package_id"])
        self.assertEqual(entry["version"], 1)
        self.assertFalse(entry["supplement"])
        self.assertIsNone(entry["supersedes"])
        self.assertTrue(entry["started_at"] <= entry["ended_at"])
        self.assertEqual(
            entry["package_manifest_digest"],
            hashlib.sha256(
                (Path(package["path"]) / "freeze-manifest.json").read_bytes()
            ).hexdigest(),
        )
        self.assertTrue(entry["verified_complete"])

        by_request = {item["request_hash"]: item for item in entry["requests"]}
        outcome_a = by_request[self.sum_a.request_hash]
        self.assertEqual(outcome_a["state"], "complete")
        self.assertEqual(outcome_a["status"], "installed")
        self.assertIn("installed", outcome_a["reason"])
        self.assertEqual(outcome_a["selection_reason"], "candidate a")
        self.assertEqual(entry["summary"]["installed"], 2)

    def test_repeated_imports_are_traceable_but_install_only_once(self) -> None:
        package = freeze(self.origin, self.selections(), self.out)
        receiver = RunStore(self.root / "receiver", partial_backend)
        first = import_package(receiver, package["path"])
        second = import_package(receiver, package["path"])

        history = receiver.import_history()
        entries = history.entries(package_id=package["package_id"])
        self.assertEqual(len(entries), 2)
        self.assertNotEqual(entries[0]["import_id"], entries[1]["import_id"])
        self.assertEqual(
            {item["status"] for item in entries[0]["requests"]}, {"installed"}
        )
        self.assertEqual(
            {item["status"] for item in entries[1]["requests"]}, {"already_present"}
        )
        self.assertIn("did not reinstall", entries[1]["requests"][0]["reason"])

        outcomes = history.request_history(self.sum_a.request_hash)
        self.assertEqual([outcome["status"] for outcome in outcomes], ["installed", "already_present"])

        records = list((self.root / "receiver" / "requests").glob("*.json"))
        self.assertEqual(len(records), 2)
        self.assertTrue(second["verified_complete"])
        self.assertNotEqual(first["import_id"], second["import_id"])

    def test_history_survives_restart_without_origin_or_package(self) -> None:
        package = freeze(self.origin, self.selections(), self.out)
        receiver_dir = self.root / "receiver"
        receiver = RunStore(receiver_dir, partial_backend)
        import_package(receiver, package["path"])

        shutil.rmtree(self.origin.root)
        shutil.rmtree(package["path"])

        reopened = RunStore(receiver_dir, partial_backend)
        history = reopened.import_history()
        entries = history.entries()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["package_id"], package["package_id"])
        self.assertEqual(
            history.entries(package_id=package["package_id"]), entries
        )
        self.assertEqual(
            history.entries(package_id="freeze-nope"), []
        )
        outcomes = history.request_history(self.sum_b.request_hash)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["state"], "complete")
        self.assertEqual(outcomes[0]["package_id"], package["package_id"])

    def test_supplement_import_is_flagged_and_failed_reasons_recorded(self) -> None:
        package = freeze(self.origin, self.selections(), self.out)
        shared_sha = hashlib.sha256(b"shared-dependency-bytes").hexdigest()
        (package_blob_dir(package["path"]) / shared_sha).write_bytes(b"corrupted")

        receiver = RunStore(self.root / "receiver", partial_backend)
        first = import_package(receiver, package["path"])
        self.assertFalse(first["verified_complete"])

        supplement = freeze(
            self.origin, self.selections(), self.out, supersedes=package["path"]
        )
        second = import_package(receiver, supplement["path"])

        history = receiver.import_history()
        entries = history.entries()
        self.assertFalse(entries[0]["supplement"])
        self.assertTrue(entries[1]["supplement"])
        self.assertEqual(entries[1]["supersedes"], package["package_id"])

        # The failed first import left explicit per-request reasons.
        states = {item["request_hash"]: item for item in entries[0]["requests"]}
        self.assertEqual(states[self.sum_a.request_hash]["state"], "damaged")
        self.assertTrue(states[self.sum_a.request_hash]["reason"])

        # Request history spans both package attempts in chronological order.
        outcomes = history.request_history(self.sum_a.request_hash)
        self.assertEqual([o["state"] for o in outcomes], ["damaged", "complete"])
        self.assertEqual([o["status"] for o in outcomes], ["damaged", "installed"])
        self.assertEqual(len(second["records"]), 2)

    def test_history_log_is_plain_jsonl_inside_store(self) -> None:
        package = freeze(self.origin, self.selections(), self.out)
        receiver = RunStore(self.root / "receiver", partial_backend)
        import_package(receiver, package["path"])
        log = self.root / "receiver" / "import-history" / "entries.jsonl"
        self.assertTrue(log.is_file())
        lines = [line for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["schema_version"], "import-history/v1")
        self.assertEqual(record["package_id"], package["package_id"])


if __name__ == "__main__":
    unittest.main()
