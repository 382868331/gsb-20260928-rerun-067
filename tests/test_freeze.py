from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from exp_records import freeze
from exp_records.backends import local_short_compute
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore


def make_request(files: dict[str, Path], parameters: dict | None = None) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=tuple(InputSpec(name, path) for name, path in files.items()),
        parameters=parameters or {},
        environment=default_environment("test/v1"),
    )


class FreezeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store_dir = self.root / "store"
        self.data = self.root / "numbers.txt"
        self.data.write_bytes(b"1 2 3\n")
        self.store = RunStore(self.store_dir, local_short_compute)
        self.summary_a = self.store.run(make_request({"numbers": self.data}, {"multiplier": 2}))
        self.summary_b = self.store.run(make_request({"numbers": self.data}, {"multiplier": 5}))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def selection(self, key, summary, **kwargs):
        return freeze.Selection(key=key, request_hash=summary.request_hash, **kwargs)

    def test_plan_marks_confirmed_and_needs_rerun(self):
        missing = freeze.Selection(key="ghost", request_hash="0" * 64)
        plan = freeze.plan_freeze(self.store, [self.selection("cand-a", self.summary_a), missing])
        statuses = {item["key"]: item["status"] for item in plan["items"]}
        self.assertEqual(statuses["cand-a"], freeze.STATUS_CONFIRMED)
        self.assertEqual(statuses["ghost"], freeze.STATUS_NEEDS_RERUN)
        self.assertFalse(plan["deliverable"])

    def test_plan_marks_untracked_files_as_unproven(self):
        phantom = self.store_dir / "artifacts" / ("f" * 64)
        phantom.mkdir(parents=True)
        (phantom / "result.json").write_text("{}")
        plan = freeze.plan_freeze(self.store, [freeze.Selection(key="phantom", request_hash="f" * 64)])
        self.assertEqual(plan["items"][0]["status"], freeze.STATUS_UNPROVEN)

    def test_export_import_roundtrip_and_reimport_is_idempotent(self):
        bundle = self.root / "bundle"
        summary = freeze.export_delivery(
            self.store,
            [self.selection("cand-a", self.summary_a, reason="best score", inputs=(("numbers", self.data),))],
            bundle,
            note="first delivery",
        )
        self.assertTrue(summary["complete"])
        self.assertEqual(summary["version"], 1)
        report = freeze.verify_bundle(bundle)
        self.assertTrue(report["complete"])
        self.assertTrue(all(item["verified"] for item in report["items"]))
        target_dir = self.root / "receiver-store"
        receiver = RunStore(target_dir, local_short_compute)
        imported = freeze.import_delivery(receiver, bundle)
        self.assertTrue(imported["complete"])
        self.assertEqual([item["key"] for item in imported["imported"]], ["cand-a"])
        again = freeze.import_delivery(receiver, bundle)
        self.assertEqual(again["imported"], [])
        self.assertEqual([item["key"] for item in again["reused"]], ["cand-a"])
        records = list((target_dir / "requests").glob("*.json"))
        self.assertEqual(len(records), 1)
        rerun = receiver.run(make_request({"numbers": self.data}, {"multiplier": 2}))
        self.assertTrue(rerun.reused)
        self.assertEqual(rerun.record_id, self.summary_a.record_id)

    def test_corrupt_shared_dependency_blocks_only_dependents(self):
        (self.summary_b.artifact_dir / "result.json").write_bytes(b"corrupted")
        selections = [
            self.selection("cand-a", self.summary_a),
            self.selection("cand-c", self.summary_a, depends_on=(self.summary_b.request_hash,)),
        ]
        summary = freeze.export_delivery(self.store, selections, self.root / "bundle")
        self.assertFalse(summary["complete"])
        self.assertEqual(summary["delivered"], ["cand-a"])
        self.assertEqual(summary["undelivered"][0]["key"], "cand-c")
        self.assertIn("dependency_unavailable", summary["undelivered"][0]["reason"])

    def test_replaced_input_blocks_only_affected_candidate(self):
        self.data.write_bytes(b"replaced after run")
        selections = [
            self.selection("cand-a", self.summary_a, inputs=(("numbers", self.data),)),
            self.selection("cand-b", self.summary_b),
        ]
        summary = freeze.export_delivery(self.store, selections, self.root / "bundle")
        self.assertEqual(summary["delivered"], ["cand-b"])
        self.assertEqual(summary["undelivered"][0]["reason"], "input_replaced_since_run:numbers")

    def test_in_progress_stage_is_blocked(self):
        staging = self.store_dir / "staging" / self.summary_a.request_hash / "attempt-1"
        staging.mkdir(parents=True)
        (self.store_dir / "requests" / (self.summary_a.request_hash + ".json")).unlink()
        plan = freeze.plan_freeze(self.store, [self.selection("cand-a", self.summary_a)])
        self.assertEqual(plan["items"][0]["status"], freeze.STATUS_BLOCKED)
        self.assertEqual(plan["items"][0]["blocked_reason"], "stage_in_progress")

    def test_partial_parameter_adoption_derives_new_identity(self):
        derived_hash = freeze.derive_request_hash(self.store, self.summary_a.request_hash, {"multiplier": 7})
        selection = freeze.Selection(key="cand-derived", request_hash=derived_hash)
        plan = freeze.plan_freeze(self.store, [selection])
        self.assertEqual(plan["items"][0]["status"], freeze.STATUS_NEEDS_RERUN)
        self.store.run(make_request({"numbers": self.data}, {"multiplier": 7}))
        plan = freeze.plan_freeze(self.store, [selection])
        self.assertEqual(plan["items"][0]["status"], freeze.STATUS_CONFIRMED)

    def test_versions_explain_additions_and_replacements(self):
        freeze.export_delivery(self.store, [self.selection("cand-a", self.summary_a)], self.root / "v1")
        self.data.write_bytes(b"4 5 6\n")
        recomputed = self.store.run(make_request({"numbers": self.data}, {"multiplier": 2}))
        summary = freeze.export_delivery(
            self.store,
            [
                self.selection("cand-a", recomputed, reason="recomputed with new input"),
                self.selection("cand-b", self.summary_b, reason="backfilled"),
            ],
            self.root / "v2",
        )
        self.assertEqual(summary["version"], 2)
        delivery = json.loads((self.root / "v2" / "delivery.json").read_text())
        self.assertEqual(delivery["supersedes"], 1)
        self.assertEqual(delivery["changes"]["added"], ["cand-b"])
        replaced = delivery["changes"]["replaced"]
        self.assertEqual(replaced[0]["key"], "cand-a")
        self.assertEqual(replaced[0]["previous_request_hash"], self.summary_a.request_hash)
        self.assertEqual(replaced[0]["basis"], "recomputed with new input")

    def test_incomplete_bundle_is_not_marked_verified(self):
        bundle = self.root / "bundle"
        freeze.export_delivery(self.store, [self.selection("cand-a", self.summary_a)], bundle)
        artifact = next((bundle / "artifacts").rglob("result.json"))
        artifact.unlink()
        report = freeze.verify_bundle(bundle)
        self.assertFalse(report["complete"])
        receiver = RunStore(self.root / "receiver", local_short_compute)
        imported = freeze.import_delivery(receiver, bundle)
        self.assertEqual(imported["imported"], [])
        self.assertFalse(imported["complete"])

    def test_export_refuses_nonempty_output_directory(self):
        out = self.root / "bundle"
        out.mkdir()
        (out / "stale").write_text("x")
        with self.assertRaises(FileExistsError):
            freeze.export_delivery(self.store, [self.selection("cand-a", self.summary_a)], out)


if __name__ == "__main__":
    unittest.main()
