from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from exp_records.freeze import (
    DELIVERY_CORRUPTED,
    DELIVERY_MISSING,
    FreezeError,
    FreezeSelection,
    freeze,
    import_package,
    load_package_manifest,
    plan_freeze,
)
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore


def shared_backend(inputs, parameters, output_dir):
    (output_dir / "shared.bin").write_bytes(b"shared-intermediate-bytes")
    (output_dir / "unique.json").write_text(
        json.dumps(parameters, sort_keys=True), encoding="utf-8"
    )
    return {"backend": "shared/v1"}


def make_request(data: bytes, parameters: dict) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data=data),),
        parameters=parameters,
        environment=default_environment("test/v1"),
    )


class FreezeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = RunStore(self.root / "store", shared_backend)
        self.req_a = make_request(b"alpha", {"multiplier": 2, "tag": "a"})
        self.req_b = make_request(b"beta", {"multiplier": 3, "tag": "b"})
        self.req_c = make_request(b"gamma", {"multiplier": 4, "tag": "c"})
        self.sum_a = self.store.run(self.req_a)
        self.sum_b = self.store.run(self.req_b)
        self.sum_c = self.store.run(self.req_c)
        self.out = self.root / "packages"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_plan_classifies_confirmed_and_missing(self) -> None:
        report = plan_freeze(
            self.store,
            [
                FreezeSelection(self.sum_a.request_hash, reason="adopted"),
                FreezeSelection("0" * 64, reason="never ran"),
            ],
        )
        statuses = {item["request_hash"]: item["status"] for item in report["items"]}
        self.assertEqual(statuses[self.sum_a.request_hash], "confirmed")
        self.assertEqual(statuses["0" * 64], "needs_redo")
        self.assertFalse(report["freezable"])

    def test_plan_marks_unproven_artifacts_without_record(self) -> None:
        ghost = "1" * 64
        (self.store.root / "artifacts" / ghost).mkdir(parents=True)
        report = plan_freeze(self.store, [FreezeSelection(ghost)])
        self.assertEqual(report["items"][0]["status"], "unproven")

    def test_plan_detects_replaced_input(self) -> None:
        report = plan_freeze(
            self.store,
            [FreezeSelection(self.sum_a.request_hash, expected_inputs={"numbers": "0" * 64})],
        )
        item = report["items"][0]
        self.assertEqual(item["status"], "needs_redo")
        self.assertIn("replaced", item["detail"])

    def test_corrupted_shared_artifact_blocks_only_its_candidate(self) -> None:
        shared = self.store.root / "artifacts" / self.sum_a.request_hash / "shared.bin"
        shared.write_bytes(b"corrupted")
        report = plan_freeze(
            self.store,
            [FreezeSelection(self.sum_a.request_hash), FreezeSelection(self.sum_b.request_hash)],
        )
        statuses = {item["request_hash"]: item["status"] for item in report["items"]}
        self.assertEqual(statuses[self.sum_a.request_hash], "needs_redo")
        self.assertEqual(statuses[self.sum_b.request_hash], "confirmed")
        with self.assertRaises(FreezeError):
            freeze(self.store, [FreezeSelection(self.sum_a.request_hash)], self.out)
        # The healthy candidate still freezes on its own.
        report_b = freeze(self.store, [FreezeSelection(self.sum_b.request_hash)], self.out)
        self.assertTrue(Path(report_b["path"]).is_dir())

    def test_freeze_dedupes_shared_artifacts_and_records_reasons(self) -> None:
        report = freeze(
            self.store,
            [
                FreezeSelection(self.sum_a.request_hash, reason="baseline"),
                FreezeSelection(self.sum_b.request_hash, adopted_parameters=("multiplier",), reason="only multiplier"),
            ],
            self.out,
        )
        package_dir = Path(report["path"])
        manifest = load_package_manifest(package_dir)
        # shared.bin is identical across both records and stored once.
        blob_count = len(list((package_dir / "artifacts").iterdir()))
        self.assertEqual(blob_count, 3)
        self.assertEqual(len(manifest["artifacts"]), 3)
        selections = {s["request_hash"]: s for s in manifest["selections"]}
        self.assertEqual(selections[self.sum_a.request_hash]["reason"], "baseline")
        self.assertEqual(
            selections[self.sum_b.request_hash]["adopted_parameters"], ["multiplier"]
        )
        self.assertEqual(
            selections[self.sum_b.request_hash]["adopted_parameter_values"],
            {"multiplier": 3},
        )

    def test_freeze_is_immutable(self) -> None:
        selection = FreezeSelection(self.sum_a.request_hash)
        first = freeze(self.store, [selection], self.out)
        with self.assertRaises(FreezeError):
            freeze(self.store, [selection], self.out)
        self.assertTrue((Path(first["path"]) / "freeze-manifest.json").is_file())

    def test_versioned_freeze_reports_changes(self) -> None:
        first = freeze(
            self.store,
            [FreezeSelection(self.sum_a.request_hash, reason="v1")],
            self.out,
        )
        second = freeze(
            self.store,
            [
                FreezeSelection(self.sum_a.request_hash, adopted_parameters=("multiplier",), reason="narrowed"),
                FreezeSelection(self.sum_b.request_hash, reason="added later"),
            ],
            self.out,
            supersedes=first["path"],
        )
        manifest = load_package_manifest(second["path"])
        self.assertEqual(manifest["version"], 2)
        self.assertEqual(manifest["supersedes"], first["package_id"])
        self.assertEqual(manifest["changes"]["added"], [self.sum_b.request_hash])
        self.assertEqual(len(manifest["changes"]["replaced"]), 1)
        self.assertEqual(manifest["changes"]["replaced"][0]["request_hash"], self.sum_a.request_hash)
        # The original package stays untouched.
        self.assertEqual(load_package_manifest(first["path"])["version"], 1)

    def test_import_verifies_and_traces_without_original_paths(self) -> None:
        report = freeze(
            self.store,
            [FreezeSelection(self.sum_a.request_hash, reason="handoff")],
            self.out,
        )
        receiver = RunStore(self.root / "receiver", shared_backend)
        result = import_package(receiver, report["path"])
        self.assertTrue(result["verified_complete"])
        self.assertEqual(result["records"][0]["status"], "installed")
        # The imported record satisfies the original request without recomputing.
        summary = receiver.run(self.req_a)
        self.assertTrue(summary.reused)
        self.assertEqual(summary.request_hash, self.sum_a.request_hash)

    def test_import_is_idempotent(self) -> None:
        report = freeze(self.store, [FreezeSelection(self.sum_a.request_hash)], self.out)
        receiver = RunStore(self.root / "receiver", shared_backend)
        first = import_package(receiver, report["path"])
        second = import_package(receiver, report["path"])
        self.assertTrue(first["verified_complete"])
        self.assertTrue(second["verified_complete"])
        self.assertEqual(second["records"][0]["status"], "already_present")
        records = list((self.root / "receiver" / "requests").glob("*.json"))
        self.assertEqual(len(records), 1)

    def test_incomplete_package_is_not_verified(self) -> None:
        report = freeze(self.store, [FreezeSelection(self.sum_a.request_hash)], self.out)
        package_dir = Path(report["path"])
        blobs = list((package_dir / "artifacts").iterdir())
        blobs[0].unlink()
        receiver = RunStore(self.root / "receiver", shared_backend)
        result = import_package(receiver, package_dir)
        self.assertFalse(result["verified_complete"])
        failed = [item for item in result["artifacts"] if item["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        # Nothing was installed from the incomplete package.
        self.assertFalse(list((self.root / "receiver" / "requests").glob("*.json")))

    def test_tampered_record_is_rejected(self) -> None:
        report = freeze(self.store, [FreezeSelection(self.sum_a.request_hash)], self.out)
        package_dir = Path(report["path"])
        record_path = package_dir / "records" / (self.sum_a.request_hash + ".json")
        record = json.loads(record_path.read_text())
        record["request"]["parameters"]["multiplier"] = 99
        record_path.write_text(json.dumps(record))
        receiver = RunStore(self.root / "receiver", shared_backend)
        result = import_package(receiver, package_dir)
        self.assertFalse(result["verified_complete"])
        self.assertEqual(result["records"][0]["status"], "failed")

    def test_partial_delivery_then_supplement_repairs_and_reports_conflict(self) -> None:
        selections = [
            FreezeSelection(self.sum_a.request_hash, reason="candidate a"),
            FreezeSelection(self.sum_b.request_hash, reason="overlapping healthy candidate"),
            FreezeSelection(self.sum_c.request_hash, reason="candidate c"),
        ]

        shared = self.store.root / "artifacts" / self.sum_a.request_hash / "shared.bin"
        shared.write_bytes(b"corrupted shared dependency")
        unique_c = self.store.root / "artifacts" / self.sum_c.request_hash / "unique.json"
        unique_c.unlink()

        first_report = freeze(self.store, selections, self.out)
        first_package = Path(first_report["path"])
        first_manifest_bytes = (first_package / "freeze-manifest.json").read_bytes()
        first_manifest = json.loads(first_manifest_bytes)
        first_status = {
            entry["request_hash"]: entry["status"] for entry in first_manifest["candidates"]
        }
        self.assertEqual(first_status[self.sum_a.request_hash], DELIVERY_CORRUPTED)
        self.assertEqual(first_status[self.sum_b.request_hash], "complete")
        self.assertEqual(first_status[self.sum_c.request_hash], DELIVERY_MISSING)
        self.assertEqual(first_manifest["records"], [self.sum_b.request_hash])

        receiver = RunStore(self.root / "receiver", shared_backend)
        first_import = import_package(receiver, first_package)
        first_record_status = {item["request_hash"]: item["status"] for item in first_import["records"]}
        self.assertEqual(first_record_status[self.sum_a.request_hash], DELIVERY_CORRUPTED)
        self.assertEqual(first_record_status[self.sum_b.request_hash], "installed")
        self.assertEqual(first_record_status[self.sum_c.request_hash], DELIVERY_MISSING)
        self.assertFalse(first_import["verified_complete"])
        self.assertTrue(receiver.run(self.req_b).reused)
        self.assertEqual(1, len(list((receiver.root / "requests").glob("*.json"))))

        shared.write_bytes(b"shared-intermediate-bytes")
        repaired_dir = self.root / "repair"
        repaired_dir.mkdir()
        metadata = shared_backend({"numbers": b"gamma"}, {"multiplier": 4, "tag": "c"}, repaired_dir)
        self.assertEqual({"backend": "shared/v1"}, metadata)
        shutil.copyfile(repaired_dir / "unique.json", unique_c)

        # A replacement computation on the sending side produces an internally
        # valid but different overlapping candidate version.
        conflict_data = b"different but verifiable overlapping result"
        conflict_sha = hashlib.sha256(conflict_data).hexdigest()
        b_artifact_dir = self.store.root / "artifacts" / self.sum_b.request_hash
        (b_artifact_dir / "alternative.bin").write_bytes(conflict_data)
        record_b_path = self.store.root / "requests" / (self.sum_b.request_hash + ".json")
        record_b = json.loads(record_b_path.read_text())
        record_b["artifacts"] = [
            {"path": "alternative.bin", "size": len(conflict_data), "sha256": conflict_sha}
        ]
        record_b_path.write_text(json.dumps(record_b), encoding="utf-8")

        second_report = freeze(self.store, selections, self.out, supersedes=first_package)
        second_package = Path(second_report["path"])

        # Source-machine artifacts are no longer needed once published.
        shutil.rmtree(self.store.root)

        second_import = import_package(receiver, second_package)
        second_record_status = {
            item["request_hash"]: item["status"] for item in second_import["records"]
        }
        self.assertEqual(second_record_status[self.sum_a.request_hash], "installed")
        self.assertEqual(second_record_status[self.sum_b.request_hash], "conflict")
        self.assertEqual(second_record_status[self.sum_c.request_hash], "installed")
        self.assertFalse(second_import["verified_complete"])

        self.assertTrue(receiver.run(self.req_a).reused)
        self.assertTrue(receiver.run(self.req_b).reused)
        self.assertTrue(receiver.run(self.req_c).reused)
        installed_record = json.loads(
            (receiver.root / "requests" / (self.sum_b.request_hash + ".json")).read_text()
        )
        self.assertEqual({item["path"] for item in installed_record["artifacts"]}, {"shared.bin", "unique.json"})
        self.assertEqual(3, len(list((receiver.root / "requests").glob("*.json"))))

        self.assertEqual(first_manifest_bytes, (first_package / "freeze-manifest.json").read_bytes())
        audit_lines = [
            json.loads(line)
            for line in (receiver.root / "delivery-imports.jsonl").read_text().splitlines()
            if line.strip()
        ]
        self.assertEqual(2, len(audit_lines))
        self.assertEqual(first_manifest["package_id"], audit_lines[0]["package_id"])
        conflict_audit = next(
            item
            for item in audit_lines[1]["candidates"]
            if item["request_hash"] == self.sum_b.request_hash
        )
        self.assertEqual("conflict", conflict_audit["status"])
        self.assertEqual("alternative.bin", conflict_audit["incoming_artifacts"][0]["path"])


if __name__ == "__main__":
    unittest.main()
