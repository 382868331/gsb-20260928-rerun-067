from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from exp_records.freeze import (
    FreezeSelection,
    freeze,
    import_package,
    load_package_manifest,
)
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore

SHARED_BYTES = b"shared-dependency-bytes"


def partial_backend(inputs, parameters, output_dir):
    if parameters.get("group") == "shared":
        (output_dir / "shared.bin").write_bytes(SHARED_BYTES)
    (output_dir / ("unique-" + parameters["tag"] + ".json")).write_text(
        json.dumps(parameters, sort_keys=True), encoding="utf-8"
    )
    return {"backend": "partial/v1"}


def conflicting_backend(inputs, parameters, output_dir):
    if parameters.get("group") == "shared":
        (output_dir / "shared.bin").write_bytes(b"tampered-shared-bytes")
    (output_dir / ("unique-" + parameters["tag"] + ".json")).write_text(
        json.dumps(parameters, sort_keys=True), encoding="utf-8"
    )
    return {"backend": "partial/v1"}


def make_request(tag: str, group: str) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data=tag.encode("utf-8")),),
        parameters={"tag": tag, "group": group},
        environment=default_environment("test/v1"),
    )


def package_blob_dir(package_path: str) -> Path:
    return Path(package_path) / "artifacts"


def manifest_digest(package_path: str) -> str:
    return hashlib.sha256(
        (Path(package_path) / "freeze-manifest.json").read_bytes()
    ).hexdigest()


class PartialDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = RunStore(self.root / "origin-store", partial_backend)
        self.req_a = make_request("a", "shared")
        self.req_b = make_request("b", "shared")
        self.req_c = make_request("c", "solo")
        self.sum_a = self.store.run(self.req_a)
        self.sum_b = self.store.run(self.req_b)
        self.sum_c = self.store.run(self.req_c)
        self.out = self.root / "packages"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def selections(self):
        return [
            FreezeSelection(self.sum_a.request_hash, reason="candidate a"),
            FreezeSelection(self.sum_b.request_hash, reason="candidate b"),
            FreezeSelection(self.sum_c.request_hash, reason="candidate c"),
        ]

    def freeze_all(self, **kwargs) -> dict:
        return freeze(self.store, self.selections(), self.out, **kwargs)

    def candidate_states(self, report) -> dict:
        return {entry["request_hash"]: entry["state"] for entry in report["candidates"]}

    def test_shared_dependency_corruption_damages_only_its_consumers(self) -> None:
        package = self.freeze_all()
        shared_sha = hashlib.sha256(SHARED_BYTES).hexdigest()
        blob = package_blob_dir(package["path"]) / shared_sha
        self.assertTrue(blob.is_file())
        blob.write_bytes(b"corrupted-shared-bytes")

        receiver = RunStore(self.root / "receiver", partial_backend)
        report = import_package(receiver, package["path"])

        self.assertFalse(report["verified_complete"])
        states = self.candidate_states(report)
        self.assertEqual(states[self.sum_a.request_hash], "damaged")
        self.assertEqual(states[self.sum_b.request_hash], "damaged")
        self.assertEqual(states[self.sum_c.request_hash], "complete")
        artifact_states = {item["sha256"]: item["status"] for item in report["artifacts"]}
        self.assertEqual(artifact_states[shared_sha], "damaged")

        # The healthy candidate is importable and reusable despite the damage.
        self.assertTrue(receiver.run(self.req_c).reused)
        # Damaged candidates never masquerade as reusable success records.
        self.assertFalse(receiver.run(self.req_a).reused)
        self.assertFalse(receiver.run(self.req_b).reused)

    def test_single_candidate_missing_file_leaves_others_importable(self) -> None:
        package = self.freeze_all()
        record_b = json.loads(
            (Path(package["path"]) / "records" / (self.sum_b.request_hash + ".json")).read_bytes()
        )
        unique_b = next(
            entry for entry in record_b["artifacts"] if entry["path"].startswith("unique-")
        )
        (package_blob_dir(package["path"]) / unique_b["sha256"]).unlink()

        receiver = RunStore(self.root / "receiver", partial_backend)
        report = import_package(receiver, package["path"])

        self.assertFalse(report["verified_complete"])
        states = self.candidate_states(report)
        self.assertEqual(states[self.sum_a.request_hash], "complete")
        self.assertEqual(states[self.sum_b.request_hash], "missing")
        self.assertEqual(states[self.sum_c.request_hash], "complete")
        self.assertTrue(receiver.run(self.req_a).reused)
        self.assertTrue(receiver.run(self.req_c).reused)
        self.assertFalse(receiver.run(self.req_b).reused)

    def test_partial_freeze_marks_undeliverable_candidates(self) -> None:
        ghost = "2" * 64
        selections = self.selections() + [FreezeSelection(ghost, reason="never ran")]
        report = freeze(self.store, selections, self.out, allow_partial=True)
        self.assertEqual(len(report["frozen"]), 3)
        self.assertEqual(len(report["undeliverable"]), 1)
        self.assertEqual(report["undeliverable"][0]["state"], "missing")
        self.assertEqual(report["undeliverable"][0]["request_hash"], ghost)

        receiver = RunStore(self.root / "receiver", partial_backend)
        result = import_package(receiver, report["path"])
        self.assertFalse(result["verified_complete"])
        states = self.candidate_states(result)
        self.assertEqual(states[ghost], "missing")
        self.assertEqual(states[self.sum_a.request_hash], "complete")

    def test_supplement_fills_gaps_and_original_store_may_disappear(self) -> None:
        partial_package = self.freeze_all()
        shared_sha = hashlib.sha256(SHARED_BYTES).hexdigest()
        (package_blob_dir(partial_package["path"]) / shared_sha).write_bytes(b"corrupted")
        before_digest = manifest_digest(partial_package["path"])

        receiver = RunStore(self.root / "receiver", partial_backend)
        first = import_package(receiver, partial_package["path"])
        self.assertEqual(
            self.candidate_states(first)[self.sum_a.request_hash], "damaged"
        )

        supplement = freeze(
            self.store,
            self.selections(),
            self.out,
            supersedes=partial_package["path"],
        )
        manifest = load_package_manifest(supplement["path"])
        self.assertEqual(manifest["version"], 2)
        self.assertEqual(manifest["supersedes"], partial_package["package_id"])

        second = import_package(receiver, supplement["path"])
        self.assertTrue(second["verified_complete"])
        states = self.candidate_states(second)
        self.assertEqual(states[self.sum_a.request_hash], "complete")
        self.assertEqual(states[self.sum_b.request_hash], "complete")
        record_states = {item["request_hash"]: item["status"] for item in second["records"]}
        # Overlapping candidates stay unique; gaps are installed exactly once.
        self.assertEqual(record_states[self.sum_a.request_hash], "installed")
        self.assertEqual(record_states[self.sum_b.request_hash], "installed")
        self.assertEqual(record_states[self.sum_c.request_hash], "already_present")
        records = list((self.root / "receiver" / "requests").glob("*.json"))
        self.assertEqual(len(records), 3)

        # The original package is an immutable delivery record.
        self.assertEqual(manifest_digest(partial_package["path"]), before_digest)

        # Reuse only needs complete dependencies; the origin store may be gone.
        shutil.rmtree(self.store.root)
        for request in (self.req_a, self.req_b, self.req_c):
            self.assertTrue(receiver.run(request).reused)

    def test_conflicting_supplement_never_overwrites_used_version(self) -> None:
        package = self.freeze_all()
        receiver = RunStore(self.root / "receiver", partial_backend)
        import_package(receiver, package["path"])
        local_record_path = (
            self.root / "receiver" / "requests" / (self.sum_a.request_hash + ".json")
        )
        local_digest = hashlib.sha256(local_record_path.read_bytes()).hexdigest()

        other_store = RunStore(self.root / "other-store", conflicting_backend)
        other_store.run(self.req_a)
        conflict_package = freeze(
            other_store,
            [FreezeSelection(self.sum_a.request_hash, reason="recomputed elsewhere")],
            self.root / "other-packages",
        )
        report = import_package(receiver, conflict_package["path"])

        self.assertFalse(report["verified_complete"])
        states = self.candidate_states(report)
        self.assertEqual(states[self.sum_a.request_hash], "conflict")
        record_item = report["records"][0]
        self.assertEqual(record_item["status"], "conflict")
        self.assertIn("not overwritten", record_item["detail"])
        self.assertNotEqual(
            record_item["local_artifacts"], record_item["incoming_artifacts"]
        )
        # The version already in use is left untouched and still reusable.
        self.assertEqual(
            hashlib.sha256(local_record_path.read_bytes()).hexdigest(), local_digest
        )
        self.assertTrue(receiver.run(self.req_a).reused)

        # Re-importing the identical package stays idempotent and unique.
        again = import_package(receiver, package["path"])
        self.assertEqual(again["records"][0]["status"], "already_present")
        records = list((self.root / "receiver" / "requests").glob("*.json"))
        self.assertEqual(len(records), 3)


if __name__ == "__main__":
    unittest.main()

