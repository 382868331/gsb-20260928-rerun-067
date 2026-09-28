from __future__ import annotations

import multiprocessing
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from experiment_repro.api import ExperimentRunner
from experiment_repro.backends import BackendResult, LocalBackend, Task
from experiment_repro.inputs import FileInput
from experiment_repro.storage import Storage


class ExperimentRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(self.id().replace(".", "-"))
        if self.tmp.exists():
            import shutil

            shutil.rmtree(self.tmp)
        self.tmp.mkdir()
        self.store_path = self.tmp / "store"

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def runner(self) -> ExperimentRunner:
        return ExperimentRunner(self.store_path, backend=DeterministicEnvironmentBackend())

    def test_same_content_is_reused_and_changed_input_executes(self) -> None:
        first = self.runner().run("sum", [1, 2], {"scale": 1})
        second = ExperimentRunner(self.store_path, backend=DeterministicEnvironmentBackend()).run(
            "sum", [1, 2], {"scale": 1}
        )
        changed = self.runner().run("sum", [1, 3], {"scale": 1})

        self.assertFalse(first.reused)
        self.assertTrue(second.reused)
        self.assertEqual(first.record_id, second.record_id)
        self.assertNotEqual(first.record_id, changed.record_id)
        self.assertIn("result.txt", second.record["artifacts"])
        self.assertTrue(self.store_path.joinpath("artifacts", first.record_id, "result.txt").is_file())

    def test_file_name_does_not_determine_identity(self) -> None:
        one = self.tmp / "one.json"
        two = self.tmp / "totally-different-name.json"
        one.write_text('{"value": "same"}', encoding="utf-8")
        two.write_text('{"value": "same"}', encoding="utf-8")

        first = self.runner().run("checksum", one.read_text(), {})
        second = self.runner().run("checksum", two.read_text(), {})

        self.assertEqual(first.record_id, second.record_id)
        self.assertTrue(second.reused)

    def test_file_identity_uses_bytes_not_path(self) -> None:
        original = self.tmp / "original.bin"
        renamed = self.tmp / "renamed.bin"
        changed = self.tmp / "changed.bin"
        original.write_bytes(b"same bytes")
        renamed.write_bytes(b"same bytes")
        changed.write_bytes(b"different bytes")

        first = self.runner().run("file-checksum", FileInput.from_path(original), {})
        second = self.runner().run("file-checksum", FileInput.from_path(renamed), {})
        third = self.runner().run("file-checksum", FileInput.from_path(changed), {})

        self.assertEqual(first.record_id, second.record_id)
        self.assertTrue(second.reused)
        self.assertNotEqual(first.record_id, third.record_id)
        self.assertNotIn(str(original), first.record["experiment"]["inputs"])
        self.assertEqual(first.record["experiment"]["inputs"]["sha256"], second.record["experiment"]["inputs"]["sha256"])

    def test_failed_run_is_not_reusable(self) -> None:
        bad_backend = DeterministicEnvironmentBackend()
        bad_backend.register("always-fail", failing_handler)
        runner = ExperimentRunner(self.store_path, backend=bad_backend)

        failure_one = runner.run("always-fail", None, {})
        failure_two = runner.run("always-fail", None, {})

        self.assertEqual((failure_one.record["status"], failure_two.record["status"]), ("failed", "failed"))
        self.assertFalse(failure_one.reused)
        self.assertFalse(failure_two.reused)
        self.assertNotEqual(failure_one.record_id, failure_two.record_id)
        self.assertEqual(Storage(self.store_path).read_index(), {})

    def test_interrupted_staging_does_not_hide_complete_record(self) -> None:
        storage = Storage(self.store_path)
        fingerprint = "0" * 64
        staging = storage.tmp_dir / "run-abandoned"
        staging.mkdir()
        (staging / "partial.txt").write_text("partial", encoding="utf-8")
        (storage.locks_dir / f"{fingerprint}.lock").write_text("", encoding="utf-8")

        result = self.runner().run("sum", [4], {})
        restarted = ExperimentRunner(self.store_path, backend=DeterministicEnvironmentBackend()).run(
            "sum", [4], {}
        )

        self.assertTrue(result.record_path.is_file())
        self.assertTrue(restarted.reused)
        self.assertEqual(restarted.record_id, result.record_id)
        self.assertIsNone(storage.successful_run_id(fingerprint))

    def test_missing_index_recovers_from_complete_record(self) -> None:
        result = self.runner().run("sum", [7], {})
        storage = Storage(self.store_path)
        storage.index_path.unlink()

        recovered = self.runner().run("sum", [7], {})

        self.assertTrue(recovered.reused)
        self.assertEqual(recovered.record_id, result.record_id)
        self.assertTrue(storage.index_path.is_file())

    def test_concurrent_identical_requests_publish_one_artifact(self) -> None:
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        gate = self.tmp / "gate"
        processes = [
            ctx.Process(target=_concurrent_worker, args=(str(self.store_path), str(gate), queue))
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        time.sleep(0.4)
        gate.write_text("go", encoding="utf-8")
        for process in processes:
            process.join(10)
            self.assertEqual(process.exitcode, 0)

        results = [queue.get() for _ in processes]
        self.assertEqual({item[0] for item in results}, {"succeeded"})
        self.assertEqual({item[1] for item in results}, {results[0][1]})
        self.assertEqual(len(list((self.store_path / "artifacts").iterdir())), 1)
        self.assertEqual(len(list((self.store_path / "runs").glob("*.json"))), 1)


class DeterministicEnvironmentBackend(LocalBackend):
    def identity(self):
        identity = super().identity()
        identity["test"] = True
        return identity


def failing_handler(task: Task) -> BackendResult:
    raise RuntimeError("intentional failure")


def gated_handler(task: Task) -> BackendResult:
    gate = Path(task.parameters["gate"])
    deadline = time.time() + 10
    while not gate.exists() and time.time() < deadline:
        time.sleep(0.02)
    path = task.work_dir / "result.txt"
    path.write_text("concurrent", encoding="utf-8")
    return BackendResult(outputs={"ok": True}, artifacts={"result.txt": path})


def _concurrent_worker(store: str, gate: str, queue: multiprocessing.Queue) -> None:
    backend = DeterministicEnvironmentBackend()
    backend.register("gated", gated_handler)
    result = ExperimentRunner(store, backend=backend).run("gated", "same", {"gate": gate})
    queue.put((result.record["status"], result.record_id))


if __name__ == "__main__":
    unittest.main()
