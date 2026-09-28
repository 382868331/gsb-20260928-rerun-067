from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from exp_records.backends import local_short_compute
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore


def request_from_files(files: dict[str, Path], parameters: dict | None = None) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=tuple(InputSpec(name, path) for name, path in files.items()),
        parameters=parameters or {},
        environment=default_environment("test/v1"),
    )


class RunStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store_dir = self.root / "store"
        self.data_one = self.root / "one.txt"
        self.data_two = self.root / "two.txt"
        self.data_one.write_bytes(b"same bytes")
        self.data_two.write_bytes(b"same bytes")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reuses_same_content_from_different_filename(self) -> None:
        first = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        )
        second = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_two})
        )

        self.assertFalse(first.reused)
        self.assertTrue(second.reused)
        self.assertEqual(first.record_id, second.record_id)

    def test_changed_input_reruns_and_creates_new_record(self) -> None:
        changed = self.root / "changed.txt"
        changed.write_bytes(b"different bytes")

        first = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        )
        second = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": changed})
        )

        self.assertNotEqual(first.record_id, second.record_id)
        self.assertFalse(second.reused)
        self.assertTrue(first.record_path.exists())
        self.assertTrue(second.record_path.exists())

    def test_changed_parameter_reruns(self) -> None:
        first = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one}, {"multiplier": 1})
        )
        second = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one}, {"multiplier": 2})
        )

        self.assertNotEqual(first.record_id, second.record_id)

    def test_failed_run_is_not_reusable(self) -> None:
        calls = 0

        def failing_backend(inputs, parameters, output_dir):
            nonlocal calls
            calls += 1
            (output_dir / "partial.txt").write_bytes(b"partial")
            raise RuntimeError("boom")

        request = request_from_files({"numbers": self.data_one})
        with self.assertRaises(RuntimeError):
            RunStore(self.store_dir, failing_backend).run(request)
        with self.assertRaises(RuntimeError):
            RunStore(self.store_dir, failing_backend).run(request)

        self.assertEqual(calls, 2)
        self.assertEqual([], list((self.store_dir / "requests").glob("*.json")))
        self.assertFalse((self.store_dir / "artifacts" / request.identity()["request_hash"]).exists())
        self.assertEqual([], list((self.store_dir / "staging").iterdir()))

    def test_failed_rewrite_preserves_previous_complete_record(self) -> None:
        complete = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        )
        interrupted_input = self.root / "interrupted.txt"
        interrupted_input.write_bytes(b"interrupted input")

        def once_failing_backend(inputs, parameters, output_dir):
            (output_dir / "partial.txt").write_bytes(b"partial")
            raise RuntimeError("interrupted")

        with self.assertRaises(RuntimeError):
            RunStore(self.store_dir, once_failing_backend).run(
                request_from_files({"numbers": interrupted_input})
            )

        recovered = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        )
        self.assertTrue(recovered.reused)
        self.assertEqual(complete.record_path, recovered.record_path)
        manifest = json.loads(recovered.record_path.read_text(encoding="utf-8"))
        self.assertEqual(2, len(manifest["artifacts"]))

    def test_concurrent_same_request_publishes_single_computation(self) -> None:
        started = threading.Event()
        finish = threading.Event()
        calls = 0

        def slow_backend(inputs, parameters, output_dir):
            nonlocal calls
            calls += 1
            started.set()
            finish.wait(2)
            return local_short_compute(inputs, parameters, output_dir)

        request = request_from_files({"numbers": self.data_one})
        store = RunStore(self.store_dir, slow_backend)
        results = []

        def worker() -> None:
            results.append(store.run(request))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        self.assertTrue(started.wait(2))
        finish.set()
        for thread in threads:
            thread.join(2)

        self.assertEqual(2, len(results))
        self.assertEqual(1, calls)
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))


if __name__ == "__main__":
    unittest.main()
