from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import time
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

    def test_same_filename_but_changed_content_has_independent_identity(self) -> None:
        first_dir = self.root / "first"
        second_dir = self.root / "second"
        first_dir.mkdir()
        second_dir.mkdir()
        first_path = first_dir / "input.txt"
        second_path = second_dir / "input.txt"
        first_path.write_bytes(b"first content")
        second_path.write_bytes(b"second content")

        first = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": first_path})
        )
        second = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": second_path})
        )

        self.assertNotEqual(first.record_id, second.record_id)
        self.assertFalse(second.reused)

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

    def test_process_exit_is_taken_over_without_reusing_active_staging(self) -> None:
        helper = self.root / "worker.py"
        helper.write_text(
            """
import os
import sys
import time
from pathlib import Path

from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore

store_dir, input_path, role = sys.argv[1:4]
started = Path(store_dir) / f"{role}-started"
stop = Path(store_dir) / "stop"

def backend(inputs, parameters, output_dir):
    (output_dir / "partial.txt").write_bytes(b"partial")
    started.write_text(str(os.getpid()), encoding="utf-8")
    if role == "first":
        while not stop.exists():
            time.sleep(0.02)
        os._exit(0)
    from exp_records.backends import local_short_compute
    return local_short_compute(inputs, parameters, output_dir)

request = ExperimentRequest(
    inputs=(InputSpec("numbers", Path(input_path)),),
    parameters={},
    environment=default_environment("test/v1"),
)
summary = RunStore(store_dir, backend).run(request)
print(summary.reused)
""",
            encoding="utf-8",
        )
        environment = dict(os.environ, PYTHONPATH=str(Path.cwd() / "src"))
        first = subprocess.Popen(
            [sys.executable, str(helper), str(self.store_dir), str(self.data_one), "first"],
            env=environment,
            stdout=subprocess.PIPE,
            text=True,
        )
        self._wait_for_marker(self.store_dir / "first-started")
        second = subprocess.Popen(
            [sys.executable, str(helper), str(self.store_dir), str(self.data_one), "second"],
            env=environment,
            stdout=subprocess.PIPE,
            text=True,
        )
        time.sleep(0.2)
        (self.store_dir / "stop").write_text("exit", encoding="utf-8")
        first_stdout, _ = first.communicate(timeout=2)
        self.assertEqual("", first_stdout)
        second_stdout, _ = second.communicate(timeout=5)

        self.assertEqual(0, second.returncode)
        self.assertEqual("False\n", second_stdout)
        request_hash = request_from_files({"numbers": self.data_one}).identity()["request_hash"]
        attempts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted((self.store_dir / "attempts").glob("*/attempt.json"))
        ]
        attempts.sort(key=lambda item: item["sequence"])
        self.assertEqual(["abandoned", "succeeded"], [item["state"] for item in attempts])
        self.assertEqual(attempts[0]["attempt_id"], attempts[1]["predecessor_attempt_ids"][0])
        self.assertEqual(attempts[1]["attempt_id"], attempts[0]["taken_over_by"])
        self.assertTrue(
            (self.store_dir / "attempts" / attempts[0]["attempt_id"] / "staging" / "artifacts" / "partial.txt").exists()
        )
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))
        self.assertEqual([request_hash], [path.name for path in (self.store_dir / "artifacts").iterdir()])
        self.assertFalse(any((self.store_dir / "staging").iterdir()))

    def test_publish_interruption_is_quarantined_before_takeover(self) -> None:
        request = request_from_files({"numbers": self.data_one})
        store = RunStore(self.store_dir, local_short_compute)

        def fail_record_publish(request_hash, manifest):
            raise RuntimeError("interrupted during record publish")

        store._publish_record = fail_record_publish
        with self.assertRaises(RuntimeError):
            store.run(request)

        recovered = RunStore(self.store_dir, local_short_compute).run(request)
        self.assertFalse(recovered.reused)
        attempts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted((self.store_dir / "attempts").glob("*/attempt.json"))
        ]
        attempts.sort(key=lambda item: item["sequence"])
        self.assertEqual(["failed", "succeeded"], [item["state"] for item in attempts])
        self.assertTrue(
            (self.store_dir / "attempts" / attempts[1]["attempt_id"] / "unpublished-artifacts").is_dir()
        )
        reason = (
            self.store_dir
            / "attempts"
            / attempts[1]["attempt_id"]
            / "takeover-reason.txt"
        ).read_text(encoding="utf-8")
        self.assertIn("record was not atomically published", reason)
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))
        self.assertEqual(1, len(list((self.store_dir / "artifacts").iterdir())))

    def test_reopened_store_detects_corrupt_artifact_and_recomputes(self) -> None:
        first = RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        )
        (first.artifact_dir / "result.json").write_bytes(b"corrupt")

        reopened = RunStore(self.store_dir, local_short_compute)
        self.assertIsNone(reopened._load_complete(first.request_hash))
        recovered = reopened.run(request_from_files({"numbers": self.data_one}))

        self.assertFalse(recovered.reused)
        self.assertTrue(recovered.record_path.is_file())
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))
        attempts = list((self.store_dir / "attempts").glob("*/attempt.json"))
        self.assertEqual(2, len(attempts))
        quarantined = list((self.store_dir / "attempts").glob("*/invalid-artifacts"))
        self.assertEqual(1, len(quarantined))
        self.assertEqual(first.record_path, recovered.record_path)
        self.assertTrue(RunStore(self.store_dir, local_short_compute).run(
            request_from_files({"numbers": self.data_one})
        ).reused)

    @staticmethod
    def _wait_for_marker(path: Path) -> None:
        deadline = time.time() + 5
        while time.time() < deadline and not path.exists():
            time.sleep(0.02)
        if not path.exists():
            raise AssertionError(f"worker did not start: {path}")


if __name__ == "__main__":
    unittest.main()
