from __future__ import annotations

import json
import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

from exp_records.backends import local_short_compute
from exp_records.models import ExperimentRequest, InputSpec, Stage, default_environment
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
        evidence = self.store_dir / "evidence" / request.identity()["request_hash"]
        self.assertTrue(any(evidence.rglob("partial.txt")))

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


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store_dir = self.root / "store"
        self.data = self.root / "numbers.txt"
        self.data.write_bytes(b"pipeline input")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_partial_rerun_reuses_verified_stage_sources(self) -> None:
        prepare_calls = []
        render_calls = []

        def prepare(inputs, parameters, dependencies, output_dir):
            prepare_calls.append(inputs["numbers"])
            (output_dir / "prepared.bin").write_bytes(inputs["numbers"] + b"|prepared")
            return {"step": "prepare"}

        def render(inputs, parameters, dependencies, output_dir):
            render_calls.append((inputs["template"], list(dependencies)))
            prepared = (dependencies["prepare"] / "prepared.bin").read_bytes()
            (output_dir / "image.svg").write_text(
                f"<svg>{prepared.decode()} {inputs['template'].decode()}</svg>",
                encoding="utf-8",
            )
            (output_dir / "params.txt").write_text("rendered", encoding="utf-8")
            return {"step": "render"}

        stages = (
            Stage("prepare", prepare, inputs=("numbers",)),
            Stage("render", render, inputs=("template",), depends_on=("prepare",)),
        )
        template_one = self.root / "template-one.txt"
        template_two = self.root / "template-two.txt"
        template_one.write_bytes(b"template one")
        template_two.write_bytes(b"template two")

        first_store = RunStore(self.store_dir, local_short_compute)
        first_request = ExperimentRequest(
            inputs=(
                InputSpec("numbers", self.data),
                InputSpec("template", template_one),
            ),
            environment=default_environment("test/v1"),
        )
        first = first_store.run_pipeline(first_request, stages)

        second_request = ExperimentRequest(
            inputs=(
                InputSpec("numbers", self.data),
                InputSpec("template", template_two),
            ),
            environment=default_environment("test/v1"),
        )
        second = RunStore(self.store_dir, local_short_compute).run_pipeline(
            second_request, stages
        )

        self.assertEqual(1, len(prepare_calls))
        self.assertEqual(2, len(render_calls))
        self.assertNotEqual(first.record_id, second.record_id)
        first_manifest = json.loads(first.record_path.read_text(encoding="utf-8"))
        second_manifest = json.loads(second.record_path.read_text(encoding="utf-8"))
        first_provenance = first_manifest["backend_metadata"]["pipeline"]
        second_provenance = second_manifest["backend_metadata"]["pipeline"]
        self.assertEqual(False, first_provenance[0]["reused"])
        self.assertEqual(True, second_provenance[0]["reused"])
        self.assertEqual(False, second_provenance[1]["reused"])
        self.assertEqual(
            first_provenance[0]["stage_hash"], second_provenance[0]["stage_hash"]
        )
        self.assertNotEqual(
            first_provenance[1]["stage_hash"], second_provenance[1]["stage_hash"]
        )
        copied_stage_record = second.artifact_dir / "stages" / "prepare" / "stage-manifest.json"
        copied = json.loads(copied_stage_record.read_text(encoding="utf-8"))
        self.assertEqual(
            second_provenance[0]["source_stage_record_sha256"],
            hashlib.sha256(copied_stage_record.read_bytes()).hexdigest(),
        )
        self.assertEqual(copied["stage_hash"], second_provenance[0]["stage_hash"])

    def test_failed_stage_leaves_evidence_and_reuses_earlier_stage(self) -> None:
        calls = {"prepare": 0, "render": 0}

        def prepare(inputs, parameters, dependencies, output_dir):
            calls["prepare"] += 1
            (output_dir / "prepared.bin").write_bytes(b"prepared")
            return {}

        def failing_render(inputs, parameters, dependencies, output_dir):
            calls["render"] += 1
            (output_dir / "partial.png").write_bytes(b"partial")
            raise RuntimeError("render failed")

        stages = (
            Stage("prepare", prepare),
            Stage("render", failing_render, depends_on=("prepare",)),
        )
        request = request_from_files({"numbers": self.data})
        store = RunStore(self.store_dir, local_short_compute)
        with self.assertRaises(RuntimeError):
            store.run_pipeline(request, stages)
        with self.assertRaises(RuntimeError):
            store.run_pipeline(request, stages)

        self.assertEqual(1, calls["prepare"])
        self.assertEqual(2, calls["render"])
        evidence_root = self.store_dir / "evidence" / "stages"
        self.assertTrue(any(evidence_root.rglob("partial.png")))
        self.assertFalse(
            list((self.store_dir / "stage_staging").glob("*/**/partial.png"))
        )


if __name__ == "__main__":
    unittest.main()


# --- follow-up: multi-process competition, takeover, publish interruption ---

import multiprocessing
import os
import time

from exp_records.store import RunStore as _RunStore


def _make_request(data_path):
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data_path),),
        parameters={"multiplier": 1},
        environment=default_environment("test/v1"),
    )


def _child_run(store_dir, data_path, result_queue):
    store = RunStore(store_dir, local_short_compute)
    summary = store.run(_make_request(data_path))
    result_queue.put((summary.reused, summary.record_id))


def _child_crash_mid_backend(store_dir, data_path, started_path):
    def backend(inputs, parameters, output_dir):
        (output_dir / "partial.txt").write_bytes(b"partial")
        Path(started_path).write_text("started")
        os._exit(1)  # simulate executor dying mid-run

    store = RunStore(store_dir, backend)
    store.run(_make_request(data_path))


class _CrashAtPublishStore(_RunStore):
    def _publish_record(self, request_hash, manifest):
        os._exit(2)  # simulate crash after artifacts are in place


def _child_crash_at_publish(store_dir, data_path):
    store = _CrashAtPublishStore(store_dir, local_short_compute)
    store.run(_make_request(data_path))


def _read_events(store_dir, request_hash):
    path = store_dir / "attempts" / f"{request_hash}.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class MultiProcessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store_dir = self.root / "store"
        self.data = self.root / "numbers.txt"
        self.data.write_bytes(b"shared bytes")
        self.ctx = multiprocessing.get_context("fork")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_two_competing_processes_single_record(self) -> None:
        queue = self.ctx.Queue()
        procs = [
            self.ctx.Process(target=_child_run, args=(self.store_dir, self.data, queue))
            for _ in range(2)
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(30)
            self.assertEqual(0, proc.exitcode)

        results = sorted(queue.get() for _ in range(2))
        self.assertEqual([(False, results[0][1]), (True, results[0][1])], results)

        request_hash = _make_request(self.data).identity()["request_hash"]
        records = list((self.store_dir / "requests").glob("*.json"))
        self.assertEqual(1, len(records))
        events = _read_events(self.store_dir, request_hash)
        kinds = [event["event"] for event in events]
        self.assertEqual(1, kinds.count("execute_start"))
        self.assertEqual(1, kinds.count("published"))
        self.assertEqual(1, kinds.count("reused"))
        self.assertEqual(2, len({event["executor_id"] for event in events}))

    def test_takeover_after_executor_exit(self) -> None:
        started = self.root / "started.txt"
        proc = self.ctx.Process(
            target=_child_crash_mid_backend, args=(self.store_dir, self.data, started)
        )
        proc.start()
        proc.join(30)
        self.assertEqual(1, proc.exitcode)
        self.assertTrue(started.exists())

        request_hash = _make_request(self.data).identity()["request_hash"]
        # staging leftovers from the dead executor must not be a cache hit
        self.assertFalse((self.store_dir / "requests" / f"{request_hash}.json").exists())

        summary = RunStore(self.store_dir, local_short_compute).run(_make_request(self.data))
        self.assertFalse(summary.reused)
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))
        self.assertEqual([], list((self.store_dir / "staging").iterdir()))

        events = _read_events(self.store_dir, request_hash)
        starts = [event for event in events if event["event"] == "execute_start"]
        self.assertEqual(2, len(starts))
        takeover = starts[1].get("takeover_basis")
        self.assertIsNotNone(takeover)
        self.assertEqual(1, len(takeover["orphaned_attempts"]))
        self.assertIn("stale_staging_dirs", takeover)
        self.assertEqual(1, len([e for e in events if e["event"] == "published"]))

    def test_publish_interruption_leaves_no_trusted_record(self) -> None:
        proc = self.ctx.Process(
            target=_child_crash_at_publish, args=(self.store_dir, self.data)
        )
        proc.start()
        proc.join(30)
        self.assertEqual(2, proc.exitcode)

        request_hash = _make_request(self.data).identity()["request_hash"]
        # artifacts were published but the record was not: nothing to trust
        self.assertTrue((self.store_dir / "artifacts" / request_hash).is_dir())
        self.assertFalse((self.store_dir / "requests" / f"{request_hash}.json").exists())

        summary = RunStore(self.store_dir, local_short_compute).run(_make_request(self.data))
        self.assertFalse(summary.reused)
        records = list((self.store_dir / "requests").glob("*.json"))
        self.assertEqual(1, len(records))
        self.assertTrue(
            any(
                (self.store_dir / "evidence" / request_hash).rglob("summary.txt")
            )
        )

        events = _read_events(self.store_dir, request_hash)
        starts = [event for event in events if event["event"] == "execute_start"]
        takeover = starts[-1].get("takeover_basis")
        self.assertIsNotNone(takeover)
        self.assertTrue(takeover["artifacts_without_record"])

    def test_corrupted_artifact_detected_after_reopen(self) -> None:
        first = RunStore(self.store_dir, local_short_compute).run(_make_request(self.data))
        self.assertFalse(first.reused)

        # corrupt one artifact after the record was published
        artifact = first.artifact_dir / "result.json"
        artifact.write_bytes(b"tampered")

        reopened = RunStore(self.store_dir, local_short_compute).run(_make_request(self.data))
        self.assertFalse(reopened.reused)
        self.assertEqual(1, len(list((self.store_dir / "requests").glob("*.json"))))
        manifest = json.loads(reopened.record_path.read_text(encoding="utf-8"))
        for item in manifest["artifacts"]:
            data = (reopened.artifact_dir / item["path"]).read_bytes()
            import hashlib

            self.assertEqual(item["sha256"], hashlib.sha256(data).hexdigest())

        events = _read_events(self.store_dir, reopened.request_hash)
        starts = [event for event in events if event["event"] == "execute_start"]
        takeover = starts[-1].get("takeover_basis")
        self.assertIsNotNone(takeover)
        self.assertTrue(takeover["record_failed_verification"])

    def test_same_filename_different_content_stays_distinct(self) -> None:
        shared = self.root / "shared.txt"
        shared.write_bytes(b"version one")
        first = RunStore(self.store_dir, local_short_compute).run(_make_request(shared))
        shared.write_bytes(b"version two")
        second = RunStore(self.store_dir, local_short_compute).run(_make_request(shared))
        self.assertNotEqual(first.record_id, second.record_id)
        self.assertFalse(second.reused)
        self.assertEqual(2, len(list((self.store_dir / "requests").glob("*.json"))))
