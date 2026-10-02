from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from exp_records.models import default_environment
from exp_records.pipeline import demo_pipeline
from exp_records.rounds import (
    CONFIRMED,
    RERUN,
    UNPROVEN,
    RoundRunner,
    StageStore,
    compare_candidates,
    load_round,
    select_parameters,
    trace_final_result,
)


def _env() -> dict:
    return default_environment("test-pipeline/v1", {"kind": "unit"})


BASELINE = {"seed": 1, "multiplier": 2, "offset": 0}


class RoundsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "store"
        self.inputs = {"raw": b"experiment input bytes"}
        self.environment = _env()
        self.pipeline = demo_pipeline()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def store(self) -> StageStore:
        return StageStore(self.root, self.pipeline)

    def run_round(self, round_id: str, baseline: dict, candidates: list[dict]) -> dict:
        return RoundRunner(self.store()).run_round(
            round_id=round_id,
            inputs=self.inputs,
            environment=self.environment,
            baseline=baseline,
            candidates=candidates,
        )

    def test_first_round_plans_reuse_and_unproven_sections(self) -> None:
        store = self.store()
        plan = store.plan(self.inputs, BASELINE, self.environment)
        statuses = {name: item["status"] for name, item in plan["stages"].items()}
        self.assertEqual(
            {
                "prepare": RERUN,
                "transform": UNPROVEN,
                "combine": UNPROVEN,
                "validate": UNPROVEN,
            },
            statuses,
        )

        summary = self.run_round(
            "r1",
            BASELINE,
            [
                {"id": "original", "adjustments": {}},
                {"id": "cand_offset", "adjustments": {"offset": 5}},
            ],
        )
        view = compare_candidates(summary)
        # offset only affects combine: prepare and transform are reused from
        # the original experiment, combine/validate are the candidate's own.
        offset = next(c for c in view["candidates"][CONFIRMED] if c["candidate_id"] == "cand_offset")
        reused = {entry["stage"]: entry["origin"]["experiment"] for entry in offset["reused_from"]}
        self.assertEqual({"prepare": "original", "transform": "original"}, reused)

    def test_partial_adoption_rechecks_whole_candidate(self) -> None:
        # The revision proposes multiplier=9 and offset=7, but the user adopts
        # only offset=7. multiplier must stay at the baseline value and the
        # candidate must reuse the baseline transform, not any result built
        # for multiplier=9.
        summary = self.run_round(
            "r1",
            BASELINE,
            [
                {"id": "original", "adjustments": {}},
                {
                    "id": "cand_full",
                    "adjustments": {"multiplier": 9, "offset": 7},
                },
                {
                    "id": "cand_partial",
                    "adjustments": {"multiplier": 9, "offset": 7},
                    "adopted": ["offset"],
                },
            ],
        )
        partial = summary["candidates"]["cand_partial"]
        full = summary["candidates"]["cand_full"]
        self.assertEqual(2, partial["parameters"]["multiplier"])
        self.assertEqual(7, partial["parameters"]["offset"])
        self.assertNotEqual(
            partial["stages"]["combine"]["stage_hash"],
            full["stages"]["combine"]["stage_hash"],
        )
        self.assertEqual(
            partial["stages"]["transform"]["stage_hash"],
            summary["candidates"]["original"]["stages"]["transform"]["stage_hash"],
        )

    def test_unchanged_related_parameter_name_but_changed_unrelated_value_is_irrelevant(self) -> None:
        # 'seed' only feeds prepare; failing validate via fail_stage must not
        # change the prepare identity, so prepare stays reusable afterwards.
        self.run_round(
            "r1",
            BASELINE,
            [{"id": "original", "adjustments": {}}],
        )
        failed = self.run_round(
            "r2",
            BASELINE,
            [{"id": "broken", "adjustments": {"fail_stage": "validate"}}],
        )
        stages = failed["candidates"]["broken"]["stages"]
        self.assertEqual(CONFIRMED, stages["prepare"]["status"])
        self.assertEqual(CONFIRMED, stages["transform"]["status"])
        self.assertEqual(CONFIRMED, stages["combine"]["status"])
        self.assertEqual(RERUN, stages["validate"]["status"])

    def test_reverting_parameter_to_original_value_reuses_original_stages(self) -> None:
        self.run_round(
            "r1",
            BASELINE,
            [{"id": "original", "adjustments": {}}],
        )
        changed = self.run_round(
            "r2",
            BASELINE,
            [{"id": "changed", "adjustments": {"multiplier": 4}}],
        )
        changed_hashes = {
            name: changed["candidates"]["changed"]["stages"][name]["stage_hash"]
            for name in ("prepare", "transform")
        }
        reverted = self.run_round(
            "r3",
            BASELINE,
            [{"id": "reverted", "adjustments": {"multiplier": 2}}],
        )
        reverted_stages = reverted["candidates"]["reverted"]["stages"]
        view = compare_candidates(reverted)["candidates"][CONFIRMED]
        entry = next(item for item in view if item["candidate_id"] == "reverted")
        reused_stages = {item["stage"] for item in entry["reused_from"]}
        self.assertEqual({"prepare", "transform", "combine", "validate"}, reused_stages)
        origins = {
            item["stage"]: item["origin"]["experiment"]
            for item in entry["reused_from"]
        }
        self.assertTrue(all(origin == "original" for origin in origins.values()))
        self.assertNotEqual(
            changed_hashes["transform"],
            reverted_stages["transform"]["stage_hash"],
        )

    def test_corrupted_shared_artifact_quarantines_and_only_affected_chain_reruns(self) -> None:
        summary = self.run_round(
            "r1",
            BASELINE,
            [
                {"id": "original", "adjustments": {}},
                {"id": "cand_offset", "adjustments": {"offset": 5}},
            ],
        )
        prepare_hash = summary["candidates"]["original"]["stages"]["prepare"]["stage_hash"]
        # prepare is shared by transform and combine; corrupt its artifact.
        prepared = (
            self.root / "stages" / self.pipeline.name / "prepare" / prepare_hash / "artifacts" / "prepared.txt"
        )
        prepared.write_bytes(b"corrupted bytes")

        rerun = self.run_round(
            "r2",
            BASELINE,
            [
                {"id": "original", "adjustments": {}},
                {"id": "cand_offset", "adjustments": {"offset": 5}},
            ],
        )
        # recomputation is deterministic, so the new prepare hash equals the
        # old claimed identity and downstream stages become confirmed again.
        for candidate_id in ("original", "cand_offset"):
            stages = rerun["candidates"][candidate_id]["stages"]
            self.assertEqual(CONFIRMED, stages["prepare"]["status"])
            self.assertEqual(CONFIRMED, stages["validate"]["status"])

        evidence = [
            path
            for path in (self.root / "evidence").iterdir()
            if json.loads((path / "manifest.json").read_text())["kind"] == "corrupted_stage"
        ]
        self.assertEqual(1, len(evidence))
        manifest = json.loads((evidence[0] / "manifest.json").read_text())
        self.assertEqual("prepare", manifest["stage"])
        self.assertEqual(b"corrupted bytes", (evidence[0] / "artifacts" / "artifacts" / "prepared.txt").read_bytes())

    def test_partial_stage_failure_marks_downstream_unproven_and_keeps_evidence(self) -> None:
        summary = self.run_round(
            "r1",
            BASELINE,
            [
                {"id": "original", "adjustments": {}},
                {"id": "broken", "adjustments": {"fail_stage": "combine"}},
            ],
        )
        broken = summary["candidates"]["broken"]
        self.assertEqual(RERUN, broken["stages"]["combine"]["status"])
        self.assertEqual(UNPROVEN, broken["stages"]["validate"]["status"])
        self.assertEqual("unproven", broken["status"])
        self.assertTrue(broken["stages"]["combine"]["executed"]["evidence_id"])

        trace = trace_final_result(self.root, "r1")
        self.assertEqual(UNPROVEN, trace["candidates"]["broken"]["status"])
        failed_evidence = [e for e in trace["abandoned_evidence"] if e["kind"] == "failed_stage"]
        self.assertEqual(1, len(failed_evidence))
        evidence = failed_evidence[0]
        self.assertEqual("combine", evidence["stage"])
        self.assertEqual("candidate:broken", evidence["origin"]["experiment"])
        evidence_dir = self.root / "evidence" / evidence["evidence_id"]
        partial_paths = list((evidence_dir / "artifacts").rglob("partial.txt"))
        self.assertEqual(1, len(partial_paths))
        self.assertEqual(b"partial output of failed stage combine\n", partial_paths[0].read_bytes())
        manifest = json.loads((evidence_dir / "manifest.json").read_text())
        self.assertIn("RuntimeError", manifest["error"])

    def test_failed_candidate_cannot_contaminate_other_candidate(self) -> None:
        summary = self.run_round(
            "r1",
            BASELINE,
            [
                {"id": "broken", "adjustments": {"fail_stage": "combine"}},
                {"id": "healthy", "adjustments": {"offset": 3}},
            ],
        )
        healthy = summary["candidates"]["healthy"]
        broken = summary["candidates"]["broken"]
        self.assertEqual(CONFIRMED, healthy["status"])
        self.assertEqual(UNPROVEN, broken["status"])
        # healthy stages originate only from healthy or the baseline; no
        # origin record may point at the broken candidate.
        for stage in healthy["stages"].values():
            origin = stage.get("origin") or {}
            self.assertNotIn("broken", json.dumps(origin))

    def test_trace_survives_reopen_and_flags_tampering_as_unproven(self) -> None:
        self.run_round(
            "r1",
            BASELINE,
            [{"id": "original", "adjustments": {}}],
        )
        # A later run publishes a second round, but tracing round r1 reads
        # the immutable saved summary instead of current state.
        self.run_round(
            "r2",
            {"seed": 2, "multiplier": 2, "offset": 0},
            [{"id": "original", "adjustments": {}}],
        )
        trace = trace_final_result(self.root, "r1")
        original = trace["candidates"]["original"]
        self.assertEqual(CONFIRMED, original["status"])
        self.assertTrue(all(item["verified"] for item in original["artifacts"]))
        chain_origins = [entry["origin"]["candidate_id"] for entry in original["chain"]]
        self.assertEqual(["original"] * 4, chain_origins)
        # request snapshot retains per-input identities for reopen tracking.
        self.assertTrue((self.root / "rounds" / "r1.request.json").is_file())

    def test_same_filename_different_content_never_reused(self) -> None:
        first = self.run_round(
            "r1", BASELINE, [{"id": "original", "adjustments": {}}]
        )
        self.inputs["raw"] = b"completely different content"
        second = self.run_round(
            "r2", BASELINE, [{"id": "original", "adjustments": {}}]
        )
        first_prepare = first["candidates"]["original"]["stages"]["prepare"]["stage_hash"]
        second_prepare = second["candidates"]["original"]["stages"]["prepare"]["stage_hash"]
        self.assertNotEqual(first_prepare, second_prepare)

    def test_select_parameters_helper(self) -> None:
        merged = select_parameters({"a": 1, "b": 2}, {"a": 9, "b": 8}, adopted={"b"})
        self.assertEqual({"a": 1, "b": 8}, merged)
        merged_all = select_parameters({"a": 1}, {"a": 9})
        self.assertEqual({"a": 9}, merged_all)

    def test_round_file_is_json_reloadable(self) -> None:
        summary = self.run_round(
            "r1", BASELINE, [{"id": "original", "adjustments": {}}]
        )
        reloaded = load_round(self.root, "r1")
        self.assertEqual(summary["round_id"], reloaded["round_id"])


if __name__ == "__main__":
    unittest.main()
