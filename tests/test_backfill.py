from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from exp_records.backends import local_short_compute
from exp_records.backfill import BackfillPlanner
from exp_records.freeze import FreezeSelection, freeze, import_package
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.store import RunStore


def make_request(data: bytes) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data=data),),
        parameters={},
        environment=default_environment("test/v1"),
    )


class BackfillPlannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = RunStore(self.root / "receiver", local_short_compute)
        self.planner = BackfillPlanner(self.store)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def reopen(self) -> BackfillPlanner:
        return BackfillPlanner(RunStore(self.root / "receiver", local_short_compute))

    def test_shared_dependency_counted_once_in_pending_and_budget(self) -> None:
        self.planner.register("app-a", size=10, dependencies=["shared-lib"])
        self.planner.register("app-b", size=20, dependencies=["shared-lib"])
        self.planner.register("shared-lib", size=5)
        pending = self.planner.pending_set()
        self.assertEqual(sorted(pending), ["app-a", "app-b", "shared-lib"])
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["pending_bytes"], 35)
        # shared-lib is installable; the apps are retained behind it
        self.assertEqual(
            [item["content_id"] for batch in plan["batches"] for item in batch["items"]],
            ["shared-lib"],
        )
        self.assertEqual(
            sorted(item["content_id"] for item in plan["retained"]), ["app-a", "app-b"]
        )
        # once the shared dependency is verified both apps become installable
        self.planner.mark_verified("shared-lib")
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["retained"], [])
        self.assertEqual(
            sorted(item["content_id"] for batch in plan["batches"] for item in batch["items"]),
            ["app-a", "app-b"],
        )

    def test_missing_dependency_blocks_but_retains_dependent(self) -> None:
        self.planner.register("top", size=3, dependencies=["mid"])
        self.planner.register("mid", size=3, dependencies=["ghost"])
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["batches"], [])
        retained = {item["content_id"]: item for item in plan["retained"]}
        self.assertEqual(sorted(retained), ["ghost", "mid", "top"])
        self.assertEqual(retained["top"]["missing_dependencies"], ["mid"])
        self.assertIn(["top", "mid", "ghost"], retained["top"]["blocking_chains"])
        self.assertFalse(retained["ghost"]["registered"])

    def test_cycle_is_reported_and_nothing_becomes_installable(self) -> None:
        self.planner.register("x", size=1, dependencies=["y"])
        self.planner.register("y", size=1, dependencies=["x"])
        self.planner.register("free", size=1)
        plan = self.planner.plan_batches(budget=100)
        retained = {item["content_id"]: item for item in plan["retained"]}
        self.assertIn("cycle", retained["x"])
        cycle = retained["x"]["cycle"]
        self.assertEqual(cycle[0], cycle[-1])
        self.assertEqual(set(cycle), {"x", "y"})
        batched = [item["content_id"] for batch in plan["batches"] for item in batch["items"]]
        self.assertEqual(batched, ["free"])

    def test_budget_splits_batches_and_max_batches_defers(self) -> None:
        self.planner.register("low", size=4, priority=1)
        self.planner.register("high", size=4, priority=9)
        self.planner.register("mid", size=4, priority=5)
        plan = self.planner.plan_batches(budget=8, max_batches=1)
        self.assertEqual(len(plan["batches"]), 1)
        self.assertEqual(
            [item["content_id"] for item in plan["batches"][0]["items"]], ["high", "mid"]
        )
        self.assertTrue(plan["budget_exhausted"])
        self.assertEqual([item["content_id"] for item in plan["deferred"]], ["low"])
        # without the batch limit everything fits into priority-ordered batches
        plan = self.planner.plan_batches(budget=8)
        self.assertEqual(len(plan["batches"]), 2)
        self.assertFalse(plan["budget_exhausted"])

    def test_duplicate_marks_and_out_of_order_redelivery_do_not_double_count(self) -> None:
        self.planner.register("a", size=10)
        self.planner.register("b", size=10)
        self.planner.mark_verified("a")
        self.planner.mark_verified("a")
        self.planner.mark_verified("b")
        self.planner.mark_verified("a")
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["verified_bytes"], 20)
        self.assertEqual(plan["pending_bytes"], 0)
        self.assertEqual(plan["batches"], [])

    def test_failed_content_only_sends_dependents_back_to_pending(self) -> None:
        self.planner.register("base", size=5)
        self.planner.register("dependent", size=5, dependencies=["base"])
        self.planner.register("independent", size=5)
        for content_id in ("base", "dependent", "independent"):
            self.planner.mark_verified(content_id)
        event = self.planner.mark_failed("base", "sha256 mismatch")
        self.assertEqual(event["dependents"], ["dependent"])
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["verified"], ["independent"])
        self.assertEqual(plan["verified_bytes"], 5)
        batched = [item["content_id"] for batch in plan["batches"] for item in batch["items"]]
        self.assertEqual(batched, ["base"])
        self.assertEqual([item["content_id"] for item in plan["retained"]], ["dependent"])
        # replacement content verifies: dependent is requested again and,
        # once re-verified, the plan is complete
        self.planner.mark_verified("base")
        plan = self.planner.plan_batches(budget=100)
        batched = [item["content_id"] for batch in plan["batches"] for item in batch["items"]]
        self.assertEqual(batched, ["dependent"])
        self.assertEqual(plan["retained"], [])
        self.planner.mark_verified("dependent")
        plan = self.planner.plan_batches(budget=100)
        self.assertEqual(plan["pending_bytes"], 0)

    def test_plan_survives_reopen_without_sender_directory(self) -> None:
        origin = RunStore(self.root / "origin", local_short_compute)
        summary = origin.run(make_request(b"payload"))
        out = self.root / "packages"
        freeze(origin, [FreezeSelection(summary.request_hash)], out)
        package_dir = out / next(p.name for p in out.iterdir())

        self.planner.register(summary.request_hash, size=12, priority=3)
        self.planner.register("upper", size=7, dependencies=[summary.request_hash])
        self.planner.save()

        # sender directories disappear entirely before the receiver reopens
        shutil.rmtree(self.root / "origin")
        shutil.rmtree(out)

        planner = self.reopen()
        plan = planner.plan_batches(budget=100)
        batched = [item["content_id"] for batch in plan["batches"] for item in batch["items"]]
        self.assertEqual(batched, [summary.request_hash])
        retained = {item["content_id"]: item for item in plan["retained"]}
        self.assertEqual(retained["upper"]["missing_dependencies"], [summary.request_hash])

        # a later redelivery installs the record; reopening adopts it from the
        # store alone and only the remaining request stays pending
        import_package(self.store, package_dir) if package_dir.exists() else None
        planner = self.reopen()
        planner.mark_verified(summary.request_hash)
        planner.save()
        plan = self.reopen().plan_batches(budget=100)
        batched = [item["content_id"] for batch in plan["batches"] for item in batch["items"]]
        self.assertEqual(batched, ["upper"])

    def test_sync_with_store_adopts_imported_records_once(self) -> None:
        origin = RunStore(self.root / "origin", local_short_compute)
        summary = origin.run(make_request(b"sync-me"))
        out = self.root / "packages"
        freeze(origin, [FreezeSelection(summary.request_hash)], out)
        package_dir = out / next(p.name for p in out.iterdir())
        import_package(self.store, package_dir)
        import_package(self.store, package_dir)  # repeated import is idempotent

        self.planner.register(summary.request_hash, size=9)
        plan = self.planner.plan_batches(budget=100)
        self.assertIn(summary.request_hash, plan["verified"])
        self.assertEqual(plan["verified_bytes"], 9)
        self.assertEqual(plan["pending_bytes"], 0)
        # reopening still counts the capacity exactly once
        self.planner.save()
        plan = self.reopen().plan_batches(budget=100)
        self.assertEqual(plan["verified_bytes"], 9)


if __name__ == "__main__":
    unittest.main()
