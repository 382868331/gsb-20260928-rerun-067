from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from exp_records.backends import local_short_compute
from exp_records.freeze import FreezeSelection, freeze, import_package
from exp_records.models import ExperimentRequest, InputSpec, default_environment
from exp_records.replenish import ReplenishmentPlanner, WantEntry
from exp_records.store import RunStore


def make_request(tag: str) -> ExperimentRequest:
    return ExperimentRequest(
        inputs=(InputSpec("numbers", data=tag.encode("utf-8")),),
        parameters={"tag": tag},
        environment=default_environment("test/v1"),
    )


class ReplenishTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.origin = RunStore(self.root / "origin", local_short_compute)
        self.receiver = RunStore(self.root / "receiver", local_short_compute)
        self.packages = self.root / "packages"
        self.hashes: dict[str, str] = {}
        for tag in ("a", "b", "c", "d"):
            summary = self.origin.run(make_request(tag))
            self.hashes[tag] = summary.request_hash
            freeze(
                self.origin,
                [FreezeSelection(request_hash=summary.request_hash)],
                self.packages,
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def package_dir(self, tag: str) -> Path:
        want = self.hashes[tag]
        for child in self.packages.iterdir():
            manifest = json.loads((child / "freeze-manifest.json").read_text())
            if want in manifest.get("records", []):
                return child
        raise AssertionError("no package for " + tag)

    def import_tag(self, tag: str) -> dict:
        return import_package(self.receiver, self.package_dir(tag))

    def corrupt(self, tag: str) -> None:
        artifact = (
            self.receiver.root / "artifacts" / self.hashes[tag] / "result.json"
        )
        artifact.write_bytes(b"tampered")

    def plan(self, wants: list[WantEntry], **kwargs) -> dict:
        return ReplenishmentPlanner(self.receiver).build_plan(wants, **kwargs)

    def states(self, plan: dict) -> dict[str, str]:
        return {item["request_hash"]: item["state"] for item in plan["requests"]}

    def test_shared_dependency_charged_once_and_batched_by_budget(self) -> None:
        # b and d both depend on a; a is wanted twice (duplicate entries).
        wants = [
            WantEntry(self.hashes["a"], size_bytes=60, priority=5),
            WantEntry(self.hashes["a"], size_bytes=60, priority=1),
            WantEntry(self.hashes["b"], depends_on=(self.hashes["a"],), size_bytes=50),
            WantEntry(self.hashes["d"], depends_on=(self.hashes["a"],), size_bytes=50),
        ]
        plan = self.plan(wants, budget_bytes=100)
        self.assertEqual(plan["merged_duplicate_requests"], [self.hashes["a"]])
        # a(60) + b(50) = 110 > 100, so a+b fill batch 0... a alone is 60,
        # b joins (110 > 100) -> new batch; d joins b (100 <= 100).
        self.assertEqual(len(plan["batches"]), 2)
        self.assertEqual(plan["batches"][0]["requests"], [self.hashes["a"]])
        self.assertEqual(plan["batches"][0]["size_bytes"], 60)
        self.assertEqual(
            plan["batches"][1]["requests"],
            sorted([self.hashes["b"], self.hashes["d"]]),
        )
        self.assertEqual(plan["batches"][1]["size_bytes"], 100)
        # priority decides batch order: higher priority first.
        self.assertEqual(plan["batches"][0]["requests"], [self.hashes["a"]])

    def test_missing_dependency_held_not_installable(self) -> None:
        self.import_tag("c")  # c verified locally, but its dependency b is absent
        wants = [
            WantEntry(self.hashes["b"], size_bytes=10),
            WantEntry(self.hashes["c"], depends_on=(self.hashes["b"],)),
        ]
        plan = self.plan(wants)
        states = self.states(plan)
        self.assertEqual(states[self.hashes["c"]], "held")
        self.assertEqual(states[self.hashes["b"]], "scheduled")
        item_c = next(
            i for i in plan["requests"] if i["request_hash"] == self.hashes["c"]
        )
        chain = item_c["blocking_chain"]
        self.assertEqual(chain[0]["request_hash"], self.hashes["b"])
        self.assertEqual(chain[0]["local_state"], "absent")

    def test_cycle_is_blocked_and_explained(self) -> None:
        wants = [
            WantEntry(self.hashes["a"], depends_on=(self.hashes["b"],)),
            WantEntry(self.hashes["b"], depends_on=(self.hashes["a"],)),
            WantEntry(self.hashes["c"]),
        ]
        plan = self.plan(wants)
        states = self.states(plan)
        self.assertEqual(states[self.hashes["a"]], "blocked")
        self.assertEqual(states[self.hashes["b"]], "blocked")
        self.assertEqual(states[self.hashes["c"]], "scheduled")
        self.assertTrue(plan["cycles"])
        item_a = next(
            i for i in plan["requests"] if i["request_hash"] == self.hashes["a"]
        )
        self.assertIn("cycle", item_a["blocking_chain"][0]["detail"])

    def test_budget_exhaustion_defers_remaining_requests(self) -> None:
        wants = [
            WantEntry(self.hashes["a"], size_bytes=100, priority=3),
            WantEntry(self.hashes["b"], size_bytes=100, priority=2),
            WantEntry(self.hashes["c"], size_bytes=100, priority=1),
        ]
        plan = self.plan(wants, budget_bytes=100, max_batches=2)
        states = self.states(plan)
        self.assertEqual(states[self.hashes["a"]], "scheduled")
        self.assertEqual(states[self.hashes["b"]], "scheduled")
        self.assertEqual(states[self.hashes["c"]], "deferred")
        self.assertEqual(len(plan["batches"]), 2)

    def test_damaged_content_only_returns_dependents_to_pending(self) -> None:
        self.import_tag("a")
        self.import_tag("b")
        self.import_tag("c")
        self.corrupt("b")
        wants = [
            WantEntry(self.hashes["a"]),
            WantEntry(self.hashes["b"], size_bytes=10),
            WantEntry(self.hashes["c"], depends_on=(self.hashes["b"],)),
            WantEntry(self.hashes["d"]),
        ]
        plan = self.plan(wants)
        states = self.states(plan)
        # Independent verified content stays installable.
        self.assertEqual(states[self.hashes["a"]], "installable")
        # Damaged content goes back to the request set.
        self.assertEqual(states[self.hashes["b"]], "scheduled")
        item_b = next(
            i for i in plan["requests"] if i["request_hash"] == self.hashes["b"]
        )
        self.assertEqual(item_b["local_state"], "damaged")
        # Its dependent is verified locally but held, not installable.
        self.assertEqual(states[self.hashes["c"]], "held")
        # Untouched independent request d is simply scheduled.
        self.assertEqual(states[self.hashes["d"]], "scheduled")

        # Recovery: redelivered package replaces the damaged content.
        self.import_tag("b")
        plan = self.plan(wants)
        states = self.states(plan)
        self.assertEqual(states[self.hashes["b"]], "installable")
        self.assertEqual(states[self.hashes["c"]], "installable")

    def test_duplicate_and_out_of_order_imports_do_not_double_count(self) -> None:
        first = self.import_tag("a")
        second = self.import_tag("a")  # duplicate import
        self.assertEqual(second["records"][0]["status"], "already_present")
        self.assertEqual(first["records"][0]["status"], "installed")
        wants = [
            WantEntry(self.hashes["a"]),
            WantEntry(self.hashes["a"]),
        ]
        plan = self.plan(wants)
        installable = [
            item for item in plan["requests"] if item["state"] == "installable"
        ]
        self.assertEqual(len(installable), 1)
        self.assertEqual(plan["summary"], {"installable": 1})

    def test_resume_after_restart_without_sender_directory(self) -> None:
        wants = [
            WantEntry(self.hashes["a"], size_bytes=10),
            WantEntry(self.hashes["b"], size_bytes=10),
            WantEntry(
                self.hashes["c"], depends_on=(self.hashes["b"],), size_bytes=10
            ),
        ]
        planner = ReplenishmentPlanner(self.receiver)
        planner.build_plan(wants, budget_bytes=100)

        # Sender side and any wants file are gone; only the receiver store remains.
        import shutil

        shutil.rmtree(self.root / "origin")
        reopened = RunStore(self.root / "receiver", local_short_compute)
        plan = ReplenishmentPlanner(reopened).resume()
        states = self.states(plan)
        self.assertEqual(states[self.hashes["a"]], "scheduled")
        self.assertEqual(states[self.hashes["c"]], "scheduled")
        item_c = next(
            i for i in plan["requests"] if i["request_hash"] == self.hashes["c"]
        )
        # The blocking chain still explains the missing dependency b.
        self.assertEqual(item_c["blocking_chain"][0]["request_hash"], self.hashes["b"])

        # After a arrives, resuming again shows progress without the sender.
        self.import_tag("a")
        plan = ReplenishmentPlanner(reopened).resume()
        states = self.states(plan)
        self.assertEqual(states[self.hashes["a"]], "installable")
        self.assertEqual(states[self.hashes["c"]], "scheduled")


if __name__ == "__main__":
    unittest.main()
