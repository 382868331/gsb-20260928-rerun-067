from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .store import RunStore

REPLENISH_SCHEMA_VERSION = "replenishment-plan/v1"

STATE_INSTALLABLE = "installable"
STATE_HELD = "held"
STATE_SCHEDULED = "scheduled"
STATE_DEFERRED = "deferred"
STATE_BLOCKED = "blocked"


@dataclass(frozen=True)
class WantEntry:
    """One request the receiver wants to obtain.

    depends_on lists request hashes that must be verified locally before this
    request may be marked installable. priority orders batching (higher
    first). size_bytes is the declared transfer size used for budget
    accounting; content shared between entries (identical request_hash) is
    only charged once.
    """

    request_hash: str
    depends_on: tuple[str, ...] = ()
    priority: int = 0
    size_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_hash": self.request_hash,
            "depends_on": sorted(self.depends_on),
            "priority": self.priority,
            "size_bytes": self.size_bytes,
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "WantEntry":
        return WantEntry(
            request_hash=raw["request_hash"],
            depends_on=tuple(raw.get("depends_on", [])),
            priority=int(raw.get("priority", 0)),
            size_bytes=int(raw.get("size_bytes", 0)),
        )


def load_wants(path: str | Path) -> list[WantEntry]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("wants file must contain a JSON array")
    return [WantEntry.from_dict(entry) for entry in raw]


class ReplenishmentPlanner:
    """Builds and persists receiver-side replenishment plans.

    The planner only consults the receiver's own store: locally verified
    records, damaged leftovers, and the persisted plan state under
    replenish/. Neither the sender's store nor any package directory is
    needed to (re)generate the remaining request batches.
    """

    def __init__(self, store: RunStore):
        self.store = store
        self.directory = store.root / "replenish"
        self.state_path = self.directory / "plan-state.json"

    def _verified(self, request_hash: str) -> bool:
        return self.store._load_complete(request_hash) is not None

    def _local_state(self, request_hash: str) -> str:
        """verified / damaged / absent for content addressed by request_hash."""
        if self._verified(request_hash):
            return "verified"
        record_path = self.store.root / "requests" / (request_hash + ".json")
        artifact_dir = self.store.root / "artifacts" / request_hash
        if record_path.is_file() or artifact_dir.exists():
            return "damaged"
        return "absent"

    def build_plan(
        self,
        wants: list[WantEntry],
        budget_bytes: int | None = None,
        max_batches: int | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        # Identical content requested several times is merged once; repeated
        # imports or out-of-order redelivery never double-charge capacity.
        entries: dict[str, WantEntry] = {}
        duplicates: list[str] = []
        for want in wants:
            if want.request_hash in entries:
                duplicates.append(want.request_hash)
                previous = entries[want.request_hash]
                merged_deps = tuple(
                    sorted(set(previous.depends_on) | set(want.depends_on))
                )
                entries[want.request_hash] = WantEntry(
                    request_hash=want.request_hash,
                    depends_on=merged_deps,
                    priority=max(previous.priority, want.priority),
                    size_bytes=max(previous.size_bytes, want.size_bytes),
                )
            else:
                entries[want.request_hash] = want

        local_state = {h: self._local_state(h) for h in entries}
        external_deps: dict[str, str] = {}
        for want in entries.values():
            for dep in want.depends_on:
                if dep not in entries and dep not in external_deps:
                    external_deps[dep] = self._local_state(dep)

        cycles = self._find_cycles(entries)
        cycle_members = {member for cycle in cycles for member in cycle}

        def unresolvable(request_hash: str, seen: frozenset[str] = frozenset()) -> bool:
            if request_hash in cycle_members:
                return True
            if request_hash in seen:
                return False
            seen = seen | {request_hash}
            want = entries.get(request_hash)
            if want is None:
                return external_deps.get(request_hash, "absent") != "verified"
            for dep in want.depends_on:
                if dep in entries or external_deps.get(dep) != "verified":
                    if unresolvable(dep, seen):
                        return True
            return False

        def deps_all_verified(request_hash: str, seen: frozenset[str] = frozenset()) -> bool:
            if request_hash in seen:
                return True
            seen = seen | {request_hash}
            want = entries.get(request_hash)
            if want is None:
                return external_deps.get(request_hash) == "verified"
            for dep in want.depends_on:
                if dep in entries:
                    if local_state[dep] != "verified" or not deps_all_verified(dep, seen):
                        return False
                elif external_deps.get(dep) != "verified":
                    return False
            return True

        blocked = {h for h in entries if unresolvable(h)}

        # Batch scheduling: priority first, budget caps each batch, and shared
        # content already placed in the current plan is never charged twice.
        schedulable = [
            want
            for h, want in entries.items()
            if h not in blocked and local_state[h] != "verified"
        ]
        schedulable.sort(key=lambda w: (-w.priority, w.request_hash))
        batches: list[dict[str, Any]] = []
        assignment: dict[str, int] = {}
        current: list[str] = []
        current_bytes = 0
        charged: set[str] = set()

        def close_batch() -> None:
            nonlocal current, current_bytes
            if current:
                batches.append(
                    {
                        "index": len(batches),
                        "size_bytes": current_bytes,
                        "requests": sorted(current),
                    }
                )
            current = []
            current_bytes = 0

        for want in schedulable:
            size = want.size_bytes
            if budget_bytes is not None and current and current_bytes + size > budget_bytes:
                close_batch()
            if budget_bytes is not None and not current and size > budget_bytes:
                # A single item larger than the budget gets its own batch,
                # flagged so the sender knows the budget was exceeded.
                batches.append(
                    {
                        "index": len(batches),
                        "size_bytes": size,
                        "requests": [want.request_hash],
                        "over_budget": True,
                    }
                )
                assignment[want.request_hash] = len(batches) - 1
                charged.add(want.request_hash)
                continue
            current.append(want.request_hash)
            if want.request_hash not in charged:
                current_bytes += size
                charged.add(want.request_hash)
            assignment[want.request_hash] = len(batches)
        close_batch()

        deferred: set[str] = set()
        if max_batches is not None and len(batches) > max_batches:
            for batch in batches[max_batches:]:
                for h in batch["requests"]:
                    deferred.add(h)
                    del assignment[h]
            batches = batches[:max_batches]

        items: list[dict[str, Any]] = []
        for h in sorted(entries):
            want = entries[h]
            item: dict[str, Any] = {
                "request_hash": h,
                "priority": want.priority,
                "size_bytes": want.size_bytes,
                "depends_on": sorted(want.depends_on),
                "local_state": local_state[h],
            }
            if h in blocked:
                item["state"] = STATE_BLOCKED
                item["blocking_chain"] = self._blocking_chain(
                    h, entries, local_state, external_deps, cycle_members
                )
            elif local_state[h] == "verified":
                if deps_all_verified(h):
                    item["state"] = STATE_INSTALLABLE
                else:
                    # Content is verified locally but a dependency is still
                    # missing or damaged: retained for later use, never
                    # marked installable.
                    item["state"] = STATE_HELD
                    item["blocking_chain"] = self._blocking_chain(
                        h, entries, local_state, external_deps, cycle_members
                    )
            elif h in deferred:
                item["state"] = STATE_DEFERRED
                item["detail"] = "transfer budget exhausted; rolls into a later batch"
                if not deps_all_verified(h):
                    item["blocking_chain"] = self._blocking_chain(
                        h, entries, local_state, external_deps, cycle_members
                    )
            elif h in assignment:
                item["state"] = STATE_SCHEDULED
                item["batch"] = assignment[h]
                if not deps_all_verified(h):
                    item["blocking_chain"] = self._blocking_chain(
                        h, entries, local_state, external_deps, cycle_members
                    )
            items.append(item)

        summary: dict[str, int] = {}
        for item in items:
            summary[item["state"]] = summary.get(item["state"], 0) + 1

        plan: dict[str, Any] = {
            "schema_version": REPLENISH_SCHEMA_VERSION,
            "kind": "replenishment-plan",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "budget_bytes": budget_bytes,
            "max_batches": max_batches,
            "batches": batches,
            "requests": items,
            "cycles": [list(cycle) for cycle in cycles],
            "summary": summary,
        }
        if duplicates:
            plan["merged_duplicate_requests"] = sorted(set(duplicates))
        if persist:
            self._persist(wants, budget_bytes, max_batches, plan)
        return plan

    def resume(
        self,
        budget_bytes: int | None = None,
        max_batches: int | None = None,
    ) -> dict[str, Any]:
        """Regenerate the remaining plan from persisted state alone.

        Works after a restart in an environment where the sender's directory
        (and the original wants file) no longer exists.
        """
        state = self._load_state()
        if state is None:
            raise FileNotFoundError(
                "no persisted replenishment state in " + str(self.directory)
            )
        wants = [WantEntry.from_dict(entry) for entry in state["wants"]]
        if budget_bytes is None:
            budget_bytes = state.get("budget_bytes")
        if max_batches is None:
            max_batches = state.get("max_batches")
        return self.build_plan(
            wants, budget_bytes=budget_bytes, max_batches=max_batches, persist=True
        )

    def _persist(
        self,
        wants: list[WantEntry],
        budget_bytes: int | None,
        max_batches: int | None,
        plan: dict[str, Any],
    ) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": REPLENISH_SCHEMA_VERSION,
            "kind": "replenishment-state",
            "saved_at": plan["generated_at"],
            "budget_bytes": budget_bytes,
            "max_batches": max_batches,
            "wants": [want.to_dict() for want in wants],
        }
        temporary = self.state_path.with_name(".plan-state." + uuid.uuid4().hex + ".tmp")
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.state_path)
        self.store._fsync_directory(self.directory)

    def _load_state(self) -> dict[str, Any] | None:
        try:
            raw = json.loads(self.state_path.read_bytes())
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(raw, dict) or not isinstance(raw.get("wants"), list):
            return None
        return raw

    @staticmethod
    def _find_cycles(entries: dict[str, WantEntry]) -> list[tuple[str, ...]]:
        cycles: list[tuple[str, ...]] = []
        seen_cycles: set[tuple[str, ...]] = set()
        color: dict[str, int] = {}
        stack: list[str] = []

        def visit(node: str) -> None:
            color[node] = 1
            stack.append(node)
            for dep in entries[node].depends_on:
                if dep not in entries:
                    continue
                if color.get(dep) == 1:
                    cycle = tuple(stack[stack.index(dep):])
                    canonical = tuple(sorted(cycle))
                    if canonical not in seen_cycles:
                        seen_cycles.add(canonical)
                        cycles.append(cycle)
                elif color.get(dep, 0) == 0:
                    visit(dep)
            stack.pop()
            color[node] = 2

        for node in sorted(entries):
            if color.get(node, 0) == 0:
                visit(node)
        return cycles

    def _blocking_chain(
        self,
        request_hash: str,
        entries: dict[str, WantEntry],
        local_state: dict[str, str],
        external_deps: dict[str, str],
        cycle_members: set[str],
    ) -> list[dict[str, str]]:
        """Explain, hop by hop, why a request cannot become installable."""
        chain: list[dict[str, str]] = []
        visited: set[str] = set()

        def walk(node: str, is_root: bool = False) -> None:
            if node in visited:
                return
            visited.add(node)
            want = entries.get(node)
            if want is None:
                state = external_deps.get(node, "absent")
                detail = {
                    "verified": "verified locally",
                    "damaged": "dependency content failed local verification and must be re-delivered",
                    "absent": "dependency is neither requested nor verified locally",
                }[state]
                if state != "verified":
                    chain.append(
                        {"request_hash": node, "local_state": state, "detail": detail}
                    )
                return
            if node in cycle_members:
                chain.append(
                    {
                        "request_hash": node,
                        "local_state": local_state[node],
                        "detail": "dependency cycle makes this request unresolvable",
                    }
                )
                return
            state = local_state[node]
            if state == "damaged":
                chain.append(
                    {
                        "request_hash": node,
                        "local_state": state,
                        "detail": "downloaded content failed verification; only its dependents return to pending",
                    }
                )
            elif state == "absent" and not is_root:
                chain.append(
                    {
                        "request_hash": node,
                        "local_state": state,
                        "detail": "requested but not yet delivered and verified",
                    }
                )
            for dep in want.depends_on:
                dep_state = (
                    local_state[dep] if dep in entries else external_deps.get(dep, "absent")
                )
                if dep_state != "verified" or dep in cycle_members:
                    walk(dep)

        walk(request_hash, is_root=True)
        return chain
