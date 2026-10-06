from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .store import RunStore

BACKFILL_SCHEMA_VERSION = "backfill-plan/v1"


class BackfillPlanner:
    """Receiver-side batched backfill plan for missing content.

    The planner persists everything it needs inside the receiver store
    (backfill/state.json, written atomically and fsynced), so a plan can be
    reopened and extended after the original sender directory is gone.

    Semantics:
    - The minimal pending set is derived from locally verified content plus
      declared dependencies: wanted items that are not verified yet, and
      their unverified transitive dependencies, deduplicated by content id.
    - A pending item whose dependencies are not all verified stays retained
      and is reported with its blocking chain; it is never marked installable.
    - Verified capacity is a set of content ids, so repeated imports and
      out-of-order redelivery never consume capacity twice.
    - Marking downloaded content as failed only sends its dependents back to
      pending; independent requests stay usable.
    """

    def __init__(self, store: RunStore):
        self.store = store
        self.directory = store.root / "backfill"
        self.path = self.directory / "state.json"
        self.items: dict[str, dict[str, Any]] = {}
        self.verified: set[str] = set()
        self.failures: list[dict[str, Any]] = []
        self._load()

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_bytes())
        except (OSError, json.JSONDecodeError):
            return
        if raw.get("schema_version") != BACKFILL_SCHEMA_VERSION:
            return
        for content_id, entry in raw.get("items", {}).items():
            self.items[content_id] = {
                "size": int(entry.get("size", 0)),
                "priority": int(entry.get("priority", 0)),
                "dependencies": list(entry.get("dependencies", [])),
                "wanted": bool(entry.get("wanted", True)),
            }
        self.verified = set(raw.get("verified", []))
        self.failures = list(raw.get("failures", []))

    def save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": BACKFILL_SCHEMA_VERSION,
            "kind": "backfill-plan-state",
            "updated_at": self._utcnow(),
            "items": {
                content_id: {
                    "size": entry["size"],
                    "priority": entry["priority"],
                    "dependencies": sorted(entry["dependencies"]),
                    "wanted": entry["wanted"],
                }
                for content_id, entry in sorted(self.items.items())
            },
            "verified": sorted(self.verified),
            "failures": self.failures,
        }
        temporary = self.directory / (".state-" + uuid.uuid4().hex + ".tmp")
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        self.store._fsync_directory(self.directory)

    def register(
        self,
        content_id: str,
        *,
        size: int = 0,
        priority: int = 0,
        dependencies: list[str] | tuple[str, ...] = (),
        wanted: bool = True,
    ) -> dict[str, Any]:
        """Declare a wanted content item. Re-registering is idempotent: it
        refreshes metadata but never revokes an already verified item."""
        entry = self.items.get(content_id)
        if entry is None:
            entry = {"size": 0, "priority": 0, "dependencies": [], "wanted": True}
            self.items[content_id] = entry
        entry["size"] = int(size)
        entry["priority"] = int(priority)
        entry["dependencies"] = sorted({str(dep) for dep in dependencies})
        entry["wanted"] = bool(wanted)
        return dict(entry)

    def mark_verified(self, content_id: str) -> None:
        self.verified.add(content_id)

    def mark_failed(self, content_id: str, reason: str = "") -> dict[str, Any]:
        """Downloaded content failed verification. Only dependents of this
        content fall back to pending; independent items stay verified."""
        self.verified.discard(content_id)
        dependents = self._dependents_of(content_id)
        self.verified.difference_update(dependents)
        event = {
            "content_id": content_id,
            "reason": reason,
            "at": self._utcnow(),
            "dependents": sorted(dependents),
        }
        self.failures.append(event)
        return event

    def sync_with_store(self) -> list[str]:
        """Adopt content already verified inside the receiver store (e.g.
        installed by import-package) without double-counting capacity."""
        adopted = []
        for content_id in sorted(self.items):
            if content_id in self.verified:
                continue
            if self.store._load_complete(content_id) is not None:
                self.verified.add(content_id)
                adopted.append(content_id)
        return adopted

    def _dependents_of(self, content_id: str) -> set[str]:
        dependents: set[str] = set()
        frontier = [content_id]
        while frontier:
            target = frontier.pop()
            for candidate, entry in self.items.items():
                if candidate in dependents or candidate == content_id:
                    continue
                if target in entry["dependencies"]:
                    dependents.add(candidate)
                    frontier.append(candidate)
        return dependents

    def _dependencies_of(self, content_id: str) -> list[str]:
        entry = self.items.get(content_id)
        return list(entry["dependencies"]) if entry else []

    def pending_set(self) -> dict[str, dict[str, Any]]:
        """Minimal set of content still to request, deduplicated so content
        shared by several requests is only counted once."""
        pending: dict[str, dict[str, Any]] = {}

        def visit(content_id: str) -> None:
            if content_id in self.verified or content_id in pending:
                return
            entry = self.items.get(content_id)
            pending[content_id] = {
                "content_id": content_id,
                "size": entry["size"] if entry else 0,
                "priority": entry["priority"] if entry else 0,
                "wanted": bool(entry and entry["wanted"]),
                "registered": entry is not None,
            }
            for dependency in self._dependencies_of(content_id):
                visit(dependency)

        for content_id, entry in self.items.items():
            if entry["wanted"]:
                visit(content_id)
        return pending

    def _find_cycle(self, start: str) -> list[str] | None:
        stack: list[str] = []
        on_stack: set[str] = set()
        visited: set[str] = set()

        def walk(node: str) -> list[str] | None:
            if node in on_stack:
                return stack[stack.index(node):] + [node]
            if node in visited:
                return None
            visited.add(node)
            stack.append(node)
            on_stack.add(node)
            for dependency in self._dependencies_of(node):
                cycle = walk(dependency)
                if cycle is not None:
                    return cycle
            stack.pop()
            on_stack.discard(node)
            return None

        return walk(start)

    def blocking_chains(self, content_id: str) -> dict[str, Any]:
        """Explain why a pending item cannot be installed yet: every chain of
        unverified dependencies, plus any dependency cycle."""
        chains: list[list[str]] = []

        def walk(node: str, trail: list[str]) -> None:
            unverified = [
                dep
                for dep in self._dependencies_of(node)
                if dep not in self.verified
            ]
            if not unverified:
                if len(trail) > 1:
                    chains.append(list(trail))
                return
            for dependency in unverified:
                if dependency in trail:
                    continue
                walk(dependency, trail + [dependency])

        walk(content_id, [content_id])
        result: dict[str, Any] = {
            "content_id": content_id,
            "verified": content_id in self.verified,
            "registered": content_id in self.items,
            "blocking_chains": chains,
            "missing_dependencies": sorted(
                {
                    dep
                    for dep in self._dependencies_of(content_id)
                    if dep not in self.verified
                }
            ),
        }
        cycle = self._find_cycle(content_id)
        if cycle is not None:
            result["cycle"] = cycle
        return result

    def plan_batches(
        self, budget: int, max_batches: int | None = None
    ) -> dict[str, Any]:
        """Pack installable pending content into batches of at most `budget`
        bytes, ordered by priority. Items past `max_batches` are deferred
        (budget exhausted); items with unverified dependencies stay retained
        and are reported with their blocking chains."""
        self.sync_with_store()
        pending = self.pending_set()

        installable: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for content_id, info in pending.items():
            if not info["registered"]:
                explanation = self.blocking_chains(content_id)
                explanation["detail"] = "dependency is missing and was never registered"
                retained.append(explanation)
                continue
            unverified_deps = [
                dep
                for dep in self._dependencies_of(content_id)
                if dep not in self.verified
            ]
            if unverified_deps:
                retained.append(self.blocking_chains(content_id))
            else:
                installable.append(info)
        installable.sort(key=lambda item: (-item["priority"], item["content_id"]))
        retained.sort(key=lambda item: item["content_id"])

        batches: list[dict[str, Any]] = []
        current: list[dict[str, Any]] = []
        current_size = 0
        deferred: list[dict[str, Any]] = []
        for item in installable:
            if current and current_size + item["size"] > budget:
                batches.append({"items": current, "total_size": current_size})
                current = []
                current_size = 0
            current.append(item)
            current_size += item["size"]
        if current:
            batches.append({"items": current, "total_size": current_size})

        if max_batches is not None and len(batches) > max_batches:
            deferred = [item for batch in batches[max_batches:] for item in batch["items"]]
            batches = batches[:max_batches]

        return {
            "schema_version": BACKFILL_SCHEMA_VERSION,
            "kind": "backfill-plan",
            "budget": budget,
            "max_batches": max_batches,
            "batches": batches,
            "deferred": deferred,
            "budget_exhausted": bool(deferred),
            "retained": retained,
            "verified": sorted(self.verified),
            "verified_bytes": sum(
                self.items[cid]["size"] for cid in self.verified if cid in self.items
            ),
            "pending_bytes": sum(info["size"] for info in pending.values()),
        }
