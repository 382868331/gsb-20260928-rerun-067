from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import canonical_json
from .store import RunStore


DELIVERY_SCHEMA = "experiment-delivery/v1"

STATUS_CONFIRMED = "confirmed"
STATUS_NEEDS_RERUN = "needs_rerun"
STATUS_UNPROVEN = "unproven"
STATUS_BLOCKED = "blocked"


@dataclass(frozen=True)
class Selection:
    """One candidate chosen for delivery.

    key: stable label used to diff delivery versions.
    request_hash: the adopted record identity.
    reason: why this candidate (or these parameter decisions) were adopted.
    depends_on: request hashes of shared intermediate records this candidate
        relies on; they are re-verified at freeze time.
    inputs: (logical name, current path) pairs to snapshot into the bundle so
        the receiver can recompute without guessing original file paths.
    """

    key: str
    request_hash: str
    reason: str = ""
    depends_on: tuple[str, ...] = ()
    inputs: tuple[tuple[str, Path], ...] = ()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def derive_request_hash(
    store: RunStore, base_hash: str, parameter_overrides: dict[str, Any]
) -> str:
    """Compute the identity of a candidate that adopts only part of another
    candidate's parameter decisions on top of an existing record."""
    manifest = store.load_manifest(base_hash)
    if manifest is None:
        raise ValueError(f"base record {base_hash} is not complete")
    payload = dict(manifest["request"])
    parameters = dict(payload.get("parameters") or {})
    parameters.update(parameter_overrides)
    payload["parameters"] = parameters
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def evaluate_selection(store: RunStore, selection: Selection) -> dict[str, Any]:
    """Classify one selection as confirmed / needs_rerun / unproven / blocked."""
    item: dict[str, Any] = {
        "key": selection.key,
        "request_hash": selection.request_hash,
        "reason": selection.reason,
        "depends_on": list(selection.depends_on),
    }
    state = store.record_state(selection.request_hash)
    manifest = store.load_manifest(selection.request_hash)

    if manifest is None:
        if state.get("in_progress"):
            item.update(status=STATUS_BLOCKED, blocked_reason="stage_in_progress")
        elif state.get("artifacts_without_record") or state.get(
            "record_failed_verification"
        ):
            item.update(
                status=STATUS_UNPROVEN,
                blocked_reason="present_files_without_verifiable_record",
            )
        else:
            item.update(status=STATUS_NEEDS_RERUN, blocked_reason="no_complete_record")
        return item

    for dependency in selection.depends_on:
        dep_state = store.record_state(dependency)
        if not dep_state["complete"]:
            item.update(
                status=STATUS_BLOCKED,
                blocked_reason=f"dependency_unavailable:{dependency}",
            )
            return item

    recorded_inputs = {
        entry["name"]: entry for entry in manifest["request"].get("inputs", [])
    }
    input_entries: dict[str, Any] = {}
    for name, path in selection.inputs:
        recorded = recorded_inputs.get(name)
        if recorded is None:
            item.update(
                status=STATUS_BLOCKED,
                blocked_reason=f"snapshot_input_not_in_record:{name}",
            )
            return item
        path = Path(path)
        if not path.is_file():
            item.update(
                status=STATUS_BLOCKED, blocked_reason=f"snapshot_input_missing:{name}"
            )
            return item
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest != recorded["sha256"] or len(data) != recorded["size"]:
            item.update(
                status=STATUS_BLOCKED,
                blocked_reason=f"input_replaced_since_run:{name}",
            )
            return item
        input_entries[name] = {"sha256": digest, "size": len(data)}

    item.update(
        status=STATUS_CONFIRMED,
        record_sha256=_sha256_file(store.root / "requests" / f"{selection.request_hash}.json"),
        inputs=input_entries,
        artifacts=manifest["artifacts"],
    )
    return item


def plan_freeze(store: RunStore, selections: list[Selection]) -> dict[str, Any]:
    """Preview which selections are deliverable before freezing anything."""
    items = [evaluate_selection(store, selection) for selection in selections]
    return {
        "schema_version": DELIVERY_SCHEMA,
        "generated_at": _utcnow(),
        "deliverable": all(item["status"] == STATUS_CONFIRMED for item in items),
        "items": items,
    }


def _delivery_versions(store: RunStore) -> list[int]:
    deliveries_dir = store.root / "deliveries"
    versions = []
    if deliveries_dir.is_dir():
        for entry in deliveries_dir.glob("v*.json"):
            try:
                versions.append(int(entry.stem[1:]))
            except ValueError:
                continue
    return sorted(versions)


def _diff_items(previous: dict[str, Any] | None, items: list[dict[str, Any]]) -> dict[str, Any]:
    previous_items = {item["key"]: item for item in (previous or {}).get("items", [])}
    current_items = {item["key"]: item for item in items}
    added, removed, replaced, unchanged = [], [], [], []
    for key, item in current_items.items():
        old = previous_items.get(key)
        if old is None:
            added.append(key)
        elif old.get("request_hash") != item.get("request_hash") or old.get(
            "status"
        ) != item.get("status"):
            replaced.append(
                {
                    "key": key,
                    "previous_request_hash": old.get("request_hash"),
                    "previous_status": old.get("status"),
                    "request_hash": item.get("request_hash"),
                    "basis": item.get("reason") or "recomputed or backfilled",
                }
            )
        else:
            unchanged.append(key)
    for key in previous_items:
        if key not in current_items:
            removed.append(key)
    return {
        "added": sorted(added),
        "removed": sorted(removed),
        "replaced": replaced,
        "unchanged": sorted(unchanged),
    }


def export_delivery(
    store: RunStore,
    selections: list[Selection],
    out_dir: str | Path,
    note: str = "",
) -> dict[str, Any]:
    """Freeze confirmed selections into an immutable, transferable bundle.

    Selections that cannot be proven are listed as undelivered in the bundle;
    confirmed candidates are exported regardless, so a broken shared artifact
    or a replaced input only blocks the candidates that depend on it.
    """
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"delivery directory {out_dir} is not empty")
    out_dir.mkdir(parents=True, exist_ok=True)

    items = [evaluate_selection(store, selection) for selection in selections]
    by_key = {selection.key: selection for selection in selections}

    files: dict[str, dict[str, Any]] = {}

    def add_file(relative: str, data: bytes) -> None:
        target = out_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        files[relative] = {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}

    delivered_items = []
    for item in items:
        if item["status"] != STATUS_CONFIRMED:
            continue
        request_hash = item["request_hash"]
        record_relative = f"records/{request_hash}.json"
        if record_relative not in files:
            add_file(
                record_relative,
                (store.root / "requests" / f"{request_hash}.json").read_bytes(),
            )
        artifact_dir = store.root / "artifacts" / request_hash
        for artifact in item["artifacts"]:
            relative = f"artifacts/{request_hash}/{artifact['path']}"
            if relative not in files:
                add_file(relative, (artifact_dir / artifact["path"]).read_bytes())
        selection = by_key[item["key"]]
        for name, _path in selection.inputs:
            relative = f"inputs/{selection.key}/{name}"
            add_file(relative, Path(_path).read_bytes())
            item["inputs"][name]["snapshot"] = relative
        delivered_items.append(item)

    versions = _delivery_versions(store)
    version = (versions[-1] + 1) if versions else 1
    previous = None
    if versions:
        previous = json.loads(
            (store.root / "deliveries" / f"v{versions[-1]:04d}.json").read_bytes()
        )

    delivery = {
        "schema_version": DELIVERY_SCHEMA,
        "version": version,
        "supersedes": previous["version"] if previous else None,
        "created_at": _utcnow(),
        "note": note,
        "complete": all(item["status"] == STATUS_CONFIRMED for item in items),
        "changes": _diff_items(previous, items),
        "items": items,
    }
    delivery_payload = json.dumps(delivery, ensure_ascii=False, sort_keys=True, indent=2)
    add_file("delivery.json", delivery_payload.encode("utf-8") + b"\n")

    manifest = {
        "schema_version": DELIVERY_SCHEMA,
        "files": files,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    deliveries_dir = store.root / "deliveries"
    deliveries_dir.mkdir(exist_ok=True)
    registry_entry = dict(delivery, bundle_manifest_sha256=_sha256_file(out_dir / "manifest.json"))
    temporary = deliveries_dir / f".v{version:04d}.{os.getpid()}.tmp"
    temporary.write_text(
        json.dumps(registry_entry, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, deliveries_dir / f"v{version:04d}.json")

    return {
        "version": version,
        "complete": delivery["complete"],
        "delivered": [item["key"] for item in delivered_items],
        "undelivered": [
            {"key": item["key"], "status": item["status"], "reason": item["blocked_reason"]}
            for item in items
            if item["status"] != STATUS_CONFIRMED
        ],
        "bundle": str(out_dir),
    }


def verify_bundle(bundle_dir: str | Path) -> dict[str, Any]:
    """Verify every file of a delivery bundle against its manifest."""
    bundle_dir = Path(bundle_dir)
    report: dict[str, Any] = {"bundle": str(bundle_dir), "complete": False, "files": {}, "items": []}
    try:
        manifest = json.loads((bundle_dir / "manifest.json").read_bytes())
        delivery = json.loads((bundle_dir / "delivery.json").read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        report["error"] = f"unreadable bundle: {exc}"
        return report

    files_ok = True
    for relative, expected in sorted(manifest.get("files", {}).items()):
        path = bundle_dir / relative
        if not path.is_file():
            report["files"][relative] = "missing"
            files_ok = False
            continue
        data = path.read_bytes()
        ok = len(data) == expected["size"] and hashlib.sha256(data).hexdigest() == expected["sha256"]
        report["files"][relative] = "ok" if ok else "corrupt"
        files_ok = files_ok and ok

    for item in delivery.get("items", []):
        entry = {"key": item["key"], "status": item["status"]}
        if item["status"] != STATUS_CONFIRMED:
            entry["verified"] = False
            entry["detail"] = item.get("blocked_reason")
        elif not files_ok:
            entry["verified"] = False
            entry["detail"] = "bundle files incomplete"
        else:
            record_data = json.loads(
                (bundle_dir / f"records/{item['request_hash']}.json").read_bytes()
            )
            recomputed = hashlib.sha256(
                canonical_json(record_data["request"])
            ).hexdigest()
            entry["verified"] = recomputed == item["request_hash"] == record_data["record_id"]
            entry["detail"] = "ok" if entry["verified"] else "record identity mismatch"
        report["items"].append(entry)

    report["complete"] = bool(files_ok and delivery.get("complete"))
    report["delivery_version"] = delivery.get("version")
    return report


def import_delivery(store: RunStore, bundle_dir: str | Path) -> dict[str, Any]:
    """Install verified records from a bundle into a store.

    Only items whose files all verify are installed; re-importing the same
    bundle reuses the existing records instead of duplicating provenance.
    """
    bundle_dir = Path(bundle_dir)
    verification = verify_bundle(bundle_dir)
    report: dict[str, Any] = {
        "bundle": str(bundle_dir),
        "imported": [],
        "reused": [],
        "skipped": [],
        "complete": verification["complete"],
    }
    if "error" in verification:
        report["error"] = verification["error"]
        return report

    delivery = json.loads((bundle_dir / "delivery.json").read_bytes())
    files_ok = all(status == "ok" for status in verification["files"].values())
    for item in delivery.get("items", []):
        key = item["key"]
        if item["status"] != STATUS_CONFIRMED or not files_ok:
            report["skipped"].append(
                {"key": key, "reason": item.get("blocked_reason", "bundle files incomplete")}
            )
            continue
        request_hash = item["request_hash"]
        record = json.loads((bundle_dir / "records" / f"{request_hash}.json").read_bytes())
        summary = store.install_record(record, bundle_dir / "artifacts" / request_hash)
        (report["reused"] if summary.reused else report["imported"]).append(
            {"key": key, "request_hash": request_hash}
        )
    return report
