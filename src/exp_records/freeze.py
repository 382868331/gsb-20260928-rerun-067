from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .import_history import ImportHistory
from .models import SCHEMA_VERSION, canonical_json
from .store import RunStore

FREEZE_SCHEMA_VERSION = "freeze-package/v1"

STATUS_CONFIRMED = "confirmed"
STATUS_NEEDS_REDO = "needs_redo"
STATUS_UNPROVEN = "unproven"

STATE_COMPLETE = "complete"
STATE_MISSING = "missing"
STATE_DAMAGED = "damaged"


@dataclass(frozen=True)
class FreezeSelection:
    """One candidate chosen for delivery.

    adopted_parameters=None means every parameter decision of the candidate is
    adopted; otherwise only the listed parameter keys are taken. expected_inputs
    pins logical input names to the digests seen at selection time so a replaced
    input file is detected before freezing.
    """

    request_hash: str
    adopted_parameters: tuple[str, ...] | None = None
    reason: str = ""
    expected_inputs: dict[str, str] = field(default_factory=dict)

    def manifest_entry(self, parameters: dict[str, Any]) -> dict[str, Any]:
        if self.adopted_parameters is None:
            adopted = dict(parameters)
        else:
            adopted = {
                key: parameters[key] for key in self.adopted_parameters if key in parameters
            }
        return {
            "request_hash": self.request_hash,
            "adopted_parameters": sorted(adopted),
            "adopted_parameter_values": adopted,
            "reason": self.reason,
        }


class FreezeError(RuntimeError):
    def __init__(self, report: dict[str, Any]):
        self.report = report
        undeliverable = [item["request_hash"] for item in report["items"] if item["status"] != STATUS_CONFIRMED]
        super().__init__("selections not deliverable: " + ", ".join(undeliverable))


def _delivery_state(item: dict[str, Any]) -> str:
    """Map a plan item to a delivery state: complete / missing / damaged."""
    if item["status"] == STATUS_CONFIRMED:
        return STATE_COMPLETE
    if item["status"] == STATUS_UNPROVEN:
        return STATE_DAMAGED
    if item.get("detail") == "record failed artifact verification":
        return STATE_DAMAGED
    return STATE_MISSING


def _record_manifest_path(store: RunStore, request_hash: str) -> Path:
    return store.root / "requests" / (request_hash + ".json")


def _load_record_manifest(store: RunStore, request_hash: str) -> dict[str, Any] | None:
    path = _record_manifest_path(store, request_hash)
    if not path.is_file():
        return None
    try:
        manifest = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError):
        return None
    return manifest if isinstance(manifest, dict) else None


def plan_item(store: RunStore, selection: FreezeSelection) -> dict[str, Any]:
    """Classify one selection as confirmed / needs_redo / unproven."""
    request_hash = selection.request_hash
    item: dict[str, Any] = {"request_hash": request_hash, "reason": selection.reason}

    complete = store._load_complete(request_hash)
    if complete is not None:
        manifest = _load_record_manifest(store, request_hash) or {}
        recorded_inputs = {
            entry["name"]: entry["sha256"]
            for entry in manifest.get("request", {}).get("inputs", [])
        }
        replaced = {
            name: {"expected": digest, "recorded": recorded_inputs.get(name)}
            for name, digest in selection.expected_inputs.items()
            if recorded_inputs.get(name) != digest
        }
        if replaced:
            item.update(
                status=STATUS_NEEDS_REDO,
                detail="input content replaced since selection",
                replaced_inputs=replaced,
            )
            return item
        item.update(
            status=STATUS_CONFIRMED,
            detail="record verified against artifacts",
            artifacts=[entry["path"] for entry in manifest.get("artifacts", [])],
        )
        return item

    basis = store._takeover_basis(request_hash) or {}
    if basis.get("artifacts_without_record"):
        item.update(
            status=STATUS_UNPROVEN,
            detail="artifacts exist but no trustworthy record proves their origin",
        )
        return item
    if basis.get("record_failed_verification"):
        item.update(status=STATUS_NEEDS_REDO, detail="record failed artifact verification")
    elif basis.get("orphaned_attempts") or basis.get("stale_staging_dirs"):
        item.update(status=STATUS_NEEDS_REDO, detail="earlier stages never finished")
    else:
        item.update(status=STATUS_NEEDS_REDO, detail="no record found")
    if basis:
        item["evidence"] = basis
    return item


def plan_freeze(store: RunStore, selections: list[FreezeSelection]) -> dict[str, Any]:
    items = [plan_item(store, selection) for selection in selections]
    return {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "kind": "freeze-plan",
        "freezable": all(item["status"] == STATUS_CONFIRMED for item in items),
        "items": items,
    }


def _selection_identity(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_hash": entry["request_hash"],
        "adopted_parameters": entry["adopted_parameters"],
        "adopted_parameter_values": entry["adopted_parameter_values"],
    }


def _diff_selections(
    previous: list[dict[str, Any]], current: list[dict[str, Any]]
) -> dict[str, Any]:
    before = {entry["request_hash"]: entry for entry in previous}
    after = {entry["request_hash"]: entry for entry in current}
    added = sorted(key for key in after if key not in before)
    removed = sorted(key for key in before if key not in after)
    replaced = []
    for key in sorted(before.keys() & after.keys()):
        old_identity = _selection_identity(before[key])
        new_identity = _selection_identity(after[key])
        if old_identity != new_identity:
            replaced.append(
                {
                    "request_hash": key,
                    "previous": old_identity,
                    "current": new_identity,
                    "reason": after[key].get("reason", ""),
                }
            )
    return {"added": added, "removed": removed, "replaced": replaced}


def load_package_manifest(package_dir: str | Path) -> dict[str, Any]:
    manifest_path = Path(package_dir) / "freeze-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("schema_version") != FREEZE_SCHEMA_VERSION:
        raise ValueError("unsupported freeze package schema in " + str(manifest_path))
    return manifest


def freeze(
    store: RunStore,
    selections: list[FreezeSelection],
    output_root: str | Path,
    supersedes: str | Path | None = None,
    allow_partial: bool = False,
) -> dict[str, Any]:
    """Publish an immutable freeze package for confirmed selections.

    Dependencies are re-verified at freeze time; any selection that is not
    confirmed aborts the whole freeze and is reported as undeliverable, unless
    allow_partial is set. A partial package still delivers every healthy
    candidate and records each undelivered candidate as missing or damaged in
    the manifest, so a later supplement package can fill the gaps.
    """
    report = plan_freeze(store, selections)
    if not report["freezable"] and not allow_partial:
        raise FreezeError(report)

    confirmed_hashes = {
        item["request_hash"] for item in report["items"] if item["status"] == STATUS_CONFIRMED
    }
    candidate_states = [
        {
            "request_hash": item["request_hash"],
            "state": _delivery_state(item),
            "plan_status": item["status"],
            "detail": item.get("detail", ""),
            "reason": item.get("reason", ""),
        }
        for item in report["items"]
        if item["request_hash"] not in confirmed_hashes
    ]
    candidate_states.sort(key=lambda entry: entry["request_hash"])

    previous_manifest: dict[str, Any] | None = None
    if supersedes is not None:
        previous_manifest = load_package_manifest(supersedes)

    selection_entries: list[dict[str, Any]] = []
    record_hashes: list[str] = []
    blobs: dict[str, Path] = {}
    for selection in selections:
        if selection.request_hash not in confirmed_hashes:
            continue
        manifest = _load_record_manifest(store, selection.request_hash)
        assert manifest is not None  # guaranteed by plan_freeze for confirmed items
        parameters = manifest.get("request", {}).get("parameters", {})
        selection_entries.append(selection.manifest_entry(parameters))
        record_hashes.append(selection.request_hash)
        artifact_dir = store.root / "artifacts" / selection.request_hash
        for entry in manifest["artifacts"]:
            sha = entry["sha256"]
            source = artifact_dir / entry["path"]
            # Content-identical artifacts shared across candidates are stored once.
            blobs.setdefault(sha, source)

    record_hashes = sorted(set(record_hashes))
    artifact_entries = [
        {"sha256": sha, "size": blobs[sha].stat().st_size} for sha in sorted(blobs)
    ]
    identity_payload = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "selections": selection_entries,
        "records": record_hashes,
        "artifacts": artifact_entries,
        "candidate_states": candidate_states,
        "supersedes": previous_manifest["package_id"] if previous_manifest else None,
    }
    package_id = "freeze-" + hashlib.sha256(canonical_json(identity_payload)).hexdigest()[:24]

    changes = None
    version = 1
    if previous_manifest is not None:
        version = int(previous_manifest.get("version", 1)) + 1
        changes = _diff_selections(
            previous_manifest.get("selections", []), selection_entries
        )

    manifest_out: dict[str, Any] = {
        **identity_payload,
        "package_id": package_id,
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "kind": "freeze-package",
    }
    if changes is not None:
        manifest_out["changes"] = changes

    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / package_id
    if target.exists():
        raise FreezeError(
            {
                "schema_version": FREEZE_SCHEMA_VERSION,
                "kind": "freeze-plan",
                "freezable": False,
                "items": [],
                "detail": "package " + package_id + " already published; freeze packages are immutable",
            }
        )

    staging = output_root / (".staging-" + uuid.uuid4().hex)
    (staging / "records").mkdir(parents=True)
    (staging / "artifacts").mkdir()
    try:
        for request_hash in record_hashes:
            shutil.copyfile(
                _record_manifest_path(store, request_hash),
                staging / "records" / (request_hash + ".json"),
            )
        for sha, source in blobs.items():
            shutil.copyfile(source, staging / "artifacts" / sha)
        payload = json.dumps(manifest_out, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        manifest_path = staging / "freeze-manifest.json"
        with manifest_path.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staging, target)
        store._fsync_directory(output_root)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "kind": "freeze-report",
        "package_id": package_id,
        "version": version,
        "path": str(target),
        "frozen": record_hashes,
        "undeliverable": candidate_states,
        "artifact_blobs": len(blobs),
    }


def import_package(store: RunStore, package_dir: str | Path) -> dict[str, Any]:
    """Verify a freeze package item by item and install records into a store.

    Re-importing the same package is idempotent: records already present and
    verified are reported as already_present and no duplicate provenance is
    created. Every candidate ends up with an explicit state: complete
    (installed/already_present), missing (content temporarily absent), damaged
    (content present but failed verification), or conflict (a different
    version is already in use locally and is left untouched, with both sides
    recorded for audit). Undelivered candidates never masquerade as reusable
    records and never block healthy candidates from being imported. Any
    non-complete item keeps the package from being marked verified_complete.
    """
    package_dir = Path(package_dir)
    manifest = load_package_manifest(package_dir)
    started_at = ImportHistory.now()
    import_id = uuid.uuid4().hex

    artifact_items: list[dict[str, Any]] = []
    verified_blobs: dict[str, bytes] = {}
    blob_states: dict[str, str] = {}
    for entry in manifest.get("artifacts", []):
        sha = entry["sha256"]
        blob_path = package_dir / "artifacts" / sha
        item: dict[str, Any] = {"sha256": sha}
        if not blob_path.is_file():
            item.update(status=STATE_MISSING, detail="artifact blob absent from package")
        else:
            data = blob_path.read_bytes()
            if hashlib.sha256(data).hexdigest() != sha or len(data) != entry["size"]:
                item.update(status=STATE_DAMAGED, detail="artifact digest or size mismatch")
            else:
                verified_blobs[sha] = data
                item["status"] = "verified"
        blob_states[sha] = item["status"]
        artifact_items.append(item)

    record_items: list[dict[str, Any]] = []
    for request_hash in manifest.get("records", []):
        item = {"request_hash": request_hash}
        record_path = package_dir / "records" / (request_hash + ".json")
        if not record_path.is_file():
            item.update(status=STATE_MISSING, detail="record file absent from package")
            record_items.append(item)
            continue
        try:
            record = json.loads(record_path.read_bytes())
            if record.get("record_id") != request_hash:
                raise ValueError("record_id does not match file name")
            payload = record.get("request")
            if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
                raise ValueError("record request payload missing or unsupported")
            recomputed = hashlib.sha256(canonical_json(payload)).hexdigest()
            if recomputed != request_hash:
                raise ValueError("record request payload does not match request hash")
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            item.update(status=STATE_DAMAGED, detail=str(exc))
            record_items.append(item)
            continue

        contents: dict[str, bytes] = {}
        dependency_state: str | None = None
        for artifact in record.get("artifacts", []):
            sha = artifact["sha256"]
            if sha in verified_blobs:
                contents[artifact["path"]] = verified_blobs[sha]
                continue
            state = blob_states.get(sha, STATE_MISSING)
            dependency_state = dependency_state or state
            item["missing_artifact" if state == STATE_MISSING else "damaged_artifact"] = artifact[
                "path"
            ]
        if dependency_state is not None:
            item.update(
                status=dependency_state,
                detail="artifact dependencies not satisfied in this package",
            )
            record_items.append(item)
            continue

        existing = store._load_complete(request_hash)
        if existing is not None:
            try:
                local_record = json.loads(existing.record_path.read_bytes())
            except (OSError, json.JSONDecodeError):
                local_record = None
            if local_record is not None and canonical_json(local_record) == canonical_json(record):
                item["status"] = "already_present"
            else:
                item.update(
                    status="conflict",
                    detail="a different version is already installed and in use; it was not overwritten",
                    local_artifacts=local_record.get("artifacts") if local_record else None,
                    incoming_artifacts=record.get("artifacts"),
                )
            record_items.append(item)
            continue

        try:
            summary = store.install_record(record, contents)
            item["status"] = "already_present" if summary.reused else "installed"
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            item.update(status=STATE_DAMAGED, detail=str(exc))
        record_items.append(item)

    declared_states = manifest.get("candidate_states", [])
    candidates = [
        {
            "request_hash": item["request_hash"],
            "state": STATE_COMPLETE
            if item["status"] in ("installed", "already_present")
            else item["status"],
            "detail": item.get("detail", ""),
        }
        for item in record_items
    ] + [
        {
            "request_hash": entry["request_hash"],
            "state": entry["state"],
            "detail": entry.get("detail", ""),
        }
        for entry in declared_states
    ]

    verified_complete = bool(record_items) and all(
        item["status"] == "verified" for item in artifact_items
    ) and all(
        item["status"] in ("installed", "already_present") for item in record_items
    ) and not declared_states
    report = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "kind": "import-report",
        "package_id": manifest.get("package_id"),
        "version": manifest.get("version"),
        "artifacts": artifact_items,
        "records": record_items,
        "candidates": candidates,
        "selections": manifest.get("selections", []),
        "verified_complete": verified_complete,
    }

    selection_reasons = {
        entry.get("request_hash"): entry.get("reason", "")
        for entry in manifest.get("selections", [])
        if isinstance(entry, dict)
    }
    record_statuses = {
        item["request_hash"]: item["status"] for item in record_items
    }
    request_outcomes = []
    for item in candidates:
        request_hash = item["request_hash"]
        state = item["state"]
        status = record_statuses.get(request_hash, state)
        reason = item.get("detail", "")
        if state == STATE_COMPLETE:
            if status == "installed":
                reason = "record verified against package contents and installed"
            elif status == "already_present":
                reason = "identical verified record already present; import did not reinstall"
        request_outcomes.append(
            {
                "request_hash": request_hash,
                "state": state,
                "status": status,
                "reason": reason,
                "selection_reason": selection_reasons.get(request_hash, ""),
            }
        )
    request_outcomes.sort(key=lambda outcome: outcome["request_hash"])

    supersedes = manifest.get("supersedes")
    history_entry = {
        "kind": "import-history",
        "import_id": import_id,
        "package_id": manifest.get("package_id"),
        "version": manifest.get("version"),
        "package_path": str(package_dir),
        "package_manifest_digest": hashlib.sha256(
            (package_dir / "freeze-manifest.json").read_bytes()
        ).hexdigest(),
        "supersedes": supersedes,
        "supplement": supersedes is not None,
        "started_at": started_at,
        "ended_at": ImportHistory.now(),
        "verified_complete": verified_complete,
        "requests": request_outcomes,
        "summary": {
            "installed": sum(item["status"] == "installed" for item in request_outcomes),
            "already_present": sum(
                item["status"] == "already_present" for item in request_outcomes
            ),
            "missing": sum(item["state"] == STATE_MISSING for item in request_outcomes),
            "damaged": sum(item["state"] == STATE_DAMAGED for item in request_outcomes),
            "conflict": sum(item["state"] == "conflict" for item in request_outcomes),
        },
    }
    store.import_history().append(history_entry)
    report["import_id"] = import_id
    return report
