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

from .models import SCHEMA_VERSION, canonical_json
from .store import RunStore

FREEZE_SCHEMA_VERSION = "freeze-package/v2"

STATUS_CONFIRMED = "confirmed"
STATUS_NEEDS_REDO = "needs_redo"
STATUS_UNPROVEN = "unproven"
DELIVERY_COMPLETE = "complete"
DELIVERY_MISSING = "missing"
DELIVERY_CORRUPTED = "corrupted"


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
    """Classify one selection and describe each unavailable dependency."""
    request_hash = selection.request_hash
    item: dict[str, Any] = {"request_hash": request_hash, "reason": selection.reason}
    dependencies: list[dict[str, Any]] = []
    has_missing_dependency = False
    has_corrupted_dependency = False
    manifest = _load_record_manifest(store, request_hash)
    artifact_dir = store.root / "artifacts" / request_hash

    if manifest is not None:
        artifacts = manifest.get("artifacts", [])
        if not isinstance(artifacts, list):
            artifacts = []
            has_corrupted_dependency = True
            dependencies.append(
                {"path": None, "status": DELIVERY_CORRUPTED, "detail": "artifact list is invalid"}
            )
        for entry in artifacts:
            if not isinstance(entry, dict):
                has_corrupted_dependency = True
                dependencies.append(
                    {"path": None, "status": DELIVERY_CORRUPTED, "detail": "artifact entry is invalid"}
                )
                continue
            dependency = {
                "path": entry.get("path"),
                "sha256": entry.get("sha256"),
                "size": entry.get("size"),
            }
            source = artifact_dir / str(entry.get("path", ""))
            try:
                data = source.read_bytes()
                if len(data) != entry.get("size") or hashlib.sha256(data).hexdigest() != entry.get("sha256"):
                    raise ValueError("digest or size mismatch")
                dependency["status"] = DELIVERY_COMPLETE
            except FileNotFoundError:
                dependency.update(status=DELIVERY_MISSING, detail="artifact file is absent")
                has_missing_dependency = True
            except (OSError, ValueError, TypeError) as exc:
                dependency.update(status=DELIVERY_CORRUPTED, detail=str(exc))
                has_corrupted_dependency = True
            dependencies.append(dependency)

    complete = store._load_complete(request_hash)
    if complete is not None:
        assert manifest is not None
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
            delivery_status=DELIVERY_COMPLETE,
            detail="record verified against artifacts",
            dependencies=dependencies,
        )
        return item

    basis = store._takeover_basis(request_hash) or {}
    if manifest is None and basis.get("artifacts_without_record"):
        item.update(
            status=STATUS_UNPROVEN,
            delivery_status=DELIVERY_MISSING,
            detail="artifacts exist but no trustworthy record proves their origin",
        )
        return item
    if manifest is None:
        item.update(status=STATUS_NEEDS_REDO, delivery_status=DELIVERY_MISSING, detail="no record found")
    elif has_corrupted_dependency:
        item.update(
            status=STATUS_NEEDS_REDO,
            delivery_status=DELIVERY_CORRUPTED,
            detail="one or more artifacts failed verification",
        )
    elif has_missing_dependency:
        item.update(
            status=STATUS_NEEDS_REDO,
            delivery_status=DELIVERY_MISSING,
            detail="one or more required artifacts are absent",
        )
    else:
        item.update(status=STATUS_NEEDS_REDO, delivery_status=DELIVERY_MISSING, detail="record is not reusable")
    item["dependencies"] = dependencies
    if basis:
        item["evidence"] = basis
    return item


def plan_freeze(store: RunStore, selections: list[FreezeSelection]) -> dict[str, Any]:
    items = [plan_item(store, selection) for selection in selections]
    return {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "kind": "freeze-plan",
        "freezable": all(item["status"] == STATUS_CONFIRMED for item in items),
        "partially_available": any(item["status"] == STATUS_CONFIRMED for item in items),
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
    if manifest.get("schema_version") not in ("freeze-package/v1", FREEZE_SCHEMA_VERSION):
        raise ValueError("unsupported freeze package schema in " + str(manifest_path))
    return manifest


def freeze(
    store: RunStore,
    selections: list[FreezeSelection],
    output_root: str | Path,
    supersedes: str | Path | None = None,
) -> dict[str, Any]:
    """Publish an immutable package containing every currently healthy candidate."""
    report = plan_freeze(store, selections)
    if not report["partially_available"]:
        raise FreezeError(report)

    previous_manifest: dict[str, Any] | None = None
    if supersedes is not None:
        previous_manifest = load_package_manifest(supersedes)

    selection_entries: list[dict[str, Any]] = []
    candidate_entries: list[dict[str, Any]] = []
    record_hashes: list[str] = []
    blobs: dict[str, Path] = {}
    for selection, item in zip(selections, report["items"]):
        manifest = _load_record_manifest(store, selection.request_hash)
        parameters = manifest.get("request", {}).get("parameters", {}) if manifest else {}
        selection_entries.append(selection.manifest_entry(parameters))
        candidate_entry = {
            "request_hash": selection.request_hash,
            "status": item.get("delivery_status", DELIVERY_MISSING),
            "detail": item.get("detail", ""),
            "dependencies": item.get("dependencies", []),
        }
        candidate_entries.append(candidate_entry)
        if item["status"] != STATUS_CONFIRMED or manifest is None:
            continue
        record_hashes.append(selection.request_hash)
        artifact_dir = store.root / "artifacts" / selection.request_hash
        for entry in manifest["artifacts"]:
            sha = entry["sha256"]
            source = artifact_dir / entry["path"]
            blobs.setdefault(sha, source)

    record_hashes = sorted(set(record_hashes))
    artifact_entries = [
        {"sha256": sha, "size": blobs[sha].stat().st_size} for sha in sorted(blobs)
    ]
    identity_payload = {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "selections": selection_entries,
        "candidates": candidate_entries,
        "records": record_hashes,
        "artifacts": artifact_entries,
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
        "complete": report["freezable"],
        "frozen": record_hashes,
        "pending": [
            {
                "request_hash": item["request_hash"],
                "status": item["delivery_status"],
                "detail": item["detail"],
            }
            for item in report["items"]
            if item["status"] != STATUS_CONFIRMED
        ],
        "undeliverable": [],
        "artifact_blobs": len(blobs),
    }


def import_package(store: RunStore, package_dir: str | Path) -> dict[str, Any]:
    """Verify a freeze package item by item and install records into a store.

    Candidates install independently. Missing or corrupted candidates are
    reported with their declared reason, while complete candidates remain
    reusable. A different already-used record version creates a conflict audit
    entry and is never overwritten.
    """
    package_dir = Path(package_dir)
    manifest = load_package_manifest(package_dir)

    artifact_items: list[dict[str, Any]] = []
    verified_blobs: dict[str, bytes] = {}
    for entry in manifest.get("artifacts", []):
        sha = entry["sha256"]
        blob_path = package_dir / "artifacts" / sha
        item: dict[str, Any] = {"sha256": sha}
        try:
            data = blob_path.read_bytes()
            if hashlib.sha256(data).hexdigest() != sha or len(data) != entry["size"]:
                raise ValueError("digest mismatch")
            verified_blobs[sha] = data
            item["status"] = "verified"
        except (OSError, ValueError) as exc:
            item.update(status="failed", detail=str(exc))
        artifact_items.append(item)

    pending_status = {
        entry.get("request_hash"): entry
        for entry in manifest.get("candidates", [])
        if isinstance(entry, dict) and entry.get("status") in (DELIVERY_MISSING, DELIVERY_CORRUPTED)
    }
    record_hashes = manifest.get("records", [])
    if not isinstance(record_hashes, list):
        record_hashes = []
    if not manifest.get("candidates"):
        pending_status = {}

    record_items: list[dict[str, Any]] = []
    candidate_keys = sorted(set(record_hashes) | set(pending_status))
    for request_hash in candidate_keys:
        item: dict[str, Any] = {"request_hash": request_hash}
        pending = pending_status.get(request_hash)
        if pending is not None:
            item.update(
                status=pending.get("status", DELIVERY_MISSING),
                detail=pending.get("detail", "candidate was not delivered completely"),
                dependencies=pending.get("dependencies", []),
            )
            record_items.append(item)
            continue
        record_path = package_dir / "records" / (request_hash + ".json")
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
            contents: dict[str, bytes] = {}
            for artifact in record.get("artifacts", []):
                sha = artifact["sha256"]
                if sha not in verified_blobs:
                    raise ValueError("artifact " + artifact["path"] + " missing or unverified")
                contents[artifact["path"]] = verified_blobs[sha]
            existing_path = store.root / "requests" / (request_hash + ".json")
            if existing_path.is_file() and store._load_complete(request_hash) is not None:
                existing = json.loads(existing_path.read_bytes())
                existing_artifacts = [
                    {key: artifact.get(key) for key in ("path", "size", "sha256")}
                    for artifact in existing.get("artifacts", [])
                    if isinstance(artifact, dict)
                ]
                incoming_artifacts = [
                    {key: artifact.get(key) for key in ("path", "size", "sha256")}
                    for artifact in record.get("artifacts", [])
                    if isinstance(artifact, dict)
                ]
                if existing_artifacts != incoming_artifacts:
                    item.update(
                        status="conflict",
                        detail="incoming record uses a different artifact version than the record already in use",
                        existing_artifacts=existing_artifacts,
                        incoming_artifacts=incoming_artifacts,
                    )
                    record_items.append(item)
                    continue
            summary = store.install_record(record, contents)
            item["status"] = "already_present" if summary.reused else "installed"
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            item.update(status="failed", detail=str(exc))
        record_items.append(item)

    verified_complete = bool(record_items) and all(
        item["status"] == "verified" for item in artifact_items
    ) and all(item["status"] in ("installed", "already_present") for item in record_items)
    store.append_delivery_event(
        {
            "event": "import_package",
            "package_id": manifest.get("package_id"),
            "version": manifest.get("version"),
            "verified_complete": verified_complete,
            "candidates": record_items,
        }
    )
    return {
        "schema_version": FREEZE_SCHEMA_VERSION,
        "kind": "import-report",
        "package_id": manifest.get("package_id"),
        "version": manifest.get("version"),
        "artifacts": artifact_items,
        "records": record_items,
        "selections": manifest.get("selections", []),
        "candidates": manifest.get("candidates", []),
        "audit_path": str(store.root / "delivery-imports.jsonl"),
        "verified_complete": verified_complete,
    }
