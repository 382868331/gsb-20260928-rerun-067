from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

from .models import canonical_json
from .store import RunStore

IMPORT_HISTORY_VERSION = "import-history/v1"


class ImportHistory:
    """Append-only, durable log of freeze-package import attempts.

    The log lives inside the receiver store (import-history/imports.jsonl),
    so it survives restarts and remains queryable after the original package
    directory (or any other data directory) has been removed.
    """

    def __init__(self, store: RunStore):
        self.store = store
        self.directory = store.root / "import-history"
        self.path = self.directory / "imports.jsonl"

    def append(self, entry: dict[str, Any]) -> dict[str, Any]:
        payload = dict(entry)
        payload.setdefault("schema_version", IMPORT_HISTORY_VERSION)
        payload.setdefault("kind", "import-history-entry")
        payload.setdefault("entry_id", uuid.uuid4().hex)
        payload.setdefault("executor_id", self.store.executor_id)
        payload.setdefault("pid", os.getpid())
        self.directory.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()
            os.fsync(stream.fileno())
        return payload

    def load(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return entries
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                entries.append(
                    {
                        "schema_version": IMPORT_HISTORY_VERSION,
                        "kind": "import-history-entry",
                        "import_result": "unreadable_log_line",
                        "raw": line,
                    }
                )
        return entries

    def query(
        self,
        *,
        package_id: str | None = None,
        request_hash: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return logged import entries, optionally filtered by package/request.

        Filtering uses only the persisted log itself; package directories and
        other original data locations are never touched.
        """
        results = self.load()
        if package_id is not None:
            results = [entry for entry in results if entry.get("package_id") == package_id]
        if request_hash is not None:

            def mentions(entry: dict[str, Any]) -> bool:
                if entry.get("request_hash") == request_hash:
                    return True
                return any(
                    item.get("request_hash") == request_hash
                    for item in entry.get("requests", [])
                )

            results = [entry for entry in results if mentions(entry)]
        return results


def manifest_identity(manifest: dict[str, Any]) -> str:
    """Content digest of a freeze manifest, independent of file location."""
    return hashlib.sha256(canonical_json(manifest)).hexdigest()
