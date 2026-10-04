from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

IMPORT_HISTORY_SCHEMA_VERSION = "import-history/v1"


class ImportHistory:
    """Append-only, durable history of package imports on the receiver side.

    Entries live inside the receiver store (import-history/entries.jsonl) so
    they survive restarts and remain queryable after the original package
    directory and the origin data directory are gone. Every import attempt is
    recorded, including repeated imports, even when nothing new is installed.
    """

    def __init__(self, store_root: str | Path):
        self.root = Path(store_root) / "import-history"
        self.root.mkdir(parents=True, exist_ok=True)
        self.entries_path = self.root / "entries.jsonl"

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def append(self, entry: dict[str, Any]) -> dict[str, Any]:
        record = dict(entry)
        record["schema_version"] = IMPORT_HISTORY_SCHEMA_VERSION
        record.setdefault("recorded_at", self.now())
        line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        with self.entries_path.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()
            os.fsync(stream.fileno())
        return record

    def entries(
        self,
        *,
        package_id: str | None = None,
        request_hash: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return history entries, optionally filtered by package or request."""
        if not self.entries_path.is_file():
            return []
        result: list[dict[str, Any]] = []
        for line in self.entries_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            if package_id is not None and record.get("package_id") != package_id:
                continue
            if request_hash is not None:
                if not any(
                    item.get("request_hash") == request_hash
                    for item in record.get("requests", [])
                ):
                    continue
            result.append(record)
        return result

    def request_history(self, request_hash: str) -> list[dict[str, Any]]:
        """Return per-request outcome records drawn from every import entry."""
        outcomes: list[dict[str, Any]] = []
        for record in self.entries(request_hash=request_hash):
            for item in record.get("requests", []):
                if item.get("request_hash") == request_hash:
                    outcomes.append(
                        {
                            "request_hash": request_hash,
                            "package_id": record.get("package_id"),
                            "import_id": record.get("import_id"),
                            "kind": record.get("kind"),
                            "supplement": record.get("supplement", False),
                            "state": item.get("state"),
                            "status": item.get("status"),
                            "reason": item.get("reason", ""),
                            "detail": item.get("detail", ""),
                            "started_at": record.get("started_at"),
                            "ended_at": record.get("ended_at"),
                        }
                    )
        return outcomes
