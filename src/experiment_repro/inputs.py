from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .records import sha256_file


@dataclass(frozen=True)
class FileInput:
    """An input file identified in records by byte content, not its path."""

    path: Path
    sha256: str
    bytes: int

    @classmethod
    def from_path(cls, path: str | Path) -> "FileInput":
        source = Path(path)
        return cls(source.resolve(), sha256_file(source), source.stat().st_size)

    def fingerprint_value(self) -> dict[str, str | int]:
        return {"type": "file", "sha256": self.sha256, "bytes": self.bytes}
