"""Manifest: JSON listing of live SSTables plus flush metadata.

Updated atomically: write ``MANIFEST.tmp``, fsync it, rename over
``MANIFEST``, fsync the directory. A crash lands on either the old or the
new manifest, never a mix, because the rename itself is atomic.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

MANIFEST_NAME = "MANIFEST"
_TMP_SUFFIX = ".tmp"


@dataclass(frozen=True, slots=True)
class SSTableMeta:
    name: str
    tier: int
    min_key: bytes
    max_key: bytes
    entry_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "tier": self.tier,
            "min_key": self.min_key.hex(),
            "max_key": self.max_key.hex(),
            "entry_count": self.entry_count,
        }

    @classmethod
    def from_dict(cls, raw: object) -> SSTableMeta:
        if not isinstance(raw, dict):
            raise ManifestCorruptError("table entry is not an object")
        try:
            name = raw["name"]
            tier = raw["tier"]
            min_key = bytes.fromhex(raw["min_key"])
            max_key = bytes.fromhex(raw["max_key"])
            entry_count = raw["entry_count"]
        except (KeyError, TypeError, ValueError) as e:
            raise ManifestCorruptError(f"bad table entry: {e}") from e
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(tier, int)
            or tier < 0
            or not isinstance(entry_count, int)
            or entry_count < 0
        ):
            raise ManifestCorruptError("bad table entry fields")
        return cls(name=name, tier=tier, min_key=min_key, max_key=max_key, entry_count=entry_count)


@dataclass
class Manifest:
    tables: list[SSTableMeta] = field(default_factory=list)
    flushed_raft_index: int = 0


class ManifestCorruptError(Exception):
    """MANIFEST exists but does not parse/validate."""


def fsync_dir(path: Path) -> None:
    """Durably publish directory entries (renames) inside ``path``."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ManifestStore:
    """Owns ``<dir>/MANIFEST``. The optional ``fault`` hook receives
    ``"after_tmp"`` / ``"after_rename"`` and may raise to simulate a crash
    between the atomic-update steps (used by crash-point tests)."""

    def __init__(self, directory: Path | str, fault: Callable[[str], None] | None = None) -> None:
        self._dir = Path(directory)
        self._fault = fault

    @property
    def directory(self) -> Path:
        return self._dir

    def load(self) -> Manifest:
        path = self._dir / MANIFEST_NAME
        if not path.exists():
            return Manifest()
        try:
            raw: object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise ManifestCorruptError(f"cannot parse MANIFEST: {e}") from e
        if not isinstance(raw, dict):
            raise ManifestCorruptError("MANIFEST root is not an object")
        tables_raw = raw.get("tables", [])
        flushed = raw.get("flushed_raft_index", 0)
        if not isinstance(tables_raw, list) or not isinstance(flushed, int) or flushed < 0:
            raise ManifestCorruptError("bad MANIFEST fields")
        return Manifest(
            tables=[SSTableMeta.from_dict(t) for t in tables_raw],
            flushed_raft_index=flushed,
        )

    def save(self, manifest: Manifest) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "tables": [t.to_dict() for t in manifest.tables],
                "flushed_raft_index": manifest.flushed_raft_index,
            },
            sort_keys=True,
        )
        tmp = self._dir / (MANIFEST_NAME + _TMP_SUFFIX)
        with open(tmp, "w", encoding="utf-8") as f:  # noqa: PTH123
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        self._fire("after_tmp")
        os.replace(tmp, self._dir / MANIFEST_NAME)
        self._fire("after_rename")
        fsync_dir(self._dir)

    def _fire(self, point: str) -> None:
        if self._fault is not None:
            self._fault(point)
