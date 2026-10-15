"""Memtable: mutable in-memory sorted overlay for the LSM engine.

Wraps :class:`SkipList` and stores one :class:`MemEntry` per key:
``(key, seq, kind, value)`` where ``kind`` is PUT or DELETE (tombstone).
A delete never physically removes the key -- the tombstone must survive
until compaction reaches the oldest data, otherwise a stale copy in an
older SSTable would reappear.

Flush rotation (immutable -> SSTable, background thread) lives in the LSM
layer (M3/M4); here ``make_immutable`` only freezes writes. This class is
not thread-safe -- the LSM serializes access.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum

from minikv.storage.skiplist import SkipList

MAX_KEY_SIZE = 1024
MAX_VALUE_SIZE = 1024 * 1024
_ENTRY_OVERHEAD_BYTES = 16  # seq u64 + kind tag + slack


class EntryKind(Enum):
    PUT = 1
    DELETE = 2


@dataclass(frozen=True, slots=True)
class MemEntry:
    key: bytes
    seq: int
    kind: EntryKind
    value: bytes  # b"" when kind is DELETE


def _mem_value_size(entry: MemEntry) -> int:
    # The key length is already charged by the skip list node itself.
    return len(entry.value) + _ENTRY_OVERHEAD_BYTES


class MemTable:
    """Mutable sorted map of ``bytes`` -> live value or tombstone."""

    def __init__(
        self,
        max_size_bytes: int = 4 * 1024 * 1024,
        rng: random.Random | None = None,
    ) -> None:
        if max_size_bytes <= 0:
            raise ValueError("max_size_bytes must be positive")
        self._max_size_bytes = max_size_bytes
        self._table: SkipList[MemEntry] = SkipList(rng=rng, value_size=_mem_value_size)
        self._immutable = False

    def __len__(self) -> int:
        # Counts every stored entry, tombstones included.
        return len(self._table)

    def __contains__(self, key: object) -> bool:
        # Live-key membership: a tombstoned key is NOT contained.
        if not isinstance(key, (bytes, bytearray)):
            return False
        return self.get(bytes(key)) is not None

    @property
    def max_size_bytes(self) -> int:
        return self._max_size_bytes

    @property
    def size_bytes(self) -> int:
        return self._table.memory_size

    @property
    def is_immutable(self) -> bool:
        return self._immutable

    @property
    def should_flush(self) -> bool:
        return self.size_bytes >= self._max_size_bytes

    def put(self, key: bytes, value: bytes, seq: int) -> None:
        self._require_mutable()
        k = _checked_key(key)
        v = _checked_value(value)
        self._table.put(k, MemEntry(key=k, seq=_checked_seq(seq), kind=EntryKind.PUT, value=v))

    def delete(self, key: bytes, seq: int) -> None:
        # Always writes a tombstone, even for absent keys, so the delete
        # shadows copies of the key in older SSTables after flush.
        self._require_mutable()
        k = _checked_key(key)
        self._table.put(k, MemEntry(key=k, seq=_checked_seq(seq), kind=EntryKind.DELETE, value=b""))

    def get(self, key: bytes) -> bytes | None:
        """Live value, or ``None`` when missing or tombstoned."""
        entry = self.get_entry(key)
        if entry is None or entry.kind is EntryKind.DELETE:
            return None
        return entry.value

    def get_entry(self, key: bytes) -> MemEntry | None:
        """Raw entry including tombstones (``None`` only when absent)."""
        if not isinstance(key, (bytes, bytearray)):
            raise TypeError("key must be bytes")
        return self._table.get(bytes(key))

    def make_immutable(self) -> None:
        self._immutable = True

    def items(self) -> Iterator[tuple[bytes, MemEntry]]:
        """All entries in key order, tombstones included."""
        return self._table.items()

    def seek(self, key: bytes) -> Iterator[tuple[bytes, MemEntry]]:
        """Entries with ``k >= key`` in order, tombstones included."""
        if not isinstance(key, (bytes, bytearray)):
            raise TypeError("key must be bytes")
        return self._table.seek(bytes(key))

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, MemEntry]]:
        """Entries in ``[start, end)``; ``None`` means open-ended."""
        if start is not None and not isinstance(start, (bytes, bytearray)):
            raise TypeError("start must be bytes or None")
        if end is not None and not isinstance(end, (bytes, bytearray)):
            raise TypeError("end must be bytes or None")
        if start is not None and end is not None and bytes(end) <= bytes(start):
            return
        it = self._table.seek(bytes(start) if start is not None else b"")
        for k, entry in it:
            if end is not None and k >= bytes(end):
                break
            yield (k, entry)

    def _require_mutable(self) -> None:
        if self._immutable:
            raise RuntimeError("memtable is immutable")


def _checked_key(key: object) -> bytes:
    if not isinstance(key, (bytes, bytearray)):
        raise TypeError("key must be bytes")
    key = bytes(key)
    if len(key) > MAX_KEY_SIZE:
        raise ValueError(f"key too large: {len(key)} > {MAX_KEY_SIZE}")
    return key


def _checked_value(value: object) -> bytes:
    if not isinstance(value, (bytes, bytearray)):
        raise TypeError("value must be bytes")
    value = bytes(value)
    if len(value) > MAX_VALUE_SIZE:
        raise ValueError(f"value too large: {len(value)} > {MAX_VALUE_SIZE}")
    return value


def _checked_seq(seq: int) -> int:
    if seq < 0:
        raise ValueError("seq must be >= 0")
    return seq
