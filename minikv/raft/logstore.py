"""Persistent Raft state: term/vote/progress meta plus the append-only log.

Files live directly in the given directory: ``raft.meta`` (small JSON with
term, voted_for, commit_index, applied_index, swapped atomically) and
``raft.log`` (binary entries)::

    +--------+--------+--------+--------+------+---------+
    | crc32  | length | term   | index  | type | payload |
    | u32    | u32    | u64    | u64    | u8   |         |
    +--------+--------+--------+--------+------+---------+

``length`` counts the bytes after the length field; ``crc32`` covers the
length field through the payload. Recovery mirrors the WAL rule: replay to
EOF or the first invalid record, truncate the torn/corrupt tail -- except a
fully-present but invalid FIRST record raises, since there is no valid
prefix to fall back on. Entry indexes must run 1, 2, 3... with no holes.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path
from typing import BinaryIO

from minikv.raft.types import EntryKind, LogEntry, PersistedState, RaftStore
from minikv.storage.manifest import fsync_dir

_HEADER = struct.Struct("<II")  # crc32, length
_FIXED = struct.Struct("<QQB")  # term u64, index u64, type u8
_META_NAME = "raft.meta"
_LOG_NAME = "raft.log"
_TMP_SUFFIX = ".tmp"


class CorruptLogError(Exception):
    """The raft log starts with an invalid record or breaks contiguity."""


def encode_entry(entry: LogEntry) -> bytes:
    """Serialize one entry (also used by tests to find record boundaries)."""
    if not isinstance(entry.kind, EntryKind):
        raise TypeError("kind must be an EntryKind")
    if entry.index < 1:
        raise ValueError("index must be >= 1")
    if entry.term < 0:
        raise ValueError("term must be >= 0")
    payload = bytes(entry.payload)
    body = _FIXED.pack(entry.term, entry.index, entry.kind.value) + payload
    length_bytes = struct.Struct("<I").pack(len(body))
    crc = zlib.crc32(length_bytes + body) & 0xFFFFFFFF
    return _HEADER.pack(crc, len(body)) + body


def decode_prefix(raw: bytes) -> tuple[list[LogEntry], int]:
    """Parse entries from ``raw``; return ``(entries, valid_end)``.

    Raises :class:`CorruptLogError` when the first record is invalid or an
    index gap appears (gaps always raise: unlike a torn tail, a gap means
    records are missing from the middle, which truncation cannot repair).
    """
    entries: list[LogEntry] = []
    off = 0
    total = len(raw)
    want = 1
    while True:
        if off == total:
            return entries, off
        if total - off < _HEADER.size:
            return entries, off  # torn header: crash mid-write
        crc, length = _HEADER.unpack_from(raw, off)
        if length < _FIXED.size:
            return _invalid_at(off, entries, want)
        end = off + _HEADER.size + length
        if end > total:
            return entries, off  # torn body: crash mid-write
        if zlib.crc32(raw[off + 4 : end]) & 0xFFFFFFFF != crc:
            return _invalid_at(off, entries, want)
        term, index, kind_value = _FIXED.unpack_from(raw, off + _HEADER.size)
        if kind_value not in (EntryKind.NOOP, EntryKind.COMMAND) or index != want:
            return _gap_or_corrupt(off, entries, want, index)
        payload = bytes(raw[off + _HEADER.size + _FIXED.size : end])
        entries.append(
            LogEntry(term=term, index=index, kind=EntryKind(kind_value), payload=payload)
        )
        want += 1
        off = end


def _invalid_at(off: int, entries: list[LogEntry], want: int) -> tuple[list[LogEntry], int]:
    if off == 0:
        raise CorruptLogError("raft log starts with an invalid record")
    return entries, off


def _gap_or_corrupt(
    off: int, entries: list[LogEntry], want: int, index: int
) -> tuple[list[LogEntry], int]:
    if index != want:
        raise CorruptLogError(f"raft log gap: want index {want}, found {index}")
    if off == 0:
        raise CorruptLogError("raft log starts with an invalid record")
    return entries, off


class LogStore(RaftStore):
    """File-backed RaftStore. Single owner (the node runtime); not thread-safe.

    ``persist()`` truncates any conflicting suffix from ``base_index``,
    appends, and fsyncs -- always, before the runtime may acknowledge. Meta
    (term/vote/commit/applied) is swapped atomically on every mutation the
    runtime reports.
    """

    def __init__(self, path: Path | str) -> None:
        self._dir = Path(path)
        self._meta_path = self._dir / _META_NAME
        self._log_path = self._dir / _LOG_NAME
        self._f: BinaryIO | None = None
        self._entries: list[LogEntry] = []
        self._log_size = 0  # durable bytes; mirrors the file length
        self._term = 0
        self._voted_for: str | None = None
        self._commit = 0
        self._applied = 0
        self._is_open = False

    def open(self) -> PersistedState:
        """Recover files, rebuild the offset index, truncate any torn tail."""
        if self._is_open:
            raise RuntimeError("log store is already open")
        self._dir.mkdir(parents=True, exist_ok=True)
        self._load_meta()
        raw = b""
        if self._log_path.exists():
            raw = self._log_path.read_bytes()
        entries, valid_end = decode_prefix(raw)
        if valid_end != len(raw):
            with open(self._log_path, "r+b") as f:  # noqa: PTH123
                f.truncate(valid_end)
        self._f = open(self._log_path, "ab")  # noqa: PTH123
        self._entries = entries
        self._log_size = valid_end
        # Clamp progress to the recovered log (a torn tail may have eaten it).
        self._commit = min(self._commit, len(entries))
        self._applied = min(self._applied, self._commit)
        self._is_open = True
        return self.load()

    def load(self) -> PersistedState:
        if not self._is_open:
            raise RuntimeError("log store is not open")
        return PersistedState(
            term=self._term,
            voted_for=self._voted_for,
            entries=tuple(self._entries),
            commit_index=self._commit,
            applied_index=self._applied,
        )

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        self._require_open()
        if term < 0:
            raise ValueError("term must be >= 0")
        self._term = term
        self._voted_for = voted_for
        self._save_meta()

    def persist(self, base_index: int, entries: tuple[LogEntry, ...]) -> None:
        """Truncate from ``base_index``, append ``entries``, fsync."""
        f = self._require_open()
        if base_index < 1 or base_index > len(self._entries) + 1:
            raise ValueError(f"base_index {base_index} out of range")
        if entries and entries[0].index != base_index:
            raise ValueError("entries must start at base_index")
        for position, entry in enumerate(entries):
            if entry.index != base_index + position:
                raise ValueError("entries must be contiguous")
        # Truncate any conflicting suffix, then append. One fsync covers
        # both, so a crash leaves the prefix or the whole batch, never a mix.
        removed = self._entries[base_index - 1 :]
        self._log_size -= sum(len(encode_entry(e)) for e in removed)
        del self._entries[base_index - 1 :]
        f.truncate(self._log_size)
        for entry in entries:
            data = encode_entry(entry)
            f.write(data)
            self._entries.append(entry)
            self._log_size += len(data)
        f.flush()
        os.fsync(f.fileno())

    def save_progress(self, commit_index: int, applied_index: int) -> None:
        self._require_open()
        if commit_index < 0 or applied_index < 0:
            raise ValueError("progress must be >= 0")
        if applied_index > commit_index:
            raise ValueError("applied_index cannot pass commit_index")
        if commit_index > len(self._entries):
            raise ValueError("commit_index cannot pass the log end")
        self._commit = commit_index
        self._applied = applied_index
        self._save_meta()

    def entry(self, index: int) -> LogEntry:
        self._require_open()
        if index < 1 or index > len(self._entries):
            raise IndexError(f"log index {index} out of range")
        return self._entries[index - 1]

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        return self.entry(index).term

    def last_index(self) -> int:
        self._require_open()
        return len(self._entries)

    def last_term(self) -> int:
        self._require_open()
        return self._entries[-1].term if self._entries else 0

    def close(self) -> None:
        f, self._f = self._f, None
        if f is not None:
            f.close()
        self._is_open = False

    def __enter__(self) -> LogStore:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _require_open(self) -> BinaryIO:
        if self._f is None or not self._is_open:
            raise RuntimeError("log store is not open")
        return self._f

    def _load_meta(self) -> None:
        if not self._meta_path.exists():
            return
        try:
            raw: object = json.loads(self._meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise CorruptLogError(f"cannot parse raft meta: {e}") from e
        if not isinstance(raw, dict):
            raise CorruptLogError("raft meta root is not an object")
        term = raw.get("term", 0)
        voted_for = raw.get("voted_for")
        commit = raw.get("commit_index", 0)
        applied = raw.get("applied_index", 0)
        if (
            not isinstance(term, int)
            or term < 0
            or (voted_for is not None and not isinstance(voted_for, str))
            or not isinstance(commit, int)
            or commit < 0
            or not isinstance(applied, int)
            or applied < 0
            or applied > commit
        ):
            raise CorruptLogError("bad raft meta fields")
        self._term, self._voted_for = term, voted_for
        self._commit, self._applied = commit, applied

    def _save_meta(self) -> None:
        payload = json.dumps(
            {
                "term": self._term,
                "voted_for": self._voted_for,
                "commit_index": self._commit,
                "applied_index": self._applied,
            },
            sort_keys=True,
        )
        tmp = self._dir / (_META_NAME + _TMP_SUFFIX)
        with open(tmp, "w", encoding="utf-8") as f:  # noqa: PTH123
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path)
        fsync_dir(self._dir)
