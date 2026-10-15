"""Write-ahead log: append-only, CRC-checked, torn-tail safe.

Record format (all integers little-endian)::

    +--------+--------+------+------------+--------+-----+--------+-------+
    | crc32  | length | type | raft_index | klen   | key | vlen   | value |
    | u32    | u32    | u8   | u64        | u32    |     | u32    |       |
    +--------+--------+------+------------+--------+-----+--------+-------+

``type``: 1 = PUT, 2 = DELETE (``vlen`` = 0, no value bytes).
``length`` counts the bytes after the length field; ``crc32`` covers the
length field through the value (everything after the crc32 field).

Recovery rule: replay records until EOF or the first invalid record (bad
CRC, short read, or malformed body), then truncate the file at the last
good record. A torn tail (short read) never raises -- an unwritten suffix
cannot contain acknowledged data. A fully-present but corrupt record with
no valid record before it raises :class:`CorruptWALError`: at offset 0
there is no prefix to fall back on, so silence would risk hiding garbage
(see DECISIONS.md D4).
"""

from __future__ import annotations

import os
import struct
import zlib
from dataclasses import dataclass
from enum import Enum, IntEnum
from pathlib import Path
from typing import BinaryIO

from minikv.storage.memtable import MAX_KEY_SIZE, MAX_VALUE_SIZE

_HEADER = struct.Struct("<II")  # crc32, length
_TYPE_RAFT = struct.Struct("<BQ")  # type u8, raft_index u64
_LEN = struct.Struct("<I")  # klen / vlen
_MIN_BODY_LEN = 1 + 8 + 4 + 4  # type + raft_index + klen + vlen (empty key/value)
MAX_RECORD_BODY_LEN = _MIN_BODY_LEN + MAX_KEY_SIZE + MAX_VALUE_SIZE


class FsyncPolicy(Enum):
    """Durability knob. ``batch`` fsyncs only on :meth:`WAL.fsync`/``close``;
    ``never`` is for benchmarks -- a crash may lose any suffix."""

    ALWAYS = "always"
    BATCH = "batch"
    NEVER = "never"


class RecordType(IntEnum):
    PUT = 1
    DELETE = 2


@dataclass(frozen=True, slots=True)
class WalRecord:
    kind: RecordType
    key: bytes
    value: bytes  # b"" when kind is DELETE
    raft_index: int


class CorruptWALError(Exception):
    """The WAL starts with a fully-present but invalid record."""


def encode_record(record: WalRecord) -> bytes:
    """Serialize one record (also used by tests to find record boundaries)."""
    if not isinstance(record.kind, RecordType):
        raise TypeError("kind must be a RecordType")
    key = _checked_key(record.key)
    value = _checked_value(record.value)
    if record.kind is RecordType.DELETE and value != b"":
        raise ValueError("DELETE records carry no value")
    raft_index = _checked_raft_index(record.raft_index)
    body = (
        _TYPE_RAFT.pack(record.kind.value, raft_index)
        + _LEN.pack(len(key))
        + key
        + _LEN.pack(len(value))
        + value
    )
    length_bytes = _LEN.pack(len(body))
    crc = zlib.crc32(length_bytes + body) & 0xFFFFFFFF
    return _HEADER.pack(crc, len(body)) + body


def decode_prefix(raw: bytes) -> tuple[list[WalRecord], int]:
    """Parse records from the start of ``raw``.

    Returns ``(records, valid_end)`` where ``valid_end`` is the offset just
    past the last good record. Raises :class:`CorruptWALError` only when the
    first record (offset 0) is fully present but invalid.
    """
    records: list[WalRecord] = []
    off = 0
    total = len(raw)
    while True:
        if off == total:
            return records, off  # clean EOF on a record boundary
        if total - off < _HEADER.size:
            return records, off  # torn header: crash mid-write, drop the tail
        crc, length = _HEADER.unpack_from(raw, off)
        if length > MAX_RECORD_BODY_LEN or length < _MIN_BODY_LEN:
            return _invalid_at(off, records)
        end = off + _HEADER.size + length
        if end > total:
            return records, off  # torn body: crash mid-write, drop the tail
        # CRC covers the length field through the value (everything past crc32).
        if zlib.crc32(raw[off + _LEN.size : end]) & 0xFFFFFFFF != crc:
            return _invalid_at(off, records)
        record = _parse_body(raw[off + _HEADER.size : end])
        if record is None:
            return _invalid_at(off, records)
        records.append(record)
        off = end


def _invalid_at(off: int, records: list[WalRecord]) -> tuple[list[WalRecord], int]:
    if off == 0:
        raise CorruptWALError("WAL starts with an invalid record: refusing to guess")
    # A corrupt record poisons everything after it (later "valid" bytes may
    # be coincidental), so the file is truncated here by the caller.
    return records, off


def _parse_body(body: bytes) -> WalRecord | None:
    kind_value, raft_index = _TYPE_RAFT.unpack_from(body, 0)
    if kind_value not in (RecordType.PUT, RecordType.DELETE):
        return None
    (klen,) = _LEN.unpack_from(body, 9)
    if klen > MAX_KEY_SIZE:
        return None
    vlen_off = 13 + klen
    if len(body) < vlen_off + 4:
        return None
    (vlen,) = _LEN.unpack_from(body, vlen_off)
    if vlen > MAX_VALUE_SIZE:
        return None
    value = body[vlen_off + 4 :]
    if len(value) != vlen or 13 + klen + 4 + vlen != len(body):
        return None
    if kind_value == RecordType.DELETE and vlen != 0:
        return None
    return WalRecord(
        kind=RecordType(kind_value),
        key=bytes(body[13 : 13 + klen]),
        value=bytes(value),
        raft_index=raft_index,
    )


class WAL:
    """Append-only log file with crash-safe recovery.

    One WAL protects one memtable; the LSM deletes the file once its
    memtable's SSTable is in the manifest. Not thread-safe.
    """

    def __init__(self, path: Path | str, fsync_policy: FsyncPolicy = FsyncPolicy.ALWAYS) -> None:
        self._path = Path(path)
        self._fsync_policy = fsync_policy
        self._f: BinaryIO | None = None
        self._record_count = 0

    @property
    def path(self) -> Path:
        return self._path

    @property
    def fsync_policy(self) -> FsyncPolicy:
        return self._fsync_policy

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def size_bytes(self) -> int:
        # What the OS sees; may lag buffered-but-unflushed batch writes.
        if not self._path.exists():
            return 0
        return self._path.stat().st_size

    def open(self) -> list[WalRecord]:
        """Recover the log and open it for append. Returns replayed records."""
        if self._f is not None:
            raise RuntimeError("WAL is already open")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._f = open(self._path, "ab")  # noqa: PTH123
            self._record_count = 0
            return []
        records, valid_end = decode_prefix(self._path.read_bytes())
        if valid_end != self._path.stat().st_size:
            with open(self._path, "r+b") as f:  # noqa: PTH123
                f.truncate(valid_end)
        self._f = open(self._path, "ab")  # noqa: PTH123
        self._record_count = len(records)
        return records

    def append(self, record: WalRecord) -> None:
        data = encode_record(record)  # validates before touching the file
        f = self._require_open()
        f.write(data)
        if self._fsync_policy is FsyncPolicy.ALWAYS:
            f.flush()
            os.fsync(f.fileno())
        self._record_count += 1

    def append_put(self, key: bytes, value: bytes, raft_index: int) -> None:
        self.append(
            WalRecord(
                kind=RecordType.PUT,
                key=_checked_key(key),
                value=_checked_value(value),
                raft_index=_checked_raft_index(raft_index),
            )
        )

    def append_delete(self, key: bytes, raft_index: int) -> None:
        self.append(
            WalRecord(
                kind=RecordType.DELETE,
                key=_checked_key(key),
                value=b"",
                raft_index=_checked_raft_index(raft_index),
            )
        )

    def fsync(self) -> None:
        """Flush + fsync (flush only under ``never``). Batch-mode commit point."""
        f = self._require_open()
        f.flush()
        if self._fsync_policy is not FsyncPolicy.NEVER:
            os.fsync(f.fileno())

    def close(self) -> None:
        # A clean close is durable under always/batch (fsync), best-effort
        # under never. Only a crash loses un-fsync'd batch data.
        f, self._f = self._f, None
        if f is None:
            return
        try:
            f.flush()
            if self._fsync_policy is not FsyncPolicy.NEVER:
                os.fsync(f.fileno())
        finally:
            f.close()

    def __enter__(self) -> WAL:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _require_open(self) -> BinaryIO:
        if self._f is None:
            raise RuntimeError("WAL is not open")
        return self._f


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


def _checked_raft_index(raft_index: int) -> int:
    if raft_index < 0:
        raise ValueError("raft_index must be >= 0")
    return raft_index
