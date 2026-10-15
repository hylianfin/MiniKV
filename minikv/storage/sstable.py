"""SSTable: immutable sorted file written by memtable flush.

Layout::

    [data block 0] ... [data block N]   entries + u32 CRC each
    [index block]                       sparse index: first_key -> (offset, size)
    [bloom block]                       serialized BloomFilter over all keys
    [footer]                            offsets, entry count, min/max key,
                                        magic, crc32, trailing footer length

Data block entries: ``[klen u32][vlen u32][kind u8][key][value]``; kind
reuses :class:`RecordType` (DELETE entries are tombstones, ``vlen`` = 0).
The footer ends with its own length (u32) so the reader can locate it from
EOF despite the variable-length min/max keys; its CRC covers everything
from ``index_offset`` through ``magic``.

Write protocol (crash safety): build ``<name>.tmp``, fsync, rename to the
final name, fsync the directory. ``flush_memtable`` then records the file
in the manifest. Readers only ever open manifest-listed files, so stray
``*.tmp`` litter from a crash is ignored.
"""

from __future__ import annotations

import os
import re
import struct
import zlib
from bisect import bisect_right
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from minikv.storage.bloom import BloomFilter
from minikv.storage.manifest import ManifestStore, SSTableMeta, fsync_dir
from minikv.storage.memtable import EntryKind, MemTable
from minikv.storage.wal import RecordType

SSTABLE_MAGIC = 0x4D4B5653544231
_DATA_ENTRY_HEAD = struct.Struct("<IIB")  # klen, vlen, kind
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_INDEX_COUNT = struct.Struct("<I")
_INDEX_ENTRY_HEAD = struct.Struct("<IQI")  # klen, offset, size
_FOOTER_HEAD = struct.Struct("<QIQIQ")  # index_off, index_size, bloom_off, bloom_size, count
_MIN_FOOTER_LEN = _FOOTER_HEAD.size + 4 + 4 + _U64.size + _U32.size  # + empty keys
_MIN_FILE_LEN = _MIN_FOOTER_LEN + _U32.size  # + trailing footer length
_SST_NAME = re.compile(r"sst_(\d+)_t\d+\.sst")
_TMP_SUFFIX = ".tmp"

_KIND_MAP = {EntryKind.PUT: RecordType.PUT, EntryKind.DELETE: RecordType.DELETE}


class CorruptSSTableError(Exception):
    """An SSTable file fails structural/CRC checks."""


@dataclass(frozen=True, slots=True)
class SSTEntry:
    key: bytes
    kind: RecordType
    value: bytes  # b"" for tombstones


@dataclass(frozen=True, slots=True)
class _Footer:
    index_off: int
    index_size: int
    bloom_off: int
    bloom_size: int
    count: int
    min_key: bytes
    max_key: bytes


class SSTableWriter:
    """One-pass writer; keys must arrive in strictly increasing order
    (``flush_memtable`` feeds it from ``memtable.items()``). Not thread-safe."""

    def __init__(
        self,
        path: Path | str,
        block_bytes: int = 4096,
        expected_keys: int = 0,
        bloom_bits_per_key: int = 10,
    ) -> None:
        if block_bytes <= 0:
            raise ValueError("block_bytes must be positive")
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._block_bytes = block_bytes
        self._f: BinaryIO | None = open(self._path, "wb")  # noqa: PTH123
        self._block = bytearray()
        self._block_first: bytes | None = None
        self._index: list[tuple[bytes, int, int]] = []
        self._bloom = BloomFilter.sized(max(expected_keys, 1), bloom_bits_per_key)
        self._offset = 0
        self._count = 0
        self._min_key: bytes | None = None
        self._last_key: bytes | None = None
        self._finished = False

    def add(self, key: bytes, kind: RecordType, value: bytes) -> None:
        if self._finished:
            raise RuntimeError("writer is finished")
        if not isinstance(kind, RecordType):
            raise TypeError("kind must be a RecordType")
        key, value = bytes(key), bytes(value)
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("keys must be added in strictly increasing order")
        entry = _DATA_ENTRY_HEAD.pack(len(key), len(value), kind.value) + key + value
        if self._block and len(self._block) + len(entry) > self._block_bytes:
            self._flush_block()
        if not self._block:
            self._block_first = key
        self._block += entry
        self._bloom.add(key)
        if self._min_key is None:
            self._min_key = key
        self._last_key = key
        self._count += 1

    def finish(self) -> tuple[int, bytes, bytes]:
        """Flush the index/bloom/footer, fsync, close. Returns
        ``(entry_count, min_key, max_key)``."""
        f = self._require_open()
        if self._count == 0:
            f.close()
            self._f = None
            self._finished = True
            raise ValueError("cannot finish an SSTable with no entries")
        if self._block:
            self._flush_block()
        index_off = self._offset
        index_bytes = _INDEX_COUNT.pack(len(self._index)) + b"".join(
            _INDEX_ENTRY_HEAD.pack(len(k), off, size) + k for k, off, size in self._index
        )
        f.write(index_bytes)
        bloom_off = index_off + len(index_bytes)
        bloom_bytes = self._bloom.to_bytes()
        f.write(bloom_bytes)
        assert self._min_key is not None and self._last_key is not None
        footer = (
            _FOOTER_HEAD.pack(index_off, len(index_bytes), bloom_off, len(bloom_bytes), self._count)
            + _U32.pack(len(self._min_key))
            + self._min_key
            + _U32.pack(len(self._last_key))
            + self._last_key
            + _U64.pack(SSTABLE_MAGIC)
        )
        footer += _U32.pack(zlib.crc32(footer) & 0xFFFFFFFF)
        footer += _U32.pack(len(footer))
        f.write(footer)
        f.flush()
        os.fsync(f.fileno())
        f.close()
        self._f = None
        self._finished = True
        return self._count, self._min_key, self._last_key

    def _flush_block(self) -> None:
        f = self._require_open()
        assert self._block_first is not None
        data = bytes(self._block)
        f.write(data)
        f.write(_U32.pack(zlib.crc32(data) & 0xFFFFFFFF))
        self._index.append((self._block_first, self._offset, len(data) + _U32.size))
        self._offset += len(data) + _U32.size
        self._block = bytearray()
        self._block_first = None

    def _require_open(self) -> BinaryIO:
        if self._f is None:
            raise RuntimeError("writer is finished")
        return self._f


class SSTableReader:
    """Reader over one SSTable file. Index and bloom are loaded up front;
    data blocks are read (and CRC-checked) on demand. Not thread-safe."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._f: BinaryIO | None = open(self._path, "rb")  # noqa: PTH123
        try:
            footer = self._read_footer()
            self._entry_count = footer.count
            self._min_key = footer.min_key
            self._max_key = footer.max_key
            self._index = self._read_index(footer.index_off, footer.index_size)
            self._bloom = self._read_bloom(footer.bloom_off, footer.bloom_size)
        except BaseException:
            f, self._f = self._f, None
            if f is not None:
                f.close()
            raise
        self._first_keys = [k for k, _, _ in self._index]

    @property
    def path(self) -> Path:
        return self._path

    @property
    def entry_count(self) -> int:
        return self._entry_count

    @property
    def min_key(self) -> bytes:
        return self._min_key

    @property
    def max_key(self) -> bytes:
        return self._max_key

    def get(self, key: bytes) -> SSTEntry | None:
        """Entry for ``key`` (tombstones included) or ``None``."""
        key = bytes(key)
        # Range check first (exact), bloom second (probabilistic): a miss in
        # either avoids all block I/O.
        if key < self._min_key or key > self._max_key:
            return None
        if key not in self._bloom:
            return None
        _, off, size = self._index[max(bisect_right(self._first_keys, key) - 1, 0)]
        for entry in self._read_block(off, size):
            if entry.key == key:
                return entry
        return None

    def iter_from(self, start: bytes) -> Iterator[SSTEntry]:
        """All entries with ``key >= start`` in order."""
        start = bytes(start)
        if start > self._max_key:
            return
        idx = max(bisect_right(self._first_keys, start) - 1, 0)
        for _, off, size in self._index[idx:]:
            for entry in self._read_block(off, size):
                if entry.key >= start:
                    yield entry

    def __iter__(self) -> Iterator[SSTEntry]:
        return self.iter_from(b"")

    def close(self) -> None:
        f, self._f = self._f, None
        if f is not None:
            f.close()

    def __enter__(self) -> SSTableReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _read_footer(self) -> _Footer:
        f = self._require_open()
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size < _MIN_FILE_LEN:
            raise CorruptSSTableError("file too short for a footer")
        f.seek(size - _U32.size)
        (footer_len,) = _U32.unpack(_require_len(f.read(_U32.size), _U32.size))
        if footer_len < _MIN_FOOTER_LEN or footer_len > size - _U32.size:
            raise CorruptSSTableError("bad footer length")
        f.seek(size - _U32.size - footer_len)
        footer = _require_len(f.read(footer_len), footer_len)
        body, stored_crc = footer[:-4], footer[-4:]
        if zlib.crc32(body) & 0xFFFFFFFF != _U32.unpack(stored_crc)[0]:
            raise CorruptSSTableError("footer CRC mismatch")
        index_off, index_size, bloom_off, bloom_size, count = _FOOTER_HEAD.unpack_from(body, 0)
        pos = _FOOTER_HEAD.size
        (min_len,) = _U32.unpack_from(body, pos)
        pos += 4
        min_key = body[pos : pos + min_len]
        pos += min_len
        if len(body) < pos + 4:
            raise CorruptSSTableError("footer key section truncated")
        (max_len,) = _U32.unpack_from(body, pos)
        pos += 4
        max_key = body[pos : pos + max_len]
        pos += max_len
        if len(body) != pos + _U64.size or _U64.unpack_from(body, pos)[0] != SSTABLE_MAGIC:
            raise CorruptSSTableError("bad footer magic")
        if count < 1 or index_size < _INDEX_COUNT.size or bloom_size == 0:
            raise CorruptSSTableError("bad footer counts")
        data_end = size - _U32.size - footer_len
        if index_off + index_size > data_end or bloom_off + bloom_size > data_end:
            raise CorruptSSTableError("footer offsets out of range")
        if bloom_off < index_off + index_size:
            raise CorruptSSTableError("bloom overlaps index")
        return _Footer(
            index_off=index_off,
            index_size=index_size,
            bloom_off=bloom_off,
            bloom_size=bloom_size,
            count=count,
            min_key=min_key,
            max_key=max_key,
        )

    def _read_index(self, off: int, size: int) -> list[tuple[bytes, int, int]]:
        f = self._require_open()
        f.seek(off)
        raw = _require_len(f.read(size), size)
        (count,) = _INDEX_COUNT.unpack_from(raw, 0)
        if count < 1:
            raise CorruptSSTableError("empty index")
        entries: list[tuple[bytes, int, int]] = []
        pos = _INDEX_COUNT.size
        prev: bytes | None = None
        for _ in range(count):
            if len(raw) < pos + _INDEX_ENTRY_HEAD.size:
                raise CorruptSSTableError("index truncated")
            klen, blk_off, blk_size = _INDEX_ENTRY_HEAD.unpack_from(raw, pos)
            pos += _INDEX_ENTRY_HEAD.size
            key = raw[pos : pos + klen]
            if len(key) != klen or blk_size < _U32.size + _DATA_ENTRY_HEAD.size:
                raise CorruptSSTableError("bad index entry")
            pos += klen
            # Index keys must be strictly increasing first-keys of blocks.
            if prev is not None and key <= prev:
                raise CorruptSSTableError("index keys out of order")
            prev = key
            entries.append((bytes(key), blk_off, blk_size))
        return entries

    def _read_bloom(self, off: int, size: int) -> BloomFilter:
        f = self._require_open()
        f.seek(off)
        try:
            return BloomFilter.from_bytes(_require_len(f.read(size), size))
        except ValueError as e:
            raise CorruptSSTableError(f"bad bloom block: {e}") from e

    def _read_block(self, off: int, size: int) -> list[SSTEntry]:
        f = self._require_open()
        f.seek(off)
        raw = _require_len(f.read(size), size)
        data, stored = raw[:-4], raw[-4:]
        if zlib.crc32(data) & 0xFFFFFFFF != _U32.unpack(stored)[0]:
            raise CorruptSSTableError("data block CRC mismatch")
        entries: list[SSTEntry] = []
        pos = 0
        while pos < len(data):
            if len(data) - pos < _DATA_ENTRY_HEAD.size:
                raise CorruptSSTableError("data block truncated")
            klen, vlen, kind_value = _DATA_ENTRY_HEAD.unpack_from(data, pos)
            pos += _DATA_ENTRY_HEAD.size
            if kind_value not in (RecordType.PUT, RecordType.DELETE):
                raise CorruptSSTableError("bad entry kind")
            if len(data) - pos < klen + vlen:
                raise CorruptSSTableError("entry overruns block")
            key = data[pos : pos + klen]
            value = data[pos + klen : pos + klen + vlen]
            pos += klen + vlen
            if kind_value == RecordType.DELETE and vlen != 0:
                raise CorruptSSTableError("tombstone carries a value")
            entries.append(SSTEntry(bytes(key), RecordType(kind_value), bytes(value)))
        return entries

    def _require_open(self) -> BinaryIO:
        if self._f is None:
            raise RuntimeError("reader is closed")
        return self._f


def _require_len(data: bytes, want: int) -> bytes:
    if len(data) != want:
        raise CorruptSSTableError("unexpected EOF")
    return data


def flush_memtable(
    memtable: MemTable,
    data_dir: Path | str,
    store: ManifestStore,
    tier: int = 0,
    block_bytes: int = 4096,
    bloom_bits_per_key: int = 10,
    fault: Callable[[str], None] | None = None,
) -> SSTableMeta | None:
    """Write ``memtable`` to a new SSTable and record it in the manifest.

    Steps: build ``*.tmp`` -> fsync -> rename -> fsync dir -> manifest
    update. ``fault`` fires at ``"after_write"`` / ``"after_rename"`` /
    ``"after_manifest"`` and may raise to simulate a crash there; any failure
    leaves the data intact (memtable + WAL still hold it, or the manifest
    already lists the complete SSTable). Returns ``None`` for an empty
    memtable without touching disk state.
    """
    directory = Path(data_dir)
    if len(memtable) == 0:
        return None
    seq = _next_sst_seq(directory)
    name = f"sst_{seq:06d}_t{tier}.sst"
    tmp = directory / (name + _TMP_SUFFIX)
    final = directory / name
    writer = SSTableWriter(
        tmp, block_bytes, expected_keys=len(memtable), bloom_bits_per_key=bloom_bits_per_key
    )
    try:
        for key, entry in memtable.items():
            writer.add(key, _KIND_MAP[entry.kind], entry.value)
        count, min_key, max_key = writer.finish()
        if fault is not None:
            fault("after_write")
        os.replace(tmp, final)
        fsync_dir(directory)
        if fault is not None:
            fault("after_rename")
        meta = SSTableMeta(
            name=name, tier=tier, min_key=min_key, max_key=max_key, entry_count=count
        )
        manifest = store.load()
        manifest.tables.append(meta)
        store.save(manifest)
        if fault is not None:
            fault("after_manifest")
        return meta
    except BaseException:
        # Best-effort tmp cleanup; a real crash would leave litter, which
        # manifest-driven readers ignore (covered by test).
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _next_sst_seq(directory: Path) -> int:
    # Scan the directory (not just the manifest) so a crashed flush's
    # renamed-but-unlisted file can never have its name reused. Stray
    # *.tmp files reserve their sequence number the same way.
    best = 0
    if directory.exists():
        for child in directory.iterdir():
            stem = (
                child.name[: -len(_TMP_SUFFIX)] if child.name.endswith(_TMP_SUFFIX) else child.name
            )
            m = _SST_NAME.fullmatch(stem)
            if m:
                best = max(best, int(m.group(1)))
    return best + 1
