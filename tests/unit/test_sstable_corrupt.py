"""Corruption-matrix tests: every structural rejection in the SSTable reader.

Each case mutates a valid file (fixing checksums around the target so the
failure lands on the intended check) and asserts CorruptSSTableError.
Index/footer faults surface on open (parsed eagerly); data-block faults
surface on read (blocks are loaded on demand).
"""

import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

import pytest

from minikv.storage.manifest import ManifestStore
from minikv.storage.memtable import MemTable
from minikv.storage.sstable import CorruptSSTableError, SSTableReader, SSTableWriter, flush_memtable
from minikv.storage.wal import RecordType

HEAD = struct.Struct("<QIQIQ")  # index_off, index_size, bloom_off, bloom_size, count
U32 = struct.Struct("<I")
U64 = struct.Struct("<Q")


@dataclass
class Layout:
    raw: bytearray
    footer_start: int
    footer_len: int
    index_off: int
    index_size: int
    bloom_off: int
    bloom_size: int
    blocks: list[tuple[int, int]]


def _valid_file(tmp_path: Path, name: str = "v.sst") -> Path:
    entries = [
        (b"a", RecordType.PUT, b"va"),
        (b"b", RecordType.PUT, b"vb" * 50),
        (b"c", RecordType.DELETE, b""),
        (b"d", RecordType.PUT, b"vd"),
    ]
    path = tmp_path / name
    writer = SSTableWriter(path, block_bytes=64, expected_keys=len(entries))
    for key, kind, value in entries:
        writer.add(key, kind, value)
    writer.finish()
    assert len(_layout(path, min_blocks=2).blocks) >= 2
    return path


def _layout(path: Path, min_blocks: int = 0) -> Layout:
    raw = bytearray(path.read_bytes())
    (footer_len,) = U32.unpack(raw[-4:])
    fstart = len(raw) - 4 - footer_len
    index_off, index_size, bloom_off, bloom_size, count = HEAD.unpack_from(raw, fstart)
    (icount,) = U32.unpack_from(raw, index_off)
    blocks: list[tuple[int, int]] = []
    pos = index_off + 4
    for _ in range(icount):
        klen, off, size = struct.Struct("<IQI").unpack_from(raw, pos)
        pos += 20 + klen
        blocks.append((off, size))
    assert len(blocks) >= min_blocks
    return Layout(raw, fstart, footer_len, index_off, index_size, bloom_off, bloom_size, blocks)


def _fix_footer_crc(raw: bytearray, fstart: int, flen: int) -> None:
    raw[fstart + flen - 4 : fstart + flen] = U32.pack(
        zlib.crc32(raw[fstart : fstart + flen - 4]) & 0xFFFFFFFF
    )


def _fix_block_crc(raw: bytearray, off: int, size: int) -> None:
    raw[off + size - 4 : off + size] = U32.pack(zlib.crc32(raw[off : off + size - 4]) & 0xFFFFFFFF)


def _write_mutated(tmp_path: Path, layout: Layout, name: str) -> Path:
    path = tmp_path / name
    path.write_bytes(bytes(layout.raw))
    return path


def _expect_open_raise(path: Path) -> None:
    with pytest.raises(CorruptSSTableError):
        SSTableReader(path)


def _expect_read_raise(path: Path, key: bytes = b"a") -> None:
    with SSTableReader(path) as reader:
        with pytest.raises(CorruptSSTableError):
            reader.get(key)


def test_footer_rejections(tmp_path: Path) -> None:
    base = _valid_file(tmp_path)

    lay = _layout(base)
    lay.raw[lay.footer_start + 40] ^= 0xFF  # inside footer body
    _expect_open_raise(_write_mutated(tmp_path, lay, "f1.sst"))  # CRC mismatch

    lay = _layout(base)
    lay.raw[lay.footer_start + 32 : lay.footer_start + 36] = U32.pack(0xFFFFFF)  # min_len huge
    _fix_footer_crc(lay.raw, lay.footer_start, lay.footer_len)
    _expect_open_raise(_write_mutated(tmp_path, lay, "f2.sst"))  # key section truncated

    lay = _layout(base)
    magic_at = lay.footer_start + lay.footer_len - 4 - 8
    lay.raw[magic_at : magic_at + 8] = b"\x00" * 8
    _fix_footer_crc(lay.raw, lay.footer_start, lay.footer_len)
    _expect_open_raise(_write_mutated(tmp_path, lay, "f3.sst"))  # bad magic

    lay = _layout(base)
    lay.raw[lay.footer_start + 24 : lay.footer_start + 32] = U64.pack(0)  # count = 0
    _fix_footer_crc(lay.raw, lay.footer_start, lay.footer_len)
    _expect_open_raise(_write_mutated(tmp_path, lay, "f4.sst"))  # bad counts

    lay = _layout(base)
    lay.raw[lay.footer_start : lay.footer_start + 8] = U64.pack(0xFFFFFF)  # index_off huge
    _fix_footer_crc(lay.raw, lay.footer_start, lay.footer_len)
    _expect_open_raise(_write_mutated(tmp_path, lay, "f5.sst"))  # offsets out of range

    lay = _layout(base)
    # bloom_off sits at head offset 12 (Q index_off, I index_size, then Q).
    lay.raw[lay.footer_start + 12 : lay.footer_start + 20] = U64.pack(lay.index_off)
    _fix_footer_crc(lay.raw, lay.footer_start, lay.footer_len)
    _expect_open_raise(_write_mutated(tmp_path, lay, "f6.sst"))  # bloom overlaps index


def test_index_rejections(tmp_path: Path) -> None:
    base = _valid_file(tmp_path)

    lay = _layout(base)
    lay.raw[lay.index_off : lay.index_off + 4] = U32.pack(0)  # count = 0
    _expect_open_raise(_write_mutated(tmp_path, lay, "i1.sst"))  # empty index

    lay = _layout(base)
    lay.raw[lay.index_off : lay.index_off + 4] = U32.pack(500)  # count huge
    _expect_open_raise(_write_mutated(tmp_path, lay, "i2.sst"))  # index truncated

    lay = _layout(base)
    lay.raw[lay.index_off + 4 : lay.index_off + 8] = U32.pack(0xFFFFFF)  # klen huge
    _expect_open_raise(_write_mutated(tmp_path, lay, "i3.sst"))  # bad index entry

    lay = _layout(base)
    # Index entry heads are "<IQI" = 16 bytes: klen[0:4], off[4:12], size[12:16].
    (first_klen,) = U32.unpack_from(lay.raw, lay.index_off + 4)
    key2_head = lay.index_off + 4 + 16 + first_klen
    (key2_len,) = U32.unpack_from(lay.raw, key2_head)
    key2_at = key2_head + 16
    assert key2_len == len(lay.raw[key2_at : key2_at + key2_len]) and key2_len > 0
    lay.raw[key2_at : key2_at + key2_len] = b"\x00" * key2_len
    _expect_open_raise(_write_mutated(tmp_path, lay, "i4.sst"))  # index out of order

    lay = _layout(base)
    lay.raw[lay.index_off + 12 : lay.index_off + 16] = U32.pack(0xFFFFFF)  # blk_size huge
    path = _write_mutated(tmp_path, lay, "i5.sst")
    _expect_read_raise(path)  # short block read -> unexpected EOF


def test_bloom_rejection(tmp_path: Path) -> None:
    lay = _layout(_valid_file(tmp_path))
    lay.raw[lay.bloom_off : lay.bloom_off + 8] = b"\x00" * 8  # num_bits = 0
    _expect_open_raise(_write_mutated(tmp_path, lay, "bl.sst"))


def test_data_block_rejections(tmp_path: Path) -> None:
    base = _valid_file(tmp_path)

    lay = _layout(base)
    off, size = lay.blocks[0]
    lay.raw[off + 8] = 9  # kind byte -> unknown
    _fix_block_crc(lay.raw, off, size)
    _expect_read_raise(_write_mutated(tmp_path, lay, "d1.sst"))  # bad entry kind

    lay = _layout(base)
    off, size = lay.blocks[0]
    lay.raw[off : off + 4] = U32.pack(0xFFFFFF)  # klen huge
    _fix_block_crc(lay.raw, off, size)
    _expect_read_raise(_write_mutated(tmp_path, lay, "d2.sst"))  # entry overruns block

    lay = _layout(base)
    off, size = lay.blocks[0]
    lay.raw[off + 8] = 2  # entry "a" is a PUT with value -> DELETE with value
    _fix_block_crc(lay.raw, off, size)
    _expect_read_raise(_write_mutated(tmp_path, lay, "d3.sst"))  # tombstone w/ value


def test_data_block_truncated_entry(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    writer = SSTableWriter(path, block_bytes=4096, expected_keys=2)
    writer.add(b"k1", RecordType.PUT, b"v" * 20)
    writer.add(b"k2", RecordType.PUT, b"w" * 20)
    writer.finish()
    lay = _layout(path)
    off, size = lay.blocks[0]
    data_len = size - 4
    (klen,) = U32.unpack_from(lay.raw, off)
    # Grow the first entry's vlen so exactly 5 bytes remain: truncated tail.
    lay.raw[off + 4 : off + 8] = U32.pack(data_len - 14 - klen)
    _fix_block_crc(lay.raw, off, size)
    _expect_read_raise(_write_mutated(tmp_path, lay, "t2.sst"), key=b"k1")


def test_lifecycle_edges(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="block_bytes"):
        SSTableWriter(tmp_path / "z.sst", block_bytes=0)
    writer = SSTableWriter(tmp_path / "z.sst")
    writer.add(b"k", RecordType.PUT, b"v")
    writer.finish()
    with pytest.raises(RuntimeError):
        writer.finish()  # already finished
    reader = SSTableReader(tmp_path / "z.sst")
    assert reader.path == tmp_path / "z.sst"
    reader.close()
    with pytest.raises(RuntimeError):
        reader.get(b"k")  # closed


def test_writer_abort(tmp_path: Path) -> None:
    path = tmp_path / "a.sst"
    writer = SSTableWriter(path)
    writer.add(b"k", RecordType.PUT, b"v")
    writer.abort()
    assert not path.exists()
    with pytest.raises(RuntimeError):
        writer.abort()  # already finished via abort
    with pytest.raises(RuntimeError):
        writer.add(b"k2", RecordType.PUT, b"v")  # finished


def test_abort_unlink_failure_swallowed(tmp_path: Path) -> None:
    import os

    if os.geteuid() == 0:
        pytest.skip("read-only dirs are still writable as root")
    sub = tmp_path / "sub"
    sub.mkdir()
    writer = SSTableWriter(sub / "a.sst")
    writer.add(b"k", RecordType.PUT, b"v")
    os.chmod(sub, 0o555)
    try:
        writer.abort()  # tmp deletion fails, swallowed
    finally:
        os.chmod(sub, 0o755)


def test_tmp_cleanup_oserror_is_swallowed(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("read-only dirs still unlinkable as root")

    class Crash(Exception):
        pass

    def crash(at: str) -> None:
        if at == "after_write":
            os.chmod(tmp_path, 0o555)
            raise Crash(at)

    mem = MemTable()
    mem.put(b"k", b"v", seq=1)
    try:
        with pytest.raises(Crash):
            flush_memtable(mem, tmp_path, ManifestStore(tmp_path), fault=crash)
    finally:
        os.chmod(tmp_path, 0o755)
