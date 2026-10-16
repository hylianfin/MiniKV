"""Unit tests for SSTable reader/writer."""

from pathlib import Path

import pytest

from minikv.storage.sstable import CorruptSSTableError, SSTableReader, SSTableWriter, SSTEntry
from minikv.storage.wal import RecordType


def _sample_entries() -> list[SSTEntry]:
    return [
        SSTEntry(b"a", RecordType.PUT, b"va"),
        SSTEntry(b"b", RecordType.PUT, b"x" * 300),  # forces multiple blocks
        SSTEntry(b"c", RecordType.DELETE, b""),
        SSTEntry(b"d", RecordType.PUT, b""),
        SSTEntry(b"e", RecordType.PUT, b"ve"),
    ]


def _write(path: Path, entries: list[SSTEntry], block_bytes: int = 128) -> Path:
    writer = SSTableWriter(path, block_bytes=block_bytes, expected_keys=len(entries))
    for e in entries:
        writer.add(e.key, e.kind, e.value)
    count, lo, hi = writer.finish()
    assert (count, lo, hi) == (len(entries), entries[0].key, entries[-1].key)
    return path


def test_flush_then_read_back_identical(tmp_path: Path) -> None:
    entries = _sample_entries()
    path = _write(tmp_path / "t.sst", entries)
    with SSTableReader(path) as reader:
        assert reader.entry_count == len(entries)
        assert (reader.min_key, reader.max_key) == (b"a", b"e")
        for e in entries:
            assert reader.get(e.key) == e
        assert list(reader) == entries


def test_get_miss_paths(tmp_path: Path) -> None:
    path = _write(tmp_path / "t.sst", _sample_entries())
    with SSTableReader(path) as reader:
        assert reader.get(b"0") is None  # below min
        assert reader.get(b"zzz") is None  # above max
        assert reader.get(b"bb") is None  # in range, absent
        assert reader.get(b"b\x00") is None  # in range, absent


def test_iter_from(tmp_path: Path) -> None:
    entries = _sample_entries()
    path = _write(tmp_path / "t.sst", entries)
    with SSTableReader(path) as reader:
        assert list(reader.iter_from(b"a")) == entries
        assert list(reader.iter_from(b"b")) == entries[1:]
        assert list(reader.iter_from(b"bb")) == entries[2:]
        assert list(reader.iter_from(b"e")) == entries[4:]
        assert list(reader.iter_from(b"z")) == []


def test_reopen_from_disk(tmp_path: Path) -> None:
    entries = _sample_entries()
    path = _write(tmp_path / "t.sst", entries)
    assert SSTableReader(path).get(b"c") == entries[2]
    SSTableReader(path).close()


def test_single_entry_table(tmp_path: Path) -> None:
    path = _write(tmp_path / "one.sst", [SSTEntry(b"only", RecordType.PUT, b"1")])
    with SSTableReader(path) as reader:
        assert reader.get(b"only") is not None
        assert list(reader) != []


def test_oversized_value_in_small_blocks(tmp_path: Path) -> None:
    entries = [SSTEntry(b"k", RecordType.PUT, b"v" * 500)]
    path = _write(tmp_path / "big.sst", entries, block_bytes=64)
    with SSTableReader(path) as reader:
        assert reader.get(b"k") == entries[0]


def test_writer_rejects_empty_and_unordered() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(ValueError, match="no entries"):
            SSTableWriter(Path(tmp) / "e.sst").finish()
        with pytest.raises(ValueError, match="bloom_bits_per_key"):
            SSTableWriter(Path(tmp) / "b.sst", bloom_bits_per_key=-1)
        w = SSTableWriter(Path(tmp) / "o.sst")
        w.add(b"b", RecordType.PUT, b"1")
        with pytest.raises(ValueError, match="increasing"):
            w.add(b"a", RecordType.PUT, b"2")
        with pytest.raises(TypeError):
            w.add(b"c", 1, b"3")  # type: ignore[arg-type]
        w.add(b"c", RecordType.PUT, b"3")
        w.finish()
        with pytest.raises(RuntimeError):
            w.add(b"d", RecordType.PUT, b"4")


def test_corrupt_data_block_detected(tmp_path: Path) -> None:
    path = _write(tmp_path / "t.sst", _sample_entries())
    raw = bytearray(path.read_bytes())
    raw[10] ^= 0xFF  # inside the first data block
    path.write_bytes(bytes(raw))
    with SSTableReader(path) as reader:
        with pytest.raises(CorruptSSTableError):
            reader.get(b"a")


def test_bad_magic_and_truncation_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path / "t.sst", _sample_entries())
    raw = path.read_bytes()
    bad = tmp_path / "bad.sst"
    bad.write_bytes(raw[:-10] + b"\x00" * 10)  # clobber footer
    with pytest.raises(CorruptSSTableError):
        SSTableReader(bad)
    short = tmp_path / "short.sst"
    short.write_bytes(b"tiny")
    with pytest.raises(CorruptSSTableError):
        SSTableReader(short)


def test_bloom_disabled_reads_correctly(tmp_path: Path) -> None:
    entries = _sample_entries()
    writer = SSTableWriter(
        tmp_path / "nb.sst", block_bytes=128, expected_keys=len(entries), bloom_bits_per_key=0
    )
    for e in entries:
        writer.add(e.key, e.kind, e.value)
    writer.finish()
    with SSTableReader(tmp_path / "nb.sst") as reader:
        assert reader._bloom is None
        for e in entries:
            assert reader.get(e.key) == e
        assert reader.get(b"bb") is None  # miss costs a block read, still correct
        assert list(reader) == entries
