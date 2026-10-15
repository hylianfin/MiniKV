"""Flush-path tests, including crash injection at every step."""

import random
from pathlib import Path

import pytest

from minikv.storage.manifest import ManifestStore
from minikv.storage.memtable import MemTable
from minikv.storage.sstable import SSTableMeta, SSTableReader, flush_memtable
from minikv.storage.wal import RecordType


def _memtable() -> MemTable:
    m = MemTable(rng=random.Random(11))
    m.put(b"a", b"va", seq=1)
    m.put(b"b", b"vb", seq=2)
    m.delete(b"c", seq=3)
    m.put(b"d", b"vd", seq=4)
    return m


def _model() -> dict[bytes, tuple[RecordType, bytes]]:
    return {
        b"a": (RecordType.PUT, b"va"),
        b"b": (RecordType.PUT, b"vb"),
        b"c": (RecordType.DELETE, b""),
        b"d": (RecordType.PUT, b"vd"),
    }


def _read_view(
    data_dir: Path, store: ManifestStore, memtables: list[MemTable]
) -> dict[bytes, tuple[RecordType, bytes]]:
    """Test-only read path: manifest SSTables oldest-first, memtables last
    (newest wins). Mirrors the LSM read order that M4 will implement."""
    view: dict[bytes, tuple[RecordType, bytes]] = {}
    for meta in store.load().tables:
        with SSTableReader(data_dir / meta.name) as reader:
            for entry in reader:
                view[entry.key] = (entry.kind, entry.value)
    for mem in memtables:
        for key, entry in mem.items():
            kind = RecordType.PUT if entry.kind.name == "PUT" else RecordType.DELETE
            view[key] = (kind, entry.value)
    return view


def test_flush_then_read_back_identical(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    meta = flush_memtable(_memtable(), tmp_path, store, block_bytes=64)
    assert isinstance(meta, SSTableMeta)
    assert meta.entry_count == 4 and meta.tier == 0
    assert meta.min_key == b"a" and meta.max_key == b"d"
    with SSTableReader(tmp_path / meta.name) as reader:
        assert [(e.key, e.kind, e.value) for e in reader] == [
            (k, kind, v) for k, (kind, v) in sorted(_model().items())
        ]


def test_flush_empty_memtable_is_noop(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    assert flush_memtable(MemTable(), tmp_path, store) is None
    assert store.load().tables == []
    assert list(tmp_path.iterdir()) == []


def test_fault_at_every_step_preserves_data(tmp_path: Path) -> None:
    class Crash(Exception):
        pass

    for point in ("after_write", "after_rename", "after_manifest"):
        data = tmp_path / point
        data.mkdir()
        store = ManifestStore(data)
        mem = _memtable()

        def crash(at: str, want: str = point) -> None:
            if at == want:
                raise Crash(at)

        with pytest.raises(Crash):
            flush_memtable(mem, data, store, block_bytes=64, fault=crash)
        # The memtable (WAL-protected in the real engine) still holds the
        # data, and whatever the manifest lists is complete and consistent.
        assert _read_view(data, store, [mem]) == _model()
        # A retry after the "crash" converges the read view. A fault after
        # the manifest update may leave the same data listed twice (the
        # retry cannot know the first update landed); both copies are
        # identical, so reads stay correct and compaction will drop the dup.
        got = flush_memtable(mem, data, store, block_bytes=64)
        assert got is not None
        assert _read_view(data, store, []) == _model()
        assert 1 <= len(store.load().tables) <= 2


def test_stray_tmp_files_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "sst_000007_t0.sst.tmp").write_bytes(b"litter from a crash")
    store = ManifestStore(tmp_path)
    meta = flush_memtable(_memtable(), tmp_path, store)
    # The stray tmp reserves sequence 7, so the new table takes 8; readers
    # only open manifest-listed files, so the litter is never touched.
    assert meta is not None and meta.name == "sst_000008_t0.sst"
    assert _read_view(tmp_path, store, []) == _model()


def test_second_flush_gets_a_fresh_name(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    first = flush_memtable(_memtable(), tmp_path, store)
    mem2 = MemTable()
    mem2.put(b"z", b"vz", seq=5)
    second = flush_memtable(mem2, tmp_path, store)
    assert first is not None and second is not None
    assert first.name != second.name
    assert [t.name for t in store.load().tables] == [first.name, second.name]
