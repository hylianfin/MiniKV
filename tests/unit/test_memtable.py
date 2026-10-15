"""Unit tests for MemTable."""

import random

import pytest

from minikv.storage.memtable import MAX_KEY_SIZE, MAX_VALUE_SIZE, EntryKind, MemTable


def test_put_get_roundtrip() -> None:
    m = MemTable(rng=random.Random(1))
    m.put(b"k", b"v", seq=1)
    assert m.get(b"k") == b"v"
    assert b"k" in m
    assert len(m) == 1


def test_delete_writes_tombstone() -> None:
    m = MemTable(rng=random.Random(2))
    m.put(b"k", b"v", seq=1)
    m.delete(b"k", seq=2)
    assert m.get(b"k") is None
    assert b"k" not in m  # tombstoned keys read as absent
    entry = m.get_entry(b"k")
    assert entry is not None and entry.kind is EntryKind.DELETE
    assert len(m) == 1  # tombstone still occupies the slot


def test_delete_absent_key_writes_tombstone() -> None:
    m = MemTable(rng=random.Random(3))
    m.delete(b"ghost", seq=1)
    assert m.get(b"ghost") is None
    assert m.get_entry(b"ghost") is not None
    assert len(m) == 1


def test_put_after_delete_resurrects() -> None:
    m = MemTable(rng=random.Random(4))
    m.delete(b"k", seq=1)
    m.put(b"k", b"v", seq=2)
    assert m.get(b"k") == b"v"
    assert b"k" in m


def test_key_value_limits() -> None:
    m = MemTable(rng=random.Random(5))
    with pytest.raises(ValueError):
        m.put(b"k" * (MAX_KEY_SIZE + 1), b"v", seq=1)
    with pytest.raises(ValueError):
        m.put(b"k", b"v" * (MAX_VALUE_SIZE + 1), seq=1)
    m.put(b"k" * MAX_KEY_SIZE, b"v", seq=1)  # boundary is fine
    with pytest.raises(TypeError):
        m.put("k", b"v", seq=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        m.put(b"k", "v", seq=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        m.put(b"k", b"v", seq=-1)


def test_immutable_freezes_writes_but_not_reads() -> None:
    m = MemTable(rng=random.Random(6))
    m.put(b"k", b"v", seq=1)
    m.make_immutable()
    assert m.is_immutable
    with pytest.raises(RuntimeError):
        m.put(b"k2", b"v", seq=2)
    with pytest.raises(RuntimeError):
        m.delete(b"k", seq=2)
    assert m.get(b"k") == b"v"
    assert list(m.items()) != []


def test_should_flush() -> None:
    m = MemTable(max_size_bytes=64, rng=random.Random(7))
    assert not m.should_flush
    m.put(b"key-1", b"value-1", seq=1)
    m.put(b"key-2", b"value-2", seq=2)
    assert m.should_flush
    assert m.size_bytes >= 64


def test_scan_range_semantics() -> None:
    m = MemTable(rng=random.Random(8))
    for k in [b"a", b"b", b"c", b"d"]:
        m.put(k, k.upper(), seq=1)
    m.delete(b"b", seq=2)  # tombstones are visible to scans (LSM needs them)
    got = [(k, e.kind) for k, e in m.scan(start=b"a", end=b"d")]
    assert [(k, kind) for k, kind in got] == [
        (b"a", EntryKind.PUT),
        (b"b", EntryKind.DELETE),
        (b"c", EntryKind.PUT),
    ]
    assert [k for k, _ in m.scan()] == [b"a", b"b", b"c", b"d"]
    assert list(m.scan(start=b"d", end=b"a")) == []
    assert list(m.scan(start=b"z")) == []


def test_seek() -> None:
    m = MemTable(rng=random.Random(9))
    for k in [b"a", b"c", b"e"]:
        m.put(k, b"v", seq=1)
    assert [k for k, _ in m.seek(b"b")] == [b"c", b"e"]
    with pytest.raises(TypeError):
        m.seek("b")  # type: ignore[arg-type]


def test_bad_max_size() -> None:
    with pytest.raises(ValueError):
        MemTable(max_size_bytes=0)


def test_defensive_branches() -> None:
    m = MemTable(rng=random.Random(10))
    assert m.max_size_bytes == 4 * 1024 * 1024
    assert 123 not in m  # type: ignore[operator]
    with pytest.raises(TypeError):
        m.get_entry("k")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        m.get("k")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        list(m.scan(start="a"))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        list(m.scan(end="z"))  # type: ignore[arg-type]
