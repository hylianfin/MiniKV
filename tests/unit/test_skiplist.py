"""Unit tests for SkipList."""

import random

import pytest

from minikv.storage.skiplist import MAX_LEVEL, SkipList


def test_empty() -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random(1))
    assert len(sl) == 0
    assert sl.get(b"a") is None
    assert list(sl.items()) == []
    assert list(sl.seek(b"a")) == []
    assert sl.delete(b"a") is False
    assert b"a" not in sl
    assert sl.memory_size == 0


def test_put_get_update() -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random(2))
    sl.put(b"k", b"v1")
    assert sl.get(b"k") == b"v1"
    assert len(sl) == 1
    sl.put(b"k", b"v2")
    assert sl.get(b"k") == b"v2"
    assert len(sl) == 1  # update, not a new entry


def test_ordered_iteration() -> None:
    sl: SkipList[int] = SkipList(rng=random.Random(3))
    keys = [b"m", b"a", b"z", b"", b"\x00\xff", b"m\x00", b"abc"]
    for i, k in enumerate(keys):
        sl.put(k, i)
    got = [k for k, _ in sl.items()]
    assert got == sorted(keys)
    assert [v for _, v in sl.items()] == [keys.index(k) for k in sorted(keys)]


def test_seek_positions() -> None:
    sl: SkipList[int] = SkipList(rng=random.Random(4))
    for i, k in enumerate([b"a", b"c", b"e", b"g"]):
        sl.put(k, i)
    assert [k for k, _ in sl.seek(b"a")] == [b"a", b"c", b"e", b"g"]
    assert [k for k, _ in sl.seek(b"b")] == [b"c", b"e", b"g"]
    assert [k for k, _ in sl.seek(b"g")] == [b"g"]
    assert list(sl.seek(b"z")) == []
    assert [k for k, _ in sl.seek(b"")] == [b"a", b"c", b"e", b"g"]


def test_delete() -> None:
    sl: SkipList[int] = SkipList(rng=random.Random(5))
    for i in range(20):
        sl.put(f"k{i:02d}".encode(), i)
    assert sl.delete(b"k05") is True
    assert sl.get(b"k05") is None
    assert sl.delete(b"k05") is False
    assert len(sl) == 19
    assert [k for k, _ in sl.items()] == sorted(k for k, _ in sl.items())
    # Delete everything; height must collapse back to 1.
    for i in range(20):
        sl.delete(f"k{i:02d}".encode())
    assert len(sl) == 0
    assert sl.level == 1
    assert list(sl.items()) == []


def test_contains_and_len() -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random(6))
    sl.put(b"x", b"1")
    assert b"x" in sl
    assert b"y" not in sl
    assert "x" not in sl  # type: ignore[operator]  # wrong type -> False, not raise
    assert len(sl) == 1


def test_invalid_key_type() -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random(7))
    with pytest.raises(TypeError):
        sl.put("s", b"v")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        sl.get("s")  # type: ignore[arg-type]


def test_memory_size_tracks_content() -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random(8))
    before = sl.memory_size
    sl.put(b"k", b"v")
    grown = sl.memory_size
    assert grown > before
    sl.put(b"k", b"much-longer-value")
    assert sl.memory_size > grown
    sl.delete(b"k")
    assert sl.memory_size == before


def test_seeded_rng_is_deterministic() -> None:
    def build(seed: int) -> SkipList[int]:
        sl: SkipList[int] = SkipList(rng=random.Random(seed))
        for i in range(200):
            sl.put(f"key{i:04d}".encode(), i)
        return sl

    a, b = build(42), build(42)
    assert a.level == b.level
    assert list(a.items()) == list(b.items())


def test_level_bounded() -> None:
    sl: SkipList[int] = SkipList(rng=random.Random(9))
    for i in range(2000):
        sl.put(f"k{i:05d}".encode(), i)
    assert 1 <= sl.level <= MAX_LEVEL
    assert len(sl) == 2000
