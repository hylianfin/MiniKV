"""Unit tests for the persistent Raft log store."""

import struct
import zlib
from pathlib import Path

import pytest

from minikv.raft.core import RaftCore
from minikv.raft.logstore import CorruptLogError, LogStore, decode_prefix, encode_entry
from minikv.raft.types import EntryKind, LogEntry, RaftConfig


def _e(index: int, term: int, payload: bytes = b"p") -> LogEntry:
    return LogEntry(term, index, EntryKind.COMMAND, payload)


def test_roundtrip_and_reopen(tmp_path: Path) -> None:
    store = LogStore(tmp_path / "r1")
    state = store.open()
    assert (state.term, state.entries, state.commit_index) == (0, (), 0)
    store.save_term_vote(3, "n2")
    store.persist(1, (_e(1, 3, b"a"), _e(2, 3, b"b")))
    store.save_progress(2, 1)
    assert store.last_index() == 2 and store.last_term() == 3
    assert store.entry(2) == _e(2, 3, b"b")
    assert store.term_at(0) == 0 and store.term_at(2) == 3
    store.close()

    reopened = LogStore(tmp_path / "r1")
    state = reopened.open()
    assert (state.term, state.voted_for) == (3, "n2")
    assert list(state.entries) == [_e(1, 3, b"a"), _e(2, 3, b"b")]
    assert (state.commit_index, state.applied_index) == (2, 1)
    reopened.close()


def test_persist_truncates_conflicting_suffix(tmp_path: Path) -> None:
    store = LogStore(tmp_path)
    store.open()
    store.persist(1, (_e(1, 1), _e(2, 1), _e(3, 2)))
    store.persist(2, (_e(2, 2, b"new"), _e(3, 2, b"new")))
    assert [e.payload for e in store.load().entries] == [b"p", b"new", b"new"]
    assert [e.term for e in store.load().entries] == [1, 2, 2]
    store.close()
    assert [e.payload for e in LogStore(tmp_path).open().entries] == [b"p", b"new", b"new"]


def test_truncation_at_every_offset_yields_prefix(tmp_path: Path) -> None:
    records = [_e(i + 1, 1, f"v{i}".encode()) for i in range(6)]
    full = b"".join(encode_entry(r) for r in records)
    ends, total = [], 0
    for r in records:
        total += len(encode_entry(r))
        ends.append(total)
    for off in range(len(full)):
        path = tmp_path / f"t{off}"
        path.mkdir()
        (path / "raft.log").write_bytes(full[:off])
        entries, valid_end = decode_prefix(full[:off])
        assert entries == [r for r, end in zip(records, ends, strict=True) if end <= off]
        store = LogStore(path)
        assert list(store.open().entries) == entries
        assert (path / "raft.log").stat().st_size == valid_end
        store.close()


def test_corrupt_first_record_raises(tmp_path: Path) -> None:
    path = tmp_path / "c"
    path.mkdir()
    # Fully present record (length matches) with a bad CRC: ambiguous, raise.
    (path / "raft.log").write_bytes(struct.pack("<II", 0, 17) + b"X" * 17)
    with pytest.raises(CorruptLogError):
        LogStore(path).open()
    # Fully present, CRC-valid, but unknown entry kind: also ambiguous.
    body = struct.pack("<QQB", 1, 1, 9)
    length = struct.pack("<I", len(body))
    crc = struct.pack("<I", zlib.crc32(length + body) & 0xFFFFFFFF)
    (path / "raft.log").write_bytes(crc + length + body)
    with pytest.raises(CorruptLogError):
        LogStore(path).open()
    # A short header alone is just a torn tail: silent.
    (path / "raft.log").write_bytes(b"\x01\x02\x03")
    assert LogStore(path).open().entries == ()


def test_index_gap_raises(tmp_path: Path) -> None:
    blob = encode_entry(_e(1, 1)) + encode_entry(_e(3, 1))  # index 2 missing
    with pytest.raises(CorruptLogError, match="gap"):
        decode_prefix(blob)
    path = tmp_path / "g"
    path.mkdir()
    (path / "raft.log").write_bytes(blob)
    with pytest.raises(CorruptLogError):
        LogStore(path).open()


def test_mid_file_corruption_truncates_to_prefix(tmp_path: Path) -> None:
    rec1 = encode_entry(_e(1, 1, b"one"))
    rec2 = bytearray(encode_entry(_e(2, 1, b"two")))
    # Flip a payload byte in record 2: bad CRC mid-file truncates to record 1.
    rec2[-1] ^= 0xFF
    path = tmp_path / "m1"
    path.mkdir()
    (path / "raft.log").write_bytes(rec1 + bytes(rec2))
    assert list(LogStore(path).open().entries) == [_e(1, 1, b"one")]

    # Declared length below the fixed header: invalid mid-file, truncate.
    path2 = tmp_path / "m2"
    path2.mkdir()
    (path2 / "raft.log").write_bytes(rec1 + struct.pack("<II", 0, 5) + b"X" * 5)
    assert list(LogStore(path2).open().entries) == [_e(1, 1, b"one")]

    # Valid CRC but unknown kind mid-file: truncate, no raise.
    body = struct.pack("<QQB", 1, 2, 9) + b"two"
    length = struct.pack("<I", len(body))
    blob = struct.pack("<I", zlib.crc32(length + body) & 0xFFFFFFFF) + length + body
    path3 = tmp_path / "m3"
    path3.mkdir()
    (path3 / "raft.log").write_bytes(rec1 + blob)
    assert list(LogStore(path3).open().entries) == [_e(1, 1, b"one")]


def test_bad_meta_raises(tmp_path: Path) -> None:
    path = tmp_path / "m"
    path.mkdir()
    (path / "raft.meta").write_text("nope{", encoding="utf-8")
    with pytest.raises(CorruptLogError):
        LogStore(path).open()
    (path / "raft.meta").write_text('{"term": -1}', encoding="utf-8")
    with pytest.raises(CorruptLogError):
        LogStore(path).open()
    (path / "raft.meta").write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(CorruptLogError):
        LogStore(path).open()


def test_context_manager_and_progress_validation(tmp_path: Path) -> None:
    with LogStore(tmp_path / "w") as store:
        assert store.load().entries == ()
        store.save_term_vote(1, None)
        with pytest.raises(ValueError):
            store.save_progress(-1, 0)
    assert (tmp_path / "w" / "raft.meta").exists()


def test_validation(tmp_path: Path) -> None:
    store = LogStore(tmp_path)
    with pytest.raises(RuntimeError):
        store.load()  # not open
    with pytest.raises(RuntimeError):
        store.persist(1, ())
    store.open()
    with pytest.raises(RuntimeError):
        store.open()  # double open
    with pytest.raises(ValueError):
        store.persist(0, (_e(1, 1),))
    with pytest.raises(ValueError):
        store.persist(5, (_e(5, 1),))  # hole
    with pytest.raises(ValueError):
        store.persist(1, (_e(2, 1),))  # must start at base
    with pytest.raises(ValueError):
        store.persist(1, (_e(1, 1), _e(3, 1)))  # not contiguous
    with pytest.raises(ValueError):
        store.save_term_vote(-1, None)
    with pytest.raises(ValueError):
        store.save_progress(0, 1)  # applied past commit
    with pytest.raises(ValueError):
        store.save_progress(9, 0)  # commit past log end
    with pytest.raises(IndexError):
        store.entry(1)  # empty log
    with pytest.raises(TypeError):
        encode_entry(LogEntry(1, 1, 99, b"x"))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        encode_entry(LogEntry(1, 0, EntryKind.COMMAND, b"x"))
    with pytest.raises(ValueError):
        encode_entry(LogEntry(-1, 1, EntryKind.COMMAND, b"x"))
    store.close()
    store.close()  # idempotent


def test_store_drives_core() -> None:
    import random
    import tempfile

    from minikv.raft.types import Role

    with tempfile.TemporaryDirectory() as tmp:
        store = LogStore(tmp)
        store.open()
        store.save_term_vote(2, "b")
        store.persist(1, (_e(1, 2, b"a"), _e(2, 2, b"b")))
        store.save_progress(1, 1)
        core = RaftCore("a", ["b"], store, random.Random(0), RaftConfig())
        assert core.term == 2
        assert core.role is Role.FOLLOWER
        assert [(e.index, e.term) for e in core.log] == [(1, 2), (2, 2)]
        assert (core.commit_index, core.last_applied) == (1, 1)
        store.close()
