"""Unit tests for the session (dedup) table."""

import pytest

from minikv.kv.sessions import SESSION_CAPACITY, SessionTable, decode_session_record
from minikv.storage.lsm import LSMEngine, LSMOptions


def _table(tmp_path, capacity: int = SESSION_CAPACITY) -> tuple[SessionTable, LSMEngine]:
    engine = LSMEngine(tmp_path / "s", LSMOptions(memtable_bytes=512))
    engine.open()
    return SessionTable(engine, capacity), engine


def test_store_lookup_roundtrip(tmp_path) -> None:
    table, engine = _table(tmp_path)
    assert table.lookup("ghost") is None
    table.store("c1", 7, b"result-bytes")
    record = table.lookup("c1")
    assert record is not None and (record.seq, record.result) == (7, b"result-bytes")
    table.store("c1", 8, b"newer")
    assert table.lookup("c1").seq == 8  # type: ignore[union-attr]
    engine.close()


def test_corrupt_record_reads_as_absent(tmp_path) -> None:
    table, engine = _table(tmp_path)
    engine.put(b"session/c1", b"\x01\x02")  # too short to decode
    assert table.lookup("c1") is None
    table.store("c1", 1, b"ok")  # heals on next store
    assert table.lookup("c1") is not None
    engine.close()


def test_lru_eviction_drops_lsm_record(tmp_path) -> None:
    table, engine = _table(tmp_path, capacity=3)
    for i in range(4):
        table.store(f"c{i}", i, b"r")
    assert table.lookup("c0") is None  # evicted, LSM record deleted too
    assert engine.get(b"session/c0") is None
    for i in range(1, 4):
        assert table.lookup(f"c{i}") is not None
    # Touch order matters: c1 used recently, c2 evicted next.
    table.lookup("c1")
    table.store("c4", 4, b"r")
    assert table.lookup("c2") is None
    assert table.lookup("c1") is not None
    engine.close()


def test_clear_cache_keeps_durable_records(tmp_path) -> None:
    table, engine = _table(tmp_path)
    table.store("c1", 1, b"r")
    table.clear_cache()
    assert table.lookup("c1") is not None  # rebuilt from LSM
    engine.close()


def test_sessions_survive_engine_restart(tmp_path) -> None:
    path = tmp_path / "s"
    table, engine = _table(tmp_path)
    table.store("c1", 9, b"cached")
    engine.close()
    engine2 = LSMEngine(path, LSMOptions(memtable_bytes=512))
    engine2.open()
    table2 = SessionTable(engine2)
    try:
        record = table2.lookup("c1")
        assert record is not None and record.seq == 9
    finally:
        engine2.close()


def test_bad_capacity() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        engine = LSMEngine(tmp, LSMOptions(memtable_bytes=512))
        engine.open()
        try:
            with pytest.raises(ValueError):
                SessionTable(engine, 0)
            assert SessionTable(engine, 5).capacity == 5
        finally:
            engine.close()


def test_session_record_codec() -> None:
    from minikv.kv.sessions import SessionRecord, encode_session_record

    record = SessionRecord(123, b"\x00\x01binary")
    assert decode_session_record(encode_session_record(record)) == record
    with pytest.raises(ValueError):
        decode_session_record(b"short")
