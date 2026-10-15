"""Unit tests for the LSM engine."""

import pytest

from minikv.storage.lsm import LSMEngine, LSMOptions
from minikv.storage.sstable import SSTableWriter
from minikv.storage.wal import FsyncPolicy, RecordType


def _opts(**kw: object) -> LSMOptions:
    base: dict[str, object] = {"memtable_bytes": 512, "compaction_trigger": 4}
    base.update(kw)
    return LSMOptions(**base)  # type: ignore[arg-type]


def test_basic_crud(tmp_path) -> None:
    engine = LSMEngine(tmp_path / "db", _opts())
    engine.open()
    assert engine.get(b"missing") is None
    engine.put(b"k", b"v", raft_index=1)
    assert engine.get(b"k") == b"v"
    assert engine.applied_index() == 1
    engine.put(b"k", b"v2", raft_index=2)
    assert engine.get(b"k") == b"v2"
    engine.delete(b"k", raft_index=3)
    assert engine.get(b"k") is None
    assert engine.applied_index() == 3
    engine.close()


def test_scan_semantics(tmp_path) -> None:
    engine = LSMEngine(tmp_path / "db", _opts())
    engine.open()
    for k in [b"a", b"b", b"c", b"d", b"e"]:
        engine.put(k, k.upper(), raft_index=1)
    engine.delete(b"b", raft_index=2)
    assert list(engine.scan(b"a", b"e")) == [(b"a", b"A"), (b"c", b"C"), (b"d", b"D")]
    assert list(engine.scan(None, None)) == [
        (b"a", b"A"),
        (b"c", b"C"),
        (b"d", b"D"),
        (b"e", b"E"),
    ]
    assert list(engine.scan(b"a", b"e", limit=2)) == [(b"a", b"A"), (b"c", b"C")]
    assert list(engine.scan(b"z", None)) == []
    assert list(engine.scan(b"d", b"a")) == []
    with pytest.raises(ValueError):
        list(engine.scan(None, None, limit=-1))
    with pytest.raises(TypeError):
        list(engine.scan("a", None))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        list(engine.scan(None, "z"))  # type: ignore[arg-type]
    engine.close()


def test_open_with_missing_sstable_raises(tmp_path) -> None:
    import json

    from minikv.storage.sstable import CorruptSSTableError

    db = tmp_path / "db"
    db.mkdir()
    real = SSTableWriter(db / "sst_000001_t0.sst", block_bytes=128, expected_keys=1)
    real.add(b"k", RecordType.PUT, b"v")
    real.finish()
    (db / "MANIFEST").write_text(
        json.dumps(
            {
                "tables": [
                    {
                        "name": "sst_000001_t0.sst",
                        "tier": 0,
                        "min_key": "6b",
                        "max_key": "6b",
                        "entry_count": 1,
                    },
                    {
                        "name": "sst_000002_t0.sst",  # ghost: opens after the real one
                        "tier": 0,
                        "min_key": "6b",
                        "max_key": "6b",
                        "entry_count": 1,
                    },
                ],
                "flushed_raft_index": 0,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(CorruptSSTableError):
        LSMEngine(db, _opts()).open()


def test_worker_error_surfaces_and_clears(tmp_path) -> None:
    engine = LSMEngine(tmp_path / "db", _opts())
    engine.open()
    engine.put(b"k", b"v", raft_index=1)
    engine._worker_error = RuntimeError("boom")  # white-box: simulate a job failure
    with pytest.raises(RuntimeError, match="boom"):
        engine.flush()
    engine.put(b"k2", b"v2", raft_index=2)  # engine still usable afterwards
    engine.flush()
    assert engine.get(b"k2") == b"v2"
    engine._worker_error = RuntimeError("boom2")
    with pytest.raises(RuntimeError, match="boom2"):
        engine.close()


def test_rotation_and_restart(tmp_path) -> None:
    path = tmp_path / "db"
    engine = LSMEngine(path, _opts())
    engine.open()
    for i in range(100):
        engine.put(f"k{i:03d}".encode(), f"v{i}".encode(), raft_index=i + 1)
    engine.close()  # flushes everything
    assert not list(path.glob("wal_*.log"))  # no WAL litter

    engine2 = LSMEngine(path, _opts())
    engine2.open()
    for i in range(100):
        assert engine2.get(f"k{i:03d}".encode()) == f"v{i}".encode()
    assert engine2.applied_index() == 100
    assert list(engine2.scan(None, None)) == [
        (f"k{i:03d}".encode(), f"v{i}".encode()) for i in range(100)
    ]
    engine2.close()


def test_crash_recovery_replays_wal(tmp_path) -> None:
    path = tmp_path / "db"
    engine = LSMEngine(path, _opts(memtable_bytes=10**9))  # nothing flushes
    engine.open()
    for i in range(20):
        engine.put(f"k{i}".encode(), f"v{i}".encode(), raft_index=i + 1)
    engine._crash()
    assert list(path.glob("wal_*.log")) != []

    engine2 = LSMEngine(path, _opts(memtable_bytes=10**9))
    engine2.open()
    for i in range(20):
        assert engine2.get(f"k{i}".encode()) == f"v{i}".encode()
    assert engine2.applied_index() == 20
    engine2.close()


def test_compact_collapses_tiers(tmp_path) -> None:
    engine = LSMEngine(tmp_path / "db", _opts(memtable_bytes=256, compaction_trigger=2))
    engine.open()
    for i in range(40):
        engine.put(f"k{i:02d}".encode(), b"x" * 20, raft_index=i + 1)
    engine.delete(b"k00", raft_index=41)
    engine.compact()
    got = dict(engine.scan(None, None))
    assert len(got) == 39 and b"k00" not in got
    assert engine.get(b"k01") == b"x" * 20
    engine.close()
    # Reopen cleanly on the compacted state.
    engine2 = LSMEngine(tmp_path / "db", _opts(memtable_bytes=256, compaction_trigger=2))
    engine2.open()
    assert dict(engine2.scan(None, None)) == got
    engine2.close()


def test_lifecycle_edges(tmp_path) -> None:
    engine = LSMEngine(tmp_path / "db", _opts())
    assert engine.path == tmp_path / "db"
    with pytest.raises(RuntimeError):
        engine.get(b"k")  # not open
    engine._crash()  # no-op before open
    engine.open()
    with pytest.raises(RuntimeError):
        engine.open()  # double open
    engine.close()
    engine.close()  # idempotent
    with pytest.raises(RuntimeError):
        engine.put(b"k", b"v")
    with pytest.raises(RuntimeError):
        engine.open()  # cannot reopen a closed engine
    # Empty open/close leaves no litter at all.
    assert list((tmp_path / "db").iterdir()) == []


def test_option_validation() -> None:
    with pytest.raises(ValueError):
        LSMOptions(memtable_bytes=0)
    with pytest.raises(ValueError):
        LSMOptions(block_bytes=0)
    with pytest.raises(ValueError):
        LSMOptions(bloom_bits_per_key=0)
    with pytest.raises(ValueError):
        LSMOptions(compaction_trigger=1)
    with pytest.raises(ValueError):
        LSMOptions(batch_interval_ms=-1)


def test_batch_policy_survives_crash(tmp_path) -> None:
    path = tmp_path / "db"
    engine = LSMEngine(
        path, _opts(memtable_bytes=10**9, fsync_policy=FsyncPolicy.BATCH, batch_interval_ms=0)
    )
    engine.open()
    for i in range(10):
        engine.put(f"k{i}".encode(), b"v", raft_index=i + 1)
    engine._crash()
    engine2 = LSMEngine(path, _opts(memtable_bytes=10**9))
    engine2.open()
    assert engine2.applied_index() == 10
    engine2.close()


def test_get_reads_sealed_memtable(tmp_path) -> None:
    """Stall the worker so a sealed memtable lingers; get() must find the
    key there (newer than any SSTable, older than nothing)."""
    import threading

    entered = threading.Event()
    release = threading.Event()

    def stall(at: str) -> None:
        if at == "after_write":
            entered.set()
            assert release.wait(30)

    engine = LSMEngine(tmp_path / "db", _opts(memtable_bytes=64))
    engine.open()
    engine._fault = stall  # white-box: stall the worker inside its flush job
    try:
        for i in range(20):  # auto-rotation seals memtables; worker stalls on job 1
            engine.put(f"k{i}".encode(), b"v", raft_index=i + 1)
            if entered.wait(5):
                break
        assert entered.is_set()
        assert engine.get(b"k0") == b"v"  # served from a sealed memtable
    finally:
        release.set()
        engine._fault = None
    engine.flush()
    assert engine.get(b"k0") == b"v"
    engine.close()
