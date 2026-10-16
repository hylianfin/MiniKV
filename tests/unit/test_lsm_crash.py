"""LSM crash tests: WAL truncation/corruption and compaction faults."""

import os
import random
import shutil
import threading
import time
from pathlib import Path

import pytest

from minikv.storage.lsm import LSMEngine, LSMOptions
from minikv.storage.manifest import Manifest, ManifestStore, SSTableMeta
from minikv.storage.sstable import CorruptSSTableError, SSTableWriter
from minikv.storage.wal import CorruptWALError, RecordType, WalRecord, encode_record


def _engine(path: Path, **kw: object) -> LSMEngine:
    base: dict[str, object] = {"memtable_bytes": 10**9}
    base.update(kw)
    return LSMEngine(path, LSMOptions(**base))  # type: ignore[arg-type]


def _populate(path: Path, n: int = 20) -> tuple[list[WalRecord], Path]:
    """20 acknowledged writes, then a simulated crash (no close/flush)."""
    engine = _engine(path)
    engine.open()
    records = []
    for i in range(n):
        key, value = f"k{i:02d}".encode(), f"v{i}".encode()
        engine.put(key, value, raft_index=i + 1)
        records.append(WalRecord(RecordType.PUT, key, value, i + 1))
    engine.crash()
    wals = list(path.glob("wal_*.log"))
    assert len(wals) == 1
    return records, wals[0]


def _boundaries(records: list[WalRecord]) -> list[int]:
    ends, total = [], 0
    for record in records:
        total += len(encode_record(record))
        ends.append(total)
    return ends


def _view(path: Path, **kw: object) -> tuple[dict[bytes, bytes], int]:
    engine = _engine(path, **kw)
    engine.open()
    try:
        return dict(engine.scan(None, None)), engine.applied_index()
    finally:
        engine.close()


def test_truncation_yields_valid_prefixes(tmp_path: Path) -> None:
    records, wal = _populate(tmp_path / "db")
    full = wal.read_bytes()
    ends = _boundaries(records)
    first_end = ends[0]
    # Every offset in the first-record region (offset-0 ambiguity), every
    # record boundary, and every offset in the final record region.
    offsets = set(range(first_end)) | set(ends) | set(range(ends[-2], len(full)))
    assert len(offsets) > 50
    for off in sorted(offsets):
        case = tmp_path / f"case{off}"
        shutil.copytree(wal.parent, case)
        (case / wal.name).write_bytes(full[:off])
        view, applied = _view(case)
        contained = [r for r, end in zip(records, ends, strict=True) if end <= off]
        assert view == {r.key: r.value for r in contained}, f"offset {off}"
        assert applied == (contained[-1].raft_index if contained else 0)


def test_single_byte_corruption_keeps_prefix_or_raises(tmp_path: Path) -> None:
    records, wal = _populate(tmp_path / "db")
    full = bytearray(wal.read_bytes())
    ends = _boundaries(records)
    rng = random.Random(20260612)
    positions = sorted(rng.sample(range(len(full)), 25))
    assert any(p < ends[0] for p in positions)  # cover the ambiguous region
    for pos in positions:
        case = tmp_path / f"c{pos}"
        shutil.copytree(wal.parent, case)
        corrupted = bytearray(full)
        corrupted[pos] ^= 0xFF
        (case / wal.name).write_bytes(bytes(corrupted))
        hit = next(i for i, end in enumerate(ends) if pos < end)
        if hit == 0:
            try:
                view, applied = _view(case)
            except CorruptWALError:
                pass
            else:  # damage mimicking a torn tail replays as empty
                assert (view, applied) == ({}, 0)
        else:
            view, applied = _view(case)
            assert view == {r.key: r.value for r in records[:hit]}
            assert applied == records[hit - 1].raft_index


def _raw_tables(directory: Path, n: int = 4) -> ManifestStore:
    """Hand-built tier-0 state (bypasses the engine, which would auto-merge)."""
    store = ManifestStore(directory)
    manifest = Manifest()
    for i in range(n):
        name = f"sst_{i + 1:06d}_t0.sst"
        writer = SSTableWriter(directory / name, block_bytes=128, expected_keys=2)
        writer.add(b"a", RecordType.PUT, f"v{i}".encode())
        writer.add(b"b", RecordType.DELETE, b"")
        count, lo, hi = writer.finish()
        manifest.tables.append(
            SSTableMeta(name=name, tier=0, min_key=lo, max_key=hi, entry_count=count)
        )
    manifest.flushed_raft_index = 7
    store.save(manifest)
    return store


def test_engine_compact_faults_then_converges(tmp_path: Path) -> None:
    class Crash(Exception):
        pass

    for point in ("after_write", "after_rename", "after_manifest"):
        sub = tmp_path / point
        sub.mkdir()
        _raw_tables(sub)
        engine = LSMEngine(sub, LSMOptions(memtable_bytes=512, compaction_trigger=2))

        def crash(at: str, want: str = point) -> None:
            if at == want:
                raise Crash(at)

        engine.open()
        with pytest.raises(Crash):
            engine.compact(fault=crash)
        engine.crash()  # the real crash: abandon in-memory state
        view, applied = _view(sub, memtable_bytes=512, compaction_trigger=2)
        assert view == {b"a": b"v3"}  # newest of the four, tombstone hides b
        assert applied == 7  # manifest raft index survives, never regresses


def test_engine_flush_faults_then_converges(tmp_path: Path) -> None:
    class Crash(Exception):
        pass

    for point in ("after_write", "after_rename", "after_manifest"):
        sub = tmp_path / point
        sub.mkdir()
        engine = LSMEngine(sub, LSMOptions(memtable_bytes=512))
        engine.open()
        engine.put(b"a", b"va", raft_index=1)
        engine.put(b"b", b"vb", raft_index=2)

        def crash(at: str, want: str = point) -> None:
            if at == want:
                raise Crash(at)

        with pytest.raises(Crash):
            engine.flush(fault=crash)
        engine.crash()
        view, applied = _view(sub, memtable_bytes=512)
        assert view == {b"a": b"va", b"b": b"vb"}
        assert applied == 2


def test_multi_wal_replay_with_sealed_backlog(tmp_path: Path) -> None:
    """Two WALs on disk: the first becomes a sealed backlog (flushed in the
    background after open), the second resumes as current. Deletes replay
    as tombstones."""

    from minikv.storage.wal import WAL

    db = tmp_path / "db"
    db.mkdir()
    w1, w2 = db / "wal_000001.log", db / "wal_000002.log"
    wal = WAL(w1)
    wal.open()
    wal.append_put(b"a", b"va", raft_index=1)
    wal.append_delete(b"gone", raft_index=2)
    wal.close()
    wal = WAL(w2)
    wal.open()
    wal.append_put(b"b", b"vb", raft_index=3)
    wal.close()

    engine = LSMEngine(db, LSMOptions(memtable_bytes=128))
    engine.open()
    try:
        # Sealed backlog flushes in the background; drain it.
        engine.flush()
        assert engine.get(b"a") == b"va"
        assert engine.get(b"gone") is None
        assert engine.get(b"b") == b"vb"
        assert engine.applied_index() == 3
        # New writes continue on the resumed WAL (sequence keeps increasing).
        engine.put(b"c", b"vc", raft_index=4)
        engine.flush()
        assert sorted(dict(engine.scan(None, None))) == [b"a", b"b", b"c"]
    finally:
        engine.close()
    assert not list(db.glob("wal_*.log"))
    # Full state survives a clean restart, including the replayed tombstone.
    engine2 = LSMEngine(db, LSMOptions(memtable_bytes=128))
    engine2.open()
    try:
        assert dict(engine2.scan(None, None)) == {b"a": b"va", b"b": b"vb", b"c": b"vc"}
        assert engine2.applied_index() == 4
    finally:
        engine2.close()


def test_rotate_on_open_when_replayed_memtable_overfull(tmp_path: Path) -> None:
    from minikv.storage.wal import WAL

    db = tmp_path / "db"
    db.mkdir()
    wal = WAL(db / "wal_000001.log")
    wal.open()
    for i in range(30):
        wal.append_put(f"k{i:02d}".encode(), b"x" * 20, raft_index=i + 1)
    wal.close()

    engine = LSMEngine(db, LSMOptions(memtable_bytes=128))
    engine.open()
    try:
        engine.flush()  # drains the rotate-on-open backlog
        assert engine.applied_index() == 30
        assert len(dict(engine.scan(None, None))) == 30
    finally:
        engine.close()


def test_crash_with_sealed_job_pending_abandons_it_safely(tmp_path: Path) -> None:
    """Stall the worker mid-flush, queue a second job, then crash: the
    pending job is abandoned (its WAL survives) and replay recovers it."""
    entered = threading.Event()
    release = threading.Event()

    def stall(at: str) -> None:
        if at == "after_write":
            entered.set()
            assert release.wait(30)

    db = tmp_path / "db"
    engine = LSMEngine(db, LSMOptions(memtable_bytes=64))
    engine.open()
    engine._fault = stall  # white-box: stall the worker inside its flush job
    try:
        for i in range(4):
            engine.put(f"s{i}".encode(), b"v", raft_index=i + 1)  # job 1 enqueued
        assert entered.wait(30)  # worker is stalled inside job 1
        for i in range(4, 8):
            engine.put(f"s{i}".encode(), b"v", raft_index=i + 1)  # more jobs pending
        crasher = threading.Thread(target=engine.crash)
        crasher.start()
        # Release only after the crash flag is set, so pending jobs are
        # deterministically abandoned instead of raced through.
        deadline = time.monotonic() + 30
        while not engine._crashed and time.monotonic() < deadline:
            time.sleep(0.01)
        assert engine._crashed
        release.set()
        crasher.join(30)
        assert not crasher.is_alive()
    finally:
        release.set()
        engine._fault = None
    view, applied = _view(db, memtable_bytes=64)
    assert view == {f"s{i}".encode(): b"v" for i in range(8)}
    assert applied == 8


def _require_non_root() -> None:
    if os.geteuid() == 0:
        pytest.skip("read-only dirs are still writable as root")


def test_wal_unlink_failure_after_flush_is_swallowed(tmp_path: Path) -> None:
    _require_non_root()
    db = tmp_path / "db"
    engine = LSMEngine(db, LSMOptions(memtable_bytes=64))
    engine.open()
    engine.put(b"k", b"v", raft_index=1)

    def chmod_then_continue(at: str) -> None:
        if at == "after_manifest":
            os.chmod(db, 0o555)  # WAL deletion below will fail

    try:
        engine.flush(fault=chmod_then_continue)  # must not raise
        assert engine.get(b"k") == b"v"
    finally:
        os.chmod(db, 0o755)
    engine.close()


def test_refresh_with_missing_file_raises(tmp_path: Path) -> None:
    db = tmp_path / "db"
    engine = LSMEngine(db, LSMOptions(memtable_bytes=512))
    engine.open()
    engine.put(b"k", b"v", raft_index=1)
    engine.flush()
    assert engine.get(b"k") == b"v"
    # Ghost entry, no file: the worker-side refresh must refuse it.
    manifest = engine._store.load()
    manifest.tables.append(
        SSTableMeta(name="sst_999999_t0.sst", tier=0, min_key=b"k", max_key=b"k", entry_count=1)
    )
    engine._store.save(manifest)
    with engine._guard:
        with pytest.raises(CorruptSSTableError):
            engine._refresh_tables_locked()
    engine.close()


def test_open_with_undeletable_empty_wal(tmp_path: Path) -> None:
    _require_non_root()
    db = tmp_path / "db"
    db.mkdir()
    (db / "wal_000001.log").write_bytes(b"")  # empty WAL
    os.chmod(db, 0o555)
    try:
        # Replay swallows the failed cleanup of the empty WAL; open then
        # fails creating a new WAL in the read-only dir -- the point is the
        # cleanup failure itself never masks recovery.
        with pytest.raises(OSError):
            LSMEngine(db, LSMOptions(memtable_bytes=512)).open()
    finally:
        os.chmod(db, 0o755)


def test_close_with_undeletable_empty_wal(tmp_path: Path) -> None:
    _require_non_root()
    db = tmp_path / "db"
    engine = LSMEngine(db, LSMOptions(memtable_bytes=512))
    engine.open()
    os.chmod(db, 0o555)
    try:
        engine.close()  # cleanup fails, swallowed; close still completes
    finally:
        os.chmod(db, 0o755)
