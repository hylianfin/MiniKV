"""Unit tests for compaction planning and execution."""

import os
from pathlib import Path

import pytest

from minikv.storage.compaction import plan_compaction, run_compaction
from minikv.storage.manifest import Manifest, ManifestStore, SSTableMeta
from minikv.storage.sstable import SSTableReader, SSTableWriter
from minikv.storage.wal import RecordType


def _write_table(
    directory: Path, name: str, entries: list[tuple[bytes, RecordType, bytes]]
) -> SSTableMeta:
    writer = SSTableWriter(directory / name, block_bytes=128, expected_keys=len(entries))
    for key, kind, value in entries:
        writer.add(key, kind, value)
    count, lo, hi = writer.finish()
    return SSTableMeta(name=name, tier=0, min_key=lo, max_key=hi, entry_count=count)


def _setup_tier(
    tmp_path: Path, n_tables: int, tier: int, keys: list[bytes], tombstone: bytes | None = None
) -> ManifestStore:
    """One table per key-set version: table i holds all keys with value i."""
    store = ManifestStore(tmp_path)
    manifest = Manifest()
    for i in range(n_tables):
        entries = [
            (
                k,
                RecordType.DELETE if tombstone is not None and k == tombstone else RecordType.PUT,
                b"" if tombstone is not None and k == tombstone else f"v{i}".encode(),
            )
            for k in keys
        ]
        meta = _write_table(tmp_path, f"sst_{i + 1:06d}_t{tier}.sst", entries)
        manifest.tables.append(
            SSTableMeta(
                name=meta.name,
                tier=tier,
                min_key=meta.min_key,
                max_key=meta.max_key,
                entry_count=meta.entry_count,
            )
        )
    store.save(manifest)
    return store


def _live_view(directory: Path, store: ManifestStore) -> dict[bytes, bytes]:
    view: dict[bytes, tuple[RecordType, bytes]] = {}
    for meta in store.load().tables:
        with SSTableReader(directory / meta.name) as reader:
            for entry in reader:
                view[entry.key] = (entry.kind, entry.value)
    return {k: v for k, (kind, v) in view.items() if kind is RecordType.PUT}


def test_no_plan_below_trigger(tmp_path: Path) -> None:
    store = _setup_tier(tmp_path, 3, 0, [b"a"])
    assert plan_compaction(store.load(), trigger=4) is None
    with pytest.raises(ValueError):
        plan_compaction(store.load(), trigger=1)


def test_plan_picks_lowest_full_tier(tmp_path: Path) -> None:
    store = _setup_tier(tmp_path, 4, 1, [b"a"])
    store2_manifest = store.load()
    # Add a full tier 0 on top (newer tables, higher sequence numbers).
    for i in range(4, 8):
        meta = _write_table(tmp_path, f"sst_{i + 1:06d}_t0.sst", [(b"a", RecordType.PUT, b"new")])
        store2_manifest.tables.append(
            SSTableMeta(name=meta.name, tier=0, min_key=b"a", max_key=b"a", entry_count=1)
        )
    store.save(store2_manifest)
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None
    assert [m.tier for m in plan.inputs] == [0] * 4
    assert plan.output_tier == 1
    assert plan.drop_tombstones is False  # tier 1 below holds older data


def test_newest_wins_and_tombstone_drops_at_bottom(tmp_path: Path) -> None:
    store = _setup_tier(tmp_path, 4, 0, [b"a", b"b"], tombstone=b"b")
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None and plan.drop_tombstones is True
    meta = run_compaction(tmp_path, plan, store, block_bytes=128)
    assert meta is not None and meta.tier == 1 and meta.entry_count == 1
    with SSTableReader(tmp_path / meta.name) as reader:
        assert [(e.key, e.kind, e.value) for e in reader] == [
            (b"a", RecordType.PUT, b"v3")  # newest of the four tables; b dropped
        ]
    # Inputs removed from manifest and disk.
    remaining = [t.name for t in store.load().tables]
    assert remaining == [meta.name]
    assert sum(1 for p in tmp_path.glob("sst_*.sst")) == 1


def test_tombstone_kept_when_older_tier_exists(tmp_path: Path) -> None:
    store = _setup_tier(tmp_path, 4, 0, [b"a", b"b"], tombstone=b"b")
    manifest = store.load()
    old = _write_table(tmp_path, "sst_000009_t1.sst", [(b"b", RecordType.PUT, b"ancient")])
    manifest.tables.append(
        SSTableMeta(name=old.name, tier=1, min_key=b"b", max_key=b"b", entry_count=1)
    )
    store.save(manifest)
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None and plan.drop_tombstones is False
    meta = run_compaction(tmp_path, plan, store, block_bytes=128)
    assert meta is not None
    # The tombstone survives so "ancient" stays shadowed.
    assert _live_view(tmp_path, store) == {b"a": b"v3"}


def test_all_tombstones_yields_no_table(tmp_path: Path) -> None:
    store = _setup_tier(tmp_path, 4, 0, [b"a"], tombstone=b"a")
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None
    assert run_compaction(tmp_path, plan, store, block_bytes=128) is None
    assert store.load().tables == []
    assert list(tmp_path.glob("sst_*.sst")) == []


def test_fault_at_every_step_keeps_view(tmp_path: Path) -> None:
    class Crash(Exception):
        pass

    for point in ("after_write", "after_rename", "after_manifest"):
        sub = tmp_path / point
        sub.mkdir()
        store = _setup_tier(sub, 4, 0, [b"a", b"b"], tombstone=b"b")
        before = _live_view(sub, store)
        plan = plan_compaction(store.load(), trigger=4)
        assert plan is not None

        def crash(at: str, want: str = point) -> None:
            if at == want:
                raise Crash(at)

        with pytest.raises(Crash):
            run_compaction(sub, plan, store, block_bytes=128, fault=crash)
        # Whatever survived (old inputs and/or the new output) reads the same.
        assert _live_view(sub, store) == before
        # Manifest is always parseable and lists only existing files.
        for meta in store.load().tables:
            assert (sub / meta.name).exists()


def test_input_unlink_failure_swallowed_and_gc_recovers(tmp_path: Path) -> None:
    import os

    if os.geteuid() == 0:
        pytest.skip("read-only dirs are still writable as root")
    store = _setup_tier(tmp_path, 4, 0, [b"a", b"b"], tombstone=b"b")
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None

    def chmod_then_continue(at: str) -> None:
        if at == "after_manifest":
            os.chmod(tmp_path, 0o555)  # input deletion below will fail

    try:
        meta = run_compaction(tmp_path, plan, store, block_bytes=128, fault=chmod_then_continue)
    finally:
        os.chmod(tmp_path, 0o755)
    assert meta is not None
    assert _live_view(tmp_path, store) == {b"a": b"v3"}
    # Stale inputs remain on disk but unlisted; the next open GCs them.
    assert len(list(tmp_path.glob("sst_*.sst"))) == 5
    from minikv.storage.lsm import LSMEngine, LSMOptions

    engine = LSMEngine(tmp_path, LSMOptions(memtable_bytes=512))
    engine.open()
    try:
        assert dict(engine.scan(None, None)) == {b"a": b"v3"}
    finally:
        engine.close()
    assert [p.name for p in tmp_path.glob("sst_*.sst")] == [meta.name]


def test_writer_failure_cleans_tmp(tmp_path: Path) -> None:
    from unittest.mock import patch

    from minikv.storage.sstable import SSTableWriter

    store = _setup_tier(tmp_path, 4, 0, [b"a", b"b"])
    plan = plan_compaction(store.load(), trigger=4)
    assert plan is not None
    real_add = SSTableWriter.add
    calls = 0

    def flaky_add(self: SSTableWriter, key: bytes, kind: object, value: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disk on fire")
        real_add(self, key, kind, value)  # type: ignore[arg-type]

    with patch.object(SSTableWriter, "add", flaky_add):
        with pytest.raises(OSError, match="disk on fire"):
            run_compaction(tmp_path, plan, store, block_bytes=128)
    assert calls == 2
    assert list(tmp_path.glob("*.tmp")) == []  # tmp cleaned up
    assert len(store.load().tables) == 4  # manifest untouched

    # Same failure with tmp deletion itself failing: cleanup must not mask
    # the original error.
    calls = 0

    def flaky_chmod_add(self: SSTableWriter, key: bytes, kind: object, value: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            os.chmod(tmp_path, 0o555)
            raise OSError("disk on fire")
        real_add(self, key, kind, value)  # type: ignore[arg-type]

    with patch.object(SSTableWriter, "add", flaky_chmod_add):
        try:
            with pytest.raises(OSError, match="disk on fire"):
                run_compaction(tmp_path, plan, store, block_bytes=128)
        finally:
            os.chmod(tmp_path, 0o755)
    assert len(store.load().tables) == 4  # manifest still untouched
