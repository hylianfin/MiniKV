"""Unit tests for the manifest store."""

from pathlib import Path

import pytest

from minikv.storage.manifest import Manifest, ManifestCorruptError, ManifestStore, SSTableMeta

META = SSTableMeta(name="sst_000001_t0.sst", tier=0, min_key=b"a", max_key=b"z", entry_count=10)


def test_save_load_roundtrip(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    assert store.directory == tmp_path
    assert store.load() == Manifest()  # missing file -> empty
    store.save(Manifest(tables=[META], flushed_raft_index=42))
    loaded = store.load()
    assert loaded == Manifest(tables=[META], flushed_raft_index=42)


def test_garbage_manifest_raises(tmp_path: Path) -> None:
    (tmp_path / "MANIFEST").write_text("not json{", encoding="utf-8")
    with pytest.raises(ManifestCorruptError):
        ManifestStore(tmp_path).load()
    (tmp_path / "MANIFEST").write_text('{"tables": "nope"}', encoding="utf-8")
    with pytest.raises(ManifestCorruptError):
        ManifestStore(tmp_path).load()
    (tmp_path / "MANIFEST").write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(ManifestCorruptError):
        ManifestStore(tmp_path).load()


@pytest.mark.parametrize(
    "entry",
    [
        42,  # not an object
        {"name": "x.sst"},  # missing keys
        {
            "name": "x.sst",
            "tier": 0,
            "min_key": "zz",  # bad hex
            "max_key": "00",
            "entry_count": 1,
        },
        {
            "name": "x.sst",
            "tier": 0,
            "min_key": 7,  # not a string
            "max_key": "00",
            "entry_count": 1,
        },
        {"name": "", "tier": 0, "min_key": "00", "max_key": "00", "entry_count": 1},
        {"name": "x.sst", "tier": -1, "min_key": "00", "max_key": "00", "entry_count": 1},
        {"name": "x.sst", "tier": 0, "min_key": "00", "max_key": "00", "entry_count": -1},
    ],
)
def test_bad_table_entries_raise(tmp_path: Path, entry: object) -> None:
    import json

    (tmp_path / "MANIFEST").write_text(
        json.dumps({"tables": [entry], "flushed_raft_index": 0}), encoding="utf-8"
    )
    with pytest.raises(ManifestCorruptError):
        ManifestStore(tmp_path).load()


def test_crash_points_keep_old_or_new_never_mix(tmp_path: Path) -> None:
    class Crash(Exception):
        pass

    def crash_at(point: str) -> None:
        raise Crash(point)

    store = ManifestStore(tmp_path)
    store.save(Manifest(tables=[META], flushed_raft_index=1))
    # Fault after tmp write, before rename: old manifest intact.
    failing = ManifestStore(tmp_path, fault=crash_at)
    with pytest.raises(Crash):
        failing.save(Manifest(tables=[], flushed_raft_index=99))
    assert store.load().flushed_raft_index == 1
    # Fault after rename, before dir fsync: new manifest already in place.
    calls: list[str] = []

    def crash_after_rename(point: str) -> None:
        calls.append(point)
        if point == "after_rename":
            raise Crash(point)

    with pytest.raises(Crash):
        ManifestStore(tmp_path, fault=crash_after_rename).save(
            Manifest(tables=[], flushed_raft_index=2)
        )
    assert calls == ["after_tmp", "after_rename"]
    assert store.load().flushed_raft_index == 2
