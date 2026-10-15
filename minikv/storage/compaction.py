"""Size-tiered compaction: merge whole tiers into the next tier.

When a tier holds ``trigger`` (default 4) SSTables, all of them merge into
one SSTable in the next tier via a k-way heap merge. For duplicate keys the
newest table wins (manifest order is oldest-first). Tombstones are dropped
only when the merge covers the bottom (oldest) tier -- otherwise an older
table outside the merge could still shadow the key and resurrect it.
"""

from __future__ import annotations

import heapq
import itertools
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from minikv.storage.manifest import Manifest, ManifestStore, SSTableMeta, fsync_dir
from minikv.storage.sstable import SSTableReader, SSTableWriter, SSTEntry, alloc_sst_path
from minikv.storage.wal import RecordType


@dataclass(frozen=True, slots=True)
class CompactionPlan:
    inputs: list[SSTableMeta]  # oldest first; later tables shadow earlier ones
    output_tier: int
    drop_tombstones: bool


def plan_compaction(manifest: Manifest, trigger: int) -> CompactionPlan | None:
    """Pick the lowest-numbered tier holding ``trigger`` or more tables."""
    if trigger < 2:
        raise ValueError("compaction_trigger must be >= 2")
    by_tier: dict[int, list[SSTableMeta]] = {}
    for meta in manifest.tables:
        by_tier.setdefault(meta.tier, []).append(meta)
    candidates = [tier for tier, metas in by_tier.items() if len(metas) >= trigger]
    if not candidates:
        return None
    tier = min(candidates)
    # The whole tier merges, so tombstones die only when that tier IS the
    # oldest data in the engine (nothing older survives outside the merge).
    return CompactionPlan(
        inputs=list(by_tier[tier]),
        output_tier=tier + 1,
        drop_tombstones=(tier == max(by_tier)),
    )


def run_compaction(
    data_dir: Path | str,
    plan: CompactionPlan,
    store: ManifestStore,
    block_bytes: int = 4096,
    bloom_bits_per_key: int = 10,
    fault: Callable[[str], None] | None = None,
) -> SSTableMeta | None:
    """Execute one plan: merge inputs, swap the manifest, delete inputs.

    Same crash protocol as flush (tmp -> fsync -> rename -> dir fsync ->
    manifest swap; ``fault`` fires at ``after_write`` / ``after_rename`` /
    ``after_manifest``). Input files are deleted only after the swap; a
    crash before that leaves them listed (still correct) or unlisted
    (reclaimed by open-time GC). Returns ``None`` when the merge yields no
    entries (every key tombstoned and droppable): inputs are simply removed.
    """
    directory = Path(data_dir)
    readers = [SSTableReader(directory / meta.name) for meta in plan.inputs]
    try:
        final, tmp = alloc_sst_path(directory, plan.output_tier)
        writer = SSTableWriter(
            tmp,
            block_bytes,
            expected_keys=sum(m.entry_count for m in plan.inputs),
            bloom_bits_per_key=bloom_bits_per_key,
        )
        written = 0
        try:
            for key, kind, value in _merge(readers, plan.drop_tombstones):
                writer.add(key, kind, value)
                written += 1
            if written == 0:
                writer.abort()
                tmp.unlink(missing_ok=True)
                _remove_from_manifest(store, plan)
                _delete_inputs(directory, plan)
                return None
            count, lo, hi = writer.finish()
        except BaseException:
            writer.abort()
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        if fault is not None:
            fault("after_write")
        os.replace(tmp, final)
        fsync_dir(directory)
        if fault is not None:
            fault("after_rename")
        meta = SSTableMeta(
            name=final.name,
            tier=plan.output_tier,
            min_key=lo,
            max_key=hi,
            entry_count=count,
        )
        manifest = store.load()
        doomed = {m.name for m in plan.inputs}
        manifest.tables = [t for t in manifest.tables if t.name not in doomed]
        manifest.tables.append(meta)
        store.save(manifest)
        if fault is not None:
            fault("after_manifest")
        _delete_inputs(directory, plan)
        return meta
    finally:
        for reader in readers:
            reader.close()


def _delete_inputs(directory: Path, plan: CompactionPlan) -> None:
    for meta_in in plan.inputs:
        try:
            (directory / meta_in.name).unlink(missing_ok=True)
        except OSError:
            pass  # open-time GC is the backstop


def _remove_from_manifest(store: ManifestStore, plan: CompactionPlan) -> None:
    manifest = store.load()
    doomed = {m.name for m in plan.inputs}
    manifest.tables = [t for t in manifest.tables if t.name not in doomed]
    store.save(manifest)


def _merge(
    readers: list[SSTableReader], drop_tombstones: bool
) -> Iterator[tuple[bytes, RecordType, bytes]]:
    """K-way merge, newest input winning each key. Inputs are oldest-first."""
    seq = itertools.count()
    heap: list[tuple[bytes, int, int, int, SSTEntry]] = []  # key, -newness, seq, src, entry
    iters = [iter(reader) for reader in readers]
    for src, it in enumerate(iters):
        first = next(it, None)
        if first is not None:
            heapq.heappush(heap, (first.key, -src, next(seq), src, first))
    while heap:
        key, _, _, src, newest = heapq.heappop(heap)
        _refill(iters, heap, seq, src)
        # Newest sorts first via -src, so later copies of this key are stale.
        while heap and heap[0][0] == key:
            _, _, _, other_src, _ = heapq.heappop(heap)
            _refill(iters, heap, seq, other_src)
        if newest.kind is RecordType.DELETE and drop_tombstones:
            continue
        yield (newest.key, newest.kind, newest.value)


def _refill(
    iters: list[Iterator[SSTEntry]],
    heap: list[tuple[bytes, int, int, int, SSTEntry]],
    seq: itertools.count[int],
    src: int,
) -> None:
    nxt = next(iters[src], None)
    if nxt is not None:
        heapq.heappush(heap, (nxt.key, -src, next(seq), src, nxt))
