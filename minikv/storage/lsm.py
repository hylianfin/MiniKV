"""LSM engine: the public storage API (§5.1).

Write path: WAL append (fsync per policy) -> mutable memtable. A full
memtable seals, a fresh memtable + WAL takes over, and the sealed one goes
to a background worker that flushes it to an SSTable and updates the
manifest. Read path: mutable memtable -> sealed memtables (newest first)
-> SSTables (newest first); the first hit wins, tombstones read as absent.

Threading: one daemon worker performs all flushes, manifest swaps, and
compactions. A single ``RLock`` serializes user reads against worker swaps,
so a swapped-out SSTable can be closed immediately -- no reader can be
inside it (documented deviation from per-reader refcounts, see D9).
``flush()`` / ``compact()`` / ``close()`` block until the worker drains;
they must never be called from the worker itself (they aren't).
"""

from __future__ import annotations

import heapq
import itertools
import queue
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from minikv.storage.compaction import plan_compaction, run_compaction
from minikv.storage.manifest import Manifest, ManifestStore
from minikv.storage.memtable import EntryKind, MemEntry, MemTable
from minikv.storage.sstable import (
    CorruptSSTableError,
    SSTableReader,
    flush_memtable,
    gc_unlisted_files,
)
from minikv.storage.wal import WAL, FsyncPolicy, RecordType, WalRecord

_WAL_NAME = re.compile(r"wal_(\d+)\.log")
_WAL_SUFFIX = ".log"


@dataclass(frozen=True, slots=True)
class LSMOptions:
    memtable_bytes: int = 4 * 1024 * 1024
    fsync_policy: FsyncPolicy = FsyncPolicy.ALWAYS
    batch_interval_ms: int = 10
    block_bytes: int = 4096
    bloom_bits_per_key: int = 10
    compaction_trigger: int = 4

    def __post_init__(self) -> None:
        if self.memtable_bytes <= 0:
            raise ValueError("memtable_bytes must be positive")
        if self.batch_interval_ms < 0:
            raise ValueError("batch_interval_ms must be >= 0")
        if self.block_bytes <= 0:
            raise ValueError("block_bytes must be positive")
        if self.bloom_bits_per_key < 1:
            raise ValueError("bloom_bits_per_key must be >= 1")
        if self.compaction_trigger < 2:
            raise ValueError("compaction_trigger must be >= 2")


@dataclass(frozen=True, slots=True)
class _Sealed:
    mem: MemTable
    wal_path: Path
    max_raft: int


@dataclass(frozen=True, slots=True)
class _FlushJob:
    mem: MemTable
    wal_path: Path
    max_raft: int


@dataclass(frozen=True, slots=True)
class _CompactJob:
    pass


@dataclass(frozen=True, slots=True)
class _StopJob:
    pass


_Job = _FlushJob | _CompactJob | _StopJob


class LSMEngine:
    """Crash-safe LSM tree. Not safe for concurrent ``flush``/``compact``/
    ``close`` calls from several threads; concurrent ``put``/``get``/``scan``
    are fine (one thread should drive maintenance)."""

    def __init__(self, path: Path | str, options: LSMOptions) -> None:
        self._dir = Path(path)
        self._options = options
        self._store = ManifestStore(self._dir)
        self._guard = threading.RLock()
        self._queue: queue.Queue[_Job] = queue.Queue()
        self._worker = threading.Thread(target=self._worker_loop, name="minikv-flush", daemon=True)
        self._worker_error: BaseException | None = None
        self._crashed = False
        self._is_open = False
        self._is_closed = False
        self._fault: Callable[[str], None] | None = None
        self._mem = MemTable(max_size_bytes=options.memtable_bytes)
        self._sealed: list[_Sealed] = []
        self._tables: dict[str, SSTableReader] = {}
        self._manifest = Manifest()
        self._wal: WAL | None = None
        self._wal_seq = 0
        self._mem_max_raft = 0
        self._applied = 0
        self._last_batch_fsync = 0.0

    @property
    def path(self) -> Path:
        return self._dir

    def open(self) -> None:
        """Recover: load manifest, GC stray SSTables, replay WALs, resume."""
        with self._guard:
            if self._is_open:
                raise RuntimeError("engine is already open")
            if self._is_closed:
                raise RuntimeError("engine is closed")
            self._dir.mkdir(parents=True, exist_ok=True)
            manifest = self._store.load()
            gc_unlisted_files(self._dir, {t.name for t in manifest.tables})
            tables: dict[str, SSTableReader] = {}
            try:
                for meta in manifest.tables:
                    try:
                        tables[meta.name] = SSTableReader(self._dir / meta.name)
                    except FileNotFoundError as e:
                        raise CorruptSSTableError(f"manifest lists missing file {meta.name}") from e
            except BaseException:
                for reader in tables.values():
                    reader.close()
                raise
            replays = self._replay_wals()
            self._manifest = manifest
            self._tables = tables
            self._applied = manifest.flushed_raft_index
            if replays:
                *sealed, (last_path, last_recs) = replays
                for wal_path, records in sealed:
                    mem, mx = _rebuild(records, self._options.memtable_bytes)
                    mem.make_immutable()
                    self._sealed.append(_Sealed(mem, wal_path, mx))
                    self._applied = max(self._applied, mx)
                mem, mx = _rebuild(last_recs, self._options.memtable_bytes)
                self._mem = mem
                self._mem_max_raft = mx
                self._applied = max(self._applied, mx)
                self._wal_seq = _wal_seq_of(last_path)
                self._wal = WAL(last_path, self._options.fsync_policy)
                self._wal.open()  # re-open for append; recovery is idempotent
                if self._mem.should_flush:
                    self._rotate_locked()
            else:
                self._new_wal_locked()
            self._worker.start()
            for job in self._sealed:
                self._queue.put(_FlushJob(job.mem, job.wal_path, job.max_raft))
            self._is_open = True

    def put(self, key: bytes, value: bytes, raft_index: int = 0) -> None:
        with self._guard:
            wal, mem = self._require_open_locked()
            wal.append_put(key, value, raft_index)  # validates before writing
            mem.put(key, value, raft_index)
            self._applied = max(self._applied, raft_index)
            self._mem_max_raft = max(self._mem_max_raft, raft_index)
            self._batch_fsync_locked()
            if mem.should_flush:
                self._rotate_locked()

    def delete(self, key: bytes, raft_index: int = 0) -> None:
        with self._guard:
            wal, mem = self._require_open_locked()
            wal.append_delete(key, raft_index)
            mem.delete(key, raft_index)
            self._applied = max(self._applied, raft_index)
            self._mem_max_raft = max(self._mem_max_raft, raft_index)
            self._batch_fsync_locked()
            if mem.should_flush:
                self._rotate_locked()

    def get(self, key: bytes) -> bytes | None:
        """Newest version across memtables then SSTables; tombstone -> None."""
        with self._guard:
            self._require_open_locked()
            entry = self._mem.get_entry(key)
            if entry is None:
                for sealed in reversed(self._sealed):
                    entry = sealed.mem.get_entry(key)
                    if entry is not None:
                        break
            if entry is not None:
                return entry.value if entry.kind is EntryKind.PUT else None
            for meta in reversed(self._manifest.tables):
                found = self._tables[meta.name].get(key)
                if found is None:
                    continue
                return found.value if found.kind is RecordType.PUT else None
            return None

    def scan(
        self, start: bytes | None, end: bytes | None, limit: int = 0
    ) -> Iterator[tuple[bytes, bytes]]:
        """Keys in ``[start, end)``; ``limit`` 0 means unlimited. The result
        is materialized under the lock so iteration never races a swap."""
        if limit < 0:
            raise ValueError("limit must be >= 0")
        with self._guard:
            self._require_open_locked()
            if start is not None and not isinstance(start, (bytes, bytearray)):
                raise TypeError("start must be bytes or None")
            if end is not None and not isinstance(end, (bytes, bytearray)):
                raise TypeError("end must be bytes or None")
            lo = bytes(start) if start is not None else b""
            hi = bytes(end) if end is not None else None
            if hi is not None and hi <= lo:
                return iter(())
            sources: list[Iterator[tuple[bytes, RecordType, bytes]]] = []
            sources.append(((k, _mem_kind(e), e.value) for k, e in self._mem.scan(start, end)))
            for sealed in reversed(self._sealed):
                sources.append(((k, _mem_kind(e), e.value) for k, e in sealed.mem.scan(start, end)))
            for meta in reversed(self._manifest.tables):
                reader = self._tables[meta.name]
                sources.append((e.key, e.kind, e.value) for e in reader.iter_from(lo))
            merged = _merge_sources(sources, hi, limit)
            return iter(merged)

    def applied_index(self) -> int:
        """Highest raft_index durably recorded (WAL or SSTable)."""
        with self._guard:
            return self._applied

    def flush(self, fault: Callable[[str], None] | None = None) -> None:
        """Seal the memtable (if non-empty) and wait for all pending
        flushes. ``fault`` is a test hook forwarded to the flush path."""
        with self._guard:
            self._require_open_locked()
            self._fault = fault
            if len(self._mem) > 0:
                self._rotate_locked()
        try:
            self._drain()
        finally:
            with self._guard:
                self._fault = None

    def compact(self, fault: Callable[[str], None] | None = None) -> None:
        """Run compaction rounds until no tier needs one. ``fault`` is a
        test hook forwarded to the compaction path."""
        with self._guard:
            self._require_open_locked()
            self._fault = fault
            self._queue.put(_CompactJob())
        try:
            self._drain()
        finally:
            with self._guard:
                self._fault = None

    def close(self) -> None:
        """Flush pending data, stop the worker, release files. Idempotent;
        a worker failure surfaces here after cleanup."""
        with self._guard:
            if self._is_closed or not self._is_open:
                self._is_closed = True
                return
            if len(self._mem) > 0:
                self._rotate_locked()
            else:
                self._drop_empty_wal_locked()
        # Drain without the guard: worker jobs need it to swap tables.
        self._queue.join()
        with self._guard:
            # A close-time rotation above left a fresh empty WAL: drop it.
            self._drop_empty_wal_locked()
            self._queue.put(_StopJob())
        self._queue.join()
        self._worker.join()
        with self._guard:
            for reader in self._tables.values():
                reader.close()
            self._tables.clear()
            self._is_open = False
            self._is_closed = True
            error, self._worker_error = self._worker_error, None
        if error is not None:
            raise error

    def crash(self) -> None:
        """Simulate an unclean shutdown (like kill -9): stop the worker and
        release files WITHOUT flushing. In-memory memtables are lost; WALs
        and SSTables stay as they are. Test and simulation hook; the engine
        cannot be used afterwards (reopen a new one on the same directory)."""
        with self._guard:
            if not self._is_open or self._is_closed:
                return
            self._crashed = True
            self._queue.put(_StopJob())
        self._worker.join()
        with self._guard:
            for reader in self._tables.values():
                reader.close()
            self._tables.clear()
            if self._wal is not None:
                self._wal.close()
            self._is_open = False
            self._is_closed = True

    def _drop_empty_wal_locked(self) -> None:
        # Only a WAL with no appends and an empty memtable is droppable;
        # sealed WALs always have pending (or finished) flush jobs.
        if self._wal is not None and len(self._mem) == 0 and self._wal.record_count == 0:
            wal, self._wal = self._wal, None
            wal.close()
            try:
                wal.path.unlink(missing_ok=True)
            except OSError:
                pass

    def _drain(self) -> None:
        # Outside the guard on purpose: worker jobs need the guard to swap.
        self._queue.join()
        with self._guard:
            error, self._worker_error = self._worker_error, None
        if error is not None:
            raise error

    def _worker_loop(self) -> None:
        while True:
            job = self._queue.get()
            try:
                if isinstance(job, _StopJob):
                    return
                if self._crashed:
                    continue  # simulated crash killed this work; abandon it
                if isinstance(job, _FlushJob):
                    self._run_flush_job(job)
                else:
                    self._run_compactions()
            except BaseException as e:  # noqa: BLE001 -- ferried to flush()/close()
                if self._worker_error is None:
                    self._worker_error = e
            finally:
                self._queue.task_done()

    def _run_flush_job(self, job: _FlushJob) -> None:
        fault = self._fault
        flush_memtable(
            job.mem,
            self._dir,
            self._store,
            tier=0,
            block_bytes=self._options.block_bytes,
            bloom_bits_per_key=self._options.bloom_bits_per_key,
            fault=fault,
            flushed_raft_index=job.max_raft,
        )
        with self._guard:
            self._refresh_tables_locked()
            try:
                job.wal_path.unlink(missing_ok=True)
            except OSError:
                pass
            self._sealed = [s for s in self._sealed if s.wal_path != job.wal_path]
        self._run_compactions()

    def _run_compactions(self) -> None:
        while True:
            plan = plan_compaction(self._store.load(), self._options.compaction_trigger)
            if plan is None:
                break
            run_compaction(
                self._dir,
                plan,
                self._store,
                block_bytes=self._options.block_bytes,
                bloom_bits_per_key=self._options.bloom_bits_per_key,
                fault=self._fault,
            )
            with self._guard:
                self._refresh_tables_locked()

    def _refresh_tables_locked(self) -> None:
        manifest = self._store.load()
        live = {meta.name for meta in manifest.tables}
        for name in list(self._tables):
            if name not in live:
                self._tables.pop(name).close()
        for meta in manifest.tables:
            if meta.name not in self._tables:
                try:
                    self._tables[meta.name] = SSTableReader(self._dir / meta.name)
                except FileNotFoundError as e:
                    raise CorruptSSTableError(f"manifest lists missing file {meta.name}") from e
        self._manifest = manifest

    def _replay_wals(self) -> list[tuple[Path, list[WalRecord]]]:
        matches = [p for p in self._dir.glob("wal_*.log") if _WAL_NAME.fullmatch(p.name)]
        paths = sorted(matches, key=_wal_seq_of)
        out: list[tuple[Path, list[WalRecord]]] = []
        for path in paths:
            wal = WAL(path, self._options.fsync_policy)
            try:
                records = wal.open()
            except BaseException:
                wal.close()
                raise
            if not records:
                wal.close()
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            wal.close()
            out.append((path, records))
        return out

    def _rotate_locked(self) -> None:
        # New WAL first: if that fails, the current memtable is untouched.
        seq = self._wal_seq + 1
        fresh = WAL(self._dir / f"wal_{seq:06d}{_WAL_SUFFIX}", self._options.fsync_policy)
        fresh.open()
        assert self._wal is not None
        old_path = self._wal.path
        self._wal.close()
        self._wal = fresh
        self._wal_seq = seq
        self._mem.make_immutable()
        self._sealed.append(_Sealed(self._mem, old_path, self._mem_max_raft))
        self._mem = MemTable(max_size_bytes=self._options.memtable_bytes)
        self._mem_max_raft = 0
        sealed = self._sealed[-1]
        self._queue.put(_FlushJob(sealed.mem, sealed.wal_path, sealed.max_raft))

    def _new_wal_locked(self) -> None:
        seq = _next_wal_seq(self._dir)
        wal = WAL(self._dir / f"wal_{seq:06d}{_WAL_SUFFIX}", self._options.fsync_policy)
        wal.open()
        self._wal = wal
        self._wal_seq = seq

    def _batch_fsync_locked(self) -> None:
        if self._options.fsync_policy is not FsyncPolicy.BATCH:
            return
        assert self._wal is not None
        now = time.monotonic()
        if (now - self._last_batch_fsync) * 1000 >= self._options.batch_interval_ms:
            self._wal.fsync()
            self._last_batch_fsync = now

    def _require_open_locked(self) -> tuple[WAL, MemTable]:
        if not self._is_open or self._is_closed or self._wal is None:
            raise RuntimeError("engine is not open")
        return self._wal, self._mem


def _rebuild(records: list[WalRecord], memtable_bytes: int) -> tuple[MemTable, int]:
    """Replay WAL records into a fresh memtable. The caller seals it when
    the memtable is a sealed backlog entry (the live one stays mutable)."""
    mem = MemTable(max_size_bytes=memtable_bytes)
    mx = 0
    for record in records:
        if record.kind is RecordType.PUT:
            mem.put(record.key, record.value, record.raft_index)
        else:
            mem.delete(record.key, record.raft_index)
        mx = max(mx, record.raft_index)
    return mem, mx


def _wal_seq_of(path: Path) -> int:
    m = _WAL_NAME.fullmatch(path.name)
    assert m is not None
    return int(m.group(1))


def _next_wal_seq(directory: Path) -> int:
    best = 0
    if directory.exists():
        for child in directory.iterdir():
            m = _WAL_NAME.fullmatch(child.name)
            if m:
                best = max(best, int(m.group(1)))
    return best + 1


def _mem_kind(entry: MemEntry) -> RecordType:
    # MemEntry.kind is EntryKind; on-disk readers use RecordType.
    if entry.kind is EntryKind.PUT:
        return RecordType.PUT
    return RecordType.DELETE


def _merge_sources(
    sources: list[Iterator[tuple[bytes, RecordType, bytes]]],
    end: bytes | None,
    limit: int,
) -> list[tuple[bytes, bytes]]:
    """Heap-merge newest-first sources; first hit per key wins."""
    seq = itertools.count()
    heap: list[tuple[bytes, int, int, RecordType, bytes]] = []
    for rank, it in enumerate(sources):
        first = next(it, None)
        if first is not None:
            heapq.heappush(heap, (first[0], rank, next(seq), first[1], first[2]))
    out: list[tuple[bytes, bytes]] = []
    while heap:
        key, rank, _, kind, value = heapq.heappop(heap)
        _refill_source(sources, heap, seq, rank)
        while heap and heap[0][0] == key:
            _, other_rank, _, _, _ = heapq.heappop(heap)
            _refill_source(sources, heap, seq, other_rank)
        if end is not None and key >= end:
            break
        if kind is RecordType.DELETE:
            continue
        out.append((key, value))
        if limit and len(out) >= limit:
            break
    return out


def _refill_source(
    sources: list[Iterator[tuple[bytes, RecordType, bytes]]],
    heap: list[tuple[bytes, int, int, RecordType, bytes]],
    seq: itertools.count[int],
    rank: int,
) -> None:
    nxt = next(sources[rank], None)
    if nxt is not None:
        heapq.heappush(heap, (nxt[0], rank, next(seq), nxt[1], nxt[2]))
