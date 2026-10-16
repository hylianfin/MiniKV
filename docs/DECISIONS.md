# DECISIONS.md

## D1 (M1): PEP 695 generics in storage
Ruff UP046/UP047 (target py312) requires `class SkipList[V]` instead of
`Generic[V]`. Adopted; mypy 2.4 checks it under strict. Requires Python >= 3.12,
which the scope already fixes.

## D2 (M1): `__contains__` means live-key membership
`key in memtable` is True only for non-tombstoned keys; `get_entry` exposes
tombstones for the LSM scan path, which needs them to shadow SSTables.

## D3 (M1): key/value size limits enforced at the memtable
Key <= 1 KiB, value <= 1 MiB (§5.1) are validated on `put`; scan bounds are
only type-checked since out-of-range bounds are legitimate query edges.

## D4 (M2): WAL raises only on ambiguous corruption at offset 0
A short read (torn tail) never raises at any offset -- an unwritten suffix
cannot hold acknowledged data. A fully-present but invalid record after
valid records truncates there (later bytes untrustworthy). At offset 0 with
no valid prefix, a fully-present invalid record raises CorruptWALError
rather than silently accepting a garbage file. Damage mimicking a torn tail
(e.g. corrupted length running past EOF) replays as empty.

## D5 (M2): WAL close() fsyncs under always/batch
Only a crash (not a clean close) can lose un-fsync'd batch data. `never`
skips fsync entirely and is benchmarks-only.

## D6 (M3): SSTable footer ends with its own length
Min/max keys are variable-length, so the footer cannot be fixed-size.
The trailing u32 length lets the reader locate the footer from EOF; the
CRC covers index_offset through magic (not the length locator itself).

## D7 (M3): on-disk entry kinds reuse wal.RecordType
PUT/DELETE tombstone semantics are identical in WAL and SSTable, so one
IntEnum serves both. memtable.EntryKind maps explicitly at flush time.

## D8 (M3): stray tmp files are ignored, sequence numbers never reused
Readers only open manifest-listed files. `_next_sst_seq` scans the
directory (including `*.tmp`) so a crashed flush's file can never have
its name recycled by a retry.

## D9 (M4): one RLock instead of SSTable reader refcounts
All manifest swaps and reader replacements happen on the single background
worker under the engine RLock, which user reads also hold. A swapped-out
reader therefore has no concurrent user, so it can be closed immediately.
Same safety as refcounts, far less machinery; documented in lsm.py.

## D10 (M4): close() flushes pending data and removes empty WALs
A clean close leaves no WAL litter: non-empty memtables flush (their WALs
are deleted post-manifest), empty WALs are deleted directly.

## D11 (M4): flush()/compact() accept a test-only fault hook
The public signatures gain an optional `fault` parameter (default None)
forwarded to the storage crash points. Production callers never pass it;
crash tests use it to simulate failure between steps.

## D12 (M5): group commit via action coalescing, not core timers
`propose()` appends synchronously (index known immediately, matching the
spec signature) while runtimes coalesce one batch window's actions:
contiguous Persists merge into one fsync, each follower keeps its latest
Send. Same network/disk effect as core-side batching, far simpler core.

## D13 (M5): core takes a read-only store seed; durability is action-ordered
The core copies PersistedState at construction and never touches the store
again. Every term/vote/log mutation emits SaveTermVote/Persist BEFORE the
dependent Sends; runtimes must execute actions in order (in-order
execution + memory/store fate-sharing is what makes duplicate-delivery
skipping and restart recovery sound).

## D14 (M5): pre-vote grant is log-check only (no voted_for gate)
Gating pre-votes on voted_for deadlocks post-crash elections (everyone
voted for the dead leader). Safety holds: pre-votes elect nothing, and
real votes keep one-vote-per-term.

## D15 (M5): fast-backup next_index strictly decreases
Jumping past the leader's own term end can overshoot the follower and
oscillate (6->3->6). The jump is capped back to the follower's conflict
point, so every rejection makes progress (bounded below by 1).

## D16 (M5): invariants re-baseline commit/applied on restart
Monotonicity is per-incarnation; a restart adopts durable state as the new
floor (tracked via a per-node generation counter). Leader histories reset
likewise. Committed-entry records are global and never reset.

## D17 (M5): handle_message takes an optional sender
RPC responses carry no author (matching proto), but check-quorum needs
contact attribution, so the runtime passes it. Requests ignore it (they
carry candidate/leader ids).

## D18 (M6): sessions live in a separate LSM engine, not a key prefix
A reserved key prefix would collide with arbitrary user bytes and leak
into scans. A second engine (same apply path, same crash semantics) is
airtight at the cost of one more WAL/memtable per node.

## D19 (M6): session record follows the data write, never precedes it
A crash between them re-applies idempotently (all ops last-writer-wins);
recording first could acknowledge data that was never written. Replay is
result-stable because the original session record (durable) is consulted
before any re-apply.

## D20 (M6): stale (older-than-recorded) sequences re-execute unrecorded
Unreachable through Raft (same-client entries apply in seq order), but the
fallback is state-safe rather than an error: execute, return fresh result,
leave the session table untouched.

## D21 (M7): per-request read mode wins; batching defaults on, flaggable off
Config `read_mode` is validated but the RPC's ReadMode decides each read.
`batching` (TOML key + `--batching/--no-batching`) exists for the M9
with/without comparison; the window still comes from `batch_window_ms`.

## D22 (M7): fsyncs under a disk lock in an executor, never under core lock
Tick stalls would cause false elections. FIFO lock fairness keeps disk
order identical to core order; a dedicated apply queue preserves global
apply order (CAS is order-sensitive) and drives client waiters.

## D23 (M7): poisoned log entries fail-stop the node
An undecodable entry cannot be skipped (order is sacred) or applied
(state would diverge). The apply loop fails all waiters, records the
error, and stop() surfaces it after cleanup.

## D24 (M7): hand-written pb2 stubs instead of stub packages
Generated protobuf has no static types; rather than adding stub
dependencies or weakening checks, `minikv_pb2*.pyi` are maintained
alongside the proto (regenerate mentally on proto change).

## D25 (M7): scans snapshot at stream start
Scan materializes under the engine lock, so a mid-stream leadership loss
cannot corrupt it; clients restart streams on NOT_LEADER (duplicates
across the restart are possible, loss/invention are not).

## D26 (M7): ReadIndex = heartbeat majority + wait applied
The leader records commit_index, confirms a heartbeat majority at the
same term, then waits for LSM application (not just core emission) before
reading. Stale-leader reads are impossible: a deposed leader cannot
assemble a same-term majority without learning the newer term.
