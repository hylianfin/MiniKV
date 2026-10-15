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
