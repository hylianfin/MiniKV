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
