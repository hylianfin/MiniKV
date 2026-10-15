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
