# PROGRESS.md

## M0 (done)
- Scaffolded repo layout (minikv/storage, raft, kv, server, client, sim, tests, bench, docker, proto).
- pyproject.toml (py3.12, grpcio, typer, pytest/hypothesis, ruff, mypy strict on storage+raft), CI workflow (lint, format, types, tests, proto compile), cluster.toml, Dockerfile/compose, AGENTS.md, README/DESIGN/architecture/benchmarks placeholders.
- Verified: `pytest` 1 passed, `ruff check` clean, `ruff format --check` clean, `mypy minikv sim` clean, `grpc_tools.protoc` compiles `proto/minikv.proto`.
- Decision: generated protobuf output excluded from repo; CI verifies compilation to /tmp.

## M1 (done)- `minikv/storage/skiplist.py`: generic `SkipList[V]` (bytes keys), max level 16, p=0.5, injectable `random.Random`, put/get/physical delete, ordered `items()`/`seek()`, approximate `memory_size`.
- `minikv/storage/memtable.py`: `MemTable` over skip list, `(key, seq, kind, value)` entries, tombstone deletes (shadow older SSTables), `get` hides tombstones / `get_entry` exposes them, `scan` with `[start, end)`, `make_immutable` freeze, `should_flush` size gate, key<=1KiB / value<=1MiB limits.
- Tests: unit + property model tests (skiplist vs sorted dict incl. seek; memtable vs tombstone model). Coverage 100% on both modules. 25 tests pass; ruff check/format and mypy strict clean.
- Decisions: PEP 695 generics (D1); live-key `__contains__` semantics (D2); limits enforced at memtable (D3).

## M2 (done)
- `minikv/storage/wal.py`: `WAL` with exact §5.4 record codec (crc32/length/type/raft_index/klen/key/vlen/value, LE), `FsyncPolicy` always/batch/never, recovery that replays to first invalid record then truncates the torn/corrupt tail, `CorruptWALError` only when record 0 is fully present but invalid.
- Tests: exhaustive truncation at every byte offset (prefix property + file truncation), single-byte corruption at every position (prefix or raise), crafted valid-CRC/malformed-body cases, fsync-policy matrix, hypothesis round-trip. 100% storage coverage. 43 tests pass; ruff check/format and mypy strict clean.
- Decisions: ambiguous-offset-0 raises, torn tail never raises (D4); close() fsyncs under always/batch (D5).

## Left
- M3..M10 per scope. Next: M3 SSTable, bloom, manifest + flush path.
