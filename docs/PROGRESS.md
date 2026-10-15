# PROGRESS.md

## M0 (done)
- Scaffolded repo layout (minikv/storage, raft, kv, server, client, sim, tests, bench, docker, proto).
- pyproject.toml (py3.12, grpcio, typer, pytest/hypothesis, ruff, mypy strict on storage+raft), CI workflow (lint, format, types, tests, proto compile), cluster.toml, Dockerfile/compose, AGENTS.md, README/DESIGN/architecture/benchmarks placeholders.
- Verified: `pytest` 1 passed, `ruff check` clean, `ruff format --check` clean, `mypy minikv sim` clean, `grpc_tools.protoc` compiles `proto/minikv.proto`.
- Decision: generated protobuf output excluded from repo; CI verifies compilation to /tmp.

## M1 (done)
- `minikv/storage/skiplist.py`: generic `SkipList[V]` (bytes keys), max level 16, p=0.5, injectable `random.Random`, put/get/physical delete, ordered `items()`/`seek()`, approximate `memory_size`.
- `minikv/storage/memtable.py`: `MemTable` over skip list, `(key, seq, kind, value)` entries, tombstone deletes (shadow older SSTables), `get` hides tombstones / `get_entry` exposes them, `scan` with `[start, end)`, `make_immutable` freeze, `should_flush` size gate, key<=1KiB / value<=1MiB limits.
- Tests: unit + property model tests (skiplist vs sorted dict incl. seek; memtable vs tombstone model). Coverage 100% on both modules. 25 tests pass; ruff check/format and mypy strict clean.
- Decisions: PEP 695 generics (D1); live-key `__contains__` semantics (D2); limits enforced at memtable (D3).

## Left
- M2..M10 per scope. Next: M2 WAL with recovery.
