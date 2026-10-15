# PROGRESS.md

## M0 (done)
- Scaffolded repo layout (minikv/storage, raft, kv, server, client, sim, tests, bench, docker, proto).
- pyproject.toml (py3.12, grpcio, typer, pytest/hypothesis, ruff, mypy strict on storage+raft), CI workflow (lint, format, types, tests, proto compile), cluster.toml, Dockerfile/compose, AGENTS.md, README/DESIGN/architecture/benchmarks placeholders.
- Verified: `pytest` 1 passed, `ruff check` clean, `ruff format --check` clean (53 files), `mypy minikv sim` clean (34 files), `grpc_tools.protoc` compiles `proto/minikv.proto`.
- Decision: generated protobuf output excluded from repo; CI verifies compilation to /tmp.

## Left
- M1..M10 per scope. Next: M1 skip list + memtable.
