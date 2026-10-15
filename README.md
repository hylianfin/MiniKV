# MiniKV

> **Build note:** this project was built with AI assistance (OpenCode), with design decisions, review and testing by the author.

A small distributed key-value store in Python: 3/5-node Raft, LSM storage engine from scratch, gRPC API, CLI, deterministic simulation tests, linearizability checker, benchmarks.

## Quickstart (M0 scaffold)

```bash
uv sync --extra dev
python -m grpc_tools.protoc -Iproto --python_out=/tmp --grpc_python_out=/tmp proto/minikv.proto
pytest
ruff check .
ruff format --check .
mypy minikv sim
```

Full quickstart (3-node cluster, CLI) lands in M7. See `docs/architecture.md`, `DESIGN.md`, `docs/benchmarks.md`.

## Layout

See scope section 3. Storage in `minikv/storage/`, Raft in `minikv/raft/`, simulator in `sim/`.

## Limitations

M0 only: scaffold compiles, no KV behavior yet.
