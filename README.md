# MiniKV

> **Build note:** this project was built with AI assistance (OpenCode), with
> design decisions, review and testing by the author (R.paco, @hylianfin).

A small distributed key-value store in Python: Raft-replicated across 3 or 5
nodes, an LSM storage engine written from scratch, a gRPC API with a client
library and CLI, deterministic simulation tests, a linearizability checker,
and published benchmarks.

## Quickstart

Prerequisites: Python 3.12+, Docker + Compose (for the 3-node cluster).

```bash
# install (editable) and generate protobuf code
pip install -e ".[dev]"
make proto

# run the full test suite
make test            # pytest
make lint            # ruff check + format check
make type            # mypy

# start a 3-node cluster
cd docker && docker compose up --build -d

# use it
NODES="localhost:7001,localhost:7002,localhost:7003"
minikv status --nodes $NODES
minikv put hello world --nodes $NODES
minikv get hello --nodes $NODES
minikv cas hello world WORLD --nodes $NODES
minikv scan --nodes $NODES
minikv del hello --nodes $NODES

# try failover: kill the leader, keep reading and writing
docker kill docker-n2-1   # check status first to pick the leader
minikv get hello --nodes $NODES
docker start docker-n2-1  # rejoins and catches up

# stop everything
docker compose down
```

Single server (advanced): `minikv server --id n1 --config docker/cluster.toml --data-dir ./data/n1`.

## Architecture

```
 Client library / CLI
        |  gRPC (KV service)
        v
 +--------------------------- Node ---------------------------+
 |  KV gRPC service                                           |
 |     | propose(command)                                     |
 |     v                                                      |
 |  Raft runtime (asyncio)  <---- gRPC (Raft) ----> other nodes
 |     |  drives                                              |
 |  Raft core (pure state machine, no I/O)                    |
 |     |  persists term, vote, log                            |
 |  Raft log store (append-only file)                         |
 |     |  committed entries                                   |
 |  KV state machine + session table                          |
 |     |                                                      |
 |  LSM engine: WAL -> memtable -> SSTables (+ compaction)    |
 +------------------------------------------------------------+
```

Write path: client -> leader -> Raft log (fsync) -> majority ->
committed -> applied to LSM -> response. Reads are linearizable by
default via ReadIndex (`--stale` opts out; `--log` routes through the log).
See [DESIGN.md](DESIGN.md) and [docs/architecture.md](docs/architecture.md).

## CLI examples

```bash
minikv put k v --nodes host1:7001,host2:7002,host3:7003
minikv get k --nodes ...                  # linearizable
minikv get k --stale --nodes ...          # any node, no guarantees
minikv del k --nodes ...
minikv cas k old new --nodes ...          # fails (exit 1) on mismatch
minikv cas k _ new --absent --nodes ...   # create-if-absent
minikv scan --start a --end z --limit 100 --nodes ...
minikv status --nodes ...
```

## Correctness evidence

- Deterministic simulation (`sim/`): elections, replication, crashes,
  partitions, loss/reorder, churn over hundreds of seeds with 6 Raft
  invariants checked every step.
- Storage crash tests: WAL truncation at every byte offset, SSTable
  corruption matrix, flush/compaction fault injection.
- Linearizability checker (`sim/linearizability.py`, Wing & Gong style):
  verified against known good/bad histories; passes on sim histories and
  on multi-thousand-op Docker chaos histories (`docker/chaos.py`).
- 100% line coverage on `minikv/` and `sim/`.

## Benchmarks

Summary (this machine: Apple silicon, APFS; reproduce with
`python bench/run_bench.py --suite all && python bench/report.py`):

| comparison | result |
|---|---|
| LSM fsync always / batch / never | 26.7k / 50.0k / 51.6k writes/s |
| 3-node group commit on / off | 1343 / 991 writes/s |
| reads linearizable / stale | 1344 / 14068 reads/s |
| missing-key reads bloom on / off | 957k / 233k reads/s |

Full tables, charts, and caveats: [docs/benchmarks.md](docs/benchmarks.md).

## Known limitations (non-goals)

- No sharding or multi-key transactions; single keyspace per cluster.
- Static membership (3 or 5 nodes from `cluster.toml`); no joint consensus.
- No authentication, TLS, or multi-tenancy; localhost-grade security.
- Correctness-first performance: fsync per Raft write, JSON meta files,
  CPython + asyncio. Honest numbers in `docs/benchmarks.md`.
- Client request IDs are per-process; session dedup caps at 10k clients (LRU).
- Python 3.12+ required (uses PEP 695 generics).

## Layout

```
minikv/storage/   skiplist, memtable, WAL, bloom, SSTable, manifest, compaction, LSM
minikv/raft/      sans-IO core, log store, asyncio node, transports
minikv/kv/        state machine (PUT/DELETE/CAS), session dedup
minikv/server/   config, gRPC services, entrypoint
minikv/client/   library + typer CLI
sim/              virtual clock, faulty network, cluster harness, invariants,
                  history + linearizability checker
bench/            workloads, runners, report generator
docker/           Dockerfile, compose, cluster.toml, chaos.py
```

## Interview cheat-sheet

`docs/WHAT_I_LEARNED.md` answers the twelve "make sure you understand it"
questions in the author's own words. `docs/DECISIONS.md` records every
nontrivial decision (D1-D29+) with reasons.
