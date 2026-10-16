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

## M3 (done)
- `minikv/storage/bloom.py`: from-scratch bloom (10 bits/key, k=7 derived, double hashing over one blake2b digest), serializable; measured FPR within 2x theory, zero false negatives.
- `minikv/storage/sstable.py`: immutable sorted files (CRC'd data blocks, sparse index, bloom, footer with magic+CRC+length trailer), range-then-bloom-then-index `get`, `iter_from` scans, tmp/fsync/rename/dir-fsync write protocol, `flush_memtable` with per-step crash injection.
- `minikv/storage/manifest.py`: JSON live-table list + `flushed_raft_index`, atomic tmp/fsync/rename/dir-fsync updates with crash-point hooks.
- Tests: flush-then-readback identical; fault at every flush/manifest step preserves data (retry converges); 20-case corruption matrix over footer/index/bloom/block rejections; hypothesis round-trips. 100% storage coverage. 83 tests pass; ruff + mypy strict clean.
- Decisions: footer length trailer (D6); on-disk kinds reuse wal.RecordType (D7); stray tmp files ignored, seq numbers never reused (D8).

## M4 (done)
- `minikv/storage/lsm.py`: `LSMOptions` + `LSMEngine` (open/put/delete/get/scan/applied_index/flush/compact/close). WAL-first writes, sealed memtables, one background worker for flush+manifest+compaction, single-RLock read/swap discipline, batch-fsync timer check, multi-WAL replay, open-time GC of stray SSTables, `_crash()` test hook.
- `minikv/storage/compaction.py`: size-tiered planner (lowest full tier, tombstone drop only at bottom tier) + k-way newest-wins merge runner with flush-identical crash protocol.
- `minikv/storage/sstable.py`: added `alloc_sst_path`, `gc_unlisted_files`, `SSTableWriter.abort()`, atomic `flushed_raft_index` bump in flush's manifest save.
- Tests: 500-example hypothesis model test (put/del/get/scan/flush/compact/restart vs dict), WAL truncation at all first-record/final-region offsets + boundaries, per-byte corruption, fault at every flush/compaction step, stalled-worker crash with pending jobs, 20+ case corruption matrices. 100% storage coverage. 117 tests pass; ruff + mypy strict clean.
- Decisions: coarse lock instead of reader refcounts (D9); close() flushes and drops empty WALs (D10); flush/compact take test-only fault hooks (D11).

## M5 (done)
- `minikv/raft/types.py`: roles, entries, all RPC messages, sans-IO actions (Send/Persist/SaveTermVote/Apply), ProposeOk/NotLeader, RaftConfig, PersistedState + RaftStore protocol.
- `minikv/raft/core.py`: sans-IO RaftCore (Figure-2 rules + pre-vote + check-quorum + fast backup + NOOP-on-election + own-term commit rule), `coalesce_actions` group-commit helper.
- `sim/`: VirtualClock, lossy/partitionable SimNetwork, SimCluster (seeded RNG, MemoryStore with fsync horizon, crash/loss/restart, batched proposes), 6-property InvariantChecker.
- Tests: 17 core unit tests (incl. Figure-8 commit rule, truncation, pre-vote, quorum, backup, coalescing), sim scenarios (election/replication 3+5 nodes @200 seeds; crash, partition, isolation, lossy, all-restart, churn, fsync-loss, pre-vote disruption @10-50 seeds), direct checker verification. 100% source coverage. 161 tests pass; ruff + mypy strict clean.
- Caught by simulation (all fixed + tested): pre-vote voted_for gate blocking post-crash elections; fast-backup forward-jump livelock; duplicated step-down block.
- Decisions: D12-D17.

## M6 (done)
- `minikv/raft/logstore.py`: file-backed `LogStore` (atomic meta JSON + CRC log with torn-tail recovery, gap detection, contiguity enforcement, fsync-before-ack persist, commit/applied progress).
- `minikv/kv/state_machine.py`: fixed binary command codec (PUT/DELETE/CAS), results codec, request envelope with session ctx, deterministic `KVStateMachine`, `KVApplier` (session-check → data → session-record, own data+session LSM engines, open/crash/reopen/close).
- `minikv/kv/sessions.py`: LSM-backed `SessionTable` with LRU (10k cap), corrupt-tolerant lookup, eviction deletes.
- `sim/cluster.py`: applier hooks (apply/crash/reopen/close), per-index applier results; `LSMEngine._crash` promoted to public `crash()`.
- Tests: logstore recovery/truncation/meta/crash-prefix matrix; codec round-trips incl. hypothesis fuzz; CAS/result dedup incl. post-crash cache; KV-over-Raft sim (dict model, failover-dedup money test, restart-all, mid-workload crash, fsync-loss pruning). 100% source coverage. 198 tests pass; ruff + mypy strict clean.
- Caught: replayed memtable left immutable (first post-reopen put crashed); CAS result presence-flag inversion.
- Decisions: D18-D20.

## M7 (done)
- `minikv/raft/node.py`: asyncio RaftNode (core lock, disk lock, batcher with group-commit window, FIFO apply queue with fail-stop poison handling, ReadIndex barrier, waiters, deferred-action backstop).
- `minikv/raft/transport.py` + `grpc_transport.py`: transport protocol + gRPC impl with message conversion.
- `minikv/server/`: TOML config, KV+Raft gRPC servicers (linearizable/log/stale reads, scan streaming, status), `serve()` entrypoint.
- `minikv/client/`: leader-tracking client (shared-seq retries, backoff, scan-restart) + typer CLI (server/put/get/del/cas/scan/status).
- `proto/gen_proto.py` + hand-written `minikv_pb2*.pyi` stubs; Makefile; Docker build/run verified for real.
- Tests: mesh-node units, servicer units, client retry units (scripted fakes), real-gRPC cluster + real-binary CLI integration (incl. kill -9 failover). 100% source coverage. 238 tests pass; ruff + mypy strict clean.
- Verified by hand: `docker compose up` → 3 nodes elect; CLI put/get/scan; `docker kill` leader → new leader term+1, no loss, writes continue; killed node rejoins and catches up.
- Caught: single-node never committing (commit only advanced on responses — added `check_commit`); `grpc.AioRpcError` attribute (it's `grpc.aio.AioRpcError`); generated grpc import needs packaging fixup; grpc.aio stalls on non-main-thread loops (CLI tests use real subprocesses).
- Decisions: D21-D26.

## Left
- M8..M10 per scope. Next: M8 linearizability checker + ReadIndex proof + chaos.
