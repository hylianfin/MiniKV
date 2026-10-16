# DESIGN.md

Decisions and tradeoffs behind MiniKV. Every D-number also lives in
`docs/DECISIONS.md` with its milestone context.

## 1. The sans-IO Raft core (the big decision)

`RaftCore` performs no I/O: no sockets, no disk, no clock reads, no global
random. It takes `(now_ms, message)` in and returns plain `Action`
dataclasses (`Send`, `Persist`, `SaveTermVote`, `Apply`) out. Two runtimes
execute the same actions: the asyncio node (real sockets/disks) and the
deterministic simulator (virtual clock, fake network).

Why: consensus bugs hide in timing and failure interleavings. Sans-IO
makes every interleaving reproducible from one integer seed, so the 1000+
simulation scenarios are cheap and exact. The price is discipline at the
boundary — runtimes must execute actions **in order** (Persist before the
Sends that depend on it) and feed every response back into the core.

Supporting choices:
- The core takes a **read-only store seed** (`PersistedState`) at
  construction and never touches it again (D13). Durability is purely
  action-ordered; in-order execution plus memory/store fate-sharing is
  what makes duplicate-delivery skipping sound.
- `handle_message` takes an optional `sender` because RPC responses carry
  no author (matching the proto) while check-quorum needs contact
  attribution (D17).
- One RLock instead of reader refcounts in the LSM for the same reason:
  swaps happen on one thread under a lock user reads also hold (D9).

## 2. Raft rules implemented

Figure 2 plus: randomized elections (300-600ms), heartbeat 100ms, log
up-to-date voting, one vote per term (persisted), AppendEntries
consistency + conflict truncation with fast-backup hints, NOOP of the new
term on election (so old-term entries commit only via the new entry --
the Figure-8 rule, directly unit-tested), commit only own-term entries by
counting, in-order apply, step-down on higher terms, check-quorum leader
stepdown with a post-election grace period, and pre-vote (default on).

Two subtleties found by testing, not by reading:
- **Pre-vote grant is log-check only** (D14). Gating on `voted_for`
  deadlocks post-crash elections: everyone voted for the dead leader.
  Safety holds because pre-votes elect nothing.
- **Fast-backup next_index must strictly decrease** (D15). Jumping past
  the leader's own term end can overshoot the follower and oscillate
  (6->3->6); the jump is capped back to the follower's conflict point.

**Group commit** batches at the action layer, not with core timers (D12):
`propose()` appends synchronously (the index is known immediately, which
the spec's signature requires) while runtimes coalesce one batch window's
actions -- contiguous Persists merge into one fsync, each follower keeps
its latest Send. Same disk/network effect, far simpler core.

**ReadIndex reads** (D26): record `commit_index`, confirm a heartbeat
majority at the same term, then wait for LSM application (not just core
emission). A deposed leader cannot assemble a same-term majority without
learning the newer term, so stale-leader reads are impossible. `READ_LOG`
serializes through the log as an empty command; `READ_STALE` reads local
state with no guarantees.

## 3. Persistence: Raft log store

`raft.meta` (JSON: term, voted_for, commit_index, applied_index, swapped
atomically) plus append-only `raft.log`
(`[crc32][length][term][index][type][payload]`). Recovery replays to the
first invalid record and truncates the torn tail; a fully-present invalid
*first* record raises (nothing valid precedes it); index gaps always raise
(truncation cannot repair a hole). `persist()` truncates-then-appends under
one fsync. Commit/applied persist in the meta file so the monotonicity
invariant holds literally across restarts (D16 documents the sim side).

## 4. LSM storage engine

WAL -> memtable (skip list) -> SSTables (+ size-tiered compaction),
all hand-written, stdlib only (`zlib.crc32`, `hashlib.blake2b`).

- **Skip list**: generic ordered bytes map, max level 16, p=0.5, injectable
  RNG, approximate memory accounting for flush decisions.
- **Memtable**: `(key, seq, kind, value)` with tombstone deletes (never
  physical removal -- tombstones must shadow older SSTables until
  compaction reaches the oldest data). 1 KiB / 1 MiB limits enforced here.
- **WAL**: exact byte format in `wal.py`; torn tails truncate silently, a
  corrupt record zero raises (D4); `close()` fsyncs under always/batch (D5).
- **SSTable**: CRC'd ~4 KiB data blocks, sparse index, bloom, footer with
  magic + CRC + trailing length (D6). Crash-safe write protocol
  (tmp -> fsync -> rename -> dir fsync); readers only open manifest-listed
  files, stray tmps are ignored and sequence numbers never reused (D8).
- **Bloom**: 10 bits/key, k=7 derived, double hashing over one blake2b
  digest. `bloom_bits_per_key=0` / `bloom_enabled=False` writes zero-length
  bloom blocks for the on/off benchmark.
- **Manifest**: JSON live-table list + `flushed_raft_index`, swapped
  atomically; crash lands on old-or-new, never a mix.
- **Compaction**: lowest full tier merges newest-wins via k-way heap merge;
  tombstones drop only when the merge covers the bottom tier. Empty merges
  just remove inputs. Unlisted files are GC'd on open.
- **Engine**: WAL-first writes, sealed memtables, one background worker
  for flush/manifest/compaction, batch-fsync timing check, multi-WAL
  replay (the live memtable stays mutable -- an M6-caught bug), open-time
  GC. `flush()`/`compact()` take test-only fault hooks (D11).

## 5. KV semantics and exactly-once

Commands: PUT (upsert), DELETE (returns existed), CAS (expected value or
absent; returns success + current). All deterministic functions of log
order, so every node converges; re-application is state-safe (all ops are
last-writer-wins), which is what makes crash replay sound.

Exactly-once comes from the **session table** (D18-D20): `(client_id,
request_seq)` + cached result, stored in its *own* LSM engine (no keyspace
overlap with user data, same crash semantics). Order inside apply is data
first, session second: a crash between them re-applies idempotently, while
the reverse could acknowledge unwritten data. Retries hit the cache without
re-applying -- including across leader failover, since sessions replicate
through the log like all state. Stale (older-than-recorded) sequences
re-execute unrecorded as an unreachable-but-safe fallback. LRU eviction at
10k clients trades memory for re-application (state-safe, possibly fresh
results), documented, not hidden.

## 6. gRPC surface and client

Proto defines KV (Put/Get/Delete/Cas/Scan/Status) and Raft services.
Per-request `ReadMode` wins over config (D21). The client generates one
`client_id`, bumps `request_seq` per mutating call, reuses it across
retries, follows `NOT_LEADER` hints with backoff, and restarts streams on
redirect (scans snapshot server-side at stream start, so restarts can
duplicate but never lose/invent items -- D25). Poisoned log entries
fail-stop the node rather than skipping (order is sacred -- D23).

## 7. Testing strategy (why we trust it)

- **Unit + property**: codecs, structures, and a 500-example LSM model test
  vs a dict; bloom FPR bounded with concentration-safe probe counts.
- **Deterministic simulation**: virtual clock + faulty network + fsync
  horizons, single-seed reproducible, 6 invariants after every step,
  1000-seed acceptance runs for election/replication.
- **Crash matrices**: WAL truncation at every byte offset, per-byte
  corruption, 20-case SSTable corruption matrix, fault injection at every
  flush/compaction/manifest step, kill -9 equivalent engine crashes.
- **Linearizability**: Wing & Gong per-key search with preconditions and
  timeout-as-optional modeling (D27-D29); the checker is itself tested
  against known good/bad histories; it passes on sim histories, live
  ReadIndex histories, and 9-17k-op chaos histories. Histories assume
  absent initial keys (D28 -- chaos uses per-run key prefixes).
- **Real clusters**: in-process 3-node gRPC tests, real-binary CLI tests,
  and hand-verified Docker failover (kill -9 leader, no loss, rejoin).

## 8. Performance posture

Correct and measured, not fast. Every write pays ~3 fsyncs (raft log,
LSM WAL, progress meta) plus group-commit batching; linearizable reads
pay a heartbeat round. Measured honestly in `docs/benchmarks.md` with
commands to reproduce, including where this box flatters fsync (APFS).
`
