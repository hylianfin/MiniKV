# WHAT I LEARNED.md

Own-words answers to the twelve interview questions (scope section 15).

1. **LSM random writes -> sequential**: writes append to the WAL and an
   in-memory skip list (both sequential/memory-speed). Reads pay by
   checking several places newest-first (memtable, then SSTables with
   bloom/index/block I/O) and merging tombstones.

2. **Crash mid–WAL-record**: recovery reads to the first invalid record
   (bad CRC or short read) and truncates there. The CRC distinguishes a
   torn tail (drop it) from real data; a corrupt *first* record raises
   because there is no valid prefix to fall back on.

3. **Manifest swap**: write temp, fsync file, rename (atomic), fsync
   directory. A crash lands on the old or the new manifest, never a mix,
   because only the rename publishes and it is atomic.

4. **Bloom filter**: guarantees no false negatives (a present key always
   hits); it can wrongly claim an absent key is present (~0.8% at our
   sizing), which costs one wasted block read, never correctness.

5. **Tombstones wait for the oldest data**: dropping a tombstone early
   would let a stale copy in an older table reappear. They die only when
   the merge covers the bottom tier, i.e. nothing older survives.

6. **One leader per term**: one vote per term per node + majority quorum
   overlap: two majorities intersect, and the intersecting voter voted
   once. Pre-vote additionally stops partitioned nodes inflating terms.

7. **Commit only own-term by counting + NOOP**: Figure 8 shows counting
   replicas of an old-term entry is unsafe (a newer leader may not have
   it). The leader appends a NOOP of its term; committing the NOOP by
   counting implicitly commits everything before it.

8. **Log matching via AppendEntries**: the leader sends prev_log_index/
   term; the follower rejects on mismatch (with fast-backup hints) and
   truncates conflicts. Same index+term therefore implies same prefix.

9. **Plain leader reads aren't linearizable**: a deposed leader (unaware,
   partitioned) can serve stale data. ReadIndex fixes it: heartbeat a
   majority *after* recording the read point (a deposed leader can't),
   then wait until that point is applied.

10. **Sessions make retries exactly-once**: `(client_id, request_seq)` +
    cached result, replicated through the log like all state. A retry
    returns the cache without re-applying -- including across failover.

11. **Sans-IO core**: no I/O means the simulator can drive the exact same
    code with a virtual clock and scripted faults, reproducibly from one
    seed. Three genuine consensus bugs were caught this way before merge.

12. **The checker**: per-key Wing & Gong search over linearization orders
    with preconditions (CAS/delete) and timeout-as-optional modeling. It
    was verified against hand-built good/bad histories (stale reads, lost
    writes, time travel, phantom CAS) before being trusted on real ones.
