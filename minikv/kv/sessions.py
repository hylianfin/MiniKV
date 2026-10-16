"""Client request deduplication: exactly-once mutating requests.

Every mutating request carries ``(client_id, request_seq)``. The table maps
each client to its highest applied sequence number plus the cached result,
and it lives in its own LSM engine -- so it is replicated (via the Raft
log, like all state) and crash-safe, with zero keyspace overlap with user
data. A retried ``request_seq`` returns the cached result without
re-applying.

Eviction (10,000 clients, LRU) trades dedup memory for safety: an evicted
client's retry re-applies, which is state-safe (all ops are last-writer-
wins) but may report a freshly computed result instead of the original.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

from minikv.storage.lsm import LSMEngine

SESSION_CAPACITY = 10_000


def session_key(client_id: str) -> bytes:
    return b"session/" + client_id.encode("utf-8")


@dataclass(frozen=True, slots=True)
class SessionRecord:
    seq: int
    result: bytes  # encoded CommandResult


def encode_session_record(record: SessionRecord) -> bytes:
    import struct

    return struct.pack("<Q", record.seq) + record.result


def decode_session_record(raw: bytes) -> SessionRecord:
    import struct

    if len(raw) < 8:
        raise ValueError("session record too short")
    (seq,) = struct.unpack_from("<Q", raw, 0)
    return SessionRecord(seq=seq, result=bytes(raw[8:]))


class SessionTable:
    """Dedup table backed by an LSM engine plus an in-memory LRU index."""

    def __init__(self, engine: LSMEngine, capacity: int = SESSION_CAPACITY) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._engine = engine
        self._capacity = capacity
        self._lru: OrderedDict[str, None] = OrderedDict()

    @property
    def capacity(self) -> int:
        return self._capacity

    def lookup(self, client_id: str) -> SessionRecord | None:
        """Newest record for ``client_id`` (None when unknown or corrupt)."""
        raw = self._engine.get(session_key(client_id))
        if raw is None:
            return None
        try:
            record = decode_session_record(raw)
        except ValueError:
            return None  # corrupt entry reads as absent; next store heals it
        self._touch(client_id)
        return record

    def store(self, client_id: str, seq: int, result: bytes) -> None:
        """Record ``seq`` as applied with its ``result``, evicting the
        least-recently-used client past capacity."""
        self._engine.put(
            session_key(client_id), encode_session_record(SessionRecord(seq, bytes(result)))
        )
        self._touch(client_id)
        while len(self._lru) > self._capacity:
            oldest, _ = self._lru.popitem(last=False)
            self._engine.delete(session_key(oldest))

    def clear_cache(self) -> None:
        """Drop the in-memory LRU (durable records survive); used on reopen."""
        self._lru.clear()

    def _touch(self, client_id: str) -> None:
        self._lru[client_id] = None
        self._lru.move_to_end(client_id)
