"""Simulated network: delays, loss, duplication, reordering, partitions.

All randomness comes from the injected ``rng`` so runs reproduce from one
seed. Delivery is pull-based: ``poll(now)`` returns every message due up
to ``now`` in (due-time, sequence) order.
"""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass, field

from minikv.raft.types import Message


@dataclass(slots=True)
class _Envelope:
    due_ms: int
    seq: int
    src: str
    dst: str
    message: Message


@dataclass
class SimNetwork:
    """Point-to-point links with per-message delay in ``[min_delay, max_delay]``.

    ``drop_p`` drops, ``dup_p`` delivers twice (the copy arrives later),
    and delay jitter reorders. ``set_partition`` restricts delivery to
    within each group; ``heal`` restores full connectivity. Messages sent
    across a partition are dropped (like a real blackhole), except ones
    already in flight keep their fate decided at send time.
    """

    rng: random.Random
    min_delay_ms: int = 1
    max_delay_ms: int = 10
    drop_p: float = 0.0
    dup_p: float = 0.0
    _pending: list[_Envelope] = field(default_factory=list)
    _seq: itertools.count[int] = field(default_factory=itertools.count)
    _groups: list[frozenset[str]] | None = None
    sent: int = 0
    delivered: int = 0
    dropped: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.min_delay_ms <= self.max_delay_ms:
            raise ValueError("bad delay range")
        if not 0.0 <= self.drop_p <= 1.0:
            raise ValueError("drop_p must sit in [0, 1]")
        if not 0.0 <= self.dup_p <= 1.0:
            raise ValueError("dup_p must sit in [0, 1]")

    def set_partition(self, groups: list[set[str]] | None) -> None:
        """Isolate ``groups`` from each other; ``None`` heals everything."""
        self._groups = None if groups is None else [frozenset(g) for g in groups]

    def can_talk(self, src: str, dst: str) -> bool:
        if self._groups is None:
            return True
        return any(src in g and dst in g for g in self._groups)

    def send(self, src: str, dst: str, message: Message, now_ms: int) -> None:
        self.sent += 1
        if not self.can_talk(src, dst) or self.rng.random() < self.drop_p:
            self.dropped += 1
            return
        delay = self.rng.randint(self.min_delay_ms, self.max_delay_ms)
        self._pending.append(_Envelope(now_ms + delay, next(self._seq), src, dst, message))
        if self.rng.random() < self.dup_p:
            extra = self.rng.randint(self.min_delay_ms, self.max_delay_ms)
            self._pending.append(
                _Envelope(now_ms + delay + extra, next(self._seq), src, dst, message)
            )

    def poll(self, now_ms: int) -> list[tuple[str, str, Message]]:
        """Due messages as ``(src, dst, message)``, earliest first."""
        due = [e for e in self._pending if e.due_ms <= now_ms]
        self._pending = [e for e in self._pending if e.due_ms > now_ms]
        due.sort(key=lambda e: (e.due_ms, e.seq))
        self.delivered += len(due)
        return [(e.src, e.dst, e.message) for e in due]

    def pending_count(self) -> int:
        return len(self._pending)
