"""Simulated cluster: N Raft cores on a virtual clock and fake network.

Everything derives from one integer ``seed``. Crashes drop the core object;
with ``fsync_mode="manual"`` un-fsynced store suffixes die with it, like a
machine losing its page cache. A failing scenario reports its seed and can
be replayed exactly.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from minikv.raft.core import RaftCore, coalesce_actions
from minikv.raft.types import (
    Action,
    Apply,
    LogEntry,
    Persist,
    PersistedState,
    ProposeResult,
    RaftConfig,
    RaftStore,
    Role,
    SaveTermVote,
    Send,
)
from sim.clock import VirtualClock
from sim.network import SimNetwork


@dataclass
class MemoryStore(RaftStore):
    """In-memory durable state with an explicit fsync horizon.

    ``entries`` / ``term`` / ``voted_for`` / ``applied`` / ``commit`` are
    the volatile copies the core's actions mutate; ``fsync()`` publishes
    them as durable. A crash truncates everything past the horizon.
    """

    term: int = 0
    voted_for: str | None = None
    entries: list[LogEntry] = field(default_factory=list)
    applied: int = 0
    commit: int = 0
    durable_term: int = 0
    durable_voted_for: str | None = None
    durable_len: int = 0
    durable_applied: int = 0
    durable_commit: int = 0

    def load(self) -> PersistedState:
        return PersistedState(
            term=self.term,
            voted_for=self.voted_for,
            entries=tuple(self.entries),
            commit_index=self.commit,
            applied_index=self.applied,
        )

    def apply_persist(self, base_index: int, entries: tuple[LogEntry, ...]) -> None:
        del self.entries[base_index - 1 :]
        self.entries.extend(entries)

    def fsync(self) -> None:
        self.durable_term = self.term
        self.durable_voted_for = self.voted_for
        self.durable_len = len(self.entries)
        self.durable_applied = self.applied
        self.durable_commit = self.commit

    def crash_lose_unfsynced(self) -> None:
        """Roll back to the fsync horizon (machine lost its page cache)."""
        del self.entries[self.durable_len :]
        self.term = self.durable_term
        self.voted_for = self.durable_voted_for
        self.applied = self.durable_applied
        self.commit = min(self.durable_commit, len(self.entries))


@dataclass
class SimNode:
    node_id: str
    core: RaftCore | None = None
    store: MemoryStore = field(default_factory=MemoryStore)
    applied: dict[int, bytes] = field(default_factory=dict)  # the "state machine"
    alive: bool = True
    generation: int = 0  # bumped on crash/restart: a new incarnation


class SimCluster:
    """Drives cores, network, stores, and appliers in lockstep."""

    STEP_MS = 5

    def __init__(
        self,
        node_ids: list[str],
        seed: int,
        config: RaftConfig | None = None,
        drop_p: float = 0.0,
        dup_p: float = 0.0,
        fsync_mode: str = "immediate",
    ) -> None:
        if fsync_mode not in ("immediate", "manual"):
            raise ValueError("fsync_mode must be immediate or manual")
        self.seed = seed
        self.config = config or RaftConfig()
        self.clock = VirtualClock()
        master = random.Random(seed)
        self.rng = random.Random(master.randrange(2**31))
        self.network = SimNetwork(
            rng=random.Random(master.randrange(2**31)),
            drop_p=drop_p,
            dup_p=dup_p,
        )
        self.fsync_mode = fsync_mode
        self.nodes: dict[str, SimNode] = {}
        for node_id in node_ids:
            peers = [n for n in node_ids if n != node_id]
            node = SimNode(node_id=node_id)
            node.core = self._new_core(node_id, peers, node.store)
            self.nodes[node_id] = node

    @property
    def now(self) -> int:
        return self.clock.now_ms

    def _new_core(self, node_id: str, peers: list[str], store: MemoryStore) -> RaftCore:
        return RaftCore(
            node_id,
            peers,
            store,
            random.Random(self.rng.randrange(2**31)),
            self.config,
        )

    def tick_all(self) -> None:
        for node in self.nodes.values():
            if node.alive and node.core is not None:
                self._execute(node.node_id, node.core.tick(self.now))

    def deliver(self) -> None:
        for src, dst, message in self.network.poll(self.now):
            node = self.nodes[dst]
            if node.alive and node.core is not None:
                self._execute(dst, node.core.handle_message(self.now, message, sender=src))

    def step(self) -> None:
        """One granule: tick everyone, deliver what's due, advance time."""
        self.tick_all()
        self.deliver()
        self.clock.advance(self.STEP_MS)

    def run(self, duration_ms: int) -> None:
        steps, leftover = divmod(duration_ms, self.STEP_MS)
        for _ in range(steps):
            self.step()
        if leftover:
            self.tick_all()
            self.deliver()
            self.clock.advance(leftover)

    def propose(self, node_id: str, payload: bytes) -> ProposeResult:
        """Propose to one node; NOT_LEADER carries the hint (may be None)."""
        node = self.nodes[node_id]
        if not node.alive or node.core is None:
            raise RuntimeError(f"node {node_id} is down")
        result, actions = node.core.propose(self.now, payload)
        self._execute(node_id, coalesce_actions(actions))
        return result

    def propose_all(self, payloads: list[tuple[str, bytes]]) -> list[ProposeResult]:
        """Propose several commands 'at once' (same millisecond). Each
        node's actions run through one coalescing pass -- this is the
        group-commit path (several appends, one fsync, one RPC per peer)."""
        batched: dict[str, list[Action]] = {}
        results: list[ProposeResult] = []
        for node_id, payload in payloads:
            node = self.nodes[node_id]
            if not node.alive or node.core is None:
                raise RuntimeError(f"node {node_id} is down")
            result, actions = node.core.propose(self.now, payload)
            results.append(result)
            batched.setdefault(node_id, []).extend(actions)
        for node_id, actions in batched.items():
            self._execute(node_id, coalesce_actions(actions))
        return results

    def crash(self, node_id: str, lose_unfsynced: bool = False) -> None:
        """Kill a node. With ``lose_unfsynced`` (manual fsync mode) the
        store rolls back to its fsync horizon and applied state follows."""
        node = self.nodes[node_id]
        node.alive = False
        node.core = None
        node.generation += 1
        if lose_unfsynced:
            node.store.crash_lose_unfsynced()
            for index in [i for i in node.applied if i > node.store.applied]:
                del node.applied[index]

    def restart(self, node_id: str) -> None:
        node = self.nodes[node_id]
        peers = [n for n in self.nodes if n != node_id]
        node.core = self._new_core(node_id, peers, node.store)
        node.alive = True
        node.generation += 1

    def partition(self, groups: list[set[str]] | None) -> None:
        self.network.set_partition(groups)

    def fsync_all(self) -> None:
        for node in self.nodes.values():
            node.store.fsync()

    def leaders(self) -> dict[int, list[str]]:
        """Live leaders grouped by term."""
        out: dict[int, list[str]] = {}
        for node in self.nodes.values():
            if node.alive and node.core is not None and node.core.role is Role.LEADER:
                out.setdefault(node.core.term, []).append(node.node_id)
        return out

    def _execute(self, node_id: str, actions: list[Action]) -> None:
        node = self.nodes[node_id]
        for action in actions:
            if isinstance(action, Persist):
                node.store.apply_persist(action.base_index, action.entries)
            elif isinstance(action, SaveTermVote):
                node.store.term = action.term
                node.store.voted_for = action.voted_for
            elif isinstance(action, Send):
                self.network.send(node_id, action.to, action.message, self.now)
            elif isinstance(action, Apply):
                node.applied[action.index] = action.payload
                node.store.applied = max(node.store.applied, action.index)
                if node.core is not None:
                    node.store.commit = max(node.store.commit, node.core.commit_index)
            if self.fsync_mode == "immediate":
                node.store.fsync()
