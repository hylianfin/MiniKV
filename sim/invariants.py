"""Simulation invariants, checked after every simulated step.

1. Election safety: at most one leader per term.
2. Leader append-only: a leader's log only grows while it leads.
3. Log matching: same (index, term) on two nodes implies identical prefixes.
4. Leader completeness: committed entries appear in every later leader.
5. State machine safety: nodes never apply different payloads at one index.
6. Commit monotonicity: commit/applied never decrease within a lifetime;
   a restart re-baselines to durable state (new incarnation).

A violation raises ``InvariantViolation`` naming the seed-replayable step.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from minikv.raft.types import LogEntry, Role

if TYPE_CHECKING:
    from minikv.raft.core import RaftCore
    from sim.cluster import SimCluster


class InvariantViolation(Exception):
    """A safety property broke. Replay the scenario seed to reproduce."""


@dataclass
class _NodeTrack:
    generation: int = 0
    max_commit: int = 0
    max_applied: int = 0
    leader_logs: dict[int, list[tuple[int, int, bytes]]] = field(default_factory=dict)


@dataclass
class InvariantChecker:
    """Stateful across steps (histories reset when a node restarts)."""

    tracks: dict[str, _NodeTrack] = field(default_factory=dict)
    committed: dict[int, tuple[int, bytes]] = field(default_factory=dict)
    generations: dict[str, int] = field(default_factory=dict)

    def check(self, cluster: SimCluster) -> None:
        self._sync_generations(cluster)
        self._election_safety(cluster)
        self._leader_append_only(cluster)
        self._log_matching(cluster)
        self._record_commits(cluster)
        self._leader_completeness(cluster)
        self._state_machine_safety(cluster)
        self._commit_monotonicity(cluster)

    def _track(self, node_id: str) -> _NodeTrack:
        return self.tracks.setdefault(node_id, _NodeTrack())

    def _sync_generations(self, cluster: SimCluster) -> None:
        for node_id, node in cluster.nodes.items():
            gen = node.generation
            if self.generations.get(node_id) != gen:
                # New incarnation: durable state is the new baseline.
                self.generations[node_id] = gen
                track = self._track(node_id)
                track.generation = gen
                track.leader_logs = {}
                if node.alive and node.core is not None:
                    track.max_commit = node.core.commit_index
                    track.max_applied = node.core.last_applied
                else:
                    track.max_commit = node.store.commit
                    track.max_applied = node.store.applied

    def _election_safety(self, cluster: SimCluster) -> None:
        by_term: dict[int, list[str]] = {}
        for node in cluster.nodes.values():
            if node.alive and node.core is not None and node.core.role is Role.LEADER:
                by_term.setdefault(node.core.term, []).append(node.node_id)
        for term, leaders in by_term.items():
            if len(leaders) > 1:
                raise InvariantViolation(f"two leaders in term {term}: {leaders}")

    def _snapshot(self, core: RaftCore) -> list[tuple[int, int, bytes]]:
        return [(e.index, e.term, e.payload) for e in core.log]

    def _leader_append_only(self, cluster: SimCluster) -> None:
        for node in cluster.nodes.values():
            if not node.alive or node.core is None or node.core.role is not Role.LEADER:
                continue
            track = self._track(node.node_id)
            current = self._snapshot(node.core)
            known = track.leader_logs.get(node.core.term)
            if known is None:
                track.leader_logs[node.core.term] = current
            elif len(current) < len(known) or current[: len(known)] != known:
                raise InvariantViolation(
                    f"leader {node.node_id} term {node.core.term} rewrote its log"
                )
            else:
                track.leader_logs[node.core.term] = current

    def _log_matching(self, cluster: SimCluster) -> None:
        logs: dict[str, list[LogEntry]] = {}
        for node in cluster.nodes.values():
            if node.alive and node.core is not None:
                logs[node.node_id] = list(node.core.log)
        ids = list(logs)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                self._pairwise_match(ids[i], logs[ids[i]], ids[j], logs[ids[j]])

    def _pairwise_match(self, a: str, la: list[LogEntry], b: str, lb: list[LogEntry]) -> None:
        by_index_b = {e.index: e for e in lb}
        for e in la:
            other = by_index_b.get(e.index)
            if (
                other is not None
                and other.term == e.term
                and (other.kind != e.kind or other.payload != e.payload)
            ):
                raise InvariantViolation(
                    f"same index+term but different entries at {e.index} ({a} vs {b})"
                )
            if other is not None and other.term == e.term:
                for prev in la:
                    if prev.index >= e.index:
                        break
                    match = by_index_b.get(prev.index)
                    if match is None or match.term != prev.term or match.payload != prev.payload:
                        raise InvariantViolation(
                            f"log matching broken before {e.index} ({a} vs {b})"
                        )

    def _record_commits(self, cluster: SimCluster) -> None:
        for node in cluster.nodes.values():
            if not node.alive or node.core is None:
                continue
            entries = {e.index: e for e in node.core.log}
            for index in range(1, node.core.commit_index + 1):
                entry = entries.get(index)
                if entry is None:
                    raise InvariantViolation(f"node {node.node_id} committed hole at {index}")
                known = self.committed.get(index)
                if known is None:
                    self.committed[index] = (entry.term, entry.payload)
                elif known != (entry.term, entry.payload):
                    raise InvariantViolation(f"committed entry {index} disagrees across nodes")

    def _leader_completeness(self, cluster: SimCluster) -> None:
        for node in cluster.nodes.values():
            if not node.alive or node.core is None or node.core.role is not Role.LEADER:
                continue
            have = {(e.index, e.term) for e in node.core.log}
            for index, (term, _) in self.committed.items():
                if term < node.core.term and (index, term) not in have:
                    raise InvariantViolation(
                        f"leader {node.node_id} term {node.core.term} lacks "
                        f"committed entry {index}@{term}"
                    )

    def _state_machine_safety(self, cluster: SimCluster) -> None:
        seen: dict[int, bytes] = {}
        for node in cluster.nodes.values():
            for index, payload in node.applied.items():
                if index in seen and seen[index] != payload:
                    raise InvariantViolation(f"divergent apply at index {index}")
                seen[index] = payload

    def _commit_monotonicity(self, cluster: SimCluster) -> None:
        for node in cluster.nodes.values():
            track = self._track(node.node_id)
            if node.alive and node.core is not None:
                commit, applied = node.core.commit_index, node.core.last_applied
            else:
                commit, applied = node.store.commit, node.store.applied
            if commit < track.max_commit or applied > commit:
                raise InvariantViolation(
                    f"node {node.node_id} commit regressed or applied past commit"
                )
            if applied < track.max_applied:
                raise InvariantViolation(f"node {node.node_id} applied regressed")
            track.max_commit = max(track.max_commit, commit)
            track.max_applied = max(track.max_applied, applied)
