"""Unit tests for the sim harness (clock, network, cluster edges) and direct
verification of the invariant checker: each invariant must fire on crafted
bad state and stay quiet on good state."""

from types import SimpleNamespace

import pytest

from minikv.raft.types import EntryKind, LogEntry, Role
from sim.clock import VirtualClock
from sim.cluster import MemoryStore, SimCluster
from sim.invariants import InvariantChecker, InvariantViolation
from sim.network import SimNetwork


def test_clock_edges() -> None:
    with pytest.raises(ValueError):
        VirtualClock(-1)
    clock = VirtualClock()
    assert clock.now_ms == 0
    assert clock.advance(10) == 10
    with pytest.raises(ValueError):
        clock.advance(-1)


def test_network_validation_and_accounting() -> None:
    import random

    with pytest.raises(ValueError):
        SimNetwork(random.Random(0), min_delay_ms=10, max_delay_ms=1)
    with pytest.raises(ValueError):
        SimNetwork(random.Random(0), drop_p=1.5)
    with pytest.raises(ValueError):
        SimNetwork(random.Random(0), dup_p=-0.1)
    net = SimNetwork(random.Random(0))
    assert net.pending_count() == 0
    net.send("a", "b", "m", 0)  # type: ignore[arg-type]
    assert net.pending_count() == 1
    assert net.poll(100) == [("a", "b", "m")]
    assert net.pending_count() == 0
    net.set_partition([{"a"}, {"b"}])
    net.send("a", "b", "m", 0)  # type: ignore[arg-type]
    assert net.pending_count() == 0 and net.dropped == 1
    net.set_partition(None)
    net.send("a", "b", "m", 0)  # type: ignore[arg-type]
    assert net.pending_count() == 1


def test_cluster_edges() -> None:

    with pytest.raises(ValueError):
        SimCluster(["a"], 0, fsync_mode="sometimes")
    cluster = SimCluster(["a", "b"], 1)
    assert cluster.now == 0
    cluster.run(7)  # exercises the leftover path (7 % 5 == 2)
    assert cluster.now == 7
    cluster.crash("a")
    with pytest.raises(RuntimeError):
        cluster.propose("a", b"x")
    with pytest.raises(RuntimeError):
        cluster.propose_all([("a", b"x")])


def _entry(i: int, term: int, payload: bytes = b"v") -> LogEntry:
    return LogEntry(term, i, EntryKind.COMMAND, payload)


def _fake_node(
    node_id: str,
    role: Role = Role.FOLLOWER,
    term: int = 1,
    log: list[LogEntry] | None = None,
    commit: int = 0,
    applied_entries: dict[int, bytes] | None = None,
    generation: int = 0,
) -> SimpleNamespace:
    entries = list(log or [])
    return SimpleNamespace(
        node_id=node_id,
        alive=True,
        generation=generation,
        core=SimpleNamespace(
            role=role,
            term=term,
            log=entries,
            commit_index=commit,
            last_applied=len(applied_entries or {}),
        ),
        store=SimpleNamespace(commit=commit, applied=len(applied_entries or {})),
        applied=dict(applied_entries or {}),
    )


def _fake_cluster(nodes: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(nodes={n.node_id: n for n in nodes})


def test_checker_passes_healthy_cluster() -> None:
    log = [_entry(1, 1, b"a"), _entry(2, 1, b"b")]
    nodes = [
        _fake_node("n1", Role.LEADER, 1, log, commit=2, applied_entries={1: b"a", 2: b"b"}),
        _fake_node("n2", Role.FOLLOWER, 1, log, commit=2, applied_entries={1: b"a", 2: b"b"}),
    ]
    InvariantChecker().check(_fake_cluster(nodes))  # must not raise


def test_checker_election_safety() -> None:
    nodes = [_fake_node("n1", Role.LEADER, 2), _fake_node("n2", Role.LEADER, 2)]
    with pytest.raises(InvariantViolation, match="two leaders"):
        InvariantChecker().check(_fake_cluster(nodes))


def test_checker_leader_append_only() -> None:
    checker = InvariantChecker()
    log = [_entry(1, 1, b"a")]
    node = _fake_node("n1", Role.LEADER, 1, log)
    checker.check(_fake_cluster([node, _fake_node("n2")]))
    node.core.log = []  # leader truncated its own log
    with pytest.raises(InvariantViolation, match="rewrote"):
        checker.check(_fake_cluster([node, _fake_node("n2")]))


def test_checker_log_matching() -> None:
    nodes = [
        _fake_node("n1", log=[_entry(1, 1, b"a")]),
        _fake_node("n2", log=[_entry(1, 1, b"DIFFERENT")]),
    ]
    with pytest.raises(InvariantViolation, match="same index"):
        InvariantChecker().check(_fake_cluster(nodes))
    # Same later term but divergent prefix terms.
    nodes = [
        _fake_node("n1", log=[_entry(1, 1, b"a"), _entry(2, 2, b"b")]),
        _fake_node(
            "n2",
            log=[
                LogEntry(9, 1, EntryKind.COMMAND, b"DIFF"),
                _entry(2, 2, b"b"),
            ],
        ),
    ]
    with pytest.raises(InvariantViolation, match="before"):
        InvariantChecker().check(_fake_cluster(nodes))


def test_checker_committed_hole_and_disagreement() -> None:
    checker = InvariantChecker()
    node = _fake_node("n1", log=[_entry(2, 1, b"b")], commit=2)
    with pytest.raises(InvariantViolation, match="hole"):
        checker.check(_fake_cluster([node]))
    checker2 = InvariantChecker()
    checker2.check(_fake_cluster([_fake_node("n1", log=[_entry(1, 1, b"a")], commit=1)]))
    with pytest.raises(InvariantViolation, match="disagrees"):
        checker2.check(_fake_cluster([_fake_node("n1", log=[_entry(1, 2, b"zzz")], commit=1)]))


def test_checker_leader_completeness() -> None:
    checker = InvariantChecker()
    checker.check(_fake_cluster([_fake_node("n1", log=[_entry(1, 1, b"a")], commit=1)]))
    new_leader = _fake_node("n2", Role.LEADER, 2, log=[])
    with pytest.raises(InvariantViolation, match="lacks committed"):
        checker.check(_fake_cluster([new_leader]))


def test_checker_state_machine_safety() -> None:
    nodes = [
        _fake_node("n1", applied_entries={1: b"a"}),
        _fake_node("n2", applied_entries={1: b"b"}),
    ]
    with pytest.raises(InvariantViolation, match="divergent"):
        InvariantChecker().check(_fake_cluster(nodes))


def test_checker_monotonicity_and_restart_rebaseline() -> None:
    checker = InvariantChecker()
    log = [_entry(1, 1, b"a"), _entry(2, 1, b"b"), _entry(3, 1, b"c")]
    node = _fake_node("n1", log=log, commit=3, applied_entries={1: b"a", 2: b"b", 3: b"c"})
    checker.check(_fake_cluster([node]))
    node.core.commit_index = 2  # regress within one lifetime: fires
    with pytest.raises(InvariantViolation, match="regressed"):
        checker.check(_fake_cluster([node]))
    # ...but the same values after a restart are the new durable baseline.
    node.core.commit_index = 3
    node.generation = 1
    node.store.commit = 2
    node.store.applied = 2
    node.applied = {1: b"a", 2: b"b"}
    checker.check(_fake_cluster([node]))  # must not raise
    # Applied sliding back without a restart still fires.
    node.applied = {1: b"a"}
    node.store.applied = 1
    node.core.last_applied = 1
    with pytest.raises(InvariantViolation, match="regressed"):
        checker.check(_fake_cluster([node]))


def test_checker_applied_past_commit() -> None:
    node = _fake_node("n1", log=[_entry(1, 1, b"a")], commit=1, applied_entries={1: b"a", 2: b"b"})
    with pytest.raises(InvariantViolation, match="past commit"):
        InvariantChecker().check(_fake_cluster([node]))


def test_memory_store_crash_horizon() -> None:
    store = MemoryStore()
    store.entries = [_entry(1, 1), _entry(2, 1)]
    store.applied = 2
    store.commit = 2
    store.fsync()
    store.entries.append(_entry(3, 1))
    store.applied = 3
    store.crash_lose_unfsynced()
    assert [e.index for e in store.entries] == [1, 2]
    assert (store.applied, store.commit) == (2, 2)
    assert store.load().entries[0].payload == b"v"
