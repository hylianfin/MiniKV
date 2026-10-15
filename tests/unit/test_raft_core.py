"""Unit tests for the sans-IO Raft core (direct message driving)."""

import random

import pytest

from minikv.raft.core import RaftCore, coalesce_actions
from minikv.raft.types import (
    Action,
    AppendEntries,
    AppendEntriesResponse,
    Apply,
    EntryKind,
    LogEntry,
    NotLeader,
    Persist,
    ProposeOk,
    RaftConfig,
    RequestVote,
    RequestVoteResponse,
    Role,
    SaveTermVote,
    Send,
)
from sim.cluster import MemoryStore


def _store(
    term: int = 0,
    entries: list[LogEntry] | None = None,
    voted_for: str | None = None,
) -> MemoryStore:
    return MemoryStore(term=term, voted_for=voted_for, entries=list(entries or []))


def _core(
    node_id: str,
    peers: list[str],
    store: MemoryStore | None = None,
    seed: int = 0,
    **cfg: object,
) -> RaftCore:
    base: dict[str, object] = {"election_timeout_min_ms": 150, "election_timeout_max_ms": 300}
    base.update(cfg)
    return RaftCore(node_id, peers, store or _store(), random.Random(seed), RaftConfig(**base))  # type: ignore[arg-type]


class Harness:
    """Hand-driven cores with explicit time and selective delivery."""

    def __init__(
        self,
        ids: list[str],
        seed: int = 0,
        stores: dict[str, MemoryStore] | None = None,
        **cfg: object,
    ) -> None:
        self.cores = {}
        for k, i in enumerate(ids):
            store = stores[i] if stores else None
            self.cores[i] = _core(i, [p for p in ids if p != i], store, seed + k, **cfg)
        self.now = 0

    def route(self, src: str, action: Action) -> list[tuple[str, Action]]:
        """Deliver one Send and collect the responses; non-Sends yield []."""
        if isinstance(action, Send):
            target = self.cores[action.to]
            return [
                (action.to, a) for a in target.handle_message(self.now, action.message, sender=src)
            ]
        return []

    def settle(self, initial: list[tuple[str, Action]], max_steps: int = 500) -> None:
        """Route sends (and their responses, recursively) until quiet."""
        queue = list(initial)
        steps = 0
        while queue and steps < max_steps:
            src, act = queue.pop(0)
            steps += 1
            queue.extend(self.route(src, act))
        assert steps < max_steps, "message storm did not settle"

    def tick_all(self) -> list[tuple[str, Action]]:
        out = []
        for node_id, core in self.cores.items():
            out.extend((node_id, a) for a in core.tick(self.now))
        return out

    def pump(self, rounds: int = 20, step_ms: int = 50) -> None:
        for _ in range(rounds):
            self.settle(self.tick_all())
            self.now += step_ms
            if len(self.leaders()) == 1:
                break

    def advance(self, ms: int) -> None:
        self.now += ms

    def leaders(self) -> list[str]:
        return [i for i, c in self.cores.items() if c.role is Role.LEADER]


def _e(i: int, term: int, kind: EntryKind = EntryKind.COMMAND) -> LogEntry:
    return LogEntry(term=term, index=i, kind=kind, payload=f"c{i}".encode())


def test_election_produces_one_leader_with_noop() -> None:
    h = Harness(["a", "b", "c"], seed=1)
    h.pump()
    assert len(h.leaders()) == 1
    leader = h.cores[h.leaders()[0]]
    assert leader.term == 1
    assert leader.log[-1].kind is EntryKind.NOOP  # new leaders append a NOOP


def test_single_node_elects_itself() -> None:
    h = Harness(["solo"], seed=2)
    h.pump()
    assert h.leaders() == ["solo"]
    h.cores["solo"].tick(h.now + 10000)  # no quorum to lose alone: stays leader
    assert h.leaders() == ["solo"]


def test_vote_denied_after_voting_and_for_stale_log() -> None:
    h = Harness(["a", "b", "c"], seed=3)
    a, b = h.cores["a"], h.cores["b"]
    # b votes for a first.
    granted = b.handle_message(0, RequestVote(1, "a", 0, 0), sender="a")
    assert any(
        isinstance(act, Send) and act.message.vote_granted  # type: ignore[union-attr]
        for _, act in [("b", x) for x in granted]
    )
    # b denies c in the same term (already voted).
    denied = b.handle_message(0, RequestVote(1, "c", 0, 0), sender="c")
    sends = [m for _, act in [("b", x) for x in denied] for m in [act] if isinstance(act, Send)]
    assert len(sends) == 1 and sends[0].message.vote_granted is False  # type: ignore[union-attr]
    # Stale log denied even with a fresh vote available elsewhere.
    c = h.cores["c"]
    c.handle_message(0, RequestVote(2, "d", 0, 0), sender="d")  # bumps c to term 2
    stale = a.handle_message(0, RequestVote(2, "d", 0, 0), sender="d")
    _ = stale
    assert a.term == 2  # higher term always steps down first
    # A candidate whose log is behind loses on the log comparison itself.
    e = _core("e", ["f"], _store(term=1, entries=[_e(1, 1)]), seed=71)
    out = e.handle_message(0, RequestVote(1, "f", 0, 0), sender="f")
    sends = [a for a in out if isinstance(a, Send)]
    assert len(sends) == 1
    assert sends[0].message.vote_granted is False  # type: ignore[union-attr]
    assert e._voted_for is None  # denied votes record nothing


def test_old_term_entry_never_commits_by_counting() -> None:
    """The Figure-8 rule: entries from older terms commit only via a newer
    own-term entry, never by replica counting."""
    e1 = LogEntry(1, 1, EntryKind.COMMAND, b"x")
    e2 = LogEntry(2, 2, EntryKind.COMMAND, b"y")
    stores = {
        "a": _store(term=2, entries=[e1, e2]),
        "b": _store(term=2, entries=[e1, e2]),
        "c": _store(term=2, entries=[e1, e2]),
    }
    h = Harness(["a", "b", "c"], seed=10, stores=stores)
    a = h.cores["a"]
    # Elect a at term 3: only a's clock runs; b and c just answer. Entry
    # carrying Sends are dropped so the NOOP stays unreplicated for now.
    h.now = 500
    for _ in range(30):
        if a.role is Role.LEADER:
            break
        h.now += 60
        queue = [("a", x) for x in a.tick(h.now)]
        for _ in range(200):
            if not queue:
                break
            src, act = queue.pop(0)
            if isinstance(act, Send) and isinstance(act.message, AppendEntries):
                if act.message.entries:
                    continue  # keep the NOOP to ourselves for the negative half
            for responder, resp in h.route(src, act):
                if isinstance(resp, Send) and resp.to == "a":
                    queue.extend(
                        ("a", y) for y in a.handle_message(h.now, resp.message, sender=responder)
                    )
                elif isinstance(resp, Send):
                    queue.append((responder, resp))
    assert a.role is Role.LEADER and a.term == 3
    assert a.commit_index == 0  # NOOP unacked; nothing committed yet
    # Commit-only heartbeats (no NOOP): acks at match=2 must NOT commit idx2.
    for peer in ("b", "c"):
        hb = AppendEntries(
            term=3,
            leader_id="a",
            prev_log_index=2,
            prev_log_term=2,
            entries=(),
            leader_commit=0,
        )
        out = h.cores[peer].handle_message(h.now, hb, sender="a")
        assert not any(isinstance(x, Persist) for x in out)  # nothing new: no fsync
        for _, resp in [(peer, x) for x in out]:
            if isinstance(resp, Send):
                a.handle_message(h.now, resp.message, sender=peer)
    assert a.commit_index == 0  # old-term entries never commit by counting
    # Now replicate the NOOP (idx3, term 3) to b: everything commits in order.
    noop_ae = a._ae_to("b")
    assert len(noop_ae.message.entries) == 1  # type: ignore[union-attr]
    resp = h.cores["b"].handle_message(h.now, noop_ae.message, sender="a")
    applies: list[Apply] = []
    for _, r in [("b", x) for x in resp]:
        if isinstance(r, Send):
            for act in a.handle_message(h.now, r.message, sender="b"):
                if isinstance(act, Apply):
                    applies.append(act)
    assert a.commit_index == 3
    assert [(x.index, x.payload) for x in applies] == [(1, b"x"), (2, b"y"), (3, b"")]


def test_conflict_truncation_overwrites_divergent_suffix() -> None:
    h = Harness(["a", "b"], seed=20)
    h.pump()
    leader_id = h.leaders()[0]
    other = "b" if leader_id == "a" else "a"
    leader, follower = h.cores[leader_id], h.cores[other]
    # Divergent uncommitted suffix on the follower (simulates a stale leader).
    follower._log.append(LogEntry(leader.term, 999, EntryKind.COMMAND, b"rogue"))
    res, acts = leader.propose(h.now, b"new")
    assert isinstance(res, ProposeOk)
    for _, act in [(leader_id, a) for a in acts]:
        if isinstance(act, Send) and act.to == other:
            out = follower.handle_message(h.now, act.message, sender=leader_id)
            assert any(isinstance(x, Persist) for x in out)  # fsync before ack
            assert any(isinstance(x, Send) for x in out)
    assert [e.index for e in follower.log] == [e.index for e in leader.log]
    assert all(e.index != 999 for e in follower.log)


def test_step_down_on_higher_term_and_not_leader_hint() -> None:
    h = Harness(["a", "b", "c"], seed=30)
    h.pump()
    leader_id = h.leaders()[0]
    follower_id = next(i for i in h.cores if i != leader_id)
    result, actions = h.cores[follower_id].propose(h.now, b"x")
    assert isinstance(result, NotLeader) and result.leader_id == leader_id
    assert actions == []
    # A stale leader learns a higher term and steps down.
    leader = h.cores[leader_id]
    leader.handle_message(h.now, RequestVote(leader.term + 5, "z", 100, 100), sender="z")
    assert leader.role is Role.FOLLOWER
    assert leader.term == h.cores[leader_id].term


def test_check_quorum_isolated_leader_steps_down() -> None:
    h = Harness(["a", "b", "c"], seed=40)
    h.pump()
    leader_id = h.leaders()[0]
    leader = h.cores[leader_id]
    # Total isolation: no inbound traffic at all.
    h.advance(601)
    leader.tick(h.now)
    assert leader.role is Role.FOLLOWER


def test_check_quorum_connected_leader_survives() -> None:
    h = Harness(["a", "b", "c"], seed=41)
    h.pump()
    leader_id = h.leaders()[0]
    # Keep one peer responsive past the max timeout; the leader must stay.
    for _ in range(30):
        h.advance(50)
        for _, act in [(leader_id, a) for a in h.cores[leader_id].tick(h.now)]:
            if isinstance(act, Send) and act.to != "c":
                for responder, resp in h.route(leader_id, act):
                    if isinstance(resp, Send):
                        h.cores[leader_id].handle_message(h.now, resp.message, sender=responder)
    assert h.cores[leader_id].role is Role.LEADER


def test_fast_backup_jumps_instead_of_stepping() -> None:
    h = Harness(["a", "b"], seed=50)
    h.pump()
    leader_id = h.leaders()[0]
    other = "b" if leader_id == "a" else "a"
    leader = h.cores[leader_id]
    for i in range(10):
        leader.propose(h.now, f"v{i}".encode())
    # Pretend the follower is far behind: force the next probe past its end
    # so the rejection carries fast-backup hints.
    follower = h.cores[other]
    leader._next[other] = 8
    ae = leader._ae_to(other)
    out = follower.handle_message(h.now, ae.message, sender=leader_id)
    send = next(x for x in out if isinstance(x, Send))
    assert send.message.success is False  # type: ignore[union-attr]
    leader.handle_message(h.now, send.message, sender=other)
    # One rejection must jump next_index near the follower's end, not by one.
    assert leader._next[other] <= 2


def test_propose_persists_before_sending() -> None:
    h = Harness(["a", "b", "c"], seed=60)
    h.pump()
    leader = h.cores[h.leaders()[0]]
    _, actions = leader.propose(h.now, b"x")
    kinds = [type(a) for a in actions]
    assert kinds[0] is Persist
    assert all(k is Send for k in kinds[1:])


def test_follower_persists_entries_before_ack() -> None:
    h = Harness(["a", "b"], seed=61)
    h.pump()
    leader_id = h.leaders()[0]
    other = "b" if leader_id == "a" else "a"
    _, acts = h.cores[leader_id].propose(h.now, b"x")
    send = next(a for a in acts if isinstance(a, Send) and a.to == other)
    out = h.cores[other].handle_message(h.now, send.message, sender=leader_id)
    assert isinstance(out[0], Persist)  # fsync first...
    assert any(isinstance(x, Send) for x in out)  # ...then ack (applies trail)


def test_coalesce_actions() -> None:
    e1 = LogEntry(1, 1, EntryKind.COMMAND, b"a")
    e2 = LogEntry(1, 2, EntryKind.COMMAND, b"b")
    ae1 = AppendEntries(1, "l", 0, 0, (e1,), 0)
    ae2 = AppendEntries(1, "l", 0, 0, (e1, e2), 0)
    hb = AppendEntries(1, "l", 0, 0, (), 0)
    merged = coalesce_actions(
        [Persist(1, (e1,)), Send("f", hb), Persist(2, (e2,)), Send("f", ae1), Send("f", ae2)]
    )
    persists = [a for a in merged if isinstance(a, Persist)]
    sends = [a for a in merged if isinstance(a, Send)]
    assert persists == [Persist(1, (e1, e2))]  # one fsync
    assert len(sends) == 1 and sends[0].message == ae2  # latest wins
    # Non-contiguous persists and cross-term sends never merge.
    e3 = LogEntry(2, 9, EntryKind.COMMAND, b"c")
    ae3 = AppendEntries(2, "l", 8, 1, (e3,), 0)
    merged2 = coalesce_actions(
        [Persist(1, (e1,)), Persist(9, (e3,)), Send("f", ae2), Send("f", ae3)]
    )
    assert [a for a in merged2 if isinstance(a, Persist)] == [Persist(1, (e1,)), Persist(9, (e3,))]
    assert [a.to for a in merged2 if isinstance(a, Send)] == ["f", "f"]


def test_config_validation() -> None:
    with pytest.raises(ValueError):
        RaftConfig(election_timeout_min_ms=600, election_timeout_max_ms=300)
    with pytest.raises(ValueError):
        RaftConfig(heartbeat_ms=500)  # slower than the election timeout
    with pytest.raises(ValueError):
        RaftConfig(heartbeat_ms=0)
    with pytest.raises(ValueError):
        RaftConfig(batch_window_ms=-1)
    with pytest.raises(ValueError):
        RaftCore("a", ["a"], _store(), __import__("random").Random(0), RaftConfig())
    with pytest.raises(ValueError):
        RaftCore("a", ["b", "b"], _store(), __import__("random").Random(0), RaftConfig())
    with pytest.raises(ValueError):
        bad = MemoryStore(
            term=1,
            entries=[LogEntry(1, 2, EntryKind.COMMAND, b"x")],  # index must be 1
        )
        RaftCore("a", ["b"], bad, __import__("random").Random(0), RaftConfig())
    assert RaftConfig().batch_window_ms == 2


def test_stale_term_rpc_denied_without_state_change() -> None:
    h = Harness(["a", "b", "c"], seed=70)
    h.pump()
    leader_id = h.leaders()[0]
    leader = h.cores[leader_id]
    term_before = leader.term
    out = leader.handle_message(h.now, RequestVote(term_before - 1, "z", 0, 0), sender="z")
    sends = [a for a in out if isinstance(a, Send)]
    assert len(sends) == 1
    resp = sends[0].message
    assert isinstance(resp, RequestVoteResponse) and resp.vote_granted is False
    assert leader.term == term_before and leader.role is Role.LEADER
    assert leader.id == leader_id and set(leader.peers) == set(h.cores) - {leader_id}
    assert leader.leader_id == leader_id


def test_conflict_term_backup_falls_back_to_conflict_index() -> None:
    """Follower rejects at an entry whose term the leader lacks: next_index
    jumps straight to the follower's conflict point."""
    h = Harness(["a", "b"], seed=80)
    h.pump()
    leader_id = h.leaders()[0]
    other = "b" if leader_id == "a" else "a"
    leader, follower = h.cores[leader_id], h.cores[other]
    for i in range(2):
        res, _ = leader.propose(h.now, f"v{i}".encode())
        assert isinstance(res, ProposeOk)
    # Follower: NOOP@T idx1, then a rogue term spanning TWO entries.
    follower._log = [
        LogEntry(leader.term, 1, EntryKind.NOOP, b""),
        LogEntry(leader.term + 5, 2, EntryKind.COMMAND, b"rogue"),
        LogEntry(leader.term + 5, 3, EntryKind.COMMAND, b"rogue"),
    ]
    leader._next[other] = 4  # probe at idx3
    ae = leader._ae_to(other)
    out = follower.handle_message(h.now, ae.message, sender=leader_id)
    send = next(x for x in out if isinstance(x, Send))
    resp = send.message
    assert isinstance(resp, AppendEntriesResponse) and not resp.success
    # Walk-back finds the rogue term's first index (covers the loop).
    assert resp.conflict_term == leader.term + 5 and resp.conflict_index == 2
    leader.handle_message(h.now, resp, sender=other)
    assert leader._next[other] == 2


def test_conflict_backup_never_jumps_forward() -> None:
    """The follower's conflict term coincides with a term the leader holds
    further along: jumping past the leader's term end would overshoot the
    follower and oscillate, so the probe falls back to the conflict point
    (strictly decreasing => termination) and then converges."""
    stores = {
        # Leader-to-be: terms 1,1,2,2 -- holds T2 through idx4.
        "a": _store(
            term=2,
            entries=[
                LogEntry(1, 1, EntryKind.COMMAND, b"x"),
                LogEntry(1, 2, EntryKind.COMMAND, b"y"),
                LogEntry(2, 3, EntryKind.COMMAND, b"z"),
                LogEntry(2, 4, EntryKind.COMMAND, b"w"),
            ],
        ),
        # Shorter log, so b grants a's campaign; rogue T2 shadows idx2.
        "b": _store(
            term=1,
            entries=[
                LogEntry(1, 1, EntryKind.COMMAND, b"x"),
                LogEntry(2, 2, EntryKind.COMMAND, b"rogue"),
            ],
        ),
    }
    # b's log is shorter, so a wins a real election (up-to-date check).
    h = Harness(["a", "b"], seed=82, stores=stores)
    h.pump()
    assert h.leaders() == ["a"]
    a = h.cores["a"]
    assert a.term == 3  # seeded term 2 + one campaign (+ NOOP idx5)
    # Strand b AFTER the election dust settles: rogue T2 shadows idx2.
    h.cores["b"]._log = [
        LogEntry(1, 1, EntryKind.COMMAND, b"x"),
        LogEntry(2, 2, EntryKind.COMMAND, b"rogue"),
    ]
    # Stage the probe as if backup already walked back to idx3.
    a._next["b"] = 3  # prev=2 (T1); b holds rogue T2 there
    ae = a._ae_to("b")
    out = h.cores["b"].handle_message(h.now, ae.message, sender="a")
    send = next(x for x in out if isinstance(x, Send))
    resp = send.message
    assert isinstance(resp, AppendEntriesResponse) and not resp.success
    assert (resp.conflict_term, resp.conflict_index) == (2, 2)
    a.handle_message(h.now, resp, sender="b")
    # past-the-term-end would be 5 (forward!); the clamp keeps 2.
    assert a._next["b"] == 2
    # And from there the exchange converges instead of oscillating.
    h.settle([("a", a._ae_to("b"))])
    assert [e.index for e in h.cores["b"].log] == [e.index for e in a.log]
    assert [e.payload for e in h.cores["b"].log] == [e.payload for e in a.log]


def test_coalesce_keeps_votes_applies_and_unmergeable_sends() -> None:
    e1 = LogEntry(1, 1, EntryKind.COMMAND, b"a")
    rv = RequestVote(2, "c", 1, 1)
    rv_pre = RequestVote(2, "c", 1, 1, pre_vote=True)
    rv_old = RequestVote(2, "c", 0, 0)  # older log: does not cover rv
    ae_short = AppendEntries(1, "l", 5, 1, (e1,), 0)
    ae_longer_earlier = AppendEntries(1, "l", 3, 1, (e1, e1), 0)  # regresses prev
    resp1 = AppendEntriesResponse(1, True, 3, 0, 0)
    resp2 = AppendEntriesResponse(1, True, 5, 0, 0)
    merged = coalesce_actions(
        [
            SaveTermVote(1, None),
            SaveTermVote(2, "c"),
            Persist(1, (e1,)),
            Send("f", rv),
            Send("f", rv),  # identical: merges
            Send("g", rv),
            Send("g", rv_pre),  # different kind of vote: kept separately
            Send("h", ae_short),
            Send("h", ae_longer_earlier),  # regressing prev: kept separately
            Send("i", rv),
            Send("i", ae_short),  # mismatched RPC kinds: kept separately
            Send("j", rv),
            Send("j", rv_old),  # older log: kept separately
            Send("k", resp1),
            Send("k", resp2),  # responses never merge: both kept, in order
            Apply(1, EntryKind.COMMAND, b"a"),
        ]
    )
    saves = [a for a in merged if isinstance(a, SaveTermVote)]
    sends = [a for a in merged if isinstance(a, Send)]
    applies = [a for a in merged if isinstance(a, Apply)]
    assert saves == [SaveTermVote(2, "c")]  # latest vote state wins
    assert [s.to for s in sends] == ["f", "g", "g", "h", "h", "i", "i", "j", "j", "k", "k"]
    assert [s.message for s in sends if s.to == "k"] == [resp1, resp2]
    assert applies == [Apply(1, EntryKind.COMMAND, b"a")]
