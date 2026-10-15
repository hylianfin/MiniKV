"""Sans-IO Raft state machine (Ongaro & Ousterhout, Figure 2 + pre-vote).

No I/O, no clocks, no global randomness: time enters as ``now_ms``,
randomness from the injected ``rng``. Every state change that must survive
a crash is reported as a ``Persist`` / ``SaveTermVote`` action BEFORE any
``Send`` that depends on it; runtimes must execute actions in order.
"""

from __future__ import annotations

import random

from minikv.raft.types import (
    Action,
    AppendEntries,
    AppendEntriesResponse,
    Apply,
    EntryKind,
    LogEntry,
    Message,
    NotLeader,
    Persist,
    ProposeOk,
    ProposeResult,
    RaftConfig,
    RaftStore,
    RequestVote,
    RequestVoteResponse,
    Role,
    SaveTermVote,
    Send,
)


class RaftCore:
    """One Raft peer's consensus logic. Not thread-safe (runtimes serialize)."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        store: RaftStore,
        rng: random.Random,
        config: RaftConfig,
    ) -> None:
        if node_id in peers:
            raise ValueError("node_id must not list itself as a peer")
        if len(set(peers)) != len(peers):
            raise ValueError("duplicate peer ids")
        persisted = store.load()
        self._id = node_id
        self._peers = list(peers)
        self._rng = rng
        self._config = config
        self._term = persisted.term
        self._voted_for = persisted.voted_for
        self._log: list[LogEntry] = list(persisted.entries)
        for position, entry in enumerate(self._log):
            if entry.index != position + 1:
                raise ValueError("persisted log has holes or bad indexes")
        # Commit can never exceed the log; applied can never exceed commit.
        self._commit = min(persisted.commit_index, self._last_index())
        self._applied = min(persisted.applied_index, self._commit)
        self._role = Role.FOLLOWER
        self._leader_id: str | None = None
        self._votes: set[str] = set()
        self._pre_vote_term: int | None = None
        self._pre_votes: set[str] = set()
        self._next: dict[str, int] = {}
        self._match: dict[str, int] = {}
        self._contact: dict[str, int] = {}
        self._leader_since: int | None = None
        self._election_deadline: int | None = None  # lazy: set on first tick
        self._heartbeat_deadline = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def peers(self) -> list[str]:
        return list(self._peers)

    @property
    def role(self) -> Role:
        return self._role

    @property
    def term(self) -> int:
        return self._term

    @property
    def leader_id(self) -> str | None:
        return self._leader_id

    @property
    def commit_index(self) -> int:
        return self._commit

    @property
    def last_applied(self) -> int:
        return self._applied

    @property
    def log(self) -> tuple[LogEntry, ...]:
        return tuple(self._log)

    def tick(self, now_ms: int) -> list[Action]:
        """Advance timers: campaign on election timeout, heartbeat as leader."""
        actions: list[Action] = []
        if self._election_deadline is None:
            self._election_deadline = now_ms + self._random_timeout()
        if self._role is Role.LEADER:
            if self._quorum_lost(now_ms):
                actions.extend(self._step_down(now_ms))
            elif now_ms >= self._heartbeat_deadline:
                for peer in self._peers:
                    actions.append(self._ae_to(peer))
                self._heartbeat_deadline = now_ms + self._config.heartbeat_ms
        elif now_ms >= self._election_deadline:
            # Candidates always bump the term; followers pre-vote first when
            # enabled (a repeat expiry just retries the pre-vote round).
            if self._role is Role.CANDIDATE or not self._config.pre_vote:
                actions.extend(self._start_election(now_ms))
            else:
                actions.extend(self._start_pre_vote(now_ms))
        actions.extend(self._drain_applies())
        return actions

    def handle_message(self, now_ms: int, msg: Message, sender: str | None = None) -> list[Action]:
        """Process one RPC. ``sender`` attributes responses for check-quorum;
        requests already carry their author."""
        actions: list[Action] = []
        if isinstance(msg, RequestVote) and msg.pre_vote:
            # Pre-vote never moves terms: it only probes electability.
            actions.extend(self._on_pre_vote(msg))
        else:
            if msg.term > self._term:
                actions.extend(self._step_down_to(msg.term, now_ms))
            # Same-term inbound traffic proves the sender is reachable, which
            # is what check-quorum needs (stepping down is always safe, so a
            # conservative over-count here errs toward stability, not risk).
            if self._role is Role.LEADER and sender in self._peers and msg.term == self._term:
                self._contact[sender] = now_ms
            if isinstance(msg, RequestVote):
                actions.extend(self._on_request_vote(msg))
            elif isinstance(msg, RequestVoteResponse):
                actions.extend(self._on_request_vote_response(msg, sender, now_ms))
            elif isinstance(msg, AppendEntries):
                actions.extend(self._on_append_entries(msg, now_ms))
            else:
                actions.extend(self._on_append_entries_response(msg, sender, now_ms))
        actions.extend(self._drain_applies())
        return actions

    def propose(self, now_ms: int, command: bytes) -> tuple[ProposeResult, list[Action]]:
        """Append a client command on the leader. The batch window is a
        runtime concern: runtimes coalesce these actions (see
        ``coalesce_actions``) instead of the core delaying the append, so
        the log index is known immediately."""
        _ = now_ms
        if self._role is not Role.LEADER:
            return NotLeader(self._leader_id), []
        entry = LogEntry(
            term=self._term,
            index=self._last_index() + 1,
            kind=EntryKind.COMMAND,
            payload=bytes(command),
        )
        self._log.append(entry)
        actions: list[Action] = [Persist(entry.index, (entry,))]
        for peer in self._peers:
            actions.append(self._ae_to(peer))
        return ProposeOk(entry.index), actions

    def _on_pre_vote(self, msg: RequestVote) -> list[Action]:
        # Only the log comparison gates a pre-vote -- deliberately NOT
        # voted_for, which constrains the current term's real vote. A node
        # that voted for a now-dead leader must still be able to probe the
        # next term, or no election could ever follow a leader crash.
        # Safety is unaffected: pre-votes elect nothing (a partitioned node
        # still needs a connected majority to campaign, and real votes keep
        # the one-vote-per-term rule).
        mine_term, mine_index = self._last_term(), self._last_index()
        grant = (msg.last_log_term > mine_term) or (
            msg.last_log_term == mine_term and msg.last_log_index >= mine_index
        )
        return [
            Send(
                msg.candidate_id,
                RequestVoteResponse(term=self._term, vote_granted=grant, pre_vote=True),
            )
        ]

    def _on_request_vote(self, msg: RequestVote) -> list[Action]:
        if msg.term < self._term:
            return [Send(msg.candidate_id, RequestVoteResponse(self._term, False))]
        if self._vote_grantable(msg.candidate_id, msg.last_log_index, msg.last_log_term):
            self._voted_for = msg.candidate_id
            # Unset: the next tick recomputes the deadline from the current
            # time, which is exactly "reset the election timer".
            self._election_deadline = None
            return [
                SaveTermVote(self._term, self._voted_for),
                Send(msg.candidate_id, RequestVoteResponse(self._term, True)),
            ]
        return [Send(msg.candidate_id, RequestVoteResponse(self._term, False))]

    def _vote_grantable(self, candidate_id: str, last_log_index: int, last_log_term: int) -> bool:
        if self._voted_for not in (None, candidate_id):
            return False
        mine_term, mine_index = self._last_term(), self._last_index()
        # Candidate wins if its log is strictly newer, or same term and at
        # least as long (the Figure 2 log comparison).
        return (last_log_term > mine_term) or (
            last_log_term == mine_term and last_log_index >= mine_index
        )

    def _on_request_vote_response(
        self, msg: RequestVoteResponse, sender: str | None, now_ms: int
    ) -> list[Action]:
        voter = sender
        if voter is None or msg.term != self._term:
            return []
        if msg.pre_vote:
            # Counted only while our own pre-vote round is open; the term on
            # the response is the respondent's unchanged term, so it must
            # still equal ours (anything newer steps us down elsewhere).
            if (
                self._pre_vote_term == self._term + 1
                and msg.term == self._term
                and msg.vote_granted
            ):
                self._pre_votes.add(voter)
                if self._is_majority(self._pre_votes | {self._id}):
                    return self._start_election(now_ms)
            return []
        if self._role is not Role.CANDIDATE or not msg.vote_granted:
            return []
        self._votes.add(voter)
        if self._is_majority(self._votes):
            return self._become_leader(now_ms)
        return []

    def _on_append_entries(self, msg: AppendEntries, now_ms: int) -> list[Action]:
        if msg.term < self._term:
            return [
                Send(
                    msg.leader_id,
                    AppendEntriesResponse(
                        term=self._term,
                        success=False,
                        match_index=0,
                        conflict_index=self._last_index() + 1,
                        conflict_term=0,
                    ),
                )
            ]
        # Valid leader contact: step down if needed, then reset the timer.
        actions: list[Action] = []
        if self._role is not Role.FOLLOWER or self._leader_id != msg.leader_id:
            actions.extend(self._become_follower(msg.leader_id, now_ms))
        else:
            self._election_deadline = now_ms + self._random_timeout()
        if msg.prev_log_index > 0 and (
            msg.prev_log_index > self._last_index()
            or self._term_at(msg.prev_log_index) != msg.prev_log_term
        ):
            return actions + [Send(msg.leader_id, self._conflict_response(msg.prev_log_index))]
        # Consistent prefix: drop any conflicting suffix, append the rest.
        # The leader always sends a contiguous suffix from our next index,
        # so the merged log keeps 1-based contiguity (holes would break it).
        new_start = msg.prev_log_index + 1
        keep = [e for e in self._log if e.index < new_start]
        incoming = [e for e in msg.entries if e.index >= new_start]
        if incoming:
            assert incoming[0].index == new_start
            # Entries we already hold are identical by log matching; persist
            # only what is actually new. (No Persist at all for pure
            # duplicates: re-persisting an empty suffix would truncate data
            # the runtime already made durable.)
            existing = {e.index: e.term for e in self._log}
            fresh = tuple(
                e for e in incoming if e.index not in existing or existing[e.index] != e.term
            )
            self._log = keep + incoming
            if fresh:
                base = min(e.index for e in fresh)
                actions.append(Persist(base, tuple(e for e in incoming if e.index >= base)))
        if msg.leader_commit > self._commit:
            self._commit = min(msg.leader_commit, self._last_index())
        return actions + [
            Send(
                msg.leader_id,
                AppendEntriesResponse(
                    term=self._term,
                    success=True,
                    match_index=self._last_index(),
                    conflict_index=0,
                    conflict_term=0,
                ),
            )
        ]

    def _on_append_entries_response(
        self, msg: AppendEntriesResponse, sender: str | None, now_ms: int
    ) -> list[Action]:
        if sender is None or self._role is not Role.LEADER or msg.term != self._term:
            return []
        self._contact[sender] = now_ms
        if msg.success:
            # Plain assignment (no max): a stale success must be able to move
            # next_index back -- the fresher rejection that moved it forward
            # belongs to a newer exchange. Reordered acks self-heal.
            self._match[sender] = msg.match_index
            self._next[sender] = msg.match_index + 1
            self._advance_commit()
            # Promptly share new entries or a fresher commit; silence
            # otherwise (the next heartbeat carries anything left).
            if self._next[sender] <= self._last_index() or self._match[sender] < self._commit:
                return [self._ae_to(sender)]
            return []
        # Fast backup: jump next_index instead of decrementing one by one.
        # Every rejection must strictly DECREASE next_index (bounded below
        # by 1) or probes can oscillate: jumping past the leader's own term
        # end is capped back to the follower's conflict point.
        if msg.conflict_term != 0:
            last_of_term = 0
            for entry in self._log:
                if entry.term == msg.conflict_term:
                    last_of_term = entry.index
            if last_of_term:
                past_term = last_of_term + 1
                self._next[sender] = (
                    past_term if past_term < self._next[sender] else msg.conflict_index
                )
            else:
                self._next[sender] = msg.conflict_index
        else:
            self._next[sender] = msg.conflict_index
        self._next[sender] = min(max(self._next[sender], 1), self._last_index() + 1)
        return [self._ae_to(sender)]

    def _start_pre_vote(self, now_ms: int) -> list[Action]:
        self._pre_vote_term = self._term + 1
        self._pre_votes = set()
        self._election_deadline = now_ms + self._random_timeout()
        if not self._peers:
            # Single node: pre-vote succeeds trivially; go campaign at once.
            return self._start_election(now_ms)
        return [
            Send(
                peer,
                RequestVote(
                    term=self._term + 1,
                    candidate_id=self._id,
                    last_log_index=self._last_index(),
                    last_log_term=self._last_term(),
                    pre_vote=True,
                ),
            )
            for peer in self._peers
        ]

    def _start_election(self, now_ms: int) -> list[Action]:
        self._term += 1
        self._voted_for = self._id
        self._role = Role.CANDIDATE
        self._leader_id = None
        self._votes = {self._id}
        self._pre_vote_term = None
        self._pre_votes = set()
        self._election_deadline = now_ms + self._random_timeout()
        actions: list[Action] = [SaveTermVote(self._term, self._voted_for)]
        if not self._peers:
            return actions + self._become_leader(now_ms)
        for peer in self._peers:
            actions.append(
                Send(
                    peer,
                    RequestVote(
                        term=self._term,
                        candidate_id=self._id,
                        last_log_index=self._last_index(),
                        last_log_term=self._last_term(),
                    ),
                )
            )
        return actions

    def _become_leader(self, now_ms: int) -> list[Action]:
        self._role = Role.LEADER
        self._leader_id = self._id
        self._leader_since = now_ms
        self._votes = set()
        self._pre_vote_term = None
        self._pre_votes = set()
        last = self._last_index()
        self._next = {peer: last + 1 for peer in self._peers}
        self._match = {peer: 0 for peer in self._peers}
        self._contact = {}
        self._heartbeat_deadline = now_ms + self._config.heartbeat_ms
        # A NOOP of the new term lets earlier-term entries commit: the
        # commit rule only counts replicas for own-term entries.
        noop = LogEntry(term=self._term, index=last + 1, kind=EntryKind.NOOP, payload=b"")
        self._log.append(noop)
        actions: list[Action] = [Persist(noop.index, (noop,))]
        for peer in self._peers:
            actions.append(self._ae_to(peer))
        return actions

    def _become_follower(self, leader_id: str | None, now_ms: int) -> list[Action]:
        _ = now_ms
        self._role = Role.FOLLOWER
        self._leader_id = leader_id
        self._leader_since = None
        self._votes = set()
        self._pre_vote_term = None
        self._pre_votes = set()
        self._next = {}
        self._match = {}
        self._contact = {}
        self._election_deadline = None  # lazy-reset on next tick/message
        return []

    def _step_down(self, now_ms: int) -> list[Action]:
        return self._become_follower(None, now_ms)

    def _step_down_to(self, term: int, now_ms: int) -> list[Action]:
        self._term = term
        self._voted_for = None
        actions: list[Action] = [SaveTermVote(self._term, None)]
        actions.extend(self._become_follower(None, now_ms))
        return actions

    def _advance_commit(self) -> None:
        # Only own-term entries commit by counting replicas. An old-term
        # entry commits implicitly once a later own-term entry commits.
        majority = self._quorum_size()
        for index in range(self._commit + 1, self._last_index() + 1):
            if self._term_at(index) != self._term:
                continue
            replicas = 1 + sum(1 for peer in self._peers if self._match.get(peer, 0) >= index)
            if replicas >= majority:
                self._commit = index

    def _drain_applies(self) -> list[Action]:
        actions: list[Action] = []
        while self._applied < self._commit:
            self._applied += 1
            entry = self._log[self._applied - 1]
            assert entry.index == self._applied
            actions.append(Apply(entry.index, entry.kind, entry.payload))
        return actions

    def _ae_to(self, peer: str) -> Send:
        prev = self._next.get(peer, self._last_index() + 1) - 1
        return Send(
            peer,
            AppendEntries(
                term=self._term,
                leader_id=self._id,
                prev_log_index=prev,
                prev_log_term=self._term_at(prev),
                entries=tuple(self._log[prev:]),
                leader_commit=self._commit,
            ),
        )

    def _conflict_response(self, prev_log_index: int) -> AppendEntriesResponse:
        if prev_log_index > self._last_index():
            # Our log is simply short: the leader should retry right past it.
            return AppendEntriesResponse(
                term=self._term,
                success=False,
                match_index=0,
                conflict_index=self._last_index() + 1,
                conflict_term=0,
            )
        term = self._term_at(prev_log_index)
        first = prev_log_index
        while first > 1 and self._term_at(first - 1) == term:
            first -= 1
        return AppendEntriesResponse(
            term=self._term,
            success=False,
            match_index=0,
            conflict_index=first,
            conflict_term=term,
        )

    def _quorum_size(self) -> int:
        return (len(self._peers) + 1) // 2 + 1

    def _is_majority(self, voters: set[str]) -> bool:
        return len(voters) >= self._quorum_size()

    def _quorum_lost(self, now_ms: int) -> bool:
        # The leader itself always counts; step down when fewer than a
        # quorum (including self) made contact within a full max timeout.
        # A grace period after election lets the first heartbeats return.
        if not self._peers:
            return False
        if (
            self._leader_since is not None
            and now_ms - self._leader_since < self._config.election_timeout_max_ms
        ):
            return False
        window = self._config.election_timeout_max_ms
        recent = sum(
            1 for peer in self._peers if now_ms - self._contact.get(peer, -(10**12)) <= window
        )
        return 1 + recent < self._quorum_size()

    def _last_index(self) -> int:
        return len(self._log)

    def _last_term(self) -> int:
        return self._log[-1].term if self._log else 0

    def _term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        return self._log[index - 1].term

    def _random_timeout(self) -> int:
        return self._rng.randint(
            self._config.election_timeout_min_ms, self._config.election_timeout_max_ms
        )


def coalesce_actions(actions: list[Action]) -> list[Action]:
    """Fold one batch window's actions: contiguous ``Persist`` runs merge
    into a single fsync, and each follower keeps only its latest ``Send``
    (later sends cover earlier ones when nothing was processed between
    them -- same term/leader, non-regressing previous index).

    Only coalesce actions produced back-to-back (no message handling or
    timer expiry between them); when in doubt, execute unmerged.
    """
    persists: list[Persist] = []
    sends_out: list[Send] = []
    send_idx: dict[str, int] = {}
    saves: list[SaveTermVote] = []
    applies: list[Apply] = []
    for action in actions:
        if isinstance(action, Persist):
            if persists and action.base_index == persists[-1].base_index + len(
                persists[-1].entries
            ):
                persists[-1] = Persist(
                    persists[-1].base_index, persists[-1].entries + action.entries
                )
            else:
                persists.append(action)
        elif isinstance(action, Send):
            prev_at = send_idx.get(action.to)
            if prev_at is not None and _send_covers(sends_out[prev_at].message, action.message):
                sends_out[prev_at] = action
            else:
                # Not mergeable with the earlier send: keep both, in order.
                send_idx[action.to] = len(sends_out)
                sends_out.append(action)
        elif isinstance(action, SaveTermVote):
            saves.append(action)
        else:
            applies.append(action)
    merged: list[Action] = []
    if saves:
        merged.append(saves[-1])  # terms/votes only move forward
    merged.extend(persists)
    merged.extend(sends_out)
    merged.extend(applies)
    return merged


def _send_covers(old: Message, new: Message) -> bool:
    """True when ``new`` carries at least what ``old`` did (same RPC kind
    and term, previous log position not regressed, entries a superset)."""
    if type(new) is not type(old):
        return False
    if new.term != old.term:
        return False
    if isinstance(old, AppendEntries) and isinstance(new, AppendEntries):
        if new.prev_log_index < old.prev_log_index:
            return False
        old_cover = old.prev_log_index + len(old.entries)
        new_cover = new.prev_log_index + len(new.entries)
        return new_cover >= old_cover and new.leader_commit >= old.leader_commit
    if isinstance(old, RequestVote) and isinstance(new, RequestVote):
        return new.pre_vote == old.pre_vote and (new.last_log_term, new.last_log_index) >= (
            old.last_log_term,
            old.last_log_index,
        )
    return False
