"""Raft vocabulary: roles, entries, messages, actions, config, store protocol.

The core is sans-IO: it never touches sockets, clocks, or disks. It reacts
to ``tick`` / ``handle_message`` / ``propose`` and returns plain ``Action``
dataclasses the runtime (asyncio node or simulator) executes in order.
Durability actions (``Persist``, ``SaveTermVote``) always precede the
``Send`` actions that depend on them.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Protocol


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class EntryKind(IntEnum):
    NOOP = 0
    COMMAND = 1


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    index: int  # 1-based; the log never has holes
    kind: EntryKind
    payload: bytes


@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False


@dataclass(frozen=True, slots=True)
class RequestVoteResponse:
    term: int
    vote_granted: bool
    pre_vote: bool = False


@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int
    conflict_index: int
    conflict_term: int


Message = RequestVote | RequestVoteResponse | AppendEntries | AppendEntriesResponse


@dataclass(frozen=True, slots=True)
class Send:
    to: str
    message: Message


@dataclass(frozen=True, slots=True)
class Persist:
    """Make the log durable through ``base_index + len(entries) - 1``.

    The runtime truncates any conflicting suffix from ``base_index`` first,
    then appends ``entries`` and fsyncs. Leaders always persist a pure
    suffix (they never truncate their own log); followers persist the
    post-truncation suffix.
    """

    base_index: int
    entries: tuple[LogEntry, ...]


@dataclass(frozen=True, slots=True)
class SaveTermVote:
    term: int
    voted_for: str | None


@dataclass(frozen=True, slots=True)
class Apply:
    index: int
    kind: EntryKind
    payload: bytes


Action = Send | Persist | SaveTermVote | Apply


@dataclass(frozen=True, slots=True)
class ProposeOk:
    index: int


@dataclass(frozen=True, slots=True)
class NotLeader:
    leader_id: str | None


ProposeResult = ProposeOk | NotLeader


@dataclass(frozen=True, slots=True)
class RaftConfig:
    election_timeout_min_ms: int = 300
    election_timeout_max_ms: int = 600
    heartbeat_ms: int = 100
    pre_vote: bool = True
    batch_window_ms: int = 2

    def __post_init__(self) -> None:
        if not 0 < self.election_timeout_min_ms <= self.election_timeout_max_ms:
            raise ValueError("bad election timeout range")
        if self.heartbeat_ms <= 0:
            raise ValueError("heartbeat_ms must be positive")
        if self.heartbeat_ms >= self.election_timeout_min_ms:
            raise ValueError("heartbeat must be quicker than the election timeout")
        if self.batch_window_ms < 0:
            raise ValueError("batch_window_ms must be >= 0")


@dataclass(frozen=True, slots=True)
class PersistedState:
    term: int
    voted_for: str | None
    entries: tuple[LogEntry, ...]
    commit_index: int
    applied_index: int


class RaftStore(Protocol):
    """Durable state seed for the core. The core copies this at
    construction and never touches the store again; all later mutations
    are reported back as ``Persist`` / ``SaveTermVote`` actions."""

    def load(self) -> PersistedState: ...
