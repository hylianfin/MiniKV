"""Hand-maintained stubs for the generated minikv_pb2 (keeps mypy honest at
the gRPC boundary; update alongside proto/minikv.proto)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

OK: int
NOT_LEADER: int
TIMEOUT: int
INVALID: int
UNAVAILABLE: int
READ_LINEARIZABLE: int
READ_LOG: int
READ_STALE: int

class Status:
    code: int
    message: str
    leader_addr: str
    def __init__(self, *, code: int = ..., message: str = ..., leader_addr: str = ...) -> None: ...

class RequestContext:
    client_id: str
    request_seq: int
    def __init__(self, *, client_id: str = ..., request_seq: int = ...) -> None: ...

class PutRequest:
    ctx: RequestContext
    key: bytes
    value: bytes
    def __init__(
        self, *, ctx: RequestContext = ..., key: bytes = ..., value: bytes = ...
    ) -> None: ...

class PutResponse:
    status: Status
    def __init__(self, *, status: Status = ...) -> None: ...

class GetRequest:
    key: bytes
    mode: int
    def __init__(self, *, key: bytes = ..., mode: int = ...) -> None: ...

class GetResponse:
    status: Status
    found: bool
    value: bytes
    def __init__(self, *, status: Status = ..., found: bool = ..., value: bytes = ...) -> None: ...

class DeleteRequest:
    ctx: RequestContext
    key: bytes
    def __init__(self, *, ctx: RequestContext = ..., key: bytes = ...) -> None: ...

class DeleteResponse:
    status: Status
    existed: bool
    def __init__(self, *, status: Status = ..., existed: bool = ...) -> None: ...

class CasRequest:
    ctx: RequestContext
    key: bytes
    expect_absent: bool
    expected: bytes
    new_value: bytes
    def __init__(
        self,
        *,
        ctx: RequestContext = ...,
        key: bytes = ...,
        expect_absent: bool = ...,
        expected: bytes = ...,
        new_value: bytes = ...,
    ) -> None: ...

class CasResponse:
    status: Status
    success: bool
    found: bool
    current: bytes
    def __init__(
        self, *, status: Status = ..., success: bool = ..., found: bool = ..., current: bytes = ...
    ) -> None: ...

class ScanRequest:
    start: bytes
    end: bytes
    limit: int
    mode: int
    def __init__(
        self, *, start: bytes = ..., end: bytes = ..., limit: int = ..., mode: int = ...
    ) -> None: ...

class ScanItem:
    key: bytes
    value: bytes
    def __init__(self, *, key: bytes = ..., value: bytes = ...) -> None: ...

class StatusRequest:
    def __init__(self) -> None: ...

class StatusResponse:
    node_id: str
    role: str
    term: int
    leader_id: str
    leader_addr: str
    commit_index: int
    last_applied: int
    log_last_index: int
    def __init__(
        self,
        *,
        node_id: str = ...,
        role: str = ...,
        term: int = ...,
        leader_id: str = ...,
        leader_addr: str = ...,
        commit_index: int = ...,
        last_applied: int = ...,
        log_last_index: int = ...,
    ) -> None: ...

class LogEntry:
    term: int
    index: int
    type: int
    payload: bytes
    def __init__(
        self, *, term: int = ..., index: int = ..., type: int = ..., payload: bytes = ...
    ) -> None: ...

class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    pre_vote: bool
    def __init__(
        self,
        *,
        term: int = ...,
        candidate_id: str = ...,
        last_log_index: int = ...,
        last_log_term: int = ...,
        pre_vote: bool = ...,
    ) -> None: ...

class RequestVoteResponse:
    term: int
    vote_granted: bool
    def __init__(self, *, term: int = ..., vote_granted: bool = ...) -> None: ...

class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: Sequence[LogEntry]
    leader_commit: int
    def __init__(
        self,
        *,
        term: int = ...,
        leader_id: str = ...,
        prev_log_index: int = ...,
        prev_log_term: int = ...,
        entries: Sequence[LogEntry] = ...,
        leader_commit: int = ...,
    ) -> None: ...

class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int
    conflict_index: int
    conflict_term: int
    def __init__(
        self,
        *,
        term: int = ...,
        success: bool = ...,
        match_index: int = ...,
        conflict_index: int = ...,
        conflict_term: int = ...,
    ) -> None: ...

DESCRIPTOR: Any
