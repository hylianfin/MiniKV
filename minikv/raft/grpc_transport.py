"""gRPC Raft transport: core messages over the Raft service."""

from __future__ import annotations

import grpc

from minikv import minikv_pb2 as pb2
from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.raft.transport import RaftTransport
from minikv.raft.types import (
    AppendEntries,
    AppendEntriesResponse,
    EntryKind,
    LogEntry,
    Message,
    RequestVote,
    RequestVoteResponse,
)


class GrpcTransport(RaftTransport):
    """Delivers to ``peers[id]`` addresses with per-RPC deadlines."""

    def __init__(self, peers: dict[str, str], timeout_s: float = 0.5) -> None:
        self._peers = dict(peers)
        self._timeout_s = timeout_s
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, pb2_grpc.RaftStub] = {}
        self._closed = False

    async def send(self, to: str, message: Message) -> Message | None:
        if self._closed or to not in self._peers:
            return None
        try:
            if isinstance(message, RequestVote):
                return await self._request_vote(to, message)
            if isinstance(message, AppendEntries):
                return await self._append_entries(to, message)
        except grpc.aio.AioRpcError:
            return None
        raise TypeError(f"transport cannot send {type(message)}")

    async def _request_vote(self, to: str, message: RequestVote) -> RequestVoteResponse:
        response = await self._stub(to).RequestVote(
            pb2.RequestVoteRequest(
                term=message.term,
                candidate_id=message.candidate_id,
                last_log_index=message.last_log_index,
                last_log_term=message.last_log_term,
                pre_vote=message.pre_vote,
            ),
            timeout=self._timeout_s,
        )
        return RequestVoteResponse(
            term=response.term,
            vote_granted=response.vote_granted,
            pre_vote=message.pre_vote,
        )

    async def _append_entries(self, to: str, message: AppendEntries) -> AppendEntriesResponse:
        response = await self._stub(to).AppendEntries(
            pb2.AppendEntriesRequest(
                term=message.term,
                leader_id=message.leader_id,
                prev_log_index=message.prev_log_index,
                prev_log_term=message.prev_log_term,
                entries=[
                    pb2.LogEntry(
                        term=e.term,
                        index=e.index,
                        type=e.kind.value,
                        payload=e.payload,
                    )
                    for e in message.entries
                ],
                leader_commit=message.leader_commit,
            ),
            timeout=self._timeout_s,
        )
        return AppendEntriesResponse(
            term=response.term,
            success=response.success,
            match_index=response.match_index,
            conflict_index=response.conflict_index,
            conflict_term=response.conflict_term,
        )

    async def close(self) -> None:
        self._closed = True
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()

    def _stub(self, peer: str) -> pb2_grpc.RaftStub:
        stub = self._stubs.get(peer)
        if stub is None:
            channel = grpc.aio.insecure_channel(self._peers[peer])
            self._channels[peer] = channel
            stub = pb2_grpc.RaftStub(channel)
            self._stubs[peer] = stub
        return stub


def message_from_request_vote(request: pb2.RequestVoteRequest) -> RequestVote:
    return RequestVote(
        term=request.term,
        candidate_id=request.candidate_id,
        last_log_index=request.last_log_index,
        last_log_term=request.last_log_term,
        pre_vote=request.pre_vote,
    )


def message_from_append_entries(request: pb2.AppendEntriesRequest) -> AppendEntries:
    kind = {0: EntryKind.NOOP, 1: EntryKind.COMMAND}
    entries: list[LogEntry] = []
    for entry in request.entries:
        if entry.type not in (0, 1):
            raise ValueError(f"unknown log entry type {entry.type}")
        entries.append(
            LogEntry(
                term=entry.term,
                index=entry.index,
                kind=kind[entry.type],
                payload=bytes(entry.payload),
            )
        )
    return AppendEntries(
        term=request.term,
        leader_id=request.leader_id,
        prev_log_index=request.prev_log_index,
        prev_log_term=request.prev_log_term,
        entries=tuple(entries),
        leader_commit=request.leader_commit,
    )
