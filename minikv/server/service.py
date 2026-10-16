"""gRPC KV and Raft service implementations over a RaftNode."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc

from minikv import minikv_pb2 as pb2
from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.kv.state_machine import (
    CasCommand,
    CasResult,
    Command,
    DeleteCommand,
    DeleteResult,
    PutCommand,
    PutResult,
    RequestContext,
    decode_result,
    encode_request,
)
from minikv.raft.grpc_transport import message_from_append_entries, message_from_request_vote
from minikv.raft.node import ProposeApplied, ProposeRedirect, RaftNode, ReadReady, ReadRedirect
from minikv.raft.types import AppendEntriesResponse, RequestVoteResponse

_DEFAULT_TIMEOUT_S = 3.0


def _ok(message: str = "") -> pb2.Status:
    return pb2.Status(code=pb2.OK, message=message)


def _not_leader(leader_addr: str | None) -> pb2.Status:
    return pb2.Status(code=pb2.NOT_LEADER, message="not the leader", leader_addr=leader_addr or "")


def _timeout(message: str) -> pb2.Status:
    return pb2.Status(code=pb2.TIMEOUT, message=message)


class KVServicer(pb2_grpc.KVServicer):
    def __init__(self, node: RaftNode, timeout_s: float = _DEFAULT_TIMEOUT_S) -> None:
        self._node = node
        self._timeout_s = timeout_s

    async def Put(
        self, request: pb2.PutRequest, context: grpc.aio.ServicerContext
    ) -> pb2.PutResponse:  # noqa: N802
        outcome = await self._propose(
            request.ctx, PutCommand(bytes(request.key), bytes(request.value))
        )
        if outcome is None:
            return pb2.PutResponse(status=_timeout("write timed out"))
        if isinstance(outcome, ProposeRedirect):
            return pb2.PutResponse(status=_not_leader(outcome.leader_addr))
        result = decode_result(outcome.result)
        assert isinstance(result, PutResult)
        return pb2.PutResponse(status=_ok())

    async def Get(
        self, request: pb2.GetRequest, context: grpc.aio.ServicerContext
    ) -> pb2.GetResponse:  # noqa: N802
        barrier = await self._read_barrier(request.mode)
        if barrier is not None:
            return pb2.GetResponse(status=barrier)
        value = self._node.applier.data.get(bytes(request.key))
        if value is None:
            return pb2.GetResponse(status=_ok(), found=False)
        return pb2.GetResponse(status=_ok(), found=True, value=value)

    async def Delete(
        self, request: pb2.DeleteRequest, context: grpc.aio.ServicerContext
    ) -> pb2.DeleteResponse:  # noqa: N802
        outcome = await self._propose(request.ctx, DeleteCommand(bytes(request.key)))
        if outcome is None:
            return pb2.DeleteResponse(status=_timeout("write timed out"))
        if isinstance(outcome, ProposeRedirect):
            return pb2.DeleteResponse(status=_not_leader(outcome.leader_addr))
        result = decode_result(outcome.result)
        assert isinstance(result, DeleteResult)
        return pb2.DeleteResponse(status=_ok(), existed=result.existed)

    async def Cas(
        self, request: pb2.CasRequest, context: grpc.aio.ServicerContext
    ) -> pb2.CasResponse:  # noqa: N802
        expected = None if request.expect_absent else bytes(request.expected)
        outcome = await self._propose(
            request.ctx, CasCommand(bytes(request.key), expected, bytes(request.new_value))
        )
        if outcome is None:
            return pb2.CasResponse(status=_timeout("write timed out"))
        if isinstance(outcome, ProposeRedirect):
            return pb2.CasResponse(status=_not_leader(outcome.leader_addr))
        result = decode_result(outcome.result)
        assert isinstance(result, CasResult)
        return pb2.CasResponse(
            status=_ok(),
            success=result.success,
            found=result.current is not None,
            current=result.current or b"",
        )

    async def Scan(
        self, request: pb2.ScanRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[pb2.ScanItem]:  # noqa: N802
        barrier = await self._read_barrier(request.mode)
        if barrier is not None:
            if barrier.code == pb2.NOT_LEADER:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(f"NOT_LEADER {barrier.leader_addr}")
            else:
                context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                context.set_details(barrier.message)
            return
        # The scan materializes under the engine lock: a consistent
        # snapshot as of the read barrier, safe to stream after it.
        start = bytes(request.start) or None
        end = bytes(request.end) or None
        for key, value in self._node.applier.data.scan(start, end, request.limit or 0):
            yield pb2.ScanItem(key=key, value=value)

    async def Status(
        self, request: pb2.StatusRequest, context: grpc.aio.ServicerContext
    ) -> pb2.StatusResponse:  # noqa: N802
        status = await self._node.status()
        return pb2.StatusResponse(
            node_id=status.node_id,
            role=status.role,
            term=status.term,
            leader_id=status.leader_id or "",
            leader_addr=status.leader_addr or "",
            commit_index=status.commit_index,
            last_applied=status.last_applied,
            log_last_index=status.log_last_index,
        )

    async def _propose(
        self, ctx: pb2.RequestContext, command: Command
    ) -> ProposeApplied | ProposeRedirect | None:
        payload = encode_request(
            RequestContext(client_id=ctx.client_id, request_seq=ctx.request_seq), command
        )
        try:
            return await self._node.propose_command(payload, self._timeout_s)
        except TimeoutError:
            return None

    async def _read_barrier(self, mode: int) -> pb2.Status | None:
        """Pre-read check; None means the read may proceed."""
        if mode == pb2.READ_STALE:
            return None
        try:
            if mode == pb2.READ_LOG:
                # Serialize the read through the log as an empty command.
                logged = await self._node.propose_command(b"", self._timeout_s)
                if isinstance(logged, ProposeRedirect):
                    return _not_leader(logged.leader_addr)
                return None
            outcome = await self._node.read_index(self._timeout_s)
            if isinstance(outcome, ReadRedirect):
                return _not_leader(outcome.leader_addr)
            assert isinstance(outcome, ReadReady)
            await self._node.wait_applied(outcome.read_index, self._timeout_s)
            return None
        except TimeoutError:
            return _timeout("read timed out")


class RaftServicer(pb2_grpc.RaftServicer):
    def __init__(self, node: RaftNode) -> None:
        self._node = node

    async def RequestVote(
        self, request: pb2.RequestVoteRequest, context: grpc.aio.ServicerContext
    ) -> pb2.RequestVoteResponse:  # noqa: N802
        message = message_from_request_vote(request)
        response = await self._node.handle_raft_message(message, request.candidate_id)
        assert isinstance(response, RequestVoteResponse)
        return pb2.RequestVoteResponse(term=response.term, vote_granted=response.vote_granted)

    async def AppendEntries(
        self, request: pb2.AppendEntriesRequest, context: grpc.aio.ServicerContext
    ) -> pb2.AppendEntriesResponse:  # noqa: N802
        message = message_from_append_entries(request)
        response = await self._node.handle_raft_message(message, request.leader_id)
        assert isinstance(response, AppendEntriesResponse)
        return pb2.AppendEntriesResponse(
            term=response.term,
            success=response.success,
            match_index=response.match_index,
            conflict_index=response.conflict_index,
            conflict_term=response.conflict_term,
        )
