"""Unit tests for the gRPC Raft transport and message conversion."""

import pytest

from minikv import minikv_pb2 as pb2
from minikv.raft.grpc_transport import (
    GrpcTransport,
    message_from_append_entries,
    message_from_request_vote,
)
from minikv.raft.types import AppendEntries, EntryKind, LogEntry, RequestVote


def test_close_and_unknown_peer() -> None:
    import asyncio

    async def main() -> None:
        transport = GrpcTransport({"n1": "127.0.0.1:1"})
        assert await transport.send("ghost", RequestVote(1, "a", 0, 0)) is None
        await transport.close()
        await transport.close()  # idempotent
        assert await transport.send("n1", RequestVote(1, "a", 0, 0)) is None

    asyncio.run(main())


def test_unreachable_returns_none() -> None:
    import asyncio

    async def main() -> None:
        # Nothing listens here: connection refused must map to None, not raise.
        transport = GrpcTransport({"n1": "127.0.0.1:1"}, timeout_s=0.2)
        try:
            assert await transport.send("n1", RequestVote(1, "a", 0, 0)) is None
        finally:
            await transport.close()

    asyncio.run(main())


def test_send_rejects_responses() -> None:
    import asyncio

    from minikv.raft.types import RequestVoteResponse

    async def main() -> None:
        transport = GrpcTransport({"n1": "127.0.0.1:1"})
        try:
            with pytest.raises(TypeError):
                await transport.send("n1", RequestVoteResponse(1, True))
        finally:
            await transport.close()

    asyncio.run(main())


def test_message_conversion() -> None:
    rv = message_from_request_vote(
        pb2.RequestVoteRequest(
            term=2, candidate_id="c", last_log_index=3, last_log_term=2, pre_vote=True
        )
    )
    assert rv == RequestVote(2, "c", 3, 2, True)
    ae = message_from_append_entries(
        pb2.AppendEntriesRequest(
            term=2,
            leader_id="l",
            prev_log_index=1,
            prev_log_term=2,
            entries=[pb2.LogEntry(term=2, index=2, type=1, payload=b"x")],
            leader_commit=1,
        )
    )
    assert ae == AppendEntries(2, "l", 1, 2, (LogEntry(2, 2, EntryKind.COMMAND, b"x"),), 1)
    with pytest.raises(ValueError, match="unknown log entry type"):
        message_from_append_entries(
            pb2.AppendEntriesRequest(
                term=2,
                leader_id="l",
                prev_log_index=1,
                prev_log_term=2,
                entries=[pb2.LogEntry(term=2, index=2, type=9, payload=b"x")],
                leader_commit=1,
            )
        )
