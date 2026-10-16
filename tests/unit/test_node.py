"""Unit tests for the asyncio RaftNode over an in-memory mesh transport."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from minikv.kv.state_machine import KVApplier, PutCommand, RequestContext, encode_request
from minikv.raft.logstore import LogStore
from minikv.raft.node import ProposeApplied, ProposeRedirect, RaftNode, ReadReady, ReadRedirect
from minikv.raft.transport import RaftTransport
from minikv.raft.types import Message, RaftConfig, RequestVote


def _payload(tag: str, seq: int = 1) -> bytes:
    return encode_request(RequestContext(f"test-{tag}", seq), PutCommand(f"k-{tag}".encode(), b"v"))


class MeshTransport(RaftTransport):
    """Direct in-process delivery between nodes in the same mesh."""

    def __init__(self, latency_s: float = 0.0, drop_all: bool = False) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.latency_s = latency_s
        self.drop_all = drop_all
        self.closed = False

    def register(self, node_id: str, node: RaftNode) -> None:
        self.nodes[node_id] = node

    def forget(self, node_id: str) -> None:
        self.nodes.pop(node_id, None)

    async def send(self, to: str, message: Message) -> Message | None:
        if self.closed or self.drop_all or to not in self.nodes:
            return None
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        return await self.nodes[to].handle_raft_message(message, self._caller(message))

    async def close(self) -> None:
        self.closed = True

    def _caller(self, message: Message) -> str:
        from minikv.raft.types import AppendEntries, RequestVote

        if isinstance(message, (RequestVote, AppendEntries)):
            return message.candidate_id if isinstance(message, RequestVote) else message.leader_id
        raise AssertionError("mesh only routes requests")


def _config() -> RaftConfig:
    return RaftConfig(election_timeout_min_ms=150, election_timeout_max_ms=300, heartbeat_ms=50)


class Mesh:
    def __init__(
        self, tmp: Path, n: int = 3, latency_s: float = 0.0, config: RaftConfig | None = None
    ) -> None:
        self.tmp = tmp
        self.transport = MeshTransport(latency_s)
        self.nodes: dict[str, RaftNode] = {}
        ids = [f"n{i}" for i in range(1, n + 1)]
        peers = {i: i for i in ids}  # addresses unused by the mesh
        for node_id in ids:
            others = [p for p in ids if p != node_id]
            node = RaftNode(
                node_id,
                peers,
                config or _config(),
                LogStore(tmp / node_id / "raft"),
                KVApplier(tmp / node_id / "kv"),
                transport=self._scoped(node_id, others),
            )
            self.nodes[node_id] = node
            self.transport.register(node_id, node)

    def _scoped(self, node_id: str, others: list[str]) -> RaftTransport:
        parent = self.transport

        class Scoped(RaftTransport):
            async def send(self, to: str, message: Message) -> Message | None:
                assert to in others, f"{node_id} dialed unknown peer {to}"
                return await parent.send(to, message)

            async def close(self) -> None:
                pass

        return Scoped()

    async def start_all(self) -> None:
        for node in self.nodes.values():
            await node.start()

    async def stop_all(self) -> None:
        for node in self.nodes.values():
            await node.stop()

    async def wait_leader(self, timeout_s: float = 10.0) -> str:
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            leaders = [
                node_id
                for node_id, node in self.nodes.items()
                if (await node.status()).role == "leader"
            ]
            if len(leaders) == 1:
                return leaders[0]
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError("no leader elected")
            await asyncio.sleep(0.05)


async def _propose_ok(node: RaftNode, payload: bytes, timeout_s: float = 5.0) -> bytes:
    outcome = await node.propose_command(payload, timeout_s)
    assert isinstance(outcome, ProposeApplied), outcome
    return outcome.result


@pytest.mark.asyncio
async def test_mesh_elects_and_replicates() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            for i in range(5):
                await _propose_ok(mesh.nodes[leader], _payload(f"cmd{i}", i + 1))
            await asyncio.sleep(0.5)  # let followers catch up
            for node_id, node in mesh.nodes.items():
                status = await node.status()
                assert status.commit_index >= 6, (node_id, status)
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_mesh_read_index_and_redirect() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            follower = next(n for n in mesh.nodes if n != leader)
            outcome = await mesh.nodes[leader].read_index(5.0)
            assert isinstance(outcome, ReadReady)
            await mesh.nodes[leader].wait_applied(outcome.read_index, 5.0)
            redirect = await mesh.nodes[follower].read_index(5.0)
            assert isinstance(redirect, ReadRedirect)
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_mesh_leader_failover_keeps_serving() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            await _propose_ok(mesh.nodes[leader], _payload("before", 1))
            await mesh.nodes[leader].stop()
            mesh.transport.forget(leader)
            del mesh.nodes[leader]
            new_leader = await mesh.wait_leader()
            assert new_leader != leader
            await _propose_ok(mesh.nodes[new_leader], _payload("after", 2))
            for node in mesh.nodes.values():
                assert (await node.status()).commit_index >= 3
        finally:
            for node in mesh.nodes.values():
                try:
                    await node.stop()
                except Exception:
                    pass


@pytest.mark.asyncio
async def test_propose_on_follower_redirects() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            follower = next(n for n in mesh.nodes if n != leader)
            outcome = await mesh.nodes[follower].propose_command(_payload("x", 1), 5.0)
            assert isinstance(outcome, ProposeRedirect)
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_single_node_commits_own_writes() -> None:
    """Regression: commit advanced only on responses, so lone nodes hung."""
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp), n=1)
        await mesh.start_all()
        try:
            assert await mesh.wait_leader() == "n1"
            result = await _propose_ok(mesh.nodes["n1"], _payload("solo", 1))
            assert result is not None
            assert (await mesh.nodes["n1"].status()).commit_index >= 2
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_blackhole_propose_and_read_time_out() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            mesh.transport.drop_all = True
            # Sub-round timeout: the first heartbeat round cannot confirm
            # (acks=1 < 2) and the deadline beats the quorum stepdown.
            with pytest.raises(TimeoutError):
                await mesh.nodes[leader].read_index(0.005)
            with pytest.raises(TimeoutError):
                await mesh.nodes[leader].propose_command(_payload("x", 1), 0.4)
            with pytest.raises(TimeoutError):
                await mesh.nodes[leader].wait_applied(999, 0.2)
        finally:
            mesh.transport.drop_all = False
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_poison_entry_fail_stops_and_surfaces_on_stop() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            with pytest.raises(ValueError, match="truncated"):
                await mesh.nodes[leader].propose_command(b"garbage-bytes", 5.0)
            with pytest.raises(ValueError, match="truncated"):
                await mesh.nodes[leader].stop()
        finally:
            mesh.transport.drop_all = True


@pytest.mark.asyncio
async def test_stop_rejects_and_fails_pending() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            mesh.transport.drop_all = True  # waiter can never complete below
            pending = asyncio.get_running_loop().create_task(
                mesh.nodes[leader].propose_command(_payload("x", 1), 30.0)
            )
            await asyncio.sleep(0.2)  # let the batcher flush and register the waiter
            await mesh.nodes[leader].stop()
            with pytest.raises(ConnectionError, match="stopping"):
                await pending
            with pytest.raises(RuntimeError):
                await mesh.nodes[leader].propose_command(_payload("y", 2), 1.0)
            with pytest.raises(RuntimeError):
                await mesh.nodes[leader].handle_raft_message(
                    RequestVote(999, "ghost", 0, 0),
                    sender="ghost",
                )
        finally:
            mesh.transport.drop_all = True


@pytest.mark.asyncio
async def test_read_index_redirect_after_stepdown() -> None:
    """Feed a higher-term response mid-round: the barrier must redirect."""

    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            term = (await node.status()).term
            node._transport = _HigherTerm(node._transport)
            from minikv.raft.node import ReadRedirect as RR

            outcome = await node.read_index(5.0)
            assert isinstance(outcome, RR)
            assert (await node.status()).term > term  # stepped down for real
        finally:
            await mesh.stop_all()


class _HigherTerm:
    """Transport wrapper answering every heartbeat with a higher term."""

    def __init__(self, inner: RaftTransport) -> None:
        self._inner = inner

    async def send(self, to: str, message: Message) -> Message | None:
        from minikv.raft.types import AppendEntriesResponse

        response = await self._inner.send(to, message)
        if response is None:
            return None
        return AppendEntriesResponse(
            term=response.term + 5,
            success=False,
            match_index=0,
            conflict_index=0,
            conflict_term=0,
        )

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_stopping_and_prestart_guards() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        node = mesh.nodes["n1"]
        assert node.node_id == "n1"
        with pytest.raises(RuntimeError, match="not running"):
            await node.status()
        with pytest.raises(RuntimeError, match="not running"):
            node._require_core()
        # Pre-start internals exit quietly.
        await node._flush_batch()
        node._stopping = True
        await node._tick_loop()
        await node._batch_loop()
        node._pending_event.set()
        await node._batch_loop()
        with pytest.raises(ValueError, match="missing from peer list"):
            RaftNode("ghost", {"n1": "x"}, node._config, node._store, node._applier)


@pytest.mark.asyncio
async def test_stop_fails_queued_pending() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            loop = asyncio.get_running_loop()
            from minikv.raft.node import _Pending

            entry = _Pending(b"x", loop.create_future())
            async with node._core_lock:
                node._pending.append(entry)
            await node.stop()
            with pytest.raises(ConnectionError, match="stopping"):
                await entry.future
        finally:
            for other in mesh.nodes.values():
                if other is not node:
                    try:
                        await other.stop()
                    except Exception:
                        pass


@pytest.mark.asyncio
async def test_empty_batch_flush_and_progress_noop() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            await mesh.nodes[leader]._flush_batch()  # nothing pending: no-op
            await mesh.nodes[leader]._save_progress_locked(0, 0)
            await mesh.nodes[leader]._save_progress_locked(0, 0)  # second is a no-op
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_propose_after_halt_fails_fast() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            await node._execute([__poison_apply()]) if False else None  # placeholder removed below
            # Poison the applier through a bad payload, then propose again.
            with pytest.raises(ValueError, match="truncated"):
                await node.propose_command(b"garbage-bytes", 5.0)
            with pytest.raises(RuntimeError, match="halted"):
                await node.propose_command(_payload("x", 9), 1.0)
        finally:
            for other in mesh.nodes.values():
                try:
                    await other.stop()
                except Exception:
                    pass


def __poison_apply():  # type: ignore[no-untyped-def]
    raise AssertionError("unused")


@pytest.mark.asyncio
async def test_extra_send_and_missing_response() -> None:
    """Become-leader-via-fed-response emits sends beyond the requester
    (extra path); a pure-response message yields no response (error)."""
    from minikv.raft.types import RequestVoteResponse

    config = RaftConfig(
        election_timeout_min_ms=150,
        election_timeout_max_ms=300,
        heartbeat_ms=50,
        pre_vote=False,  # blackholed nodes must still campaign, not pre-vote
    )
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp), config=config)
        await mesh.start_all()
        try:
            await mesh.wait_leader()
            mesh.transport.drop_all = True  # freeze the world: timers fire
            # Every node campaigns for real now (no pre-vote gate); wait for one.
            candidate = None
            for _ in range(200):
                await asyncio.sleep(0.05)
                for node_id, node in mesh.nodes.items():
                    status = await node.status()
                    if status.role == "candidate":
                        candidate = (node_id, status.term)
                        break
                if candidate:
                    break
            assert candidate is not None
            node_id, term = candidate
            node = mesh.nodes[node_id]
            grant = RequestVoteResponse(term=term, vote_granted=True, pre_vote=False)
            # Self-vote + this grant = majority: elects, sending to all peers.
            # The granter's send is the "response"; the others go extra.
            others = [n for n in mesh.nodes if n != node_id]
            response = await node.handle_raft_message(grant, sender=others[0])
            assert (await node.status()).role == "leader"
            # The fed grant elected it: a NOOP of the new term is appended,
            # and replication sends went out (one per peer, blackholed).
            from minikv.raft.types import AppendEntries, EntryKind

            assert node._core.log[-1].kind is EntryKind.NOOP  # type: ignore[union-attr]
            assert isinstance(response, AppendEntries)
        finally:
            mesh.transport.drop_all = False
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_response_only_message_has_no_response() -> None:
    from minikv.raft.types import AppendEntriesResponse

    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            await asyncio.sleep(0.3)  # let replication settle: fully caught up
            with pytest.raises(RuntimeError, match="no response"):
                await node.handle_raft_message(
                    AppendEntriesResponse(
                        term=1, success=True, match_index=1, conflict_index=0, conflict_term=0
                    ),
                    sender="ghost",
                )
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_white_box_core_absent_paths() -> None:
    from minikv.raft.node import _ApplyItem
    from minikv.raft.types import AppendEntries

    async def canned(to: str, message: object) -> object:
        from minikv.raft.types import AppendEntriesResponse

        return AppendEntriesResponse(1, True, 1, 0, 0)

    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            from minikv.raft.types import Send

            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            ae = AppendEntries(1, "x", 0, 0, (), 0)
            # First the same paths with a live core.
            node._transport.send = canned  # type: ignore[method-assign]
            await node._send_and_feed(Send("n2", ae))
            # Then with the core gone mid-flight.
            node._core = None  # white-box
            await node._execute([Send("n2", ae)])
            await node._send_and_feed(Send("n2", ae))
            node._apply_queue.put_nowait(_ApplyItem(1, b""))
            await node._tick_loop()
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_deferred_actions_rerun_next_tick() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            import minikv.raft.node as node_mod
            from minikv.raft.types import EntryKind, LogEntry, Send

            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            other = next(n for n in mesh.nodes if n != leader)
            follower = mesh.nodes[other]
            # Diverge the follower at idx1 so the next probe is rejected;
            # the rejection's backup resend is a followup action.
            first = follower._core.log[0]  # type: ignore[union-attr]
            follower._core._log = [  # type: ignore[union-attr]
                LogEntry(first.term + 9, 1, EntryKind.COMMAND, b"rogue")
            ]
            node._core._next[other] = 2  # type: ignore[union-attr]
            send = node._core._ae_to(other)  # type: ignore[union-attr]
            assert isinstance(send, Send)
            old_cap = node_mod._EXECUTE_ROUND_CAP
            node_mod._EXECUTE_ROUND_CAP = 0  # defer after the first round
            try:
                await node._execute([send])
                assert node._deferred != []
            finally:
                node_mod._EXECUTE_ROUND_CAP = old_cap
            await node._execute([])  # deferred prepend runs now, converges
            assert node._deferred == []
            assert (await node.status()).role == "leader"  # nothing broke
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_cancelled_and_failing_sends() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = Mesh(Path(tmp))
        await mesh.start_all()
        try:
            from minikv.raft.types import AppendEntries

            leader = await mesh.wait_leader()
            node = mesh.nodes[leader]
            release = asyncio.Event()

            async def blocking(to: str, message: object) -> object:
                await release.wait()
                raise AssertionError("should have been cancelled")

            node._transport.send = blocking  # type: ignore[method-assign]
            slow = asyncio.get_running_loop().create_task(
                node._safe_send("n2", AppendEntries(1, "x", 0, 0, (), 0))
            )
            await asyncio.sleep(0.05)
            slow.cancel()
            with pytest.raises(asyncio.CancelledError):
                await slow
            release.set()

            async def boom(to: str, message: object) -> object:
                raise RuntimeError("boom")

            node._transport.send = boom  # type: ignore[method-assign]
            assert await node._safe_send("n2", AppendEntries(1, "x", 0, 0, (), 0)) is None
        finally:
            await mesh.stop_all()
