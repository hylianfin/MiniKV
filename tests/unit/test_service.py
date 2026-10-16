"""Servicer-level tests: KV/Raft gRPC handlers over mesh nodes (no sockets)."""

import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from minikv import minikv_pb2 as pb2
from minikv.server.service import KVServicer, RaftServicer
from tests.unit.test_node import Mesh


def _ctx(seq: int) -> pb2.RequestContext:
    return pb2.RequestContext(client_id="test-client", request_seq=seq)


async def _mesh(tmp: Path) -> Mesh:
    mesh = Mesh(tmp)
    await mesh.start_all()
    leader = await mesh.wait_leader()
    mesh.leader_id = leader  # type: ignore[attr-defined]
    return mesh


@pytest.mark.asyncio
async def test_kv_put_get_delete_cas() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = await _mesh(Path(tmp))
        try:
            leader = mesh.leader_id
            svc = KVServicer(mesh.nodes[leader], timeout_s=5.0)
            put = await svc.Put(pb2.PutRequest(ctx=_ctx(1), key=b"k", value=b"v"), None)  # type: ignore[arg-type]
            assert put.status.code == pb2.OK
            get = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_LINEARIZABLE), None)  # type: ignore[arg-type]
            assert get.found and bytes(get.value) == b"v"
            stale = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_STALE), None)  # type: ignore[arg-type]
            assert stale.found
            logged = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_LOG), None)  # type: ignore[arg-type]
            assert logged.found
            missing = await svc.Get(pb2.GetRequest(key=b"nope", mode=pb2.READ_LINEARIZABLE), None)  # type: ignore[arg-type]
            assert not missing.found
            cas = await svc.Cas(
                pb2.CasRequest(ctx=_ctx(2), key=b"k", expected=b"v", new_value=b"v2"),
                None,  # type: ignore[arg-type]
            )
            assert cas.success and bytes(cas.current) == b"v2"
            bad_cas = await svc.Cas(
                pb2.CasRequest(ctx=_ctx(3), key=b"k", expected=b"v", new_value=b"v3"),
                None,  # type: ignore[arg-type]
            )
            assert not bad_cas.success and bytes(bad_cas.current) == b"v2"
            absent_cas = await svc.Cas(
                pb2.CasRequest(ctx=_ctx(4), key=b"fresh", expect_absent=True, new_value=b"n"),
                None,  # type: ignore[arg-type]
            )
            assert absent_cas.success
            deleted = await svc.Delete(pb2.DeleteRequest(ctx=_ctx(5), key=b"k"), None)  # type: ignore[arg-type]
            assert deleted.existed
            gone = await svc.Delete(pb2.DeleteRequest(ctx=_ctx(6), key=b"k"), None)  # type: ignore[arg-type]
            assert not gone.existed
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_kv_redirects_on_follower() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = await _mesh(Path(tmp))
        try:
            follower = next(n for n in mesh.nodes if n != mesh.leader_id)
            svc = KVServicer(mesh.nodes[follower], timeout_s=5.0)
            put = await svc.Put(pb2.PutRequest(ctx=_ctx(1), key=b"k", value=b"v"), None)  # type: ignore[arg-type]
            assert put.status.code == pb2.NOT_LEADER
            get = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_LINEARIZABLE), None)  # type: ignore[arg-type]
            assert get.status.code == pb2.NOT_LEADER
            delete = await svc.Delete(pb2.DeleteRequest(ctx=_ctx(2), key=b"k"), None)  # type: ignore[arg-type]
            assert delete.status.code == pb2.NOT_LEADER
            cas = await svc.Cas(pb2.CasRequest(ctx=_ctx(3), key=b"k", new_value=b"v"), None)  # type: ignore[arg-type]
            assert cas.status.code == pb2.NOT_LEADER
            logged = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_LOG), None)  # type: ignore[arg-type]
            assert logged.status.code == pb2.NOT_LEADER
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_kv_timeouts_map_to_status() -> None:
    # One fresh mesh, all ops back-to-back inside the election grace period
    # so the node is still leader for every flush (timeouts, not redirects).
    with tempfile.TemporaryDirectory() as tmp:
        mesh = await _mesh(Path(tmp))
        try:
            leader = mesh.leader_id
            mesh.transport.drop_all = True  # nothing can commit anymore
            svc = KVServicer(mesh.nodes[leader], timeout_s=0.05)
            put = await svc.Put(pb2.PutRequest(ctx=_ctx(1), key=b"k", value=b"v"), None)  # type: ignore[arg-type]
            assert put.status.code == pb2.TIMEOUT
            delete = await svc.Delete(pb2.DeleteRequest(ctx=_ctx(2), key=b"k"), None)  # type: ignore[arg-type]
            assert delete.status.code == pb2.TIMEOUT
            cas = await svc.Cas(pb2.CasRequest(ctx=_ctx(3), key=b"k", new_value=b"v"), None)  # type: ignore[arg-type]
            assert cas.status.code == pb2.TIMEOUT
            get = await svc.Get(pb2.GetRequest(key=b"k", mode=pb2.READ_LINEARIZABLE), None)  # type: ignore[arg-type]
            assert get.status.code == pb2.TIMEOUT

            class Ctx:
                def __init__(self) -> None:
                    self.codes: list[object] = []

                def set_code(self, code: object) -> None:
                    self.codes.append(code)

                def set_details(self, details: str) -> None:
                    pass

            ctx = Ctx()
            assert [
                i async for i in svc.Scan(pb2.ScanRequest(mode=pb2.READ_LINEARIZABLE), ctx)
            ] == []  # type: ignore[arg-type]
            import grpc

            assert ctx.codes == [grpc.StatusCode.DEADLINE_EXCEEDED]
        finally:
            mesh.transport.drop_all = False
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_scan_and_status() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = await _mesh(Path(tmp))
        try:
            leader = mesh.leader_id
            svc = KVServicer(mesh.nodes[leader], timeout_s=5.0)
            for i in range(5):
                response = await svc.Put(
                    pb2.PutRequest(ctx=_ctx(i + 1), key=f"k{i}".encode(), value=b"v"),
                    None,  # type: ignore[arg-type]
                )
                assert response.status.code == pb2.OK
            items = [
                item
                async for item in svc.Scan(
                    pb2.ScanRequest(start=b"k1", end=b"k4", mode=pb2.READ_LINEARIZABLE),
                    None,  # type: ignore[arg-type]
                )
            ]
            assert [(bytes(i.key), bytes(i.value)) for i in items] == [
                (b"k1", b"v"),
                (b"k2", b"v"),
                (b"k3", b"v"),
            ]
            limited = [
                item
                async for item in svc.Scan(pb2.ScanRequest(limit=2, mode=pb2.READ_STALE), None)  # type: ignore[arg-type]
            ]
            assert len(limited) == 2
            status = await svc.Status(pb2.StatusRequest(), None)  # type: ignore[arg-type]
            assert status.node_id == leader and status.role == "leader"
            # Scan on a follower aborts with a redirect, not items.
            follower = next(n for n in mesh.nodes if n != leader)
            fsvc = KVServicer(mesh.nodes[follower], timeout_s=5.0)
            ctx = SimpleNamespace(codes=[], details=[])

            class Ctx:
                def set_code(self, code):  # type: ignore[no-untyped-def]
                    ctx.codes.append(code)

                def set_details(self, details):  # type: ignore[no-untyped-def]
                    ctx.details.append(details)

            assert [
                i async for i in fsvc.Scan(pb2.ScanRequest(mode=pb2.READ_LINEARIZABLE), Ctx())
            ] == []  # type: ignore[arg-type]
            import grpc

            assert ctx.codes == [grpc.StatusCode.FAILED_PRECONDITION]
            assert ctx.details[0].startswith("NOT_LEADER")
            stale_items = [i async for i in fsvc.Scan(pb2.ScanRequest(mode=pb2.READ_STALE), Ctx())]
            assert len(stale_items) == 5
        finally:
            await mesh.stop_all()


@pytest.mark.asyncio
async def test_raft_servicers_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        mesh = await _mesh(Path(tmp))
        try:
            leader = mesh.leader_id
            follower = next(n for n in mesh.nodes if n != leader)
            rsvc = RaftServicer(mesh.nodes[follower])
            # A stale vote request is denied (followers already voted).
            denied = await rsvc.RequestVote(
                pb2.RequestVoteRequest(
                    term=1, candidate_id="ghost", last_log_index=0, last_log_term=0
                ),
                None,  # type: ignore[arg-type]
            )
            assert denied.vote_granted is False
            # A well-formed heartbeat from the leader is accepted.
            status = await mesh.nodes[leader].status()
            accepted = await rsvc.AppendEntries(
                pb2.AppendEntriesRequest(
                    term=status.term,
                    leader_id=leader,
                    prev_log_index=status.log_last_index,
                    prev_log_term=status.term,
                    leader_commit=status.commit_index,
                ),
                None,  # type: ignore[arg-type]
            )
            assert accepted.success is True
            # A higher-term campaign steps the leader down through its servicer.
            lsvc = RaftServicer(mesh.nodes[leader])
            stepped = await lsvc.RequestVote(
                pb2.RequestVoteRequest(
                    term=status.term + 5,
                    candidate_id="ghost",
                    last_log_index=10**6,
                    last_log_term=status.term + 5,
                ),
                None,  # type: ignore[arg-type]
            )
            assert stepped.vote_granted is True
            assert (await mesh.nodes[leader].status()).role == "follower"
        finally:
            await mesh.stop_all()
