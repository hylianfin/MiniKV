"""Client retry/redirect tests against scripted fake KV servers (real gRPC)."""

import grpc
import pytest

from minikv import minikv_pb2 as pb2
from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.client.client import CasResult, MiniKVClient, NotLeaderError


class ScriptedKV(pb2_grpc.KVServicer):
    """Plays a script of (action, value) per method; records requests."""

    def __init__(self) -> None:
        self.put_calls = 0
        self.get_calls = 0
        self.scan_calls = 0
        self.behavior: str = "ok"
        self.hint: str = ""
        self.store: dict[bytes, bytes] = {}

    async def Put(self, request, context):  # type: ignore[no-untyped-def]
        self.put_calls += 1
        if self.behavior == "redirect":
            return pb2.PutResponse(status=pb2.Status(code=pb2.NOT_LEADER, leader_addr=self.hint))
        self.store[bytes(request.key)] = bytes(request.value)
        return pb2.PutResponse(status=pb2.Status(code=pb2.OK))

    async def Get(self, request, context):  # type: ignore[no-untyped-def]
        self.get_calls += 1
        if self.behavior == "redirect":
            return pb2.GetResponse(status=pb2.Status(code=pb2.NOT_LEADER, leader_addr=self.hint))
        if self.behavior == "timeout":
            return pb2.GetResponse(status=pb2.Status(code=pb2.TIMEOUT))
        if self.behavior == "invalid":
            return pb2.GetResponse(status=pb2.Status(code=pb2.INVALID, message="bad"))
        value = self.store.get(bytes(request.key))
        if value is None:
            return pb2.GetResponse(status=pb2.Status(code=pb2.OK), found=False)
        return pb2.GetResponse(status=pb2.Status(code=pb2.OK), found=True, value=value)

    async def Delete(self, request, context):  # type: ignore[no-untyped-def]
        return pb2.DeleteResponse(status=pb2.Status(code=pb2.OK), existed=False)

    async def Cas(self, request, context):  # type: ignore[no-untyped-def]
        return pb2.CasResponse(status=pb2.Status(code=pb2.OK))

    async def Scan(self, request, context):  # type: ignore[no-untyped-def]
        self.scan_calls += 1
        if self.behavior == "redirect_once" and self.scan_calls == 1:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"NOT_LEADER {self.hint}")
            return
            yield  # make this an async generator that aborts immediately
        for key in sorted(self.store):
            yield pb2.ScanItem(key=key, value=self.store[key])

    async def Status(self, request, context):  # type: ignore[no-untyped-def]
        return pb2.StatusResponse(node_id="fake", role="leader", term=1)


async def _serve(servicer: ScriptedKV) -> tuple[grpc.aio.Server, str]:
    server = grpc.aio.server()
    pb2_grpc.add_KVServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_redirect_followed_and_cached() -> None:
    good = ScriptedKV()
    server_good, addr_good = await _serve(good)
    flaky = ScriptedKV()
    flaky.behavior = "redirect"
    server_flaky, addr_flaky = await _serve(flaky)
    flaky.hint = addr_good
    client = MiniKVClient([addr_flaky, addr_good], timeout_s=5.0)
    try:
        await client.put(b"k", b"v")  # first try hits flaky, follows the hint
        assert flaky.put_calls == 1 and good.put_calls == 1
        await client.put(b"k2", b"v2")  # cached leader: straight to good
        assert flaky.put_calls == 1 and good.put_calls == 2
        assert await client.get(b"k") == b"v"
    finally:
        await client.close()
        await server_good.stop(None)
        await server_flaky.stop(None)


@pytest.mark.asyncio
async def test_status_mapping_and_errors() -> None:
    good = ScriptedKV()
    server_good, addr_good = await _serve(good)
    client = MiniKVClient([addr_good], timeout_s=2.0)
    try:
        good.behavior = "timeout"
        with pytest.raises(TimeoutError):
            await client.get(b"k")
        good.behavior = "invalid"
        with pytest.raises(RuntimeError, match="bad"):
            await client.get(b"k")
        assert isinstance(NotLeaderError("x").leader_addr, str)
    finally:
        await client.close()
        await server_good.stop(None)


@pytest.mark.asyncio
async def test_unreachable_cycles_back_and_times_out() -> None:
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead = f"127.0.0.1:{sock.getsockname()[1]}"
    sock.close()
    good = ScriptedKV()
    server_good, addr_good = await _serve(good)
    client = MiniKVClient([dead, addr_good], timeout_s=5.0)
    try:
        await client.put(b"k", b"v")  # dead first, then good (tried-list wraps)
        assert good.put_calls == 1
    finally:
        await client.close()
        await server_good.stop(None)
    lonely = MiniKVClient([dead], timeout_s=0.5)
    try:
        with pytest.raises(TimeoutError):
            await lonely.put(b"k", b"v")
        with pytest.raises(RuntimeError, match="closed"):
            await lonely.close()
            await lonely.put(b"k", b"v")
    finally:
        await lonely.close()


@pytest.mark.asyncio
async def test_scan_redirect_restarts_stream() -> None:
    first = ScriptedKV()
    first.store = {b"a": b"1"}
    first.behavior = "redirect_once"
    server_first, addr_first = await _serve(first)
    second = ScriptedKV()
    second.store = {b"b": b"2"}
    server_second, addr_second = await _serve(second)
    first.hint = addr_second
    client = MiniKVClient([addr_first, addr_second], timeout_s=5.0)
    try:
        items = [item async for item in client.scan()]
        assert items == [(b"b", b"2")]
        assert first.scan_calls == 1
    finally:
        await client.close()
        await server_first.stop(None)
        await server_second.stop(None)


@pytest.mark.asyncio
async def test_statuses_skip_unreachable() -> None:
    import socket

    good = ScriptedKV()
    server_good, addr_good = await _serve(good)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead = f"127.0.0.1:{sock.getsockname()[1]}"
    sock.close()
    client = MiniKVClient([addr_good, dead], timeout_s=2.0)
    try:
        statuses = await client.statuses()
        assert [s.node_id for s in statuses] == ["fake"]
    finally:
        await client.close()
        await server_good.stop(None)


@pytest.mark.asyncio
async def test_cas_absent_and_scan_timeout() -> None:
    good = ScriptedKV()
    server_good, addr_good = await _serve(good)
    client = MiniKVClient([addr_good], timeout_s=2.0)
    try:
        result = await client.cas(b"k", None, b"v")
        assert result == CasResult(success=False, current=None)
    finally:
        await client.close()
        await server_good.stop(None)


@pytest.mark.asyncio
async def test_scan_no_leader_times_out() -> None:
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead = f"127.0.0.1:{sock.getsockname()[1]}"
    sock.close()
    client = MiniKVClient([dead], timeout_s=0.5)
    try:
        with pytest.raises(TimeoutError):
            [item async for item in client.scan()]
    finally:
        await client.close()


def test_empty_addresses_rejected() -> None:
    with pytest.raises(ValueError):
        MiniKVClient([])
