"""Async client library with leader discovery and safe retries.

One random ``client_id`` per instance; mutating calls take the next
``request_seq`` and REUSE it across retries of the same logical write, so
server-side dedup makes retries exactly-once. Reads never retry blindly:
only ``NOT_LEADER`` redirects (same request, new target). Scans snapshot
server-side at stream start, so a restarted stream may duplicate items
across the restart (documented; no items are ever lost or invented).
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import grpc

from minikv import minikv_pb2 as pb2
from minikv import minikv_pb2_grpc as pb2_grpc

ResponseT = TypeVar(
    "ResponseT", pb2.PutResponse, pb2.GetResponse, pb2.DeleteResponse, pb2.CasResponse
)


@dataclass(frozen=True, slots=True)
class CasResult:
    success: bool
    current: bytes | None


@dataclass(frozen=True, slots=True)
class NodeStatus:
    node_id: str
    role: str
    term: int
    leader_id: str
    leader_addr: str
    commit_index: int
    last_applied: int
    log_last_index: int


class NotLeaderError(Exception):
    def __init__(self, leader_addr: str | None) -> None:
        super().__init__(f"not leader (hint: {leader_addr})")
        self.leader_addr = leader_addr


class MiniKVClient:
    def __init__(self, addresses: list[str], timeout_s: float = 3.0) -> None:
        if not addresses:
            raise ValueError("at least one address is required")
        self._addresses = list(addresses)
        self._timeout_s = timeout_s
        self._client_id = uuid.uuid4().hex
        self._seq = 0
        self._leader: str | None = None
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, pb2_grpc.KVStub] = {}
        self._closed = False

    async def put(self, key: bytes, value: bytes) -> None:
        self._seq += 1
        ctx = pb2.RequestContext(client_id=self._client_id, request_seq=self._seq)

        async def call(addr: str) -> pb2.PutResponse:
            return await self._stub(addr).Put(
                pb2.PutRequest(ctx=ctx, key=bytes(key), value=bytes(value)),
                timeout=self._timeout_s,
            )

        response = await self._with_leader(call)
        _check(response.status)

    async def get(self, key: bytes, mode: int = pb2.READ_LINEARIZABLE) -> bytes | None:
        async def call(addr: str) -> pb2.GetResponse:
            return await self._stub(addr).Get(
                pb2.GetRequest(key=bytes(key), mode=mode), timeout=self._timeout_s
            )

        response = await self._with_leader(call)
        _check(response.status)
        return bytes(response.value) if response.found else None

    async def delete(self, key: bytes) -> bool:
        self._seq += 1
        ctx = pb2.RequestContext(client_id=self._client_id, request_seq=self._seq)

        async def call(addr: str) -> pb2.DeleteResponse:
            return await self._stub(addr).Delete(
                pb2.DeleteRequest(ctx=ctx, key=bytes(key)), timeout=self._timeout_s
            )

        response = await self._with_leader(call)
        _check(response.status)
        return response.existed

    async def cas(self, key: bytes, expected: bytes | None, new: bytes) -> CasResult:
        self._seq += 1
        ctx = pb2.RequestContext(client_id=self._client_id, request_seq=self._seq)
        if expected is None:
            request = pb2.CasRequest(
                ctx=ctx, key=bytes(key), expect_absent=True, new_value=bytes(new)
            )
        else:
            request = pb2.CasRequest(
                ctx=ctx, key=bytes(key), expected=bytes(expected), new_value=bytes(new)
            )

        async def call(addr: str) -> pb2.CasResponse:
            return await self._stub(addr).Cas(request, timeout=self._timeout_s)

        response = await self._with_leader(call)
        _check(response.status)
        return CasResult(
            success=response.success,
            current=bytes(response.current) if response.found else None,
        )

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        limit: int = 0,
        mode: int = pb2.READ_LINEARIZABLE,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        request = pb2.ScanRequest(
            start=bytes(start or b""), end=bytes(end or b""), limit=limit, mode=mode
        )
        for key, value in await self._scan_all(request):
            yield key, value

    async def statuses(self) -> list[NodeStatus]:
        """Status of every configured node (best effort, skips unreachable)."""
        out: list[NodeStatus] = []
        for addr in self._addresses:
            try:
                response = await self._stub(addr).Status(
                    pb2.StatusRequest(), timeout=self._timeout_s
                )
            except grpc.aio.AioRpcError:
                continue
            out.append(
                NodeStatus(
                    node_id=response.node_id,
                    role=response.role,
                    term=response.term,
                    leader_id=response.leader_id,
                    leader_addr=response.leader_addr,
                    commit_index=response.commit_index,
                    last_applied=response.last_applied,
                    log_last_index=response.log_last_index,
                )
            )
        return out

    async def close(self) -> None:
        self._closed = True
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()

    async def _scan_all(self, request: pb2.ScanRequest) -> list[tuple[bytes, bytes]]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_s
        tried: set[str] = set()
        target = self._leader if self._leader in self._addresses else None
        delay = 0.05
        while True:
            addr = target or self._pick(tried)
            try:
                items = [
                    (bytes(item.key), bytes(item.value))
                    async for item in self._stub(addr).Scan(request, timeout=self._timeout_s)
                ]
            except grpc.aio.AioRpcError as e:
                hint = _redirect_hint(e)
                target = self._retarget(addr, tried, hint)
            else:
                self._leader = addr
                return items
            if loop.time() >= deadline:
                raise TimeoutError("scan found no leader in time")
            await asyncio.sleep(delay + random.random() * 0.05)
            delay = min(delay * 2, 1.0)

    async def _with_leader(self, call: Callable[[str], Awaitable[ResponseT]]) -> ResponseT:
        """Run ``call(addr)`` against the leader, following redirects."""
        if self._closed:
            raise RuntimeError("client is closed")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_s
        tried: set[str] = set()
        target = self._leader if self._leader in self._addresses else None
        delay = 0.05
        while True:
            addr = target or self._pick(tried)
            try:
                result = await call(addr)
            except grpc.aio.AioRpcError as e:
                target = self._retarget(addr, tried, _redirect_hint(e))
            else:
                status = result.status
                if status.code == pb2.NOT_LEADER:
                    target = self._retarget(addr, tried, status.leader_addr or None)
                else:
                    self._leader = addr
                    return result
            if loop.time() >= deadline:
                raise TimeoutError("no leader available")
            await asyncio.sleep(delay + random.random() * 0.05)
            delay = min(delay * 2, 1.0)

    def _pick(self, tried: set[str]) -> str:
        remaining = [a for a in self._addresses if a not in tried]
        if not remaining:
            tried.clear()
            remaining = list(self._addresses)
        return remaining[0]

    def _retarget(self, addr: str, tried: set[str], hint: str | None) -> str | None:
        tried.add(addr)
        if hint and hint in self._addresses:
            self._leader = hint
            return hint
        return None

    def _stub(self, addr: str) -> pb2_grpc.KVStub:
        stub = self._stubs.get(addr)
        if stub is None:
            channel = grpc.aio.insecure_channel(addr)
            self._channels[addr] = channel
            stub = pb2_grpc.KVStub(channel)
            self._stubs[addr] = stub
        return stub


def _redirect_hint(error: grpc.aio.AioRpcError) -> str | None:
    if error.code() == grpc.StatusCode.FAILED_PRECONDITION:
        details = error.details() or ""
        if details.startswith("NOT_LEADER"):
            return details.removeprefix("NOT_LEADER ").strip() or None
    return None


def _check(status: pb2.Status) -> None:
    # NOT_LEADER never reaches here (_with_leader redirects it internally).
    if status.code == pb2.OK:
        return
    if status.code == pb2.TIMEOUT:
        raise TimeoutError(status.message or "request timed out")
    raise RuntimeError(f"request failed: {status.message} (code {status.code})")
