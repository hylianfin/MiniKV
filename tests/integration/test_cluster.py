"""End-to-end over real gRPC: 3 nodes, client ops, leader failover.

Uses tight election timeouts on localhost so the suite stays fast while
still exercising real elections, real RPCs, and real disk files.
"""

import asyncio
import socket
import tempfile
from pathlib import Path

import grpc
import pytest

from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.client.client import MiniKVClient
from minikv.kv.state_machine import KVApplier
from minikv.raft.logstore import LogStore
from minikv.raft.node import RaftNode
from minikv.raft.types import RaftConfig
from minikv.server.service import KVServicer, RaftServicer


def _free_ports(n: int) -> list[int]:
    sockets = [socket.socket() for _ in range(n)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class LiveNode:
    def __init__(self, node_id: str, peers: dict[str, str], base: Path) -> None:
        config = RaftConfig(
            election_timeout_min_ms=150, election_timeout_max_ms=300, heartbeat_ms=50
        )
        self.bind = peers[node_id]
        self.node = RaftNode(
            node_id,
            peers,
            config,
            LogStore(base / node_id / "raft"),
            KVApplier(base / node_id / "kv"),
        )
        self.server = grpc.aio.server()

    async def start(self) -> None:
        await self.node.start()
        pb2_grpc.add_KVServicer_to_server(KVServicer(self.node), self.server)
        pb2_grpc.add_RaftServicer_to_server(RaftServicer(self.node), self.server)
        self.server.add_insecure_port(self.bind)
        await self.server.start()

    async def stop(self) -> None:
        await self.server.stop(grace=1)
        await self.node.stop()


async def _wait_leader(nodes: dict[str, LiveNode], timeout_s: float = 20.0) -> str:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        leaders = [
            node_id
            for node_id, live in nodes.items()
            if (await live.node.status()).role == "leader"
        ]
        if len(leaders) == 1:
            return leaders[0]
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("no leader elected")
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_grpc_cluster_put_get_failover() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ports = _free_ports(3)
        addrs = [f"127.0.0.1:{p}" for p in ports]
        peers = {f"n{i + 1}": addr for i, addr in enumerate(addrs)}
        nodes: dict[str, LiveNode] = {}
        for node_id in peers:
            nodes[node_id] = LiveNode(node_id, peers, Path(tmp))
        for live in nodes.values():
            await live.start()
        client = MiniKVClient(addrs, timeout_s=5.0)
        try:
            leader = await _wait_leader(nodes)
            await client.put(b"hello", b"world")
            assert await client.get(b"hello") == b"world"
            assert await client.get(b"missing") is None
            for i in range(20):
                await client.put(f"k{i}".encode(), f"v{i}".encode())
            # Kill the leader: writes must continue, nothing acknowledged lost.
            await nodes[leader].stop()
            del nodes[leader]
            new_leader = await _wait_leader(nodes)
            assert new_leader != leader
            assert await client.get(b"hello") == b"world"
            await client.put(b"after", b"failover")
            assert await client.get(b"after") == b"failover"
            assert await client.delete(b"hello") is True
            assert await client.get(b"hello") is None
            result = await client.cas(b"after", b"failover", b"swapped")
            assert result.success and result.current == b"swapped"
            seen = {k: v async for k, v in client.scan()}
            assert seen[b"after"] == b"swapped"
            assert b"hello" not in seen
            statuses = await client.statuses()
            assert {s.node_id for s in statuses} == set(nodes)
            assert sum(1 for s in statuses if s.role == "leader") == 1
        finally:
            await client.close()
            for live in nodes.values():
                await live.stop()
