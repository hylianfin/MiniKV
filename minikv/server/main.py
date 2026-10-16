"""Server entrypoint: assemble LogStore + KVApplier + RaftNode + gRPC."""

from __future__ import annotations

import asyncio
import signal
from pathlib import Path

import grpc

from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.kv.state_machine import KVApplier
from minikv.raft.logstore import LogStore
from minikv.raft.node import RaftNode
from minikv.server.config import load_config
from minikv.server.service import KVServicer, RaftServicer


async def serve(
    node_id: str, config_path: Path | str, data_dir: Path | str, batching: bool | None = None
) -> None:
    """Start one node; run until SIGINT/SIGTERM, then shut down cleanly."""
    config = load_config(config_path)
    peers = config.peers()
    if node_id not in peers:
        raise ValueError(f"node {node_id} not in cluster config")
    data = Path(data_dir)
    store = LogStore(data / "raft")
    applier = KVApplier(data / "kv", config.storage)
    node = RaftNode(
        node_id,
        peers,
        config.raft,
        store,
        applier,
        batching=config.batching if batching is None else batching,
    )
    await node.start()
    server = grpc.aio.server()
    pb2_grpc.add_KVServicer_to_server(KVServicer(node), server)
    pb2_grpc.add_RaftServicer_to_server(RaftServicer(node), server)
    own_addr = peers[node_id]
    host, _, port = own_addr.rpartition(":")
    bound = server.add_insecure_port(f"[::]:{port}" if host in ("", "0.0.0.0") else own_addr)
    if bound == 0:
        raise RuntimeError(f"could not bind {own_addr}")
    await server.start()
    print(f"minikv {node_id} listening on {own_addr}", flush=True)

    stop = asyncio.Event()

    def _halt(*_: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _halt)
        except NotImplementedError:
            pass
    await stop.wait()
    await server.stop(grace=2)
    await node.stop()
