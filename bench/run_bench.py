"""Benchmark runners: single-node LSM and 3-node clusters, wall-clock bounded.

Usage:
    python bench/run_bench.py --suite lsm-fsync      # comparison 1
    python bench/run_bench.py --suite cluster-batch   # comparison 2
    python bench/run_bench.py --suite reads           # comparison 3
    python bench/run_bench.py --suite bloom           # comparison 4
    python bench/run_bench.py --suite headline        # workloads x concurrency
    python bench/run_bench.py --suite all
Results land in bench/results/<suite>-<name>.json for report.py.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import socket
import tempfile
import time
from pathlib import Path

import grpc

from bench.workloads import Workload, key_for, pick_op, value_for
from minikv import minikv_pb2 as pb2
from minikv import minikv_pb2_grpc as pb2_grpc
from minikv.client.client import MiniKVClient
from minikv.kv.state_machine import KVApplier
from minikv.raft.logstore import LogStore
from minikv.raft.node import RaftNode
from minikv.raft.types import RaftConfig
from minikv.server.service import KVServicer, RaftServicer
from minikv.storage.lsm import LSMEngine, LSMOptions
from minikv.storage.wal import FsyncPolicy

RESULTS = Path(__file__).parent / "results"


def percentiles_ms(samples_ns: list[int]) -> tuple[float, float, float]:
    ordered = sorted(samples_ns)
    n = len(ordered)
    if n == 0:
        return 0.0, 0.0, 0.0

    def at(q: float) -> float:
        return ordered[min(n - 1, int(q * n))] / 1e6

    return at(0.50), at(0.95), at(0.99)


def machine_info() -> dict[str, object]:
    import multiprocessing
    import platform

    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpus": multiprocessing.cpu_count(),
    }


def bench_lsm(workload: Workload, options: LSMOptions, label: str) -> dict[str, object]:
    """Direct engine puts/gets, single-threaded, timed phase + preload."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = LSMEngine(Path(tmp), options)
        engine.open()
        rng = random.Random(1234)
        raft = 0
        # Preload for read phases (untimed here; counted in wall time below).
        if workload.kind in ("mixed", "read_only"):
            preload = min(workload.keyspace, 20000)
            for i in range(preload):
                raft += 1
                engine.put(f"k{i:06d}".encode(), value_for(rng, workload.value_size), raft)
            engine.flush()
        if workload.kind == "read_miss":
            # Even keys preloaded (flushed to SSTables); odd keys are absent
            # but inside every table's [min, max], forcing bloom/index reads.
            preload = 20000
            for i in range(0, preload * 2, 2):
                raft += 1
                engine.put(f"k{i:06d}".encode(), value_for(rng, workload.value_size), raft)
            engine.flush()
        lat: list[int] = []
        ops = 0
        end = time.perf_counter() + workload.duration_s
        while time.perf_counter() < end:
            op = pick_op(rng, workload.kind)
            if workload.kind == "read_miss":
                key = f"k{(rng.randrange(workload.keyspace) * 2 + 1):06d}".encode()
            else:
                key = key_for(rng, workload.keyspace)
            start = time.perf_counter_ns()
            if op == "put":
                raft += 1
                engine.put(key, value_for(rng, workload.value_size), raft)
            else:
                engine.get(key)
            lat.append(time.perf_counter_ns() - start)
            ops += 1
        seconds = workload.duration_s
        p50, p95, p99 = percentiles_ms(lat)
        result = {
            "suite": "lsm",
            "label": label,
            "workload": workload.name,
            "kind": workload.kind,
            "value_size": workload.value_size,
            "concurrency": 1,
            "fsync": options.fsync_policy.value,
            "bloom": options.bloom_enabled,
            "ops": ops,
            "seconds": round(seconds, 2),
            "ops_per_sec": round(ops / seconds, 1),
            "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3),
            "p99_ms": round(p99, 3),
            "machine": machine_info(),
        }
        engine.close()
        return result


class _LiveCluster:
    """Three in-process nodes with real gRPC between them."""

    def __init__(self, base: Path, options: LSMOptions, batching: bool) -> None:
        ports = _free_ports(3)
        addrs = [f"127.0.0.1:{p}" for p in ports]
        self.peers = {f"n{i + 1}": addr for i, addr in enumerate(addrs)}
        config = RaftConfig(
            election_timeout_min_ms=150, election_timeout_max_ms=300, heartbeat_ms=50
        )
        self.nodes: dict[str, RaftNode] = {}
        self.servers: list[grpc.aio.Server] = []
        for node_id in self.peers:
            node = RaftNode(
                node_id,
                self.peers,
                config,
                LogStore(base / node_id / "raft"),
                KVApplier(base / node_id / "kv", options),
                batching=batching,
            )
            self.nodes[node_id] = node
        self.addrs = addrs

    async def start(self) -> None:
        for node in self.nodes.values():
            await node.start()
        for node_id, node in self.nodes.items():
            server = grpc.aio.server()
            pb2_grpc.add_KVServicer_to_server(KVServicer(node), server)
            pb2_grpc.add_RaftServicer_to_server(RaftServicer(node), server)
            server.add_insecure_port(self.peers[node_id])
            await server.start()
            self.servers.append(server)

    async def stop(self) -> None:
        for server in self.servers:
            await server.stop(grace=1)
        for node in self.nodes.values():
            await node.stop()

    async def wait_leader(self, timeout_s: float = 20.0) -> str:
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
            await asyncio.sleep(0.1)


def _free_ports(n: int) -> list[int]:
    sockets = [socket.socket() for _ in range(n)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


async def _cluster_worker(
    client: MiniKVClient,
    workload: Workload,
    seed: int,
    lat: list[int],
    deadline: float,
) -> int:
    rng = random.Random(seed)
    ops = 0
    mode = pb2.READ_STALE if workload.read_mode == "stale" else pb2.READ_LINEARIZABLE
    while time.perf_counter() < deadline:
        op = pick_op(rng, workload.kind)
        if workload.kind == "read_miss":
            key = f"k{(rng.randrange(workload.keyspace) * 2 + 1):06d}".encode()
        else:
            key = key_for(rng, workload.keyspace)
        start = time.perf_counter_ns()
        if op == "put":
            await client.put(key, value_for(rng, workload.value_size))
        else:
            await client.get(key, mode=mode)
        lat.append(time.perf_counter_ns() - start)
        ops += 1
    return ops


async def bench_cluster_async(
    workload: Workload, options: LSMOptions, batching: bool, label: str
) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as tmp:
        cluster = _LiveCluster(Path(tmp), options, batching)
        await cluster.start()
        try:
            await cluster.wait_leader()
            if workload.kind in ("mixed", "read_only"):
                await _preload(cluster, workload, 20000)
            if workload.kind == "read_miss":
                await _preload_even(cluster, workload, 20000)
                for node in cluster.nodes.values():
                    node.applier.data.flush()
            clients = [
                MiniKVClient(cluster.addrs, timeout_s=10.0) for _ in range(workload.concurrency)
            ]
            try:
                lat: list[int] = []
                deadline = time.perf_counter() + workload.duration_s
                per_worker = await asyncio.gather(
                    *(
                        _cluster_worker(client, workload, 9000 + i, lat, deadline)
                        for i, client in enumerate(clients)
                    )
                )
            finally:
                for client in clients:
                    await client.close()
            ops = sum(per_worker)
            seconds = workload.duration_s
            p50, p95, p99 = percentiles_ms(lat)
            return {
                "suite": "cluster",
                "label": label,
                "workload": workload.name,
                "kind": workload.kind,
                "value_size": workload.value_size,
                "concurrency": workload.concurrency,
                "read_mode": workload.read_mode,
                "batching": batching,
                "bloom": options.bloom_enabled,
                "fsync": options.fsync_policy.value,
                "ops": ops,
                "seconds": round(seconds, 2),
                "ops_per_sec": round(ops / seconds, 1),
                "p50_ms": round(p50, 3),
                "p95_ms": round(p95, 3),
                "p99_ms": round(p99, 3),
                "machine": machine_info(),
            }
        finally:
            await cluster.stop()


async def _preload_even(cluster: _LiveCluster, workload: Workload, count: int) -> None:
    """Preload even keys so odd-key reads miss inside table key ranges."""
    clients = [MiniKVClient(cluster.addrs, timeout_s=15.0) for _ in range(8)]
    try:
        rng = random.Random(778)

        async def put_some(keys: list[int]) -> None:
            for i in keys:
                await clients[i % len(clients)].put(
                    f"k{i:06d}".encode(), value_for(rng, workload.value_size)
                )

        evens = list(range(0, count * 2, 2))
        step = max(1, len(evens) // 8)
        await asyncio.gather(*[put_some(evens[i : i + step]) for i in range(0, len(evens), step)])
    finally:
        for client in clients:
            await client.close()


async def _preload(cluster: _LiveCluster, workload: Workload, count: int) -> None:
    clients = [MiniKVClient(cluster.addrs, timeout_s=15.0) for _ in range(8)]
    try:
        rng = random.Random(777)

        async def put_some(start: int, stop: int) -> None:
            for i in range(start, stop):
                await clients[i % len(clients)].put(
                    f"k{i:06d}".encode(), value_for(rng, workload.value_size)
                )

        step = max(1, count // 8)
        await asyncio.gather(*[put_some(i, min(i + step, count)) for i in range(0, count, step)])
    finally:
        for client in clients:
            await client.close()


def bench_cluster(
    workload: Workload, options: LSMOptions, batching: bool, label: str
) -> dict[str, object]:
    return asyncio.run(bench_cluster_async(workload, options, batching, label))


def save(result: dict[str, object], name: str) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.json"
    path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(
        f"{result['label']}: {result['ops_per_sec']} ops/s "
        f"(p50 {result['p50_ms']}ms p95 {result['p95_ms']}ms p99 {result['p99_ms']}ms)"
    )
    return path


SUITES: dict[str, list[tuple[str, Workload]]] = {
    # Comparison 1: fsync policies, single LSM, write-only 128B.
    "lsm-fsync": [
        ("lsm-write-always", Workload("write-only-128B", "write_only", 128, 1, 8, 100000)),
        ("lsm-write-batch", Workload("write-only-128B", "write_only", 128, 1, 8, 100000)),
        ("lsm-write-never", Workload("write-only-128B", "write_only", 128, 1, 8, 100000)),
    ],
    # Comparison 2: group commit on/off, 3-node, write-only, conc 16.
    "cluster-batch": [
        (
            "cluster-write-batch-on",
            Workload("write-only-128B-c16", "write_only", 128, 16, 10, 100000),
        ),
        (
            "cluster-write-batch-off",
            Workload("write-only-128B-c16", "write_only", 128, 16, 10, 100000),
        ),
    ],
    # Comparison 3: linearizable vs stale reads, 3-node, conc 16.
    "reads": [
        (
            "cluster-read-linearizable",
            Workload("read-only-128B-c16", "read_only", 128, 16, 10, 100000, "linearizable"),
        ),
        (
            "cluster-read-stale",
            Workload("read-only-128B-c16", "read_only", 128, 16, 10, 100000, "stale"),
        ),
    ],
    # Comparison 4: bloom on/off for missing-key reads (flushed SSTables).
    "bloom": [
        ("lsm-miss-bloom-on", Workload("read-miss-128B", "read_miss", 128, 1, 8, 100000)),
        ("lsm-miss-bloom-off", Workload("read-miss-128B", "read_miss", 128, 1, 8, 100000)),
    ],
    # Headline matrix: workloads x concurrency on the default 3-node cluster.
    "headline": [
        (f"cluster-{kind}-{size}B-c{c}", Workload(f"{kind}-{size}B-c{c}", kind, size, c, 8, 100000))
        for kind in ("write_only", "mixed", "read_only")
        for size in (128,)
        for c in (1, 16, 64)
    ]
    + [
        (
            "cluster-write-4KiB-c16",
            Workload("write-only-4KiB-c16", "write_only", 4096, 16, 10, 100000),
        ),
    ],
}

LSM_OPTS = {
    "lsm-write-always": LSMOptions(fsync_policy=FsyncPolicy.ALWAYS),
    "lsm-write-batch": LSMOptions(fsync_policy=FsyncPolicy.BATCH),
    "lsm-write-never": LSMOptions(fsync_policy=FsyncPolicy.NEVER),
    "lsm-miss-bloom-on": LSMOptions(fsync_policy=FsyncPolicy.NEVER, bloom_enabled=True),
    "lsm-miss-bloom-off": LSMOptions(fsync_policy=FsyncPolicy.NEVER, bloom_enabled=False),
}


def run_suite(name: str) -> None:
    for label, workload in SUITES[name]:
        if label.startswith("lsm-"):
            result = bench_lsm(workload, LSM_OPTS[label], label)
        else:
            batching = label != "cluster-write-batch-off"
            options = LSMOptions()
            result = bench_cluster(workload, options, batching, label)
        save(result, label)


def main() -> None:
    parser = argparse.ArgumentParser(description="MiniKV benchmarks")
    parser.add_argument("--suite", default="all", choices=[*SUITES, "all"])
    args = parser.parse_args()
    names = list(SUITES) if args.suite == "all" else [args.suite]
    for name in names:
        print(f"== suite {name} ==")
        run_suite(name)


if __name__ == "__main__":
    main()
