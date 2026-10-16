"""Linearizability over the real ReadIndex path: concurrent writers and
linearizable readers hammer a live 3-node cluster while every operation's
real-time interval is recorded; the history must linearize per key."""

import asyncio
import tempfile
import time
from pathlib import Path

import pytest

from minikv import minikv_pb2 as pb2
from minikv.client.client import MiniKVClient
from sim.history import History, OpRecord
from sim.linearizability import check_history
from tests.integration.test_cluster import LiveNode, _free_ports, _wait_leader

KEYS = [f"k{i}".encode() for i in range(30)]


async def _writer(
    client: MiniKVClient,
    history: History,
    lock: asyncio.Lock,
    stop: asyncio.Event,
    tag: str,
    rounds: int,
) -> None:
    seq = 0
    for r in range(rounds):
        if stop.is_set():
            return
        key = KEYS[(r + len(tag)) % len(KEYS)]
        seq += 1
        value = f"{tag}-v{r}".encode()
        invoke = time.monotonic() * 1000
        try:
            if r % 7 == 6:
                current = await client.get(key, mode=pb2.READ_LINEARIZABLE)
                ok = await client.cas(key, current, value)
                result: object = (ok.success, ok.current)
                op, args = "cas", (current, value)
            elif r % 7 == 5:
                existed = await client.delete(key)
                result, op, args = (existed,), "delete", ()
            else:
                await client.put(key, value)
                result, op, args = None, "put", (value,)
        except (TimeoutError, RuntimeError):
            result, op = None, ("cas", "delete", "put")[r % 3]
            args = {"cas": (None, value), "delete": (), "put": (value,)}[op]
            async with lock:
                history.append(OpRecord(tag, key, op, args, invoke, None, result))
            continue
        ret = time.monotonic() * 1000
        async with lock:
            history.append(OpRecord(tag, key, op, args, invoke, ret, result))


async def _reader(
    client: MiniKVClient,
    history: History,
    lock: asyncio.Lock,
    stop: asyncio.Event,
    tag: str,
    rounds: int,
) -> None:
    for r in range(rounds):
        if stop.is_set():
            return
        key = KEYS[(r * 3 + len(tag)) % len(KEYS)]
        invoke = time.monotonic() * 1000
        try:
            value = await client.get(key, mode=pb2.READ_LINEARIZABLE)
        except (TimeoutError, RuntimeError):
            continue  # read timeouts observe nothing; skip (not timeouts of writes)
        ret = time.monotonic() * 1000
        result = (False, b"") if value is None else (True, value)
        async with lock:
            history.append(OpRecord(tag, key, "get", (), invoke, ret, result))


@pytest.mark.asyncio
async def test_live_reads_and_writes_linearize() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ports = _free_ports(3)
        addrs = [f"127.0.0.1:{p}" for p in ports]
        peers = {f"n{i + 1}": addr for i, addr in enumerate(addrs)}
        nodes: dict[str, LiveNode] = {n: LiveNode(n, peers, Path(tmp)) for n in peers}
        for live in nodes.values():
            await live.start()
        try:
            await _wait_leader(nodes)
            history = History()
            lock = asyncio.Lock()
            stop = asyncio.Event()
            clients = [MiniKVClient(addrs, timeout_s=3.0) for _ in range(6)]
            try:
                tasks = [
                    asyncio.create_task(_writer(clients[i], history, lock, stop, f"w{i}", 25))
                    for i in range(4)
                ] + [
                    asyncio.create_task(_reader(clients[4 + i], history, lock, stop, f"r{i}", 40))
                    for i in range(2)
                ]
                await asyncio.gather(*tasks)
            finally:
                for client in clients:
                    await client.close()
            counts: dict[bytes, int] = {}
            for record in history.records:
                counts[record.key] = counts.get(record.key, 0) + 1
            worst = max(counts.values())
            print(f"\nchecked {len(history.records)} ops over {len(counts)} keys (max {worst}/key)")
            assert worst <= 120, f"history too dense per key for the checker ({worst})"
            result = check_history(history)
            assert result.ok, f"key={result.key} {result.detail}"
        finally:
            for live in nodes.values():
                await live.stop()
