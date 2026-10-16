"""Docker chaos test: real faults against a real 3-node compose cluster.

1. `docker compose up`, wait for a leader.
2. 8 concurrent clients, mixed put/get/delete/cas with unique values,
   recording a full operation history.
3. Every 5-10s: kill the leader, pause a follower, or restart a node.
4. Stop faults, wait for recovery, read back every key.
5. Assert: every acknowledged write is present-or-overwritten, and the
   recorded history linearizes per key.

Usage: ``python docker/chaos.py [--duration 60] [--nodes host:ports...]``.
Runs against an already-up cluster by default; pass ``--compose`` to let
it manage `docker compose up/down` itself.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from minikv import minikv_pb2 as pb2  # noqa: E402
from minikv.client.client import MiniKVClient  # noqa: E402
from sim.history import History, OpRecord  # noqa: E402
from sim.linearizability import check_history  # noqa: E402


def _keys(seed: int) -> list[bytes]:
    # Unique per run: histories always start from absent keys, even when
    # volumes (and old keys) survive between runs.
    return [f"chaos-{seed}-k{i}".encode() for i in range(60)]


async def _writer(
    client: MiniKVClient,
    history: History,
    lock: asyncio.Lock,
    stop: asyncio.Event,
    tag: str,
    seed: int,
    duration_s: float,
    keys: list[bytes],
) -> None:
    """Mixed workload; every outcome (or timeout) lands in the history."""
    rng = random.Random(seed)
    seq = 0
    end = time.monotonic() + duration_s
    while time.monotonic() < end and not stop.is_set():
        key = rng.choice(keys)
        choice = rng.random()
        seq += 1
        invoke = time.monotonic() * 1000
        try:
            if choice < 0.5:
                value = f"{tag}-{seq}".encode()
                await client.put(key, value)
                record = OpRecord(tag, key, "put", (value,), invoke, time.monotonic() * 1000, None)
            elif choice < 0.7:
                value = await client.get(key, mode=pb2.READ_LINEARIZABLE)
                result: object = (False, b"") if value is None else (True, value)
                record = OpRecord(tag, key, "get", (), invoke, time.monotonic() * 1000, result)
            elif choice < 0.85:
                existed = await client.delete(key)
                record = OpRecord(
                    tag, key, "delete", (), invoke, time.monotonic() * 1000, (existed,)
                )
            else:
                current = await client.get(key, mode=pb2.READ_LINEARIZABLE)
                value = f"{tag}-cas{seq}".encode()
                outcome = await client.cas(key, current, value)
                record = OpRecord(
                    tag,
                    key,
                    "cas",
                    (current, value),
                    invoke,
                    time.monotonic() * 1000,
                    (outcome.success, outcome.current),
                )
        except (TimeoutError, RuntimeError):
            op = (
                "put"
                if choice < 0.5
                else ("get" if choice < 0.7 else ("delete" if choice < 0.85 else "cas"))
            )
            args: tuple[object, ...] = ()
            record = OpRecord(tag, key, op, args, invoke, None, None)
        async with lock:
            history.append(record)
        await asyncio.sleep(rng.random() * 0.02)


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=Path(__file__).parent,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=60)


async def _wait_leader(client: MiniKVClient, timeout_s: float = 30.0) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for status in await client.statuses():
            if status.role == "leader":
                return status.node_id
        await asyncio.sleep(0.5)
    raise TimeoutError("no leader elected")


async def _fault_loop(
    stop: asyncio.Event, seed: int, containers: list[str], nodes: str, duration_s: float
) -> None:
    rng = random.Random(seed)
    end = time.monotonic() + duration_s
    probe = MiniKVClient(nodes.split(","), timeout_s=5.0)
    try:
        while time.monotonic() < end and not stop.is_set():
            await asyncio.sleep(5 + rng.random() * 5)
            if stop.is_set():
                break
            try:
                leader = await _wait_leader(probe, timeout_s=10.0)
            except TimeoutError:
                continue
            choice = rng.random()
            leader_container = f"docker-{leader}-1"
            followers = [c for c in containers if c != leader_container]
            if choice < 0.4:
                print(f"CHAOS kill leader {leader}", flush=True)
                _docker("kill", leader_container)
                await asyncio.sleep(2)
                _docker("start", leader_container)
            elif choice < 0.7 and followers:
                victim = rng.choice(followers)
                print(f"CHAOS pause {victim}", flush=True)
                _docker("pause", victim)
                await asyncio.sleep(3)
                _docker("unpause", victim)
            else:
                victim = rng.choice(containers)
                print(f"CHAOS restart {victim}", flush=True)
                _docker("restart", victim, "-t", "2")
    finally:
        await probe.close()


async def main() -> int:
    parser = argparse.ArgumentParser(description="MiniKV docker chaos test")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--nodes", default="localhost:7001,localhost:7002,localhost:7003")
    parser.add_argument("--compose", action="store_true", help="manage compose up/down here")
    parser.add_argument("--seed", type=int, default=20260612)
    parser.add_argument("--history-out", default="")
    args = parser.parse_args()

    if args.compose:
        print("compose up...", flush=True)
        _compose("up", "-d", "--build")
    try:
        return await run(args.nodes, args.duration, args.seed, args.history_out)
    finally:
        if args.compose:
            _compose("down")


async def run(nodes: str, duration_s: float, seed: int, history_out: str) -> int:
    addrs = nodes.split(",")
    containers = ["docker-n1-1", "docker-n2-1", "docker-n3-1"]
    history = History()
    lock = asyncio.Lock()
    stop = asyncio.Event()
    clients = [MiniKVClient(addrs, timeout_s=5.0) for _ in range(8)]
    try:
        leader = await _wait_leader(clients[0])
        print(f"leader is {leader}; starting load + faults", flush=True)
        fault_task = asyncio.create_task(_fault_loop(stop, seed + 1, containers, nodes, duration_s))
        keys = _keys(seed)
        await asyncio.gather(
            *(
                _writer(c, history, lock, stop, f"c{i}", seed + 100 + i, duration_s, keys)
                for i, c in enumerate(clients)
            )
        )
        stop.set()
        await fault_task
        print("load done; recovering...", flush=True)
        await asyncio.sleep(10)
        probe = MiniKVClient(addrs, timeout_s=10.0)
        try:
            final_leader = await _wait_leader(probe, timeout_s=30.0)
            print(f"recovered, leader {final_leader}", flush=True)
            # Read-back: every acknowledged write present-or-overwritten.
            acked: dict[bytes, tuple[float, bytes | None]] = {}
            for record in history.records:
                if record.return_ms is None or record.op == "get":
                    continue
                prev = acked.get(record.key)
                if prev is None or record.return_ms > prev[0]:
                    if record.op == "put":
                        (value,) = record.args  # type: ignore[misc]
                        assert isinstance(value, bytes)
                        acked[record.key] = (record.return_ms, value)
                    elif record.op == "delete":
                        acked[record.key] = (record.return_ms, None)
                    else:
                        success, current = record.result  # type: ignore[misc]
                        if success:
                            _, new_value = record.args  # type: ignore[misc]
                            assert isinstance(new_value, bytes)
                            acked[record.key] = (record.return_ms, new_value)
            errors = 0
            for key in keys:
                value = await probe.get(key, mode=pb2.READ_LINEARIZABLE)
                want = acked.get(key)
                if want is None:
                    continue  # never acknowledged: anything goes
                _, want_value = want
                # A later (possibly unacknowledged) write may have won.
                later = [
                    r
                    for r in history.records
                    if r.key == key and r.invoke_ms > want[0] and r.op in ("put", "delete", "cas")
                ]
                if value != want_value and not later:
                    print(f"LOST WRITE {key!r}: want {want_value!r}, got {value!r}", flush=True)
                    errors += 1
            print(f"read-back: {len(keys)} keys, {errors} lost", flush=True)
            if errors:
                return 1
            result = check_history(history)
            print(
                f"linearizability: {'OK' if result.ok else 'FAILED'} "
                f"({len(history.records)} ops, key={result.key}, {result.detail})",
                flush=True,
            )
            if history_out:
                Path(history_out).write_text(history.to_json(), encoding="utf-8")
            return 0 if result.ok else 1
        finally:
            await probe.close()
    finally:
        for client in clients:
            await client.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
