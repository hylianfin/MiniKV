"""CLI tests against real `minikv server` processes + the real CLI binary.

Real processes (not in-process servers): forking CLI children out of a
process that itself runs grpc.aio servers breaks the child's gRPC. This
is also the most faithful acceptance path: the exact binaries users run.
"""

import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

MINIKV = shutil.which("minikv")
pytestmark = pytest.mark.skipif(MINIKV is None, reason="minikv entry point not installed")


def _free_ports(n: int) -> list[int]:
    sockets = [socket.socket() for _ in range(n)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class LiveCluster:
    """Three real server processes on localhost."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        ports = _free_ports(3)
        self.addrs = [f"127.0.0.1:{p}" for p in ports]
        self.nodes_arg = ",".join(self.addrs)
        lines = ["[cluster]", "nodes = ["]
        for i, addr in enumerate(self.addrs):
            lines.append(f'  {{ id = "n{i + 1}", addr = "{addr}" }},')
        lines += [
            "]",
            "[raft]",
            "election_timeout_ms = [150, 300]",
            "heartbeat_ms = 50",
        ]
        (tmp / "cluster.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.procs: dict[str, subprocess.Popen[bytes]] = {}
        for i in range(3):
            node_id = f"n{i + 1}"
            self.procs[node_id] = subprocess.Popen(
                [
                    MINIKV,
                    "server",
                    "--id",
                    node_id,
                    "--config",
                    str(tmp / "cluster.toml"),
                    "--data-dir",
                    str(tmp / node_id),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [MINIKV, *args, "--nodes", self.nodes_arg],
            capture_output=True,
            text=True,
            timeout=30,
        )

    def statuses(self) -> dict[str, str]:
        out: dict[str, str] = {}
        result = self.cli("status")
        assert result.returncode == 0, result.stderr
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            out[parts[0]] = line
        return out

    def wait_leader(self, timeout_s: float = 20.0) -> str:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                statuses = self.statuses()
            except AssertionError:
                time.sleep(0.2)
                continue
            leaders = [n for n, line in statuses.items() if "role=leader" in line]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.2)
        raise TimeoutError("no leader elected")

    def kill(self, node_id: str) -> None:
        proc = self.procs.pop(node_id)
        proc.kill()
        proc.wait()

    def stop_all(self) -> None:
        for proc in self.procs.values():
            proc.terminate()
        for proc in self.procs.values():
            proc.wait()
        self.procs.clear()


def test_cli_end_to_end_with_failover() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cluster = LiveCluster(Path(tmp))
        try:
            leader = cluster.wait_leader()
            assert cluster.cli("put", "hello", "world").returncode == 0
            assert cluster.cli("get", "hello").stdout.strip() == "world"
            assert "(not found)" in cluster.cli("get", "ghost").stdout
            assert cluster.cli("cas", "hello", "world", "WORLD").returncode == 0
            lost = cluster.cli("cas", "hello", "world", "LOSES")
            assert lost.returncode == 1 and "mismatch" in lost.stdout
            assert cluster.cli("cas", "fresh", "x", "y", "--absent").returncode == 0
            scanned = cluster.cli("scan")
            assert scanned.returncode == 0 and "hello\tWORLD" in scanned.stdout
            assert cluster.cli("del", "hello").returncode == 0
            assert "role=leader" in cluster.cli("status").stdout
            # Kill -9 the leader; the CLI must keep working via the new one.
            cluster.kill(leader)
            new_leader = cluster.wait_leader()
            assert new_leader != leader
            assert cluster.cli("put", "after", "failover").returncode == 0
            assert cluster.cli("get", "after").stdout.strip() == "failover"
            assert cluster.cli("get", "after", "--stale").stdout.strip() == "failover"
            assert cluster.cli("get", "hello").stdout.strip() == "(not found)"
        finally:
            cluster.stop_all()
