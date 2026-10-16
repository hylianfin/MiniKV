"""Linearizability over simulated histories, including a leader-kill phase.

Clients interleave proposals and leader-served reads on virtual time while
the invariant checker watches every step; afterwards the recorded history
must linearize per key. Timeouts (from the kill phase) model as optional
operations.
"""

import tempfile
from pathlib import Path

from minikv.kv.state_machine import (
    CasCommand,
    CasResult,
    DeleteCommand,
    DeleteResult,
    PutCommand,
    RequestContext,
    decode_result,
    encode_request,
)
from minikv.raft.types import NotLeader, ProposeOk
from minikv.storage.lsm import LSMOptions
from sim.cluster import SimCluster
from sim.history import History, OpRecord
from sim.invariants import InvariantChecker
from sim.linearizability import check_history
from tests.sim.helpers import run_checked, run_seeds, wait_for_leader


def _applier_options() -> LSMOptions:
    return LSMOptions(memtable_bytes=4096, compaction_trigger=4, block_bytes=512)


class SimClient:
    """One sequential client: one outstanding op, recorded with bounds."""

    def __init__(self, cluster: SimCluster, checker: InvariantChecker, name: str) -> None:
        self.cluster = cluster
        self.checker = checker
        self.name = name
        self.seq = 0
        self.model: dict[bytes, bytes] = {}

    def _leader(self) -> str:
        return wait_for_leader(self.cluster, self.checker, timeout_ms=3000)

    def _await_index(
        self, index: int, payload: bytes, timeout_ms: int
    ) -> tuple[float | None, dict[int, bytes]]:
        """Run until all alive nodes applied; return (return_ms, results)."""
        elapsed = 0
        while elapsed < timeout_ms:
            run_checked(self.cluster, self.checker, 10)
            elapsed += 10
            alive = [n for n, node in self.cluster.nodes.items() if node.alive]
            if all(self.cluster.nodes[n].applied.get(index) == payload for n in alive):
                results = {n: self.cluster.nodes[n].results[index] for n in alive}
                return float(self.cluster.now), results
        return None, {}

    def submit(self, key: bytes, command: object, timeout_ms: int = 2000) -> OpRecord:  # type: ignore[type-arg]
        from minikv.kv.state_machine import Command

        assert isinstance(command, (PutCommand, DeleteCommand, CasCommand))
        self.seq += 1
        ctx = RequestContext(self.name, self.seq)
        payload = encode_request(ctx, command)  # type: ignore[arg-type]
        _ = Command
        invoke = float(self.cluster.now)
        op, args = _describe(command)
        elapsed = 0
        index = -1
        leader = self._leader()
        while elapsed < timeout_ms:
            result = self.cluster.propose(leader, payload)
            self.checker.check(self.cluster)
            if isinstance(result, NotLeader):
                leader = result.leader_id or self._leader()
                elapsed += 20
                run_checked(self.cluster, self.checker, 20)
                continue
            assert isinstance(result, ProposeOk)
            index = result.index
            break
        if index < 0:
            return OpRecord(self.name, key, op, args, invoke, None, None)
        ret, results = self._await_index(index, payload, timeout_ms - elapsed)
        if ret is None:
            return OpRecord(self.name, key, op, args, invoke, None, None)
        first = results[next(iter(results))]
        decoded = decode_result(first)
        assert all(v == first for v in results.values()), "replicas disagree on result"
        return OpRecord(self.name, key, op, args, invoke, ret, _summarize(op, decoded))

    def read(self, key: bytes) -> OpRecord:
        """Linearizable read from the current leader's engine (instant)."""
        invoke = float(self.cluster.now)
        leader = self._leader()
        value = self.cluster.nodes[leader].applier.data.get(key)  # type: ignore[union-attr]
        self.checker.check(self.cluster)
        ret = float(self.cluster.now) + 1
        if value is None:
            return OpRecord(self.name, key, "get", (), invoke, ret, (False, b""))
        return OpRecord(self.name, key, "get", (), invoke, ret, (True, value))


def _describe(command: object) -> tuple[str, tuple[object, ...]]:  # type: ignore[type-arg]
    if isinstance(command, PutCommand):
        return "put", (command.value,)
    if isinstance(command, DeleteCommand):
        return "delete", ()
    assert isinstance(command, CasCommand)
    return "cas", (command.expected, command.new_value)


def _summarize(op: str, result: object) -> object:  # type: ignore[type-arg]

    if op == "put":
        return None
    if op == "delete":
        assert isinstance(result, DeleteResult)
        return (result.existed,)
    assert isinstance(result, CasResult)
    return (result.success, result.current)


def test_sim_histories_linearize_with_faults() -> None:
    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ids = ["n1", "n2", "n3"]

            def factory(node_id: str):  # type: ignore[no-untyped-def]
                from minikv.kv.state_machine import KVApplier

                return KVApplier(Path(tmp) / node_id, _applier_options())

            cluster = SimCluster(ids, seed, applier_factory=factory)
            checker = InvariantChecker()
            history = History()
            try:
                clients = [SimClient(cluster, checker, f"c{i}") for i in range(3)]
                wait_for_leader(cluster, checker)
                keys = [f"k{i}".encode() for i in range(6)]

                def workload(rounds: int, start: int) -> None:
                    for r in range(rounds):
                        client = clients[(r + start) % len(clients)]
                        key = keys[(r * 2 + start) % len(keys)]
                        mode = (r + start) % 5
                        if mode == 0:
                            history.append(client.read(key))
                        elif mode == 1:
                            history.append(client.submit(key, DeleteCommand(key)))
                        elif mode == 2:
                            live = client.cluster.nodes[client._leader()].applier.data.get(key)  # type: ignore[union-attr]
                            history.append(
                                client.submit(key, CasCommand(key, live, f"v{r}".encode()))
                            )
                        else:
                            history.append(client.submit(key, PutCommand(key, f"v{r}".encode())))
                        run_checked(cluster, checker, 15)

                workload(40, 0)
                # Kill the leader mid-load: timeouts must still linearize.
                leader = wait_for_leader(cluster, checker)
                cluster.crash(leader)
                checker.check(cluster)
                workload(25, 100)
                cluster.restart(leader)
                workload(15, 200)
                run_checked(cluster, checker, 2000)
            finally:
                cluster.shutdown()
            result = check_history(history)
            assert result.ok, f"seed {seed}: key={result.key} {result.detail}"

    run_seeds(scenario, range(30), "sim-lin")
