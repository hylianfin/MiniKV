"""KV-over-Raft simulation: commands through consensus into real LSM engines.

Each node runs a KVApplier (data + session engines). Scenarios check the
M6 acceptance: crash/restart preserves acknowledged state, request retries
hit the session cache (exactly-once, even across leader failover), and all
nodes converge to identical applied state.
"""

import tempfile
from pathlib import Path

from minikv.kv.state_machine import (
    CasCommand,
    CasResult,
    Command,
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
from sim.invariants import InvariantChecker
from tests.sim.helpers import run_checked, run_seeds, wait_for_leader


def _applier_options() -> LSMOptions:
    return LSMOptions(memtable_bytes=4096, compaction_trigger=4, block_bytes=512)


def _cluster(tmp: Path, seed: int, n: int = 3) -> SimCluster:
    ids = [f"n{i}" for i in range(1, n + 1)]

    def factory(node_id: str):  # type: ignore[no-untyped-def]
        from minikv.kv.state_machine import KVApplier

        return KVApplier(tmp / node_id, _applier_options())

    cluster = SimCluster(ids, seed, applier_factory=factory)
    return cluster


def _submit(
    cluster: SimCluster,
    checker: InvariantChecker,
    ctx: RequestContext,
    command: Command,
) -> tuple[int, bytes]:
    """Propose a request via the leader until applied; return (index, result)."""
    payload = encode_request(ctx, command)
    leader = wait_for_leader(cluster, checker)
    elapsed = 0
    while elapsed < 8000:
        result = cluster.propose(leader, payload)
        checker.check(cluster)
        if isinstance(result, NotLeader):
            leader = result.leader_id or wait_for_leader(cluster, checker)
            elapsed += 50
            run_checked(cluster, checker, 50)
            continue
        assert isinstance(result, ProposeOk)
        elapsed = 0
        while elapsed < 8000:
            run_checked(cluster, checker, 50)
            elapsed += 50
            alive = [n for n, node in cluster.nodes.items() if node.alive]
            if all(idx_result(cluster, n, result.index) is not None for n in alive):
                first = idx_result(cluster, alive[0], result.index)
                assert first is not None
                return result.index, first
    raise AssertionError(f"request {ctx} never applied (seed {cluster.seed})")


def idx_result(cluster: SimCluster, node_id: str, index: int) -> bytes | None:
    return cluster.nodes[node_id].results.get(index)


def _live_data(cluster: SimCluster) -> list[dict[bytes, bytes]]:
    return [
        dict(node.applier.data.scan(None, None)) for node in cluster.nodes.values() if node.alive
    ]  # type: ignore[union-attr]


def test_kv_model_matches_dict() -> None:
    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cluster = _cluster(Path(tmp), seed)
            checker = InvariantChecker()
            try:
                wait_for_leader(cluster, checker)
                model: dict[bytes, bytes] = {}
                seq = 0
                for i in range(30):
                    seq += 1
                    ctx = RequestContext("client-a", seq)
                    key = f"k{i % 5}".encode()
                    op = i % 4
                    if op == 0:
                        _, raw = _submit(cluster, checker, ctx, PutCommand(key, f"v{i}".encode()))
                        model[key] = f"v{i}".encode()
                    elif op == 1:
                        was_present = key in model
                        _, raw = _submit(cluster, checker, ctx, DeleteCommand(key))
                        assert decode_result(raw) == DeleteResult(was_present)
                        model.pop(key, None)
                    elif op == 2:
                        expected = model.get(key)
                        _, raw = _submit(
                            cluster,
                            checker,
                            ctx,
                            CasCommand(key, expected, f"c{i}".encode()),
                        )
                        result = decode_result(raw)
                        assert isinstance(result, CasResult)
                        assert result.success is True
                        assert result.current == f"c{i}".encode()
                        model[key] = f"c{i}".encode()
                    else:
                        # Read-your-writes from the leader's engine.
                        leader = wait_for_leader(cluster, checker)
                        assert cluster.nodes[leader].applier.data.get(key) == model.get(key)  # type: ignore[union-attr]
                for view in _live_data(cluster):
                    assert view == model
            finally:
                cluster.shutdown()

    run_seeds(scenario, range(10), "kv-model")


def test_retry_after_failover_returns_cached_result() -> None:
    """The money test: CAS committed on L1, response lost, retry on L2
    after L1 dies. Without dedup the retry would re-apply (success=False);
    with sessions it returns the original success=True."""

    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cluster = _cluster(Path(tmp), seed)
            checker = InvariantChecker()
            try:
                leader = wait_for_leader(cluster, checker)
                ctx = RequestContext("client-x", 1)
                _, put_raw = _submit(cluster, checker, ctx, PutCommand(b"k", b"v0"))
                assert put_raw is not None
                cas_ctx = RequestContext("client-x", 2)
                index, first = _submit(cluster, checker, cas_ctx, CasCommand(b"k", b"v0", b"v1"))
                first_result = decode_result(first)
                assert isinstance(first_result, CasResult) and first_result.success
                # Leader dies before the client sees anything more. Retry the
                # identical request: it must come back cached, not re-applied.
                cluster.crash(leader)
                checker.check(cluster)
                new_leader = wait_for_leader(cluster, checker)
                assert new_leader != leader
                retry_index, retry_raw = _submit(
                    cluster, checker, cas_ctx, CasCommand(b"k", b"v0", b"v1")
                )
                assert retry_index != index  # a brand-new log entry...
                assert retry_raw == first  # ...with the identical cached result
                assert decode_result(retry_raw).success is True  # type: ignore[union-attr]
                for view in _live_data(cluster):
                    assert view == {b"k": b"v1"}
            finally:
                cluster.shutdown()

    run_seeds(scenario, range(10), "failover-dedup")


def test_restart_all_preserves_kv_state() -> None:
    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cluster = _cluster(Path(tmp), seed)
            checker = InvariantChecker()
            try:
                wait_for_leader(cluster, checker)
                seq = 0
                for i in range(12):
                    seq += 1
                    _submit(
                        cluster,
                        checker,
                        RequestContext("c", seq),
                        PutCommand(f"k{i}".encode(), b"v"),
                    )
                for node_id in list(cluster.nodes):
                    cluster.crash(node_id)
                for node_id in list(cluster.nodes):
                    cluster.restart(node_id)
                run_checked(cluster, checker, 3000)
                views = _live_data(cluster)
                assert len(views) == 3 and all(v == views[0] for v in views[1:])
                assert len(views[0]) == 12
                # A retry of an old request hits the post-restart session cache.
                dup_ctx = RequestContext("c", 5)
                dup_index, dup_raw = _submit(cluster, checker, dup_ctx, PutCommand(b"k4", b"v"))
                assert dup_raw == _submit(cluster, checker, dup_ctx, PutCommand(b"k4", b"v"))[1]
            finally:
                cluster.shutdown()

    run_seeds(scenario, range(5), "kv-restart")


def test_kv_survives_leader_crash_mid_workload() -> None:
    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cluster = _cluster(Path(tmp), seed)
            checker = InvariantChecker()
            try:
                leader = wait_for_leader(cluster, checker)
                seq = 0
                for i in range(8):
                    seq += 1
                    _submit(
                        cluster,
                        checker,
                        RequestContext("c", seq),
                        PutCommand(f"w{i}".encode(), b"v"),
                    )
                cluster.crash(leader)
                run_checked(cluster, checker, 300)
                cluster.restart(leader)
                run_checked(cluster, checker, 2000)
                for i in range(8, 12):
                    seq += 1
                    _submit(
                        cluster,
                        checker,
                        RequestContext("c", seq),
                        PutCommand(f"w{i}".encode(), b"v"),
                    )
                views = _live_data(cluster)
                assert all(v == views[0] for v in views[1:])
                assert len(views[0]) == 12
            finally:
                cluster.shutdown()

    run_seeds(scenario, range(5), "kv-crash")


def test_lossy_crash_prunes_unfsynced_results() -> None:
    def scenario(seed: int) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ids = ["n1", "n2", "n3"]

            def factory(node_id: str):  # type: ignore[no-untyped-def]
                from minikv.kv.state_machine import KVApplier

                return KVApplier(Path(tmp) / node_id, _applier_options())

            cluster = SimCluster(ids, seed, fsync_mode="manual", applier_factory=factory)
            checker = InvariantChecker()
            try:
                leader = wait_for_leader(cluster, checker)
                _submit(cluster, checker, RequestContext("c", 1), PutCommand(b"k", b"v"))
                # No fsync: everything applied is volatile on every node.
                assert cluster.nodes[leader].results != {}
                cluster.crash(leader, lose_unfsynced=True)
                checker.check(cluster)
                assert cluster.nodes[leader].results == {}  # rolled back with the store
            finally:
                cluster.shutdown()

    run_seeds(scenario, range(5), "kv-loss")
