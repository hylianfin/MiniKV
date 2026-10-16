"""Shared deterministic-scenario drivers (seed-replayable, invariant-checked)."""

from minikv.raft.types import NotLeader, ProposeOk
from sim.cluster import SimCluster
from sim.invariants import InvariantChecker, InvariantViolation


def run_checked(cluster: SimCluster, checker: InvariantChecker, duration_ms: int) -> None:
    steps, leftover = divmod(duration_ms, SimCluster.STEP_MS)
    for _ in range(steps):
        cluster.step()
        checker.check(cluster)
    if leftover:
        cluster.tick_all()
        cluster.deliver()
        cluster.clock.advance(leftover)
        checker.check(cluster)


def wait_for_leader(cluster: SimCluster, checker: InvariantChecker, timeout_ms: int = 8000) -> str:
    elapsed = 0
    while elapsed < timeout_ms:
        run_checked(cluster, checker, 50)
        elapsed += 50
        leaders = cluster.leaders()
        if len(leaders) == 1 and len(next(iter(leaders.values()))) == 1:
            return next(iter(next(iter(leaders.values()))))
    raise AssertionError(f"no single leader after {timeout_ms}ms (seed {cluster.seed})")


def wait_applied(
    cluster: SimCluster,
    checker: InvariantChecker,
    index: int,
    payload: bytes,
    nodes: set[str],
    timeout_ms: int = 8000,
) -> None:
    elapsed = 0
    while elapsed < timeout_ms:
        run_checked(cluster, checker, 50)
        elapsed += 50
        if all(cluster.nodes[n].applied.get(index) == payload for n in nodes):
            return
    raise AssertionError(f"index {index} never applied on {nodes} (seed {cluster.seed})")


def propose_sync(
    cluster: SimCluster, checker: InvariantChecker, payload: bytes, timeout_ms: int = 8000
) -> int:
    """Propose via the leader (following hints) until applied everywhere alive."""
    leader = wait_for_leader(cluster, checker, timeout_ms)
    elapsed = 0
    index = -1
    while elapsed < timeout_ms:
        result = cluster.propose(leader, payload)
        checker.check(cluster)
        if isinstance(result, NotLeader):
            leader = result.leader_id or leader
            if leader not in cluster.nodes or not cluster.nodes[leader].alive:
                leader = wait_for_leader(cluster, checker, timeout_ms)
            elapsed += 50
            run_checked(cluster, checker, 50)
            continue
        assert isinstance(result, ProposeOk)
        index = result.index
        break
    assert index >= 0, f"propose never accepted (seed {cluster.seed})"
    wait_applied(
        cluster,
        checker,
        index,
        payload,
        {n for n, node in cluster.nodes.items() if node.alive},
    )
    return index


def run_seeds(scenario: object, seeds: range, label: str) -> None:  # type: ignore[type-arg]
    for seed in seeds:
        try:
            scenario(seed)
        except InvariantViolation as e:
            raise AssertionError(f"{label} seed {seed}: invariant: {e}") from e
        except Exception as e:
            raise AssertionError(f"{label} seed {seed}: {e!r}") from e
