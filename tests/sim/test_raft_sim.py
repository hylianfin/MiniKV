"""Deterministic Raft simulation scenarios (M5: election + replication @200 seeds).

Every scenario runs from a single integer seed; a failure raises with the
seed attached so it replays exactly. The invariant checker runs after every
step inside ``run_checked``.
"""

from minikv.raft.types import NotLeader, ProposeOk, RaftConfig, Role
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


def scenario_election(n: int, seed: int) -> None:
    ids = [f"n{i}" for i in range(1, n + 1)]
    cluster = SimCluster(ids, seed)
    checker = InvariantChecker()
    leader = wait_for_leader(cluster, checker)
    assert cluster.nodes[leader].core.term >= 1  # type: ignore[union-attr]
    run_checked(cluster, checker, 1000)  # stable: still exactly one leader
    assert wait_for_leader(cluster, checker, 1000) == leader


def scenario_replication(n: int, seed: int, batched: bool = False) -> None:
    ids = [f"n{i}" for i in range(1, n + 1)]
    cluster = SimCluster(ids, seed)
    checker = InvariantChecker()
    wait_for_leader(cluster, checker)
    if batched:
        leader = wait_for_leader(cluster, checker)
        results = cluster.propose_all([(leader, f"cmd{i}".encode()) for i in range(10)])
        assert all(isinstance(r, ProposeOk) for r in results)
        run_checked(cluster, checker, 2000)
    else:
        for i in range(10):
            propose_sync(cluster, checker, f"cmd{i}".encode())
    logs = [list(node.core.log) for node in cluster.nodes.values() if node.alive]  # type: ignore[union-attr]
    assert all(log == logs[0] for log in logs[1:])
    applied = [node.applied for node in cluster.nodes.values() if node.alive]
    assert all(a == applied[0] for a in applied[1:])
    assert len(applied[0]) == 11  # NOOP + 10 commands


def test_election_3node() -> None:
    run_seeds(lambda s: scenario_election(3, s), range(200), "election/3")


def test_election_5node() -> None:
    run_seeds(lambda s: scenario_election(5, s), range(200), "election/5")


def test_replication_3node() -> None:
    run_seeds(lambda s: scenario_replication(3, s), range(200), "replication/3")


def test_replication_5node() -> None:
    run_seeds(lambda s: scenario_replication(5, s), range(200), "replication/5")


def test_replication_batched() -> None:
    run_seeds(lambda s: scenario_replication(3, s, batched=True), range(50), "batched/3")


def test_leader_crash_and_restart() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        leader = wait_for_leader(cluster, checker)
        for i in range(5):
            propose_sync(cluster, checker, f"a{i}".encode())
        cluster.crash(leader)
        checker.check(cluster)
        run_checked(cluster, checker, 500)
        new_leader = wait_for_leader(cluster, checker)
        assert new_leader != leader
        for i in range(5, 10):
            propose_sync(cluster, checker, f"a{i}".encode())
        cluster.restart(leader)
        run_checked(cluster, checker, 2000)
        applied = [n.applied for n in cluster.nodes.values()]
        assert all(a == applied[0] for a in applied[1:])
        assert len(applied[0]) >= 11  # NOOPs (2 terms) + 10 commands

    run_seeds(scenario, range(20), "leader-crash")


def test_follower_crash_catch_up() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        leader = wait_for_leader(cluster, checker)
        victim = next(n for n in cluster.nodes if n != leader)
        cluster.crash(victim)
        for i in range(8):
            propose_sync(cluster, checker, f"k{i}".encode())
        cluster.restart(victim)
        run_checked(cluster, checker, 3000)
        logs = [list(n.core.log) for n in cluster.nodes.values()]  # type: ignore[union-attr]
        assert all(log == logs[0] for log in logs[1:])

    run_seeds(scenario, range(20), "follower-crash")


def test_minority_partition_heals() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        wait_for_leader(cluster, checker)
        cluster.partition([{"n1", "n2"}, {"n3"}])
        # Drive proposals on the majority side only; n3 cannot hear them.
        majority = {"n1", "n2"}
        for i in range(5):
            for _ in range(10):  # isolated n3 may briefly still believe it leads
                leader = wait_for_leader(cluster, checker)
                if leader in majority:
                    break
                run_checked(cluster, checker, 700)  # past the quorum window
            assert leader in majority
            result = cluster.propose(leader, f"p{i}".encode())
            assert isinstance(result, ProposeOk)
            wait_applied(cluster, checker, result.index, f"p{i}".encode(), majority)
        cluster.partition(None)
        run_checked(cluster, checker, 3000)
        logs = [list(n.core.log) for n in cluster.nodes.values()]  # type: ignore[union-attr]
        assert all(log == logs[0] for log in logs[1:])

    run_seeds(scenario, range(20), "minority-partition")


def test_isolated_leader_steps_down() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        leader = wait_for_leader(cluster, checker)
        others = {n for n in cluster.nodes if n != leader}
        cluster.partition([{leader}, others])
        run_checked(cluster, checker, 1500)  # past the max election timeout
        assert cluster.nodes[leader].core.role is Role.FOLLOWER  # type: ignore[union-attr]
        run_checked(cluster, checker, 500)
        # The majority side elects its own leader at a higher term.
        majority_leaders = [
            n
            for n in others
            if cluster.nodes[n].core.role is Role.LEADER  # type: ignore[union-attr]
        ]
        assert len(majority_leaders) == 1
        cluster.partition(None)
        run_checked(cluster, checker, 3000)
        assert len(cluster.leaders()) == 1

    run_seeds(scenario, range(20), "isolation")


def test_lossy_network_converges() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed, drop_p=0.1, dup_p=0.05)
        checker = InvariantChecker()
        for i in range(10):
            propose_sync(cluster, checker, f"l{i}".encode(), timeout_ms=20000)
        logs = [list(n.core.log) for n in cluster.nodes.values()]  # type: ignore[union-attr]
        assert all(log == logs[0] for log in logs[1:])

    run_seeds(scenario, range(20), "lossy")


def test_all_restart() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        wait_for_leader(cluster, checker)
        for i in range(5):
            propose_sync(cluster, checker, f"r{i}".encode())
        before = {n: dict(node.applied) for n, node in cluster.nodes.items()}
        for node_id in list(cluster.nodes):
            cluster.crash(node_id)
        for node_id in list(cluster.nodes):
            cluster.restart(node_id)
        run_checked(cluster, checker, 3000)
        for node_id, node in cluster.nodes.items():
            for index, payload in before[node_id].items():
                assert node.applied.get(index) == payload
        for i in range(5, 8):
            propose_sync(cluster, checker, f"r{i}".encode())

    run_seeds(scenario, range(20), "all-restart")


def test_leader_churn() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed)
        checker = InvariantChecker()
        dead: str | None = None
        for round_ in range(3):
            leader = wait_for_leader(cluster, checker)
            propose_sync(cluster, checker, f"c{round_}".encode())
            if dead is not None:
                cluster.restart(dead)  # keep a quorum alive
            cluster.crash(leader)
            dead = leader
            run_checked(cluster, checker, 300)
        assert dead is not None
        cluster.restart(dead)
        wait_for_leader(cluster, checker)

    run_seeds(scenario, range(20), "churn")


def test_crash_loses_only_uncommitted() -> None:
    def scenario(seed: int) -> None:
        cluster = SimCluster(["n1", "n2", "n3"], seed, fsync_mode="manual")
        checker = InvariantChecker()
        leader = wait_for_leader(cluster, checker)
        for i in range(5):
            propose_sync(cluster, checker, f"safe{i}".encode())
        cluster.fsync_all()  # everything so far is durable
        for i in range(5, 8):
            cluster.propose(leader, f"risky{i}".encode())  # volatile only
        run_checked(cluster, checker, 200)
        committed = cluster.nodes[leader].core.commit_index  # type: ignore[union-attr]
        assert committed >= 6  # the fsynced prefix (NOOP + 5) is committed
        snapshot = {
            (e.index, e.term, e.payload)
            for node in cluster.nodes.values()
            for e in node.core.log  # type: ignore[union-attr]
            if e.index <= committed
        }
        cluster.crash(leader, lose_unfsynced=True)
        run_checked(cluster, checker, 300)
        cluster.restart(leader)
        run_checked(cluster, checker, 3000)
        # Every entry committed before the crash is present somewhere alive.
        live = {
            (e.index, e.term, e.payload)
            for node in cluster.nodes.values()
            if node.alive
            for e in node.core.log  # type: ignore[union-attr]
        }
        assert snapshot <= live

    run_seeds(scenario, range(20), "crash-loss")


def test_pre_vote_blocks_partitioned_disruption() -> None:
    """An isolated follower must not inflate terms (pre-vote on) -- while
    without pre-vote the same isolation disrupts the stable leader."""

    def scenario(seed: int, pre_vote: bool) -> tuple[str, int]:
        config = RaftConfig(pre_vote=pre_vote)
        cluster = SimCluster(["n1", "n2", "n3"], seed, config=config)
        checker = InvariantChecker()
        leader = wait_for_leader(cluster, checker)
        run_checked(cluster, checker, 1500)  # let overlapping campaigns settle
        term_before = cluster.nodes[leader].core.term  # type: ignore[union-attr]
        assert all(
            n.core.term == term_before  # type: ignore[union-attr]
            for n in cluster.nodes.values()
        )
        victim = next(n for n in cluster.nodes if n != leader)
        cluster.partition([{victim}, {n for n in cluster.nodes if n != victim}])
        for _ in range(20):  # victim's timer fires repeatedly in isolation
            run_checked(cluster, checker, 100)
            if pre_vote:
                # The pre-vote guarantee: without a connected majority the
                # victim can never even *start* a real campaign.
                assert cluster.nodes[victim].core.term == term_before  # type: ignore[union-attr]
        if not pre_vote:
            # Without pre-vote the victim inflates terms while isolated.
            assert cluster.nodes[victim].core.term > term_before  # type: ignore[union-attr]
        cluster.partition(None)
        run_checked(cluster, checker, 2000)
        leader_after = wait_for_leader(cluster, checker)
        term_after = cluster.nodes[leader_after].core.term  # type: ignore[union-attr]
        return leader_after, term_after - term_before

    for seed in range(10):
        _, bump = scenario(seed, True)
        # At most one legitimate handover: the caught-up victim may win a
        # fair election after rejoining, but never inflated terms in exile.
        assert bump <= 1, f"pre-vote on but terms ran away (seed {seed})"
    for seed in range(10):
        _, bump = scenario(seed, False)
        assert bump > 0, f"pre-vote off but no disruption (seed {seed})"
