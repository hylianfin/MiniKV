"""Model-based test: random put/delete/get/scan/flush/compact/restart vs a dict.

The spec requires 500 examples. Each example uses tiny tables (frequent
rotation) and a low compaction trigger so every path -- sealed memtables,
multi-table merges, tombstone drops, WAL replay -- is exercised.
"""

import tempfile
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from minikv.storage.lsm import LSMEngine, LSMOptions

keys = st.binary(min_size=0, max_size=4)
small_vals = st.binary(min_size=0, max_size=8)
put_op = st.tuples(st.just("put"), keys, small_vals)
del_op = st.tuples(st.just("del"), keys)
get_op = st.tuples(st.just("get"), keys)
scan_op = st.tuples(st.just("scan"), st.none() | keys, st.none() | keys, st.integers(0, 5))
ops = st.lists(
    st.one_of(
        put_op,
        put_op,
        put_op,
        del_op,
        del_op,
        get_op,
        get_op,
        scan_op,
        st.just(("flush",)),
        st.just(("compact",)),
        st.just(("restart",)),
    ),
    max_size=30,
)


def _expected_scan(
    model: dict[bytes, bytes], start: bytes | None, end: bytes | None, limit: int
) -> list[tuple[bytes, bytes]]:
    out = [(k, v) for k, v in sorted(model.items())]
    if start is not None:
        out = [(k, v) for k, v in out if k >= start]
    if end is not None:
        out = [(k, v) for k, v in out if k < end]
    if end is not None and start is not None and end <= start:
        return []
    if limit:
        out = out[:limit]
    return out


@given(ops)
@settings(max_examples=500, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_lsm_model(operations: list[tuple]) -> None:  # type: ignore[type-arg]
    opts = LSMOptions(memtable_bytes=256, compaction_trigger=3, block_bytes=256)
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp) / "db"
        engine = LSMEngine(directory, opts)
        engine.open()
        try:
            model: dict[bytes, bytes] = {}
            applied = 0
            raft = 0
            for op in operations:
                tag = op[0]
                if tag == "put":
                    raft += 1
                    engine.put(op[1], op[2], raft_index=raft)
                    model[op[1]] = op[2]
                    applied = raft
                elif tag == "del":
                    raft += 1
                    engine.delete(op[1], raft_index=raft)
                    model.pop(op[1], None)
                    applied = raft
                elif tag == "get":
                    assert engine.get(op[1]) == model.get(op[1])
                elif tag == "scan":
                    assert list(engine.scan(op[1], op[2], op[3])) == _expected_scan(
                        model, op[1], op[2], op[3]
                    )
                elif tag == "flush":
                    engine.flush()
                elif tag == "compact":
                    engine.compact()
                else:  # restart: clean close + reopen must lose nothing
                    engine.close()
                    engine = LSMEngine(directory, opts)
                    engine.open()
                assert engine.applied_index() == applied
            assert list(engine.scan(None, None)) == sorted(model.items())
            assert engine.applied_index() == applied
        finally:
            engine.close()
