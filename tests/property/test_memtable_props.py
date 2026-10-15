"""Property tests: MemTable vs dict-with-tombstones model."""

import random

from hypothesis import given
from hypothesis import strategies as st

from minikv.storage.memtable import EntryKind, MemTable

keys = st.binary(min_size=0, max_size=4)
vals = st.binary(max_size=8)
put_op = st.tuples(st.just("put"), keys, vals)
del_op = st.tuples(st.just("del"), keys)
ops = st.lists(st.one_of(put_op, del_op), max_size=200)


@given(ops)
def test_model_with_tombstones(operations: list[tuple[str, bytes, bytes]]) -> None:
    m = MemTable(rng=random.Random())
    # Model maps key -> value or None (None = tombstoned).
    model: dict[bytes, bytes | None] = {}
    for seq, op in enumerate(operations):
        if op[0] == "put":
            m.put(op[1], op[2], seq=seq)
            model[op[1]] = op[2]
        else:
            m.delete(op[1], seq=seq)
            model[op[1]] = None
        assert m.get(op[1]) == model[op[1]]
    live = {k: v for k, v in model.items() if v is not None}
    for k, v in live.items():
        assert m.get(k) == v
        assert k in m
    for k, v in model.items():
        if v is None:
            assert m.get(k) is None
            assert k not in m
    # Full entry view: every model key present, sorted, kinds match.
    entries = list(m.items())
    assert [k for k, _ in entries] == sorted(model.keys())
    for k, entry in entries:
        assert entry.key == k
        assert entry.kind is (EntryKind.PUT if model[k] is not None else EntryKind.DELETE)
