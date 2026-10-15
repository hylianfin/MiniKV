"""Property tests: SkipList vs sorted dict."""

import random

from hypothesis import given
from hypothesis import strategies as st

from minikv.storage.skiplist import SkipList

keys = st.binary(min_size=0, max_size=4)
vals = st.binary(max_size=8)
put_op = st.tuples(st.just("put"), keys, vals)
del_op = st.tuples(st.just("del"), keys)
ops = st.lists(st.one_of(put_op, del_op), max_size=200)


@given(ops)
def test_model_matches_dict(operations: list[tuple[str, bytes, bytes]]) -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random())
    model: dict[bytes, bytes] = {}
    for op in operations:
        if op[0] == "put":
            _, k, v = op
            sl.put(k, v)
            model[k] = v
        else:
            _, k = op[:2]
            assert sl.delete(k) == (k in model)
            model.pop(k, None)
        # Spot-check live reads against the model after every op.
        for probe in (op[1], b""):
            assert sl.get(probe) == model.get(probe)
    assert list(sl.items()) == sorted(model.items())
    assert len(sl) == len(model)


@given(ops, keys)
def test_seek_matches_sorted_model(
    operations: list[tuple[str, bytes, bytes]], start: bytes
) -> None:
    sl: SkipList[bytes] = SkipList(rng=random.Random())
    model: dict[bytes, bytes] = {}
    for op in operations:
        if op[0] == "put":
            _, k, v = op
            sl.put(k, v)
            model[k] = v
        else:
            model.pop(op[1], None)
            sl.delete(op[1])
    expected = sorted((k, v) for k, v in model.items() if k >= start)
    assert list(sl.seek(start)) == expected
