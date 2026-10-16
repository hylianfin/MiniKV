"""The checker checks itself: known-good histories pass, known-bad fail.

Bad cases mirror the classic violations: stale read, lost write (read
sees nothing that was acknowledged), time travel (new value then old),
broken CAS, phantom delete. A sensitivity case shows single-result flips
of a good history are caught.
"""

import pytest

from sim.history import History, OpRecord
from sim.linearizability import check_history


def _put(client: str, key: bytes, value: bytes, invoke: float, ret: float) -> OpRecord:
    return OpRecord(client, key, "put", (value,), invoke, ret, None)


def _get(client: str, key: bytes, found: bool, value: bytes, invoke: float, ret: float) -> OpRecord:
    return OpRecord(client, key, "get", (), invoke, ret, (found, value))


def _delete(client: str, key: bytes, existed: bool, invoke: float, ret: float) -> OpRecord:
    return OpRecord(client, key, "delete", (), invoke, ret, (existed,))


def _cas(
    client: str,
    key: bytes,
    expected: bytes | None,
    new: bytes,
    success: bool,
    current: bytes | None,
    invoke: float,
    ret: float,
) -> OpRecord:
    return OpRecord(client, key, "cas", (expected, new), invoke, ret, (success, current))


def test_sequential_reads_writes_pass() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _get("c1", b"k", True, b"v1", 2, 3),
            _put("c1", b"k", b"v2", 4, 5),
            _get("c2", b"k", True, b"v2", 6, 7),
            _delete("c1", b"k", True, 8, 9),
            _get("c2", b"k", False, b"", 10, 11),
        ]
    )
    result = check_history(history)
    assert result.ok, result.detail


def test_concurrent_read_may_see_either_value() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 10),  # slow write spans everything
            _get("c2", b"k", True, b"v1", 2, 3),  # sees the concurrent write
        ]
    )
    assert check_history(history).ok
    history2 = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _get("c2", b"k", False, b"", 2, 3),  # write completed first: lost
        ]
    )
    assert not check_history(history2).ok


def test_stale_read_rejected() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _put("c1", b"k", b"v2", 2, 3),
            _get("c2", b"k", True, b"v1", 4, 5),  # v1 long overwritten
        ]
    )
    assert not check_history(history).ok


def test_lost_write_rejected() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),  # acknowledged...
            _get("c2", b"k", False, b"", 2, 3),  # ...then invisible
        ]
    )
    assert not check_history(history).ok


def test_time_travel_rejected() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _put("c1", b"k", b"v2", 2, 3),
            _get("c2", b"k", True, b"v2", 4, 5),
            _get("c2", b"k", True, b"v1", 6, 7),  # back in time
        ]
    )
    assert not check_history(history).ok


def test_cas_histories() -> None:
    good = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _cas("c2", b"k", b"v1", b"v2", True, b"v2", 2, 3),
            _cas("c3", b"k", b"v1", b"v3", False, b"v2", 4, 5),  # stale expected
            _get("c1", b"k", True, b"v2", 6, 7),
        ]
    )
    assert check_history(good).ok
    bad = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _cas("c2", b"k", b"WRONG", b"v2", True, b"v2", 2, 3),  # success lies
        ]
    )
    assert not check_history(bad).ok


def test_timeout_modeling() -> None:
    # A timed-out write may or may not have taken effect: both readings pass.
    applied = History(
        [
            OpRecord("c1", b"k", "put", (b"v1",), 0, None, None),
            _get("c2", b"k", True, b"v1", 2, 3),
        ]
    )
    assert check_history(applied).ok
    dropped = History(
        [
            OpRecord("c1", b"k", "put", (b"v1",), 0, None, None),
            _get("c2", b"k", False, b"", 2, 3),
        ]
    )
    assert check_history(dropped).ok
    # ...but a timed-out write cannot unseat an acknowledged one seen later.
    conflict = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            OpRecord("c1", b"k", "put", (b"v0",), 0, None, None),
            _get("c2", b"k", True, b"v0", 2, 3),
            _get("c2", b"k", True, b"v1", 4, 5),
        ]
    )
    assert not check_history(conflict).ok


def test_timeout_of_every_op_kind() -> None:
    """Timed-out deletes, gets, and CASs model as specified: deletes/CASs
    become optional writes, timed-out reads constrain nothing."""
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            OpRecord("c1", b"k", "delete", (), 2, None, None),  # maybe deleted
            OpRecord("c2", b"k", "get", (), 3, None, None),  # observed nothing
            OpRecord("c1", b"k", "cas", (b"v1", b"v2"), 4, None, None),  # maybe CASd
            _get("c2", b"k", True, b"v2", 6, 7),  # ...and indeed it was
        ]
    )
    assert check_history(history).ok
    # Same prefix, but the final read sees v1: also linearizable (the
    # timed-out ops simply never took effect).
    history2 = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            OpRecord("c1", b"k", "delete", (), 2, None, None),
            OpRecord("c2", b"k", "get", (), 3, None, None),
            OpRecord("c1", b"k", "cas", (b"v1", b"v2"), 4, None, None),
            _get("c2", b"k", True, b"v1", 6, 7),
        ]
    )
    assert check_history(history2).ok


def test_history_json_roundtrip() -> None:
    history = History(
        [
            _put("c1", b"k", b"v1", 0, 1),
            _cas("c2", b"k", b"v1", b"v2", True, b"v2", 2, 3),
            OpRecord("c3", b"k", "put", (b"v3",), 4, None, None),
        ]
    )
    assert History.from_json(history.to_json()).records == history.records
    assert check_history(History.from_json(history.to_json())).ok


def test_sensitivity_single_flip_caught() -> None:
    records = [
        _put("c1", b"k", b"v1", 0, 1),
        _put("c1", b"k", b"v2", 2, 3),
        _get("c2", b"k", True, b"v2", 4, 5),
    ]
    assert check_history(History(list(records))).ok
    flipped = list(records)
    flipped[2] = _get("c2", b"k", True, b"v1", 4, 5)  # stale
    assert not check_history(History(flipped)).ok


def test_read_from_unlinearized_concurrent_write() -> None:
    """The read linearizes off a write that is itself stuck behind an
    earlier op: R sees W's value while W is still unlinearized."""
    from sim.linearizability import check_key

    ops = [
        _get("c2", b"k", True, b"v2", 1.5, 3),  # R: overlaps W below
        OpRecord("c1", b"k", "put", (b"v2",), 2, 20, None),  # W: blocked by U
        _put("c1", b"k", b"v0", 0, 1),  # U: must linearize first
    ]
    assert check_history(History(list(ops))).ok
    assert check_key(b"k", list(ops)).ok
    assert check_key(b"k", []).ok


def test_empty_and_unknown_op() -> None:
    assert check_history(History([])).ok
    with pytest.raises(ValueError, match="unknown op"):
        check_history(History([OpRecord("c", b"k", "frob", (), 0, 1, None)]))


def test_malformed_histories_rejected() -> None:
    bad: list[OpRecord] = [
        OpRecord("c", b"k", "delete", (), 0, 1, ("yes",)),  # existed not bool
        OpRecord("c", b"k", "delete", (), 0, 1, ()),  # wrong arity
        OpRecord("c", b"k", "get", (), 0, 1, (True,)),  # wrong arity
        OpRecord("c", b"k", "get", (), 0, 1, ("x", b"v")),  # found not bool
        OpRecord("c", b"k", "get", (), 0, 1, (True, "v")),  # value not bytes
        OpRecord("c", b"k", "cas", (b"e",), 0, 1, (True, b"v")),  # args arity
        OpRecord("c", b"k", "cas", ("e", b"n"), 0, 1, (True, b"v")),  # expected not bytes
        OpRecord("c", b"k", "cas", (b"e", "n"), 0, 1, (True, b"v")),  # new not bytes
        OpRecord("c", b"k", "cas", (b"e", b"n"), 0, 1, ("y", b"v")),  # success not bool
        OpRecord("c", b"k", "cas", (b"e", b"n"), 0, 1, (True, "v")),  # current not bytes
        OpRecord("c", b"k", "put", (), 0, 1, None),  # missing value
        OpRecord("c", b"k", "put", ("x",), 0, 1, None),  # value not bytes
    ]
    for record in bad:
        with pytest.raises(ValueError):
            check_history(History([record]))


def test_blocked_concurrent_write_skipped() -> None:
    """R reads v2 while two v2-writes are unlinearized: the first (W1) is
    itself blocked by an earlier op, so the scan skips it and succeeds
    through the second (W2). Genuinely linearizable as U, W2, R, W1."""
    ops = [
        _get("c2", b"k", True, b"v2", 1.5, 3),  # R
        OpRecord("c1", b"k", "put", (b"v2",), 2.5, 20, None),  # W1: blocked by U
        OpRecord("c1", b"k", "put", (b"v2",), 0.5, 20, None),  # W2: free
        _put("c1", b"k", b"v0", 0, 1.5),  # U: precedes W1 only
    ]
    result = check_history(History(list(ops)))
    assert result.ok, result.detail


def test_precondition_failing_write_skipped_then_rejected() -> None:
    """A completed CAS whose expected value never existed cannot linearize;
    the scan skips it on precondition failure and the history is rejected."""
    ops = [
        _get("c2", b"k", True, b"v2", 1.5, 3),  # R
        _cas("c1", b"k", b"v9", b"v2", True, b"v2", 0.5, 20),  # phantom success
        _put("c1", b"k", b"v0", 0, 1.5),  # U
    ]
    result = check_history(History(list(ops)))
    assert not result.ok


def test_pre_ok_rejects_malformed() -> None:
    from sim.linearizability import _pre_ok

    with pytest.raises(ValueError, match="bad precondition"):
        _pre_ok("not-a-tuple", None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown precondition"):
        _pre_ok(("bogus", b"x"), None)
