"""Linearizability checker for single-key register histories.

Each key is checked independently with a memoized Wing & Gong style depth
search over linearization orders. Reads, failed CASs, and delete-existed
flags constrain the value; writes (including successful CASs and deletes)
move it. Real-time precedence is exact: an operation can linearize only
after everything that completed before it started.

Timed-out operations observed nothing: timed-out reads constrain nothing
(dropped), timed-out writes may linearize anywhere after invocation (or
never). This over-approximates -- a history that passes definitely has a
valid linearization; one that fails has none.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass

from sim.history import History, OpRecord

_INF = math.inf


@dataclass(frozen=True, slots=True)
class _NormOp:
    invoke: float
    ret: float  # +inf for timeouts
    is_write: bool
    value: bytes | None  # write target / expected read value
    pre_eq: object  # sentinel or expected value (bytes | None)
    must: bool  # completed ops must linearize; timed-out writes may not


_NO_PRE = object()


@dataclass(frozen=True, slots=True)
class CheckResult:
    ok: bool
    key: bytes | None
    detail: str


def check_history(history: History) -> CheckResult:
    """Check every key; return the first failure (or success)."""
    for key, ops in history.per_key().items():
        ok, detail = _check_key(ops)
        if not ok:
            return CheckResult(False, key, detail)
    return CheckResult(True, None, f"linearizable ({len(history.records)} ops)")


def check_key(key: bytes, ops: list[OpRecord]) -> CheckResult:
    ok, detail = _check_key(ops)
    return CheckResult(ok, None if ok else key, detail)


def _check_key(ops: list[OpRecord]) -> tuple[bool, str]:
    normalized: list[_NormOp] = []
    for op in ops:
        item = _normalize(op)
        if item is not None:
            normalized.append(item)
    norm = normalized
    if not norm:
        return True, "empty"
    must_mask = 0
    for i, norm_op in enumerate(norm):
        if norm_op.must:
            must_mask |= 1 << i
    sys.setrecursionlimit(max(sys.getrecursionlimit(), 10000 + len(norm) * 4))
    memo: dict[tuple[int, bytes | None], bool] = {}
    if _dfs(tuple(norm), must_mask, 0, None, memo):
        return True, f"linearizable ({len(norm)} ops)"
    return False, f"no linearization for {len(norm)} ops"


def _normalize(op: OpRecord) -> _NormOp | None:
    ret = op.return_ms if op.return_ms is not None else _INF
    timed_out = op.return_ms is None
    if op.op == "put":
        value = _one_bytes(op.args, 0, "put value")
        return _NormOp(op.invoke_ms, ret, True, value, _NO_PRE, not timed_out)
    if op.op == "delete":
        if timed_out:
            return _NormOp(op.invoke_ms, ret, True, None, _NO_PRE, False)
        if not isinstance(op.result, tuple) or len(op.result) != 1:
            raise ValueError("bad delete result")
        existed = op.result[0]
        if not isinstance(existed, bool):
            raise ValueError("bad delete result")
        return _NormOp(op.invoke_ms, ret, True, None, ("present", existed), True)
    if op.op == "get":
        if timed_out:
            return None  # observed nothing; constrains nothing
        if (
            not isinstance(op.result, tuple)
            or len(op.result) != 2
            or not isinstance(op.result[0], bool)
            or not isinstance(op.result[1], bytes)
        ):
            raise ValueError("bad get result")
        found = op.result[0]
        value = op.result[1]
        assert isinstance(found, bool) and isinstance(value, bytes)
        return _NormOp(op.invoke_ms, ret, False, value if found else None, _NO_PRE, True)
    if op.op == "cas":
        if (
            not isinstance(op.args, tuple)
            or len(op.args) != 2
            or (op.args[0] is not None and not isinstance(op.args[0], bytes))
            or not isinstance(op.args[1], bytes)
        ):
            raise ValueError("bad cas args")
        expected = op.args[0]
        new_value = op.args[1]
        assert expected is None or isinstance(expected, bytes)
        assert isinstance(new_value, bytes)
        if timed_out:
            return _NormOp(op.invoke_ms, ret, True, new_value, _NO_PRE, False)
        if (
            not isinstance(op.result, tuple)
            or len(op.result) != 2
            or not isinstance(op.result[0], bool)
            or (op.result[1] is not None and not isinstance(op.result[1], bytes))
        ):
            raise ValueError("bad cas result")
        success = op.result[0]
        current = op.result[1]
        assert isinstance(success, bool)
        assert current is None or isinstance(current, bytes)
        if success:
            return _NormOp(op.invoke_ms, ret, True, new_value, ("eq", expected), True)
        return _NormOp(op.invoke_ms, ret, False, current, _NO_PRE, True)
    raise ValueError(f"unknown op {op.op!r}")


def _one_bytes(args: tuple[object, ...], index: int, what: str) -> bytes:
    if len(args) != index + 1 or not isinstance(args[index], bytes):
        raise ValueError(f"bad {what}")
    value = args[index]
    assert isinstance(value, bytes)
    return value


def _pre_ok(pre: object, value: bytes | None) -> bool:
    if pre is _NO_PRE:
        return True
    if not isinstance(pre, tuple) or len(pre) != 2:
        raise ValueError("bad precondition")
    kind, want = pre
    if kind == "eq":
        return bool(value == want)
    if kind == "present":
        return bool((value is not None) == want)
    raise ValueError(f"unknown precondition {kind!r}")


def _strictly_before(a: _NormOp, b: _NormOp) -> bool:
    return a.ret < b.invoke


def _overlaps(a: _NormOp, b: _NormOp) -> bool:
    return a.invoke < b.ret and b.invoke < a.ret


def _dfs(
    ops: tuple[_NormOp, ...],
    must_mask: int,
    done_mask: int,
    value: bytes | None,
    memo: dict[tuple[int, bytes | None], bool],
) -> bool:
    if (done_mask & must_mask) == must_mask:
        return True
    key = (done_mask, value)
    if key in memo:
        return memo[key]
    n = len(ops)
    for i in range(n):
        if done_mask >> i & 1:
            continue
        op = ops[i]
        # Real-time precedence: everything completed before op started
        # must already be linearized.
        if any(not done_mask >> j & 1 and _strictly_before(ops[j], op) for j in range(n)):
            continue
        if op.is_write:
            if not _pre_ok(op.pre_eq, value):
                continue
            if _dfs(ops, must_mask, done_mask | (1 << i), op.value, memo):
                memo[key] = True
                return True
        else:
            if op.value == value and _dfs(ops, must_mask, done_mask | (1 << i), value, memo):
                memo[key] = True
                return True
            # Read from a concurrent unlinearized write: linearize both.
            for j in range(n):
                if done_mask >> j & 1:
                    continue
                w = ops[j]
                if not w.is_write or w.value != op.value or not _overlaps(w, op):
                    continue
                if any(not done_mask >> k & 1 and _strictly_before(ops[k], w) for k in range(n)):
                    continue
                if not _pre_ok(w.pre_eq, value):
                    continue
                if _dfs(ops, must_mask, done_mask | (1 << i) | (1 << j), op.value, memo):
                    memo[key] = True
                    return True
    memo[key] = False
    return False
