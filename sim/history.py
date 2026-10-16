"""Operation histories for linearizability checking.

An OpRecord captures one client operation's real-time interval plus its
outcome. Timed-out operations (``return_ms is None``) observed nothing and
may have taken effect at any point after ``invoke_ms`` -- or never.
Histories serialize to JSON so chaos runs can be checked offline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class OpRecord:
    client: str
    key: bytes
    op: str  # put | get | delete | cas
    args: tuple[object, ...]
    # put: (value: bytes,)
    # get: ()
    # delete: ()
    # cas: (expected: bytes | None, new_value: bytes)
    invoke_ms: float
    return_ms: float | None  # None = timed out, result unknown
    result: object
    # put: None (ack)
    # get: (found: bool, value: bytes)
    # delete: (existed: bool,)
    # cas: (success: bool, current: bytes | None)


@dataclass
class History:
    records: list[OpRecord] = field(default_factory=list)

    def append(self, record: OpRecord) -> None:
        self.records.append(record)

    def per_key(self) -> dict[bytes, list[OpRecord]]:
        out: dict[bytes, list[OpRecord]] = {}
        for record in self.records:
            out.setdefault(record.key, []).append(record)
        return out

    def to_json(self) -> str:
        return json.dumps([_record_to_json(r) for r in self.records], sort_keys=True)

    @classmethod
    def from_json(cls, blob: str) -> History:
        history = cls()
        for raw in json.loads(blob):
            history.append(_record_from_json(raw))
        return history


def _bytes_to_hex(value: object) -> object:
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, tuple):
        return [_bytes_to_hex(v) for v in value]
    return value


def _hex_to_bytes(value: object) -> object:
    if isinstance(value, dict) and set(value.keys()) == {"$bytes"}:
        return bytes.fromhex(value["$bytes"])
    if isinstance(value, list):
        return tuple(_hex_to_bytes(v) for v in value)
    return value


def _record_to_json(record: OpRecord) -> dict[str, object]:
    return {
        "client": record.client,
        "key": record.key.hex(),
        "op": record.op,
        "args": _bytes_to_hex(record.args),
        "invoke_ms": record.invoke_ms,
        "return_ms": record.return_ms,
        "result": _bytes_to_hex(record.result),
    }


def _record_from_json(raw: dict[str, object]) -> OpRecord:
    key = raw["key"]
    assert isinstance(key, str)
    op = raw["op"]
    assert isinstance(op, str)
    client = raw["client"]
    assert isinstance(client, str)
    invoke = raw["invoke_ms"]
    assert isinstance(invoke, (int, float))
    ret = raw["return_ms"]
    assert ret is None or isinstance(ret, (int, float))
    args = _hex_to_bytes(raw["args"])
    assert isinstance(args, tuple)
    return OpRecord(
        client=client,
        key=bytes.fromhex(key),
        op=op,
        args=args,
        invoke_ms=float(invoke),
        return_ms=None if ret is None else float(ret),
        result=_hex_to_bytes(raw["result"]),
    )
