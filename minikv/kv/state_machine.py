"""KV state machine: deterministic command execution over the LSM engine.

Commands use a small fixed binary format (no protobuf dependency here)::

    PUT:    [op=1 u8][klen u32][key][vlen u32][value]
    DELETE: [op=2 u8][klen u32][key]
    CAS:    [op=3 u8][klen u32][key][absent u8][elen u32][expected][vlen u32][new]

Requests wrap a command with the deduplication context::

    [idlen u16][client_id utf8][seq u64][command...]

``KVStateMachine`` executes one command against the data engine.
``KVApplier`` is the full replicated-state-machine entry point: session
check first (cached result on retry), then data write, then session record.
Data first, session second: a crash between them re-applies idempotently
(all ops are last-writer-wins), while the reverse order could acknowledge
data that was never written.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

from minikv.kv.sessions import SessionTable
from minikv.storage.lsm import LSMEngine, LSMOptions

_OP_PUT = 1
_OP_DELETE = 2
_OP_CAS = 3
_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")


@dataclass(frozen=True, slots=True)
class RequestContext:
    client_id: str
    request_seq: int


@dataclass(frozen=True, slots=True)
class PutCommand:
    key: bytes
    value: bytes


@dataclass(frozen=True, slots=True)
class DeleteCommand:
    key: bytes


@dataclass(frozen=True, slots=True)
class CasCommand:
    key: bytes
    expected: bytes | None  # None means "must be absent"
    new_value: bytes


Command = PutCommand | DeleteCommand | CasCommand


@dataclass(frozen=True, slots=True)
class PutResult:
    pass


@dataclass(frozen=True, slots=True)
class DeleteResult:
    existed: bool


@dataclass(frozen=True, slots=True)
class CasResult:
    success: bool
    current: bytes | None  # None means absent


CommandResult = PutResult | DeleteResult | CasResult


def encode_command(command: Command) -> bytes:
    if isinstance(command, PutCommand):
        return (
            _U8.pack(_OP_PUT)
            + _U32.pack(len(command.key))
            + command.key
            + _U32.pack(len(command.value))
            + command.value
        )
    if isinstance(command, DeleteCommand):
        return _U8.pack(_OP_DELETE) + _U32.pack(len(command.key)) + command.key
    if isinstance(command, CasCommand):
        if command.expected is None:
            expect = _U8.pack(1) + _U32.pack(0)
        else:
            expect = _U8.pack(0) + _U32.pack(len(command.expected)) + command.expected
        return (
            _U8.pack(_OP_CAS)
            + _U32.pack(len(command.key))
            + command.key
            + expect
            + _U32.pack(len(command.new_value))
            + command.new_value
        )
    raise TypeError(f"unknown command type: {type(command)}")


def decode_command(raw: bytes) -> Command:
    if not raw:
        raise ValueError("empty command")
    op = raw[0]
    if op == _OP_PUT:
        key, pos = _take_bytes(raw, 1)
        value, end = _take_bytes(raw, pos)
        _expect_end(raw, end)
        return PutCommand(key, value)
    if op == _OP_DELETE:
        key, end = _take_bytes(raw, 1)
        _expect_end(raw, end)
        return DeleteCommand(key)
    if op == _OP_CAS:
        key, pos = _take_bytes(raw, 1)
        if pos >= len(raw):
            raise ValueError("truncated CAS")
        absent = raw[pos]
        if absent not in (0, 1):
            raise ValueError("bad CAS absent flag")
        pos += 1
        expected, pos = _take_bytes(raw, pos)
        new_value, end = _take_bytes(raw, pos)
        _expect_end(raw, end)
        return CasCommand(key, None if absent else expected, new_value)
    raise ValueError(f"unknown op {op}")


def encode_result(result: CommandResult) -> bytes:
    if isinstance(result, PutResult):
        return _U8.pack(0)
    if isinstance(result, DeleteResult):
        return _U8.pack(1) + _U8.pack(1 if result.existed else 0)
    if isinstance(result, CasResult):
        if result.current is None:
            return _U8.pack(2) + _U8.pack(0) + _U8.pack(0)
        return (
            _U8.pack(2)
            + _U8.pack(1 if result.success else 0)
            + _U8.pack(1)
            + _U32.pack(len(result.current))
            + result.current
        )
    raise TypeError(f"unknown result type: {type(result)}")


def decode_result(raw: bytes) -> CommandResult:
    if not raw:
        raise ValueError("empty result")
    tag = raw[0]
    if tag == 0:
        _expect_end(raw, 1)
        return PutResult()
    if tag == 1:
        if len(raw) != 2 or raw[1] not in (0, 1):
            raise ValueError("bad delete result")
        return DeleteResult(existed=bool(raw[1]))
    if tag == 2:
        if len(raw) < 3 or raw[1] not in (0, 1) or raw[2] not in (0, 1):
            raise ValueError("bad CAS result flags")
        if raw[2] == 0:  # absent: no value bytes follow
            _expect_end(raw, 3)
            return CasResult(success=bool(raw[1]), current=None)
        current, end = _take_bytes(raw, 3)
        _expect_end(raw, end)
        return CasResult(success=bool(raw[1]), current=current)
    raise ValueError(f"unknown result tag {tag}")


def encode_request(ctx: RequestContext, command: Command) -> bytes:
    client = ctx.client_id.encode("utf-8")
    if len(client) > 0xFFFF:
        raise ValueError("client_id too long")
    if ctx.request_seq < 0:
        raise ValueError("request_seq must be >= 0")
    return _U16.pack(len(client)) + client + _U64.pack(ctx.request_seq) + encode_command(command)


def decode_request(raw: bytes) -> tuple[RequestContext, Command]:
    if len(raw) < 2:
        raise ValueError("truncated request")
    (idlen,) = _U16.unpack_from(raw, 0)
    if len(raw) < 2 + idlen + 8:
        raise ValueError("truncated request")
    try:
        client_id = bytes(raw[2 : 2 + idlen]).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError(f"bad client_id: {e}") from e
    (seq,) = _U64.unpack_from(raw, 2 + idlen)
    return RequestContext(client_id, seq), decode_command(raw[2 + idlen + 8 :])


def _take_bytes(raw: bytes, pos: int) -> tuple[bytes, int]:
    if len(raw) < pos + 4:
        raise ValueError("truncated field")
    (size,) = _U32.unpack_from(raw, pos)
    pos += 4
    if len(raw) < pos + size:
        raise ValueError("truncated field")
    return bytes(raw[pos : pos + size]), pos + size


def _expect_end(raw: bytes, end: int) -> None:
    if end != len(raw):
        raise ValueError("trailing bytes")


class KVStateMachine:
    """Executes decoded commands against the data LSM engine."""

    def __init__(self, engine: LSMEngine) -> None:
        self._engine = engine

    def apply(self, raft_index: int, command: Command) -> CommandResult:
        """Apply once. Re-applying the same command is state-safe (all ops
        are last-writer-wins); only CAS results can differ, which is why
        the applier consults sessions first."""
        if isinstance(command, PutCommand):
            self._engine.put(command.key, command.value, raft_index)
            return PutResult()
        if isinstance(command, DeleteCommand):
            existed = self._engine.get(command.key) is not None
            self._engine.delete(command.key, raft_index)
            return DeleteResult(existed)
        if isinstance(command, CasCommand):
            current = self._engine.get(command.key)
            if command.expected is None:
                if current is not None:
                    return CasResult(False, current)
            elif current != command.expected:
                return CasResult(False, current)
            self._engine.put(command.key, command.new_value, raft_index)
            return CasResult(True, command.new_value)
        raise TypeError(f"unknown command type: {type(command)}")


class KVApplier:
    """The replicated state machine: sessions + data, crash-safe.

    Owns two LSM engines (``<dir>/data`` and ``<dir>/sessions``).
    Lifecycle mirrors the engines: ``open`` / ``crash`` (abandon without
    flushing, like kill -9) / ``reopen`` (same directories) / ``close``.
    """

    def __init__(self, directory: Path | str, options: LSMOptions | None = None) -> None:
        self._dir = Path(directory)
        self._options = options or LSMOptions()
        self._data: LSMEngine | None = None
        self._sessions: SessionTable | None = None
        self._session_engine: LSMEngine | None = None

    @property
    def data(self) -> LSMEngine:
        if self._data is None:
            raise RuntimeError("applier is not open")
        return self._data

    def open(self) -> None:
        if self._data is not None:
            raise RuntimeError("applier is already open")
        data = LSMEngine(self._dir / "data", self._options)
        data.open()
        sessions = LSMEngine(self._dir / "sessions", self._options)
        sessions.open()
        self._data = data
        self._session_engine = sessions
        self._sessions = SessionTable(sessions)

    def apply(self, raft_index: int, payload: bytes) -> bytes:
        """Apply one committed Raft payload; return the encoded result.
        Empty payloads (Raft NOOPs) are no-ops returning ``b""``."""
        if self._data is None or self._sessions is None:
            raise RuntimeError("applier is not open")
        if not payload:
            return b""
        ctx, command = decode_request(payload)
        record = self._sessions.lookup(ctx.client_id)
        if record is not None and record.seq == ctx.request_seq:
            return record.result  # retried request: exactly-once
        if record is None or ctx.request_seq > record.seq:
            result = KVStateMachine(self._data).apply(raft_index, command)
            encoded = encode_result(result)
            self._sessions.store(ctx.client_id, ctx.request_seq, encoded)
            return encoded
        # Stale sequence (older than recorded): re-execute without recording.
        # Unreachable through Raft (same-client entries apply in seq order),
        # but state-safe if it ever happens.
        return encode_result(KVStateMachine(self._data).apply(raft_index, command))

    def crash(self) -> None:
        """Abandon both engines without flushing (simulates kill -9)."""
        if self._data is not None:
            self._data.crash()
            self._data = None
        if self._session_engine is not None:
            self._session_engine.crash()
            self._session_engine = None
        self._sessions = None

    def reopen(self) -> None:
        """Reopen after ``crash`` (or a process restart) on the same dirs."""
        if self._data is not None:
            raise RuntimeError("applier is already open")
        self.open()

    def close(self) -> None:
        if self._data is not None:
            self._data.close()
            self._data = None
        if self._session_engine is not None:
            self._session_engine.close()
            self._session_engine = None
        self._sessions = None
