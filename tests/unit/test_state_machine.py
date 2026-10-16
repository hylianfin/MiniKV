"""Unit tests for the KV state machine, applier, and codecs."""

import pytest

from minikv.kv.state_machine import (
    CasCommand,
    CasResult,
    DeleteCommand,
    DeleteResult,
    KVApplier,
    KVStateMachine,
    PutCommand,
    PutResult,
    RequestContext,
    decode_command,
    decode_request,
    decode_result,
    encode_command,
    encode_request,
    encode_result,
)
from minikv.storage.lsm import LSMOptions


def _applier(tmp_path, **kw: object) -> KVApplier:  # type: ignore[no-untyped-def]
    opts = LSMOptions(memtable_bytes=512, **kw)  # type: ignore[arg-type]
    applier = KVApplier(tmp_path / "kv", opts)
    applier.open()
    return applier


def test_command_codec_roundtrips() -> None:
    commands = [
        PutCommand(b"", b""),
        PutCommand(b"k", b"v" * 100),
        DeleteCommand(b"k"),
        CasCommand(b"k", None, b"new"),
        CasCommand(b"k", b"", b"new"),
        CasCommand(b"k", b"exp", b""),
    ]
    for command in commands:
        assert decode_command(encode_command(command)) == command


def test_command_codec_rejects_garbage() -> None:
    for blob in [
        b"",
        b"\x09trailing-op",
        b"\x01",
        b"\x01\x05\x00\x00\x00abc",  # truncated key
        b"\x01\x01\x00\x00\x00k\x01\x00\x00\x00vTRAIL",  # trailing bytes
        b"\x03\x01\x00\x00\x00k",  # truncated right after the key
        b"\x03\x01\x00\x00\x00k\x02",  # bad CAS flag
        b"\x03\x01\x00\x00\x00k\x00",  # truncated after flag
    ]:
        with pytest.raises(ValueError):
            decode_command(blob)
    with pytest.raises(TypeError):
        encode_command("nope")  # type: ignore[arg-type]


def test_result_codec_roundtrips() -> None:
    results = [
        PutResult(),
        DeleteResult(True),
        DeleteResult(False),
        CasResult(True, b"new"),
        CasResult(False, b"cur"),
        CasResult(False, None),
    ]
    for result in results:
        assert decode_result(encode_result(result)) == result


def test_result_codec_rejects_garbage() -> None:
    for blob in [b"", b"\x07", b"\x01", b"\x01\x02", b"\x02\x00", b"\x02\x01\x01"]:
        with pytest.raises(ValueError):
            decode_result(blob)
    with pytest.raises(TypeError):
        encode_result("nope")  # type: ignore[arg-type]


def test_request_codec() -> None:
    ctx = RequestContext("client-1", 41)
    payload = encode_request(ctx, PutCommand(b"k", b"v"))
    back_ctx, back_cmd = decode_request(payload)
    assert (back_ctx, back_cmd) == (ctx, PutCommand(b"k", b"v"))
    with pytest.raises(ValueError):
        decode_request(b"\x01")
    with pytest.raises(ValueError):
        decode_request(b"\xff\xffabc")  # truncated id
    with pytest.raises(ValueError):
        decode_request(b"\x02\x00\xff\xfe" + b"\x00" * 8)  # bad client_id bytes
    with pytest.raises(ValueError):
        encode_request(RequestContext("c", -1), PutCommand(b"k", b"v"))
    with pytest.raises(ValueError):
        encode_request(RequestContext("c" * 0x10000, 0), PutCommand(b"k", b"v"))


def test_apply_rejects_unknown_command(tmp_path) -> None:
    applier = _applier(tmp_path)
    try:
        sm = KVStateMachine(applier.data)
        with pytest.raises(TypeError):
            sm.apply(1, "nope")  # type: ignore[arg-type]
        with pytest.raises(RuntimeError):
            _ = KVApplier(tmp_path / "unopened").data
    finally:
        applier.close()


def test_crud_and_cas_semantics(tmp_path) -> None:
    applier = _applier(tmp_path)
    try:
        sm = KVStateMachine(applier.data)
        assert sm.apply(1, PutCommand(b"k", b"v1")) == PutResult()
        assert applier.data.get(b"k") == b"v1"
        assert sm.apply(2, CasCommand(b"k", b"wrong", b"v2")) == CasResult(False, b"v1")
        assert sm.apply(3, CasCommand(b"k", b"v1", b"v2")) == CasResult(True, b"v2")
        assert sm.apply(4, CasCommand(b"absent", None, b"n")) == CasResult(True, b"n")
        assert sm.apply(5, CasCommand(b"k", None, b"n")) == CasResult(False, b"v2")
        assert sm.apply(6, DeleteCommand(b"k")) == DeleteResult(True)
        assert sm.apply(7, DeleteCommand(b"k")) == DeleteResult(False)
        assert sm.apply(8, CasCommand(b"k", None, b"back")) == CasResult(True, b"back")
    finally:
        applier.close()


def test_reapply_is_state_safe(tmp_path) -> None:
    applier = _applier(tmp_path)
    try:
        payloads = [
            encode_request(RequestContext("c", 1), PutCommand(b"k", b"v1")),
            encode_request(RequestContext("c", 2), CasCommand(b"k", b"v1", b"v2")),
            encode_request(RequestContext("c", 3), DeleteCommand(b"gone")),
        ]
        for index, payload in enumerate(payloads, start=1):
            applier.apply(index, payload)
        before = dict(applier.data.scan(None, None))
        for index, payload in enumerate(payloads, start=1):  # replay everything
            applier.apply(index, payload)
        assert dict(applier.data.scan(None, None)) == before
    finally:
        applier.close()


def test_restart_resume_and_noop(tmp_path) -> None:
    path = tmp_path / "kv"
    applier = KVApplier(path, LSMOptions(memtable_bytes=512))
    applier.open()
    applier.apply(1, encode_request(RequestContext("c", 1), PutCommand(b"k", b"v")))
    assert applier.apply(2, b"") == b""  # Raft NOOP: no-op
    assert applier.data.applied_index() == 1  # NOOP carries no raft index
    applier.close()

    resumed = KVApplier(path, LSMOptions(memtable_bytes=512))
    resumed.open()
    try:
        assert resumed.data.applied_index() == 1
        resumed.apply(2, encode_request(RequestContext("c", 2), PutCommand(b"k2", b"v2")))
        assert resumed.data.get(b"k") == b"v"
        assert resumed.data.get(b"k2") == b"v2"
    finally:
        resumed.close()


def test_applier_lifecycle(tmp_path) -> None:
    applier = KVApplier(tmp_path / "kv")
    with pytest.raises(RuntimeError):
        applier.apply(1, b"x")
    applier.open()
    with pytest.raises(RuntimeError):
        applier.open()
    with pytest.raises(RuntimeError):
        applier.reopen()
    applier.crash()
    applier.reopen()  # works after crash
    applier.close()
    applier.close()  # idempotent


def test_stale_seq_reexecutes_without_recording(tmp_path) -> None:
    applier = _applier(tmp_path)
    try:
        ctx5 = RequestContext("c", 5)
        first = applier.apply(1, encode_request(ctx5, PutCommand(b"k", b"v5")))
        # Older sequence arriving late: re-executed (state-safe), not recorded.
        stale = applier.apply(2, encode_request(RequestContext("c", 3), PutCommand(b"k", b"v3")))
        assert decode_result(stale) == PutResult()
        assert applier.data.get(b"k") == b"v3"
        # The recorded seq is still 5: a retry of it hits the cache.
        again = applier.apply(3, encode_request(ctx5, PutCommand(b"k", b"v5")))
        assert again == first
    finally:
        applier.close()
