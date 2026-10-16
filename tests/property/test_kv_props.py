"""Property tests: KV command/result/request codec round-trips."""

from hypothesis import given
from hypothesis import strategies as st

from minikv.kv.state_machine import (
    CasCommand,
    CasResult,
    Command,
    CommandResult,
    DeleteCommand,
    DeleteResult,
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

keys = st.binary(min_size=0, max_size=32)
vals = st.binary(min_size=0, max_size=64)
commands = st.one_of(
    st.builds(PutCommand, key=keys, value=vals),
    st.builds(DeleteCommand, key=keys),
    st.builds(CasCommand, key=keys, expected=st.none() | vals, new_value=vals),
)
results = st.one_of(
    st.just(PutResult()),
    st.builds(DeleteResult, existed=st.booleans()),
    st.builds(CasResult, success=st.booleans(), current=st.none() | vals),
)
contexts = st.builds(
    RequestContext,
    client_id=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",)), min_size=0, max_size=16
    ),
    request_seq=st.integers(min_value=0, max_value=2**64 - 1),
)


@given(commands)
def test_command_roundtrip(command: Command) -> None:
    assert decode_command(encode_command(command)) == command


@given(results)
def test_result_roundtrip(result: CommandResult) -> None:
    # (True, None) is unencodable by construction; the applier never makes it.
    if isinstance(result, CasResult) and result.success and result.current is None:
        return
    assert decode_result(encode_result(result)) == result


@given(contexts, commands)
def test_request_roundtrip(ctx: RequestContext, command: Command) -> None:
    assert decode_request(encode_request(ctx, command)) == (ctx, command)
