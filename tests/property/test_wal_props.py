"""Property tests: WAL round-trip for arbitrary record bytes."""

import tempfile
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from minikv.storage.wal import WAL, FsyncPolicy, RecordType, WalRecord

kinds = st.sampled_from(list(RecordType))
record = st.builds(
    WalRecord,
    kind=kinds,
    key=st.binary(min_size=0, max_size=64),
    value=st.binary(min_size=0, max_size=256),
    raft_index=st.integers(min_value=0, max_value=10**9),
).map(
    # DELETE carries no value by construction.
    lambda r: (
        WalRecord(RecordType.DELETE, r.key, b"", r.raft_index) if r.kind is RecordType.DELETE else r
    )
)


@given(st.lists(record, max_size=50), st.sampled_from(list(FsyncPolicy)))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_roundtrip(records: list[WalRecord], policy: FsyncPolicy) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "prop.wal"
        wal = WAL(path, policy)
        assert wal.open() == []
        for rec in records:
            wal.append(rec)
        wal.close()
        reread = WAL(path, policy)
        assert reread.open() == records
        assert reread.record_count == len(records)
        reread.close()
