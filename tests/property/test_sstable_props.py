"""Property tests: SSTable write/read round-trip."""

import tempfile
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from minikv.storage.sstable import SSTableReader, SSTableWriter, SSTEntry
from minikv.storage.wal import RecordType

entry = st.builds(
    SSTEntry,
    key=st.binary(min_size=1, max_size=16),
    kind=st.sampled_from(list(RecordType)),
    value=st.binary(min_size=0, max_size=64),
).map(lambda e: SSTEntry(e.key, RecordType.DELETE, b"") if e.kind is RecordType.DELETE else e)


@given(
    st.lists(entry, min_size=1, max_size=100),
    st.integers(min_value=32, max_value=1024),
)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_roundtrip(entries: list[SSTEntry], block_bytes: int) -> None:
    # Writer needs strictly increasing, deduplicated keys.
    latest: dict[bytes, SSTEntry] = {}
    for e in entries:
        latest[e.key] = e
    want = [latest[k] for k in sorted(latest)]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "prop.sst"
        writer = SSTableWriter(path, block_bytes=block_bytes, expected_keys=len(want))
        for e in want:
            writer.add(e.key, e.kind, e.value)
        count, lo, hi = writer.finish()
        assert (count, lo, hi) == (len(want), want[0].key, want[-1].key)
        with SSTableReader(path) as reader:
            assert reader.entry_count == len(want)
            assert list(reader) == want
            for e in want:
                assert reader.get(e.key) == e
