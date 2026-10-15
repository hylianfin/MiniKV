"""Unit tests for the WAL."""

from pathlib import Path

import pytest

from minikv.storage.wal import (
    WAL,
    CorruptWALError,
    FsyncPolicy,
    RecordType,
    WalRecord,
    decode_prefix,
    encode_record,
)

SAMPLE: list[WalRecord] = [
    WalRecord(RecordType.PUT, b"k1", b"v1", 1),
    WalRecord(RecordType.PUT, b"k2", b"x" * 100, 2),
    WalRecord(RecordType.DELETE, b"k1", b"", 3),
    WalRecord(RecordType.PUT, b"", b"empty-key", 4),
    WalRecord(RecordType.PUT, b"k3", b"", 5),
    WalRecord(RecordType.DELETE, b"missing", b"", 6),
]


def _write(path: Path, records: list[WalRecord], policy: FsyncPolicy) -> WAL:
    wal = WAL(path, policy)
    wal.open()
    for record in records:
        wal.append(record)
    wal.close()
    return wal


def _boundaries(records: list[WalRecord]) -> list[int]:
    ends: list[int] = []
    total = 0
    for record in records:
        total += len(encode_record(record))
        ends.append(total)
    return ends


def test_roundtrip_and_replay(tmp_path: Path) -> None:
    path = tmp_path / "test.wal"
    _write(path, SAMPLE, FsyncPolicy.ALWAYS)
    wal = WAL(path)
    assert wal.open() == SAMPLE
    assert wal.record_count == len(SAMPLE)
    wal.close()


def test_empty_file_replays_nothing(tmp_path: Path) -> None:
    wal = WAL(tmp_path / "empty.wal")
    assert wal.open() == []
    wal.close()


def test_short_header_is_torn_tail(tmp_path: Path) -> None:
    path = tmp_path / "t.wal"
    path.write_bytes(b"\x01\x02\x03")  # fewer than 8 header bytes
    assert WAL(path).open() == []


def test_garbage_first_record_raises(tmp_path: Path) -> None:
    path = tmp_path / "g.wal"
    path.write_bytes(b"this is not a wal record, long enough to parse badly....")
    with pytest.raises(CorruptWALError):
        WAL(path).open()


def test_truncation_at_every_offset_yields_a_valid_prefix(tmp_path: Path) -> None:
    full = b"".join(encode_record(r) for r in SAMPLE)
    ends = _boundaries(SAMPLE)
    assert len(full) > 100  # the test is vacuous on tiny files
    for off in range(len(full)):
        path = tmp_path / f"t{off}.wal"
        path.write_bytes(full[:off])
        replayed = WAL(path).open()
        expected = [r for r, end in zip(SAMPLE, ends, strict=True) if end <= off]
        assert replayed == expected, f"offset {off}"
        # The torn tail must be gone from the file itself.
        expected_size = ends[len(expected) - 1] if expected else 0
        assert path.stat().st_size == expected_size


def test_single_byte_corruption_keeps_valid_prefix(tmp_path: Path) -> None:
    full = bytearray(b"".join(encode_record(r) for r in SAMPLE))
    ends = _boundaries(SAMPLE)
    for pos in range(len(full)):
        path = tmp_path / f"c{pos}.wal"
        corrupted = bytearray(full)
        corrupted[pos] ^= 0xFF
        path.write_bytes(bytes(corrupted))
        # Which record did we hit?
        hit = next(i for i, end in enumerate(ends) if pos < end)
        if hit == 0:
            # No valid record precedes the damage. A fully-present but
            # invalid record is ambiguous and must raise; damage that
            # mimics a torn tail (e.g. a corrupted length running past
            # EOF) replays as empty, which is also safe.
            try:
                assert WAL(path).open() == []
            except CorruptWALError:
                pass
        else:
            assert WAL(path).open() == SAMPLE[:hit], f"pos {pos}"


def test_corrupt_middle_record_drops_its_suffix(tmp_path: Path) -> None:
    full = bytearray(b"".join(encode_record(r) for r in SAMPLE))
    ends = _boundaries(SAMPLE)
    # Flip a key byte inside record 1 (a PUT): CRC must fail there.
    pos = ends[0] + 8 + 9 + 4  # header + type/raft + klen
    full[pos] ^= 0xFF
    path = tmp_path / "mid.wal"
    path.write_bytes(bytes(full))
    assert WAL(path).open() == SAMPLE[:1]
    assert path.stat().st_size == ends[0]


def test_decode_prefix_reports_valid_end() -> None:
    full = b"".join(encode_record(r) for r in SAMPLE)
    records, valid_end = decode_prefix(full + b"\x99\x99")
    assert records == SAMPLE
    assert valid_end == len(full)


@pytest.mark.parametrize("policy", list(FsyncPolicy))
def test_fsync_policies_roundtrip(tmp_path: Path, policy: FsyncPolicy) -> None:
    path = tmp_path / "p.wal"
    wal = WAL(path, policy)
    wal.open()
    for record in SAMPLE:
        wal.append(record)
    wal.fsync()  # batch-mode commit point; must hold buffered data
    wal.close()
    assert WAL(path, policy).open() == SAMPLE


def test_validation() -> None:
    wal = WAL("/tmp/never-opened.wal")
    with pytest.raises(RuntimeError):
        wal.append(SAMPLE[0])  # not open
    with pytest.raises(RuntimeError):
        wal.fsync()
    wal.close()  # closing a never-opened WAL is fine
    with pytest.raises(ValueError):
        encode_record(WalRecord(RecordType.PUT, b"k" * 1025, b"v", 0))
    with pytest.raises(ValueError):
        encode_record(WalRecord(RecordType.PUT, b"k", b"v", -1))
    with pytest.raises(ValueError):
        encode_record(WalRecord(RecordType.DELETE, b"k", b"nonempty", 0))
    with pytest.raises(TypeError):
        encode_record(WalRecord(42, b"k", b"v", 0))  # type: ignore[arg-type]


def test_double_open_raises(tmp_path: Path) -> None:
    wal = WAL(tmp_path / "d.wal")
    wal.open()
    with pytest.raises(RuntimeError):
        wal.open()
    wal.close()


def test_open_creates_parent_dirs(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "dir" / "w.wal"
    wal = WAL(path)
    assert wal.open() == []
    wal.append_put(b"k", b"v", raft_index=7)
    wal.close()
    assert WAL(path).open() == [WalRecord(RecordType.PUT, b"k", b"v", 7)]


def test_malformed_body_with_valid_crc(tmp_path: Path) -> None:
    """Bodies that pass the CRC but fail structural checks.

    Each case is tried alone (must raise: nothing valid precedes it) and
    after one good record (must truncate to that prefix).
    """
    import struct
    import zlib

    def craft(body: bytes) -> bytes:
        length_bytes = struct.pack("<I", len(body))
        crc = zlib.crc32(length_bytes + body) & 0xFFFFFFFF
        return struct.pack("<II", crc, len(body)) + body

    good = encode_record(SAMPLE[0])
    valid_body = encode_record(SAMPLE[1])[8:]
    bad_bodies = [
        b"\x03" + b"\x00" * 16,  # unknown type
        struct.pack("<BQ", 1, 0) + struct.pack("<I", 9999) + b"01234567",  # klen > max
        struct.pack("<BQ", 1, 0) + struct.pack("<I", 10) + b"0123456789",  # vlen missing
        struct.pack("<BQ", 1, 0)
        + struct.pack("<I", 1)
        + b"k"
        + struct.pack("<I", 9999999),  # vlen > max
        valid_body + b"\x00",  # trailing garbage inside the claimed length
        struct.pack("<BQ", 2, 0)
        + struct.pack("<I", 1)
        + b"k"
        + struct.pack("<I", 1)
        + b"v",  # DELETE with value
        b"\x01\x02",  # shorter than the minimum body
    ]
    for i, body in enumerate(bad_bodies):
        blob = craft(body)
        first = tmp_path / f"bad{i}.wal"
        first.write_bytes(blob)
        with pytest.raises(CorruptWALError):
            WAL(first).open()
        second = tmp_path / f"badpre{i}.wal"
        second.write_bytes(good + blob)
        assert WAL(second).open() == SAMPLE[:1]
        assert second.stat().st_size == len(good)


def test_helper_validation_and_properties(tmp_path: Path) -> None:
    wal = WAL(tmp_path / "meta.wal", FsyncPolicy.BATCH)
    assert wal.path == tmp_path / "meta.wal"
    assert wal.fsync_policy is FsyncPolicy.BATCH
    assert wal.size_bytes == 0  # file does not exist yet
    wal.open()
    with pytest.raises(TypeError):
        wal.append_put("k", b"v", raft_index=0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        wal.append_put(b"k", "v", raft_index=0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        wal.append_delete(b"k" * 2000, raft_index=0)
    with pytest.raises(ValueError):
        wal.append_put(b"k", b"v" * (1024 * 1024 + 1), raft_index=0)
    with pytest.raises(ValueError):
        wal.append_put(b"k", b"v", raft_index=-1)
    wal.append_put(b"k", b"v", raft_index=0)
    wal.fsync()  # batch policy buffers; size is visible only after flush
    assert wal.size_bytes > 0  # file exists now
    wal.close()


def test_append_helpers_and_context_manager(tmp_path: Path) -> None:
    path = tmp_path / "h.wal"
    with WAL(path, FsyncPolicy.BATCH) as wal:
        wal.append_put(b"k", b"v", raft_index=1)
        wal.append_delete(b"k", raft_index=2)
        assert wal.record_count == 2
    assert WAL(path).open() == [
        WalRecord(RecordType.PUT, b"k", b"v", 1),
        WalRecord(RecordType.DELETE, b"k", b"", 2),
    ]
