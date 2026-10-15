"""Unit tests for the bloom filter."""

import pytest

from minikv.storage.bloom import BloomFilter


def test_basic_membership() -> None:
    b = BloomFilter.sized(100)
    assert b.num_probes == 7  # round(10 * ln2)
    assert b"" not in b
    b.add(b"hello")
    b.add(b"")
    assert b"hello" in b
    assert b"" in b
    assert "hello" not in b  # wrong type -> False
    assert b"other" not in b  # overwhelmingly likely; FPR test quantifies


def test_serialization_roundtrip() -> None:
    b = BloomFilter.sized(50)
    for i in range(50):
        b.add(f"key{i}".encode())
    clone = BloomFilter.from_bytes(b.to_bytes())
    assert clone.num_bits == b.num_bits
    assert clone.num_probes == b.num_probes
    for i in range(50):
        assert f"key{i}".encode() in clone


def test_empty_filter_serializes() -> None:
    clone = BloomFilter.from_bytes(BloomFilter.sized(0).to_bytes())
    assert b"anything" not in clone


def test_from_bytes_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        BloomFilter.from_bytes(b"short")
    with pytest.raises(ValueError):
        BloomFilter.from_bytes(BloomFilter.sized(10).to_bytes() + b"\x00")
    with pytest.raises(ValueError):
        BloomFilter.from_bytes(b"\x00" * 12)


def test_bad_parameters() -> None:
    with pytest.raises(ValueError):
        BloomFilter(0, 7)
    with pytest.raises(ValueError):
        BloomFilter(64, 0)
    with pytest.raises(ValueError):
        BloomFilter.sized(-1)
    with pytest.raises(ValueError):
        BloomFilter.sized(10, bits_per_key=0)
    assert BloomFilter.sized(10).estimated_fpr(0) == 0.0


def test_measured_fpr_within_2x_of_theory() -> None:
    n, probes = 1000, 10000
    b = BloomFilter.sized(n)
    inserted = [f"insert:{i}".encode() for i in range(n)]
    for k in inserted:
        b.add(k)
    for k in inserted:
        assert k in b  # no false negatives, ever
    fp = sum(1 for i in range(probes) if f"absent:{i}".encode() in b)
    theory = b.estimated_fpr(n)
    assert fp / probes <= 2 * theory, f"fpr {fp / probes:.4f} vs theory {theory:.4f}"
