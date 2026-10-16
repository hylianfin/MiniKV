"""Property tests: bloom filter (no false negatives, bounded FPR)."""

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from minikv.storage.bloom import BloomFilter

key_lists = st.lists(st.binary(min_size=0, max_size=16), min_size=1, max_size=300, unique=True)
# Ratio bounds need enough keys for the theory (~0.8%) to clear granularity.
sized_lists = st.lists(st.binary(min_size=0, max_size=16), min_size=50, max_size=300, unique=True)


@given(key_lists)
def test_no_false_negatives(keys: list[bytes]) -> None:
    b = BloomFilter.sized(len(keys))
    for k in keys:
        b.add(k)
    for k in keys:
        assert k in b
    assert BloomFilter.from_bytes(b.to_bytes()).num_bits == b.num_bits
    for k in keys:
        assert k in BloomFilter.from_bytes(b.to_bytes())


@given(sized_lists)
@settings(max_examples=10, suppress_health_check=[HealthCheck.too_slow])
def test_fpr_within_2x_of_theory(keys: list[bytes]) -> None:
    n = len(keys)
    b = BloomFilter.sized(n)
    for k in keys:
        b.add(k)
    present = set(keys)
    probes = 0
    fp = 0
    i = 0
    # 8000 probes concentrates the rate: P(measured > 2x theory) is ~1e-9
    # per example at these parameters, so this is not a flake source.
    while probes < 8000:
        candidate = f"probe:{i}".encode()
        i += 1
        if candidate in present:
            continue
        probes += 1
        fp += candidate in b
    assert fp / probes <= 2 * b.estimated_fpr(n) + 1e-6
