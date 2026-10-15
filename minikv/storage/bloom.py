"""Bloom filter written from scratch (no third-party libraries).

Sized at ``bits_per_key`` (default 10); the probe count derives from the
standard optimum ``k = round(bits_per_key * ln 2)`` (7 at 10 bits/key).
Positions come from double hashing over a single ``blake2b`` digest.
Guarantees: no false negatives; false positive rate near the theoretical
``(1 - e^(-k*n/m))^k`` for the sized capacity (degrades gracefully past it).
"""

from __future__ import annotations

import hashlib
import struct

LN2 = 0.6931471805599453
_HEADER = struct.Struct("<QI")  # num_bits u64, num_probes u32


class BloomFilter:
    """Probabilistic membership set over ``bytes`` keys. Not thread-safe."""

    def __init__(self, num_bits: int, num_probes: int) -> None:
        if num_bits <= 0:
            raise ValueError("num_bits must be positive")
        if num_probes < 1:
            raise ValueError("num_probes must be >= 1")
        self._num_bits = num_bits
        self._num_probes = num_probes
        self._bits = bytearray((num_bits + 7) // 8)

    @classmethod
    def sized(cls, n_keys: int, bits_per_key: int = 10) -> BloomFilter:
        if n_keys < 0:
            raise ValueError("n_keys must be >= 0")
        if bits_per_key < 1:
            raise ValueError("bits_per_key must be >= 1")
        num_bits = max(64, n_keys * bits_per_key)
        num_probes = max(1, round(bits_per_key * LN2))
        return cls(num_bits, num_probes)

    @property
    def num_bits(self) -> int:
        return self._num_bits

    @property
    def num_probes(self) -> int:
        return self._num_probes

    def add(self, key: bytes) -> None:
        for pos in self._positions(bytes(key)):
            self._bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (bytes, bytearray)):
            return False
        return all(self._bits[pos // 8] & (1 << (pos % 8)) for pos in self._positions(bytes(key)))

    def estimated_fpr(self, n_keys: int) -> float:
        """Theoretical false positive rate at ``n_keys`` inserted keys."""
        import math

        if n_keys <= 0:
            return 0.0
        fill = 1.0 - math.exp(-self._num_probes * n_keys / self._num_bits)
        return fill**self._num_probes

    def to_bytes(self) -> bytes:
        return _HEADER.pack(self._num_bits, self._num_probes) + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilter:
        if len(data) < _HEADER.size:
            raise ValueError("bloom block too short")
        num_bits, num_probes = _HEADER.unpack_from(data, 0)
        if num_bits <= 0 or num_probes < 1 or num_probes > 30:
            raise ValueError("bloom block has invalid parameters")
        expect = (num_bits + 7) // 8
        if len(data) != _HEADER.size + expect:
            raise ValueError("bloom block size mismatch")
        filtr = cls(num_bits, num_probes)
        filtr._bits = bytearray(data[_HEADER.size :])
        return filtr

    def _positions(self, key: bytes) -> list[int]:
        # One 128-bit digest feeds double hashing; forcing h2 odd keeps the
        # probe sequence from cycling through a divisor of num_bits.
        digest = hashlib.blake2b(key, digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little") | 1
        return [(h1 + i * h2) % self._num_bits for i in range(self._num_probes)]
