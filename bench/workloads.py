"""Workload definitions and deterministic key/value generation."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Workload:
    """One timed phase. Wall-clock bounded so runs take predictable time."""

    name: str
    kind: str  # write_only | mixed | read_only | read_miss
    value_size: int  # bytes per value
    concurrency: int
    duration_s: float
    keyspace: int  # keys are k000000..k{keyspace-1}
    read_mode: str = "linearizable"  # linearizable | stale (reads only)


def key_for(rng: random.Random, keyspace: int) -> bytes:
    return f"k{rng.randrange(keyspace):06d}".encode()


def value_for(rng: random.Random, size: int) -> bytes:
    # Deterministic pseudo-random bytes (not compressible-trivially).
    return bytes(rng.randrange(256) for _ in range(size))


def pick_op(rng: random.Random, kind: str) -> str:
    if kind == "write_only":
        return "put"
    if kind == "read_only" or kind == "read_miss":
        return "get"
    return "put" if rng.random() < 0.5 else "get"  # mixed 50/50
