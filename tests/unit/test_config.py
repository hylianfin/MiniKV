"""Unit tests for cluster.toml parsing."""

import pytest

from minikv.server.config import load_config

FULL = """
[cluster]
nodes = [
  { id = "n1", addr = "127.0.0.1:7001" },
  { id = "n2", addr = "127.0.0.1:7002" },
  { id = "n3", addr = "127.0.0.1:7003" },
]

[raft]
election_timeout_ms = [300, 600]
heartbeat_ms = 100
pre_vote = true
read_mode = "readindex"
batch_window_ms = 2
batching = false

[storage]
memtable_bytes = 4194304
fsync_policy = "always"
batch_interval_ms = 10
block_bytes = 4096
bloom_bits_per_key = 10
compaction_trigger = 4
"""

MINIMAL = """
[cluster]
nodes = [{ id = "n1", addr = "127.0.0.1:7001" }]
"""


def _write(tmp_path, text: str):  # type: ignore[no-untyped-def]
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "cluster.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_full_config(tmp_path) -> None:
    config = load_config(_write(tmp_path, FULL))
    assert config.peers() == {
        "n1": "127.0.0.1:7001",
        "n2": "127.0.0.1:7002",
        "n3": "127.0.0.1:7003",
    }
    assert (config.raft.election_timeout_min_ms, config.raft.election_timeout_max_ms) == (300, 600)
    assert config.raft.pre_vote is True
    assert config.batching is False
    assert config.storage.memtable_bytes == 4194304


def test_minimal_config_defaults(tmp_path) -> None:
    config = load_config(_write(tmp_path, MINIMAL))
    assert config.peers() == {"n1": "127.0.0.1:7001"}
    assert config.batching is True
    assert config.read_mode == "readindex"


def test_bad_configs(tmp_path) -> None:
    cases = [
        "not toml [[",
        "[cluster]\nnodes = []",
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }, { id = "n1", addr = "y" }]',
        '[cluster]\nnodes = [{ id = "n1" }]',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\nelection_timeout_ms = [600]',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\nelection_timeout_ms = ["a", "b"]',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\nread_mode = "bogus"',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\nbatching = "yes"',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\npre_vote = "yes"',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[raft]\nheartbeat_ms = true',
        '[cluster]\nnodes = [{ id = "n1", addr = "x" }]\n[storage]\nfsync_policy = "sometimes"',
        '[cluster]\nnodes = "nope"',
        'cluster = "nope"',
    ]
    for i, text in enumerate(cases):
        with pytest.raises(ValueError):
            load_config(_write(tmp_path / str(i), text))
    with pytest.raises(ValueError):
        load_config(tmp_path / "missing.toml")
