"""Cluster configuration (TOML) shared by servers.

``read_mode`` is validated but advisory: every RPC carries its own
``ReadMode`` and the server honors the request (D21). ``batching`` is an
optional extra key (default true) plus a CLI override for the M9
with/without comparison.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from minikv.raft.types import RaftConfig
from minikv.storage.lsm import LSMOptions
from minikv.storage.wal import FsyncPolicy

READ_MODES = ("readindex", "log", "stale")


@dataclass(frozen=True, slots=True)
class NodeAddress:
    id: str
    addr: str


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    nodes: tuple[NodeAddress, ...]
    raft: RaftConfig
    read_mode: str = "readindex"
    batching: bool = True
    storage: LSMOptions = field(default_factory=LSMOptions)

    def peers(self) -> dict[str, str]:
        return {n.id: n.addr for n in self.nodes}


def load_config(path: Path | str) -> ClusterConfig:
    """Parse and validate ``cluster.toml``; raise ValueError on problems."""
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ValueError(f"cannot read config {path}: {e}") from e
    cluster = raw.get("cluster", {})
    raft = raw.get("raft", {})
    storage = raw.get("storage", {})
    if not isinstance(cluster, dict) or not isinstance(raft, dict) or not isinstance(storage, dict):
        raise ValueError("config sections must be tables")

    nodes_raw = cluster.get("nodes", [])
    if not isinstance(nodes_raw, list) or not nodes_raw:
        raise ValueError("[cluster] nodes must be a non-empty list")
    nodes: list[NodeAddress] = []
    for entry in nodes_raw:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("id"), str)
            or not isinstance(entry.get("addr"), str)
            or not entry["id"]
            or not entry["addr"]
        ):
            raise ValueError("each node needs string id and addr")
        nodes.append(NodeAddress(entry["id"], entry["addr"]))
    if len({n.id for n in nodes}) != len(nodes):
        raise ValueError("duplicate node ids")

    election = raft.get("election_timeout_ms", [300, 600])
    if (
        not isinstance(election, list)
        or len(election) != 2
        or not all(isinstance(v, int) for v in election)
    ):
        raise ValueError("[raft] election_timeout_ms must be [min, max]")
    read_mode = raft.get("read_mode", "readindex")
    if read_mode not in READ_MODES:
        raise ValueError(f"[raft] read_mode must be one of {READ_MODES}")
    batching = raft.get("batching", True)
    if not isinstance(batching, bool):
        raise ValueError("[raft] batching must be a bool")
    raft_config = RaftConfig(
        election_timeout_min_ms=election[0],
        election_timeout_max_ms=election[1],
        heartbeat_ms=_require_int(raft, "heartbeat_ms", 100),
        pre_vote=_require_bool(raft, "pre_vote", True),
        batch_window_ms=_require_int(raft, "batch_window_ms", 2),
    )

    fsync_raw = storage.get("fsync_policy", "always")
    try:
        fsync_policy = FsyncPolicy(fsync_raw)
    except ValueError:
        raise ValueError("[storage] fsync_policy must be always, batch or never") from None
    storage_options = LSMOptions(
        memtable_bytes=_require_int(storage, "memtable_bytes", 4 * 1024 * 1024),
        fsync_policy=fsync_policy,
        batch_interval_ms=_require_int(storage, "batch_interval_ms", 10),
        block_bytes=_require_int(storage, "block_bytes", 4096),
        bloom_bits_per_key=_require_int(storage, "bloom_bits_per_key", 10),
        bloom_enabled=_require_bool(storage, "bloom_enabled", True),
        compaction_trigger=_require_int(storage, "compaction_trigger", 4),
    )
    return ClusterConfig(
        nodes=tuple(nodes),
        raft=raft_config,
        read_mode=read_mode,
        batching=batching,
        storage=storage_options,
    )


def _require_int(section: dict[object, object], key: str, default: int) -> int:
    value = section.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"[{key}] must be an integer")
    return value


def _require_bool(section: dict[object, object], key: str, default: bool) -> bool:
    value = section.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"[{key}] must be a bool")
    return value
