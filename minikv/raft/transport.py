"""Raft RPC transport abstraction.

The node runtime drives the sans-IO core and delivers its ``Send`` actions
through this interface; responses flow back into the core. ``None`` means
unreachable (timeout, refused, partitioned) -- the core simply retries on
the next tick. gRPC is one implementation; tests use in-memory fakes.
"""

from __future__ import annotations

from typing import Protocol

from minikv.raft.types import Message


class RaftTransport(Protocol):
    async def send(self, to: str, message: Message) -> Message | None:
        """Deliver one message; return the peer's response or None."""
        ...

    async def close(self) -> None:
        """Release channels/sockets. Idempotent."""
        ...
