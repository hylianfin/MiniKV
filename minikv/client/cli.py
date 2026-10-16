"""MiniKV command line: server + KV operations."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

import typer

from minikv import minikv_pb2 as pb2

app = typer.Typer(help="MiniKV: small Raft-replicated key-value store.")


def _run[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


def _nodes(value: str) -> list[str]:
    return [n.strip() for n in value.split(",") if n.strip()]


@app.command()
def server(
    id: str = typer.Option(..., "--id", help="This node's id (must match cluster.toml)"),
    config: Path = typer.Option(..., "--config", help="Path to cluster.toml"),
    data_dir: Path = typer.Option(..., "--data-dir", help="Node data directory"),
    batching: bool = typer.Option(True, "--batching/--no-batching", help="Group-commit batching"),
) -> None:
    """Run one MiniKV node."""
    from minikv.server.main import serve

    asyncio.run(serve(id, config, data_dir, batching=batching))


@app.command()
def put(
    key: str = typer.Argument(...),
    value: str = typer.Argument(...),
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
) -> None:
    """Put KEY VALUE (linearizable)."""
    from minikv.client.client import MiniKVClient

    async def main() -> None:
        client = MiniKVClient(_nodes(nodes))
        try:
            await client.put(key.encode(), value.encode())
        finally:
            await client.close()

    _run(main())
    typer.echo("ok")


@app.command()
def get(
    key: str = typer.Argument(...),
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
    stale: bool = typer.Option(False, "--stale", help="Read from any node, no guarantees"),
    log: bool = typer.Option(False, "--log", help="Serialize the read through the Raft log"),
) -> None:
    """Get KEY (linearizable by default)."""
    from minikv.client.client import MiniKVClient

    mode = pb2.READ_STALE if stale else (pb2.READ_LOG if log else pb2.READ_LINEARIZABLE)

    async def main() -> bytes | None:
        client = MiniKVClient(_nodes(nodes))
        try:
            return await client.get(key.encode(), mode=mode)
        finally:
            await client.close()

    value = _run(main())
    if value is None:
        typer.echo("(not found)")
    else:
        typer.echo(value.decode("utf-8", errors="replace"))


@app.command(name="del")
def delete(
    key: str = typer.Argument(...),
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
) -> None:
    """Delete KEY."""
    from minikv.client.client import MiniKVClient

    async def main() -> bool:
        client = MiniKVClient(_nodes(nodes))
        try:
            return await client.delete(key.encode())
        finally:
            await client.close()

    typer.echo("deleted" if _run(main()) else "(not found)")


@app.command()
def cas(
    key: str = typer.Argument(...),
    expected: str = typer.Argument(...),
    new: str = typer.Argument(...),
    absent: bool = typer.Option(False, "--absent", help="Expect the key to be absent"),
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
) -> None:
    """Compare-and-swap KEY from EXPECTED to NEW (or --absent for create)."""
    from minikv.client.client import MiniKVClient

    async def main() -> tuple[bool, bytes | None]:
        client = MiniKVClient(_nodes(nodes))
        try:
            result = await client.cas(
                key.encode(), None if absent else expected.encode(), new.encode()
            )
            return result.success, result.current
        finally:
            await client.close()

    success, current = _run(main())
    if success:
        typer.echo("ok")
    else:
        shown = "(absent)" if current is None else current.decode("utf-8", errors="replace")
        typer.echo(f"mismatch: current={shown}")
        raise typer.Exit(code=1)


@app.command()
def scan(
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
    start: str = typer.Option("", "--start", help="Start key (inclusive)"),
    end: str = typer.Option("", "--end", help="End key (exclusive)"),
    limit: int = typer.Option(0, "--limit", help="Max items (0 = all)"),
    stale: bool = typer.Option(False, "--stale", help="Read from any node, no guarantees"),
) -> None:
    """Scan a key range."""
    from minikv.client.client import MiniKVClient

    async def main() -> list[tuple[bytes, bytes]]:
        client = MiniKVClient(_nodes(nodes))
        try:
            return [
                item
                async for item in client.scan(
                    start.encode() or None,
                    end.encode() or None,
                    limit,
                    mode=pb2.READ_STALE if stale else pb2.READ_LINEARIZABLE,
                )
            ]
        finally:
            await client.close()

    for key, value in _run(main()):
        typer.echo(
            f"{key.decode('utf-8', errors='replace')}\t{value.decode('utf-8', errors='replace')}"
        )


@app.command()
def status(
    nodes: str = typer.Option(..., "--nodes", help="Comma-separated host:port list"),
) -> None:
    """Show every node's role, term, leader, and progress."""
    from minikv.client.client import MiniKVClient, NodeStatus

    async def main() -> list[NodeStatus]:
        client = MiniKVClient(_nodes(nodes))
        try:
            return await client.statuses()
        finally:
            await client.close()

    for info in _run(main()):
        typer.echo(
            f"{info.node_id} role={info.role} term={info.term} "
            f"leader={info.leader_id or '-'} commit={info.commit_index} "
            f"applied={info.last_applied} log={info.log_last_index}"
        )
