"""Asyncio runtime driving the sans-IO Raft core (the M7 node).

Concurrency design (documented because it is load-bearing):
- ``_core_lock`` serializes every core interaction (ticks, proposes,
  message handling, state reads). Nothing else touches the core.
- Durability (Persist/SaveTermVote) runs in an executor under a separate
  ``_disk_lock`` so fsyncs never stall ticks, while FIFO lock fairness
  keeps disk order identical to core order.
- Sends go out concurrently (``asyncio.gather``); responses re-enter the
  core in order, looping until quiet (catch-up chains strictly shrink
  ``next_index`` or advance ``match_index``, so they terminate; a
  deferral backstop covers the pathological case without dropping).
- Applies flow through a FIFO queue into one consumer task, preserving
  global index order (required: CAS is order-sensitive). The consumer
  completes per-index client waiters, checkpoints raft progress, and
  tracks ``lsm_applied`` -- the durable apply point linearizable reads
  wait for (core ``last_applied`` only means "emitted").
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Sequence
from dataclasses import dataclass

from minikv.kv.state_machine import KVApplier
from minikv.raft.core import RaftCore, coalesce_actions
from minikv.raft.grpc_transport import GrpcTransport
from minikv.raft.logstore import LogStore
from minikv.raft.transport import RaftTransport
from minikv.raft.types import (
    Action,
    Apply,
    Message,
    NotLeader,
    Persist,
    RaftConfig,
    Role,
    SaveTermVote,
    Send,
)

TICK_MS = 10
_RPC_TIMEOUT_S = 0.5
_EXECUTE_ROUND_CAP = 64


@dataclass
class NodeStatus:
    node_id: str
    role: str
    term: int
    leader_id: str | None
    leader_addr: str | None
    commit_index: int
    last_applied: int
    log_last_index: int


@dataclass
class ProposeApplied:
    result: bytes


@dataclass
class ProposeRedirect:
    leader_addr: str | None


ProposeOutcome = ProposeApplied | ProposeRedirect


@dataclass
class ReadReady:
    read_index: int


@dataclass
class ReadRedirect:
    leader_addr: str | None


ReadOutcome = ReadReady | ReadRedirect


@dataclass
class _Pending:
    payload: bytes
    future: asyncio.Future[ProposeOutcome]
    cancelled: bool = False


@dataclass
class _ApplyItem:
    index: int
    payload: bytes


class RaftNode:
    """One cluster member. ``peers`` maps every member id (including self)
    to its address; only the others are dialed."""

    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        config: RaftConfig,
        store: LogStore,
        applier: KVApplier,
        transport: RaftTransport | None = None,
        batching: bool = True,
        tick_ms: int = TICK_MS,
    ) -> None:
        if node_id not in peers:
            raise ValueError(f"node {node_id} missing from peer list")
        self._id = node_id
        self._addrs = dict(peers)
        self._peer_ids = [p for p in peers if p != node_id]
        self._config = config
        self._store = store
        self._applier = applier
        self._transport = transport or GrpcTransport(
            {p: peers[p] for p in self._peer_ids}, timeout_s=_RPC_TIMEOUT_S
        )
        self._batch_window_s = (config.batch_window_ms / 1000) if batching else 0.0
        self._tick_ms = tick_ms
        self._core: RaftCore | None = None
        self._core_lock = asyncio.Lock()
        self._disk_lock = asyncio.Lock()
        self._pending: list[_Pending] = []
        self._pending_event = asyncio.Event()
        self._waiters: dict[int, list[asyncio.Future[ProposeOutcome]]] = {}
        self._apply_queue: asyncio.Queue[_ApplyItem] = asyncio.Queue()
        self._deferred: list[Action] = []
        self._lsm_applied = 0
        self._apply_error: BaseException | None = None
        self._saved_commit = -1
        self._saved_applied = -1
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = False

    @property
    def node_id(self) -> str:
        return self._id

    @property
    def applier(self) -> KVApplier:
        return self._applier

    async def start(self) -> None:
        """Recover stores, rebuild the core, launch tick/batch/apply loops."""
        self._store.open()
        self._applier.open()
        self._core = RaftCore(self._id, self._peer_ids, self._store, random.Random(), self._config)
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self._tick_loop(), name=f"raft-tick-{self._id}"),
            loop.create_task(self._batch_loop(), name=f"raft-batch-{self._id}"),
            loop.create_task(self._apply_loop(), name=f"raft-apply-{self._id}"),
        ]

    async def stop(self) -> None:
        self._stopping = True
        if self._tasks:
            tick, batch, apply = self._tasks
            tick.cancel()
            batch.cancel()
            # Drain remaining applies before releasing the engines: the core
            # still holds them (applied < commit is recoverable, but a clean
            # close should not abandon work).
            await self._apply_queue.join()
            apply.cancel()
            for task in self._tasks:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            self._tasks = []
        async with self._core_lock:
            for futures in self._waiters.values():
                for future in futures:
                    if not future.done():
                        future.set_exception(ConnectionError("node is stopping"))
            self._waiters.clear()
            for pending in self._pending:
                if not pending.future.done():
                    pending.future.set_exception(ConnectionError("node is stopping"))
            self._pending.clear()
        self._applier.close()
        self._store.close()
        await self._transport.close()
        if self._apply_error is not None:
            raise self._apply_error

    async def status(self) -> NodeStatus:
        async with self._core_lock:
            core = self._require_core()
            leader_id = core.leader_id
            return NodeStatus(
                node_id=self._id,
                role=core.role.value,
                term=core.term,
                leader_id=leader_id,
                leader_addr=self._addrs.get(leader_id) if leader_id else None,
                commit_index=core.commit_index,
                last_applied=core.last_applied,
                log_last_index=len(core.log),
            )

    async def propose_command(self, payload: bytes, timeout_s: float) -> ProposeOutcome:
        """Propose a client payload; resolve when it applies (or redirect)."""
        if self._stopping:
            raise RuntimeError("node is stopping")
        if self._apply_error is not None:
            raise RuntimeError(f"applier halted: {self._apply_error}")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[ProposeOutcome] = loop.create_future()
        entry = _Pending(payload, future)
        async with self._core_lock:
            self._require_core()
            self._pending.append(entry)
            self._pending_event.set()
        try:
            return await asyncio.wait_for(future, timeout_s)
        except TimeoutError:
            entry.cancelled = True
            raise

    async def read_index(self, timeout_s: float) -> ReadOutcome:
        """ReadIndex barrier: confirm leadership via a majority heartbeat
        round, then return the commit point the caller must wait out."""
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("read-index timed out")
            async with self._core_lock:
                core = self._require_core()
                if core.role is not Role.LEADER:
                    return ReadRedirect(self._leader_addr_of(core.leader_id))
                term0 = core.term
                read_idx = core.commit_index
                heartbeats = [s for s in core.heartbeat(self._now_ms()) if isinstance(s, Send)]
            responses = await asyncio.gather(
                *(self._safe_send(s.to, s.message) for s in heartbeats),
                return_exceptions=True,
            )
            followups: list[Action] = []
            async with self._core_lock:
                core = self._require_core()
                acks = 1  # self
                for send, response in zip(heartbeats, responses, strict=True):
                    if isinstance(response, BaseException) or response is None:
                        continue
                    # Stale replies are harmless: the core ignores term
                    # mismatches, and only same-term replies ack.
                    followups.extend(core.handle_message(self._now_ms(), response, sender=send.to))
                    if response.term == term0:
                        acks += 1
                if core.role is not Role.LEADER or core.term != term0:
                    await self._execute(followups)
                    return ReadRedirect(self._leader_addr_of(core.leader_id))
                confirmed = acks >= (len(self._peer_ids) + 1) // 2 + 1
            await self._execute(followups)
            if confirmed:
                return ReadReady(read_idx)
            await asyncio.sleep(0.01)

    async def wait_applied(self, read_idx: int, timeout_s: float) -> None:
        """Wait until the LSM has durably applied ``read_idx``."""
        deadline = time.monotonic() + timeout_s
        while self._lsm_applied < read_idx:
            if time.monotonic() >= deadline:
                raise TimeoutError("wait for apply timed out")
            await asyncio.sleep(0.005)

    async def handle_raft_message(self, message: Message, sender: str) -> Message:
        """Execute one inbound Raft RPC; return its response message."""
        if self._stopping:
            raise RuntimeError("node is stopping")
        async with self._core_lock:
            core = self._require_core()
            actions = core.handle_message(self._now_ms(), message, sender=sender)
            persist = [a for a in actions if isinstance(a, (Persist, SaveTermVote))]
            sends = [a for a in actions if isinstance(a, Send)]
            applies = [a for a in actions if isinstance(a, Apply)]
            await self._persist_locked(persist)
            response = next((s.message for s in sends if s.to == sender), None)
            for item in applies:
                self._apply_queue.put_nowait(_ApplyItem(item.index, item.payload))
            extra = [s for s in sends if s.to != sender]
        for send in extra:
            await self._send_and_feed(send)
        if response is None:
            raise RuntimeError(f"no response produced for message from {sender}")
        return response

    async def _tick_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self._tick_ms / 1000)
            async with self._core_lock:
                if self._core is None or self._stopping:
                    return
                actions = self._core.tick(self._now_ms())
            await self._execute(actions)

    async def _batch_loop(self) -> None:
        while not self._stopping:
            await self._pending_event.wait()
            self._pending_event.clear()
            if self._batch_window_s > 0:
                await asyncio.sleep(self._batch_window_s)
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        async with self._core_lock:
            if self._core is None or self._stopping:
                return
            core = self._core
            batch, self._pending = self._pending, []
            batch = [e for e in batch if not e.cancelled and not e.future.done()]
            if not batch:
                return
            actions: list[Action] = []
            for entry in batch:
                result, acts = core.propose(self._now_ms(), entry.payload)
                if isinstance(result, NotLeader):
                    if not entry.future.done():
                        entry.future.set_result(
                            ProposeRedirect(self._leader_addr_of(result.leader_id))
                        )
                else:
                    actions.extend(acts)
                    self._waiters.setdefault(result.index, []).append(entry.future)
            # Lone nodes (and only they) can advance the commit here: with
            # peers, responses drive it; without, nothing else ever would.
            actions.extend(core.check_commit())
        await self._execute(coalesce_actions(actions))

    async def _apply_loop(self) -> None:
        # Runs until cancelled by stop(): the queue is the only source of
        # work, and stop() drains it first, so no stopping flag is needed.
        while True:
            try:
                item = await self._apply_queue.get()
            except asyncio.CancelledError:
                return
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, self._applier.apply, item.index, item.payload
                )
                self._lsm_applied = max(self._lsm_applied, item.index)
                async with self._core_lock:
                    if self._core is None:
                        return
                    for future in self._waiters.pop(item.index, []):
                        if not future.done():
                            future.set_result(ProposeApplied(result))
                    await self._save_progress_locked(self._core.commit_index, item.index)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 -- fail-stop below, nothing to do
                # A poisoned entry cannot be skipped (order is sacred):
                # fail everything pending and halt the applier (fail-stop).
                self._apply_error = e
                async with self._core_lock:
                    for futures in self._waiters.values():
                        for future in futures:
                            if not future.done():
                                future.set_exception(e)
                    self._waiters.clear()
                return
            finally:
                self._apply_queue.task_done()

    async def _execute(self, actions: list[Action]) -> None:
        """Persist, send concurrently, feed responses until quiet.

        A round cap guards against pathological chatter; leftovers are
        deferred to the next call (all action kinds are idempotent to
        re-run: persists rewrite, sends re-send, applies deduplicate by
        index order).
        """
        if self._deferred:
            actions = self._deferred + actions
            self._deferred = []
        rounds = 0
        pending = actions
        while pending:
            persist = [a for a in pending if isinstance(a, (Persist, SaveTermVote))]
            sends = [a for a in pending if isinstance(a, Send)]
            applies = [a for a in pending if isinstance(a, Apply)]
            for item in applies:
                self._apply_queue.put_nowait(_ApplyItem(item.index, item.payload))
            if persist:
                await self._persist_actions(persist)
            followups: list[Action] = []
            if sends:
                responses = await asyncio.gather(*(self._safe_send(s.to, s.message) for s in sends))
                async with self._core_lock:
                    if self._core is None:
                        return
                    core = self._core
                    for send, response in zip(sends, responses, strict=True):
                        if response is None:
                            continue
                        followups.extend(
                            core.handle_message(self._now_ms(), response, sender=send.to)
                        )
            pending = followups
            rounds += 1
            if rounds > _EXECUTE_ROUND_CAP:
                self._deferred = pending
                break

    async def _send_and_feed(self, send: Send) -> None:
        response = await self._safe_send(send.to, send.message)
        if response is None:
            return
        async with self._core_lock:
            if self._core is None:
                return
            actions = self._core.handle_message(self._now_ms(), response, sender=send.to)
        await self._execute(actions)

    async def _persist_actions(self, actions: Sequence[Action]) -> None:
        async with self._disk_lock:
            loop = asyncio.get_running_loop()
            for action in actions:
                if isinstance(action, Persist):
                    await loop.run_in_executor(
                        None, self._store.persist, action.base_index, action.entries
                    )
                else:
                    assert isinstance(action, SaveTermVote)
                    await loop.run_in_executor(
                        None, self._store.save_term_vote, action.term, action.voted_for
                    )

    async def _persist_locked(self, actions: Sequence[Action]) -> None:
        # Callers hold _core_lock; disk goes through the shared disk lock in
        # an executor so fsyncs never stall other core interactions.
        await self._persist_actions(actions)

    async def _save_progress_locked(self, commit: int, applied: int) -> None:
        if (commit, applied) == (self._saved_commit, self._saved_applied):
            return
        self._saved_commit, self._saved_applied = commit, applied
        loop = asyncio.get_running_loop()
        async with self._disk_lock:
            await loop.run_in_executor(None, self._store.save_progress, commit, applied)

    def _leader_addr_of(self, leader_id: str | None) -> str | None:
        return self._addrs.get(leader_id) if leader_id else None

    async def _safe_send(self, to: str, message: Message) -> Message | None:
        # A transport must return None when unreachable, but never trust
        # it: anything short of cancellation becomes unreachable here so a
        # dead peer cannot kill the tick loop.
        try:
            return await self._transport.send(to, message)
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    def _require_core(self) -> RaftCore:
        if self._core is None:
            raise RuntimeError("node is not running")
        return self._core

    @staticmethod
    def _now_ms() -> int:
        return int(time.monotonic() * 1000)
