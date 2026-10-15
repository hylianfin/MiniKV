"""Skip list written from scratch (no third-party data structure libraries).

Backs the memtable: ordered ``bytes`` keys with average O(log n) operations.
Max level 16, promotion probability 0.5. The RNG is injectable so tests
(and later the simulator) can seed it; production code passes ``None``.

Memory accounting is approximate on purpose: each node charges
``len(key) + value_size(value) + 8 * level + _NODE_OVERHEAD_BYTES``.
It only drives memtable flush decisions, so tracking data volume
monotonically matters more than exactness.
"""

from __future__ import annotations

import random
import sys
from collections.abc import Callable, Iterator

MAX_LEVEL = 16
_PROMOTE_PROBABILITY = 0.5
_NODE_OVERHEAD_BYTES = 32


def _default_value_size[V](value: V) -> int:
    """Best-effort size: exact for bytes, ``sys.getsizeof`` fallback."""
    if isinstance(value, (bytes, bytearray)):
        return len(value)
    return sys.getsizeof(value)


class _Node[V]:
    """One tower in the skip list. The head holds ``b""`` and no value."""

    __slots__ = ("forward", "key", "value")

    def __init__(self, key: bytes, value: V | None, level: int) -> None:
        self.key = key
        self.value = value
        self.forward: list[_Node[V] | None] = [None] * level


class SkipList[V]:
    """Ordered map from ``bytes`` to ``V``. ``None`` values are not supported
    (``get`` returns ``None`` for a missing key). Not thread-safe."""

    def __init__(
        self,
        rng: random.Random | None = None,
        value_size: Callable[[V], int] | None = None,
    ) -> None:
        self._rng = rng if rng is not None else random.Random()
        self._value_size = value_size if value_size is not None else _default_value_size
        self._head: _Node[V] = _Node(b"", None, MAX_LEVEL)
        self._level = 1
        self._count = 0
        self._size_bytes = 0

    def __len__(self) -> int:
        return self._count

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (bytes, bytearray)):
            return False
        return self._find_node(bytes(key)) is not None

    @property
    def memory_size(self) -> int:
        """Approximate bytes held, for flush decisions."""
        return self._size_bytes

    @property
    def level(self) -> int:
        """Current height (mostly useful for tests)."""
        return self._level

    def put(self, key: bytes, value: V) -> None:
        key = self._checked_key(key)
        update: list[_Node[V]] = [self._head] * MAX_LEVEL
        self._fill_update(key, update)
        existing = update[0].forward[0]
        if existing is not None and existing.key == key:
            # Update in place: tower shape (and its pointer cost) is unchanged.
            self._size_bytes -= self._node_size(existing)
            existing.value = value
            self._size_bytes += self._node_size(existing)
            return
        new_level = self._random_level()
        if new_level > self._level:
            for i in range(self._level, new_level):
                update[i] = self._head
            self._level = new_level
        node = _Node(key, value, new_level)
        for i in range(new_level):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node
        self._count += 1
        self._size_bytes += self._node_size(node)

    def get(self, key: bytes) -> V | None:
        node = self._find_node(self._checked_key(key))
        if node is None:
            return None
        value = node.value
        assert value is not None  # only the head holds None
        return value

    def delete(self, key: bytes) -> bool:
        """Physical removal. Returns False if the key was absent.

        Note: the memtable does NOT use this (it writes tombstones so that
        deletes shadow older SSTables); it exists for direct users/tests.
        """
        key = self._checked_key(key)
        update: list[_Node[V]] = [self._head] * MAX_LEVEL
        self._fill_update(key, update)
        target = update[0].forward[0]
        if target is None or target.key != key:
            return False
        for i in range(self._level):
            if update[i].forward[i] is not target:
                break
            update[i].forward[i] = target.forward[i]
        self._size_bytes -= self._node_size(target)
        self._count -= 1
        while self._level > 1 and self._head.forward[self._level - 1] is None:
            self._level -= 1
        return True

    def items(self) -> Iterator[tuple[bytes, V]]:
        cur = self._head.forward[0]
        while cur is not None:
            value = cur.value
            assert value is not None  # only the head holds None
            yield (cur.key, value)
            cur = cur.forward[0]

    def seek(self, key: bytes) -> Iterator[tuple[bytes, V]]:
        """Yield ``(key, value)`` pairs with ``k >= key`` in order."""
        start = self._checked_key(key)
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.forward[i]
            while nxt is not None and nxt.key < start:
                node = nxt
                nxt = node.forward[i]
        cur = node.forward[0]
        while cur is not None:
            value = cur.value
            assert value is not None  # only the head holds None
            yield (cur.key, value)
            cur = cur.forward[0]

    def _fill_update(self, key: bytes, update: list[_Node[V]]) -> None:
        """Set ``update[i]`` to the last node at level ``i`` before ``key``."""
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.forward[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.forward[i]
            update[i] = node

    def _find_node(self, key: bytes) -> _Node[V] | None:
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.forward[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.forward[i]
        cand = node.forward[0]
        if cand is not None and cand.key == key:
            return cand
        return None

    def _random_level(self) -> int:
        level = 1
        while level < MAX_LEVEL and self._rng.random() < _PROMOTE_PROBABILITY:
            level += 1
        return level

    def _node_size(self, node: _Node[V]) -> int:
        value = node.value
        content = 0 if value is None else self._value_size(value)
        return len(node.key) + content + 8 * len(node.forward) + _NODE_OVERHEAD_BYTES

    @staticmethod
    def _checked_key(key: object) -> bytes:
        if not isinstance(key, (bytes, bytearray)):
            raise TypeError("key must be bytes")
        return bytes(key)
