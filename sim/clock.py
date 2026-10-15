"""Virtual clock: time only advances when the simulation advances it."""

from __future__ import annotations


class VirtualClock:
    """Monotonic millisecond clock driven explicitly by the harness."""

    def __init__(self, start_ms: int = 0) -> None:
        if start_ms < 0:
            raise ValueError("start_ms must be >= 0")
        self._now = start_ms

    @property
    def now_ms(self) -> int:
        return self._now

    def advance(self, delta_ms: int) -> int:
        if delta_ms < 0:
            raise ValueError("clocks do not run backwards")
        self._now += delta_ms
        return self._now
