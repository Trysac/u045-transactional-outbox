"""Test doubles for the ``Publisher`` port."""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable

from .models import OutboxEvent

__all__ = ["FailingPublisher", "RecordingPublisher"]


class RecordingPublisher:
    """Thread-safe publisher that remembers the last ``max_events`` events it received."""

    def __init__(self, max_events: int = 10_000) -> None:
        if max_events < 1:
            raise ValueError("max_events must be at least 1")
        self._events: deque[OutboxEvent] = deque(maxlen=max_events)
        self._lock = threading.Lock()

    @property
    def events(self) -> tuple[OutboxEvent, ...]:
        """The recorded events, oldest first."""
        with self._lock:
            return tuple(self._events)

    def publish(self, event: OutboxEvent) -> None:
        """Record ``event``."""
        with self._lock:
            self._events.append(event)


class FailingPublisher:
    """Publisher whose first ``fail_times`` calls raise, then records like ``RecordingPublisher``.

    ``error_factory`` builds the exception to raise; the default is a ``RuntimeError``.
    """

    def __init__(
        self,
        fail_times: int,
        error_factory: Callable[[], Exception] | None = None,
        max_events: int = 10_000,
    ) -> None:
        if fail_times < 0:
            raise ValueError("fail_times must not be negative")
        self._remaining = fail_times
        self._error_factory = error_factory
        self._recorder = RecordingPublisher(max_events)
        self._lock = threading.Lock()
        self._calls = 0

    @property
    def calls(self) -> int:
        """Total number of ``publish`` calls, failed or not."""
        with self._lock:
            return self._calls

    @property
    def events(self) -> tuple[OutboxEvent, ...]:
        """Events that were published successfully."""
        return self._recorder.events

    def publish(self, event: OutboxEvent) -> None:
        """Raise while failures remain, otherwise record ``event``."""
        with self._lock:
            self._calls += 1
            should_fail = self._remaining > 0
            if should_fail:
                self._remaining -= 1
        if should_fail:
            if self._error_factory is None:
                raise RuntimeError("simulated publish failure")
            raise self._error_factory()
        self._recorder.publish(event)
