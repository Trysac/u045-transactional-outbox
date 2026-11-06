"""Relay that delivers claimed outbox events to a publisher."""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from ._time import delta_to_micros, require_aware
from .errors import OutboxError
from .models import OutboxEvent, RelayEvent, RelayEventKind, RelayStats
from .store import MAX_CLAIM_LIMIT, OutboxStore

logger = logging.getLogger(__name__)

_DEFAULT_BASE_DELAY = timedelta(seconds=1)
_DEFAULT_MAX_DELAY = timedelta(minutes=10)
_DEFAULT_LEASE = timedelta(seconds=30)
_MAX_ERROR_MESSAGE_CHARS = 200
_MAX_BACKOFF_SHIFT = 32


class Publisher(Protocol):
    """Port to the message broker. ``publish`` raises any exception to signal failure."""

    def publish(self, event: OutboxEvent) -> None:
        """Deliver ``event``; delivery is at least once, so consumers deduplicate on event_id."""


def describe_error(exc: BaseException) -> str:
    """Return ``Type: message`` with the message cut to 200 chars and no traceback."""
    name = type(exc).__name__
    try:
        message = str(exc)[:_MAX_ERROR_MESSAGE_CHARS]
    except Exception:
        message = ""
    return f"{name}: {message}" if message else name


class Relay:
    """Claims due events from a store and publishes them, one batch per ``relay_once`` call.

    Failures back off with full jitter (see the AWS Architecture Blog post "Exponential
    Backoff And Jitter"): the delay after the n-th failure is uniform in
    ``[0, min(max_delay, base_delay * 2 ** (n - 1))]``. After ``max_attempts`` failures the
    event becomes dead. If a lease expires while an event is being published, the outcome
    is not recorded and the event is redelivered later (at-least-once delivery).

    Raises:
        ValueError: if a numeric argument is out of range.
    """

    def __init__(
        self,
        store: OutboxStore,
        publisher: Publisher,
        clock: Callable[[], datetime],
        max_attempts: int = 8,
        base_delay: timedelta = _DEFAULT_BASE_DELAY,
        max_delay: timedelta = _DEFAULT_MAX_DELAY,
        lease: timedelta = _DEFAULT_LEASE,
        batch_size: int = 100,
        rng: random.Random | None = None,
        on_event: Callable[[RelayEvent], None] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if delta_to_micros(base_delay) < 1:
            raise ValueError("base_delay must be positive")
        if max_delay < base_delay:
            raise ValueError("max_delay must not be smaller than base_delay")
        if delta_to_micros(lease) < 1:
            raise ValueError("lease must be positive")
        if batch_size < 1 or batch_size > MAX_CLAIM_LIMIT:
            raise ValueError("batch_size must be between 1 and 10000")
        self._store = store
        self._publisher = publisher
        self._clock = clock
        self._max_attempts = max_attempts
        self._base_delay_us = delta_to_micros(base_delay)
        self._max_delay_us = delta_to_micros(max_delay)
        self._lease = lease
        self._batch_size = batch_size
        self._rng = rng if rng is not None else random.SystemRandom()
        self._on_event = on_event

    def _now(self) -> datetime:
        return require_aware(self._clock())

    def _backoff(self, attempts: int) -> timedelta:
        shift = min(attempts - 1, _MAX_BACKOFF_SHIFT)
        cap_us = min(self._max_delay_us, self._base_delay_us << shift)
        return timedelta(microseconds=round(self._rng.random() * cap_us))

    def _emit(self, event: RelayEvent) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event)
        except Exception as exc:
            logger.warning(
                "on_event callback failed for event %s: %s",
                event.event_id,
                type(exc).__name__,
            )

    def _record_success(self, lease_token: str, event: OutboxEvent) -> RelayEventKind | None:
        try:
            self._store.mark_published(lease_token, event.event_id)
        except OutboxError as err:
            if err.code != "lease_lost":
                raise
            logger.warning("lease lost for event %s; it will be redelivered", event.event_id)
            return None
        self._emit(
            RelayEvent(
                kind="published",
                event_id=event.event_id,
                topic=event.topic,
                attempts=event.attempts,
                error_type=None,
            )
        )
        return "published"

    def _record_failure(
        self,
        lease_token: str,
        event: OutboxEvent,
        exc: Exception,
    ) -> RelayEventKind | None:
        attempts = event.attempts + 1
        next_at: datetime | None = None
        kind: RelayEventKind = "dead"
        if attempts < self._max_attempts:
            next_at = self._now() + self._backoff(attempts)
            kind = "failed"
        try:
            self._store.mark_failed(lease_token, event.event_id, describe_error(exc), next_at)
        except OutboxError as err:
            if err.code != "lease_lost":
                raise
            logger.warning("lease lost for event %s; it will be redelivered", event.event_id)
            return None
        self._emit(
            RelayEvent(
                kind=kind,
                event_id=event.event_id,
                topic=event.topic,
                attempts=attempts,
                error_type=type(exc).__name__,
            )
        )
        return kind

    def relay_once(self) -> RelayStats:
        """Claim one batch, publish it in ``seq`` order and record every outcome.

        Raises:
            OutboxError: ``invalid_clock`` for a naive clock value; storage errors other than
                ``lease_lost`` propagate.
        """
        lease_token, events = self._store.claim(self._batch_size, self._now(), self._lease)
        counts = {"published": 0, "failed": 0, "dead": 0}
        for event in events:
            try:
                self._publisher.publish(event)
            except Exception as exc:
                kind = self._record_failure(lease_token, event, exc)
            else:
                kind = self._record_success(lease_token, event)
            if kind is not None:
                counts[kind] += 1
        return RelayStats(
            published=counts["published"],
            failed=counts["failed"],
            dead=counts["dead"],
        )
