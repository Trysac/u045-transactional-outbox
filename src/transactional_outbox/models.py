"""Immutable data types shared by the store and the relay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

Status = Literal["pending", "published", "dead"]
RelayEventKind = Literal["published", "failed", "dead"]


@dataclass(frozen=True)
class OutboxEvent:
    """An event as stored in the outbox.

    Attributes:
        event_id: Unique id; consumers deduplicate on it (delivery is at least once).
        seq: Monotonic sequence number that defines delivery order.
        topic: Destination topic.
        key: Optional ordering key; events sharing a key are delivered in order.
        payload: JSON-compatible dict, equal to the enqueued payload.
        headers: String headers.
        created_at: UTC time of the enqueue.
        attempts: Number of failed delivery attempts recorded so far.
        status: Row status.
    """

    event_id: str
    seq: int
    topic: str
    key: str | None
    payload: Mapping[str, Any]
    headers: Mapping[str, str]
    created_at: datetime
    attempts: int
    status: Status


@dataclass(frozen=True)
class RelayStats:
    """Outcome counters of one ``Relay.relay_once`` call.

    ``failed`` counts failures that will be retried, ``dead`` counts events moved to the
    dead state. Events whose lease was lost are not counted.
    """

    published: int
    failed: int
    dead: int


@dataclass(frozen=True)
class RelayEvent:
    """Observability notification; it never carries payloads, headers or messages.

    ``attempts`` is the number of failed attempts recorded on the row after the operation.
    """

    kind: RelayEventKind
    event_id: str
    topic: str
    attempts: int
    error_type: str | None
