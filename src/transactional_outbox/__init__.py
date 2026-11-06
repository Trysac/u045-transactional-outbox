"""Transactional outbox: enqueue events in the caller's transaction, relay at least once.

Relaying uses leases, jittered backoff and dead-lettering.
"""

from __future__ import annotations

import logging

from .errors import OutboxError
from .models import OutboxEvent, RelayEvent, RelayStats
from .relay import Publisher, Relay
from .store import OutboxStore, SqliteOutboxStore

__all__ = [
    "OutboxError",
    "OutboxEvent",
    "OutboxStore",
    "Publisher",
    "Relay",
    "RelayEvent",
    "RelayStats",
    "SqliteOutboxStore",
    "__version__",
]

__version__ = "0.1.0"

logging.getLogger(__name__).addHandler(logging.NullHandler())
