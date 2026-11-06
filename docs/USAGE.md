# Usage and semantics

`transactional_outbox` writes events into a table in the same database transaction as your
business data and relays them later with leases, jittered backoff and dead-lettering.

## Quick start

```python
import sqlite3
from datetime import datetime, timezone

from transactional_outbox import Relay, SqliteOutboxStore
from transactional_outbox.testing import RecordingPublisher


def clock() -> datetime:
    return datetime.now(timezone.utc)


store = SqliteOutboxStore("outbox.db", clock)
conn = sqlite3.connect("outbox.db")
store.create_schema(conn)
store.enqueue(conn, "orders", {"id": 1}, key="order-1")  # joins the caller's transaction
conn.commit()  # the business write and the event commit together

relay = Relay(store, RecordingPublisher(), clock)
print(relay.relay_once())  # RelayStats(published=1, failed=0, dead=0)
```

The library has no daemon loop: call `relay_once()` from your scheduler, worker or a loop you own.

## State machine

- `pending -> published` (terminal; a published row is never rewritten).
- `pending -> dead` after `max_attempts` failed deliveries.
- `dead -> pending` only through `requeue_dead()`, which resets `attempts` to 0 and makes the row eligible now.

`mark_published` and `mark_failed` check, in order: the row exists, its status is `pending`, the lease token
matches, and `lease_expires_at > now`. Any failure raises `OutboxError` with code `lease_lost` and leaves the
row unchanged. A row exactly at `next_attempt_at` is eligible.

## Delivery guarantees

- Delivery is **at least once**. A crash after `publish` but before `mark_published`, or a lease that
  expires during `publish`, causes redelivery. Consumers must deduplicate on `event_id`.
- Events with the same non-`None` key are delivered in `seq` order: `claim` skips a row while an older
  pending row with the same key exists, including one that is backing off or leased by another relay.
  Events with key `None` are not ordered.
- **Dead rows do not block their key.** After a dead-letter, later events with the same key flow, which
  breaks per-key order for that key. Requeue dead rows deliberately.
- Backoff is full jitter (AWS Architecture Blog, "Exponential Backoff And Jitter"): after the n-th failure
  the delay is uniform in `[0, min(max_delay, base_delay * 2 ** (n - 1))]`.

## Limits

| Item | Limit | Error |
|---|---|---|
| Payload | dict tree, at most 262144 JSON bytes (`json.dumps(sort_keys=True, separators=(",", ":"), allow_nan=False)`), depth 32 | `payload_invalid`, `payload_too_large` |
| Payload values | `str`, `int`, finite `float`, `bool`, `None`, `list`, `dict` with `str` keys; tuples and other types are rejected | `payload_invalid` |
| Topic | `[a-z0-9][a-z0-9._-]{0,127}` | `invalid_topic` |
| Key | `None` or at most 1024 characters | `payload_invalid` |
| Headers | at most 32 entries, names and values at most 1024 characters | `payload_invalid` |
| Table name | `^[a-z_][a-z0-9_]{0,62}$` | `invalid_table` |
| Rows | `max_rows` caps pending plus dead rows; published rows are removed by `purge_published` | `outbox_full` |
| Clock | timezone-aware datetimes only | `invalid_clock` |

Argument errors that are not data errors (for example `batch_size=0` or an in-memory SQLite path) raise
`ValueError`.

## Operational notes

- `last_error` holds `ExceptionType: message` with the message cut to 200 characters and no traceback.
  The message itself may still contain text from your broker client; do not put credentials in exception
  messages.
- `RelayEvent` (the `on_event` hook) carries only kind, event id, topic, attempt count and error type.
- `SqliteOutboxStore` opens a short connection per call. Use a database file; `:memory:` is rejected
  because it cannot be shared between connections.
- `claim` uses `BEGIN IMMEDIATE` and a busy timeout of 30 seconds, so concurrent relays serialize their
  claims and never lease the same row at the same time.
- Published rows stay until `purge_published(older_than)` removes them (by publication time).
