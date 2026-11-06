# PostgreSQL reference (not shipped as a driver)

The library ships only the SQLite store. Implement the `OutboxStore` protocol for PostgreSQL with the
schema and claim query below. They mirror the SQLite store and are reference material: this repository
does not run them in its tests.

## DDL

```sql
CREATE TABLE outbox (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    msg_key TEXT,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'published', 'dead')),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_claim_idx ON outbox (status, next_attempt_at);
CREATE INDEX outbox_key_idx ON outbox (msg_key, status, seq);
```

## Claim with `FOR UPDATE SKIP LOCKED`

Parameters: `$1` now, `$2` lease duration (interval), `$3` limit, `$4` lease token.

```sql
WITH candidates AS (
    SELECT e.seq
    FROM outbox AS e
    WHERE e.status = 'pending'
      AND e.next_attempt_at <= $1
      AND (e.lease_token IS NULL OR e.lease_expires_at <= $1)
      AND NOT EXISTS (
          SELECT 1 FROM outbox AS o
          WHERE o.msg_key = e.msg_key AND o.status = 'pending' AND o.seq < e.seq
      )
    ORDER BY e.seq
    LIMIT $3
    FOR UPDATE OF e SKIP LOCKED
)
UPDATE outbox AS t
SET lease_token = $4, lease_expires_at = $1 + $2
FROM candidates
WHERE t.seq = candidates.seq
RETURNING t.seq, t.event_id, t.topic, t.msg_key, t.payload, t.headers,
          t.created_at, t.attempts, t.status;
```

Sort the returned rows by `seq` before publishing. `mark_published` and `mark_failed` should run in a
transaction that selects the row `FOR UPDATE` and applies the same four ordered checks as the SQLite store
(row exists, status pending, token equal, `lease_expires_at > now`), raising `lease_lost` otherwise.

Caveat: a row skipped because another relay holds a lock is still visible to the `NOT EXISTS` subquery as
pending, so a later event with the same key is never claimed ahead of it.
