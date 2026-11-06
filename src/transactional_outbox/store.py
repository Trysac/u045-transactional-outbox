"""Storage port and the SQLite implementation of the outbox."""

from __future__ import annotations

import hmac
import json
import logging
import secrets
import sqlite3
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from typing import Any, Protocol, cast

from ._codec import encode_headers, encode_payload, validate_key, validate_table, validate_topic
from ._time import delta_to_micros, from_micros, to_micros
from .errors import OutboxError
from .models import OutboxEvent, Status

logger = logging.getLogger(__name__)

MAX_CLAIM_LIMIT = 10_000
MAX_REQUEUE_IDS = 100_000
_BUSY_TIMEOUT_SECONDS = 30.0
_MAX_ERROR_CHARS = 1_024
_ID_CHUNK = 500

# SQL templates. "<<t>>" is replaced by a table name that already matched
# [a-z_][a-z0-9_]{0,62}; every value is a bound parameter.
_CREATE_TABLE = """CREATE TABLE IF NOT EXISTS "<<t>>" (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    msg_key TEXT,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'published', 'dead')),
    next_attempt_at INTEGER NOT NULL,
    lease_token TEXT,
    lease_expires_at INTEGER,
    last_error TEXT,
    published_at INTEGER
)"""
_CREATE_CLAIM_INDEX = """CREATE INDEX IF NOT EXISTS "<<t>>_claim_idx"
    ON "<<t>>" (status, next_attempt_at)"""
_CREATE_KEY_INDEX = """CREATE INDEX IF NOT EXISTS "<<t>>_key_idx"
    ON "<<t>>" (msg_key, status, seq)"""
_INSERT = """INSERT INTO "<<t>>" (
    event_id, topic, msg_key, payload, headers, created_at, attempts, status,
    next_attempt_at
)
SELECT ?, ?, ?, ?, ?, ?, 0, 'pending', ?
WHERE (SELECT COUNT(*) FROM "<<t>>" WHERE status IN ('pending', 'dead')) < ?"""
_SELECT_SEQ = """SELECT seq FROM "<<t>>" WHERE event_id = ?"""
_SELECT_CLAIMABLE = """SELECT e.seq, e.event_id, e.topic, e.msg_key, e.payload, e.headers,
    e.created_at, e.attempts, e.status
FROM "<<t>>" AS e
WHERE e.status = 'pending'
  AND e.next_attempt_at <= ?
  AND (e.lease_token IS NULL OR e.lease_expires_at <= ?)
  AND NOT EXISTS (
    SELECT 1 FROM "<<t>>" AS o
    WHERE o.msg_key = e.msg_key AND o.status = 'pending' AND o.seq < e.seq
  )
ORDER BY e.seq
LIMIT ?"""
_LEASE = """UPDATE "<<t>>" SET lease_token = ?, lease_expires_at = ? WHERE seq = ?"""
_SELECT_LEASE = """SELECT status, lease_token, lease_expires_at
FROM "<<t>>" WHERE event_id = ?"""
_MARK_PUBLISHED = """UPDATE "<<t>>" SET status = 'published', published_at = ?,
    lease_token = NULL, lease_expires_at = NULL
WHERE event_id = ?"""
_MARK_DEAD = """UPDATE "<<t>>" SET status = 'dead', attempts = attempts + 1, last_error = ?,
    lease_token = NULL, lease_expires_at = NULL
WHERE event_id = ?"""
_MARK_RETRY = """UPDATE "<<t>>" SET attempts = attempts + 1, next_attempt_at = ?,
    last_error = ?, lease_token = NULL, lease_expires_at = NULL
WHERE event_id = ?"""
_REQUEUE_ALL = """UPDATE "<<t>>" SET status = 'pending', attempts = 0, next_attempt_at = ?,
    lease_token = NULL, lease_expires_at = NULL
WHERE status = 'dead'"""
_REQUEUE_IDS = """UPDATE "<<t>>" SET status = 'pending', attempts = 0, next_attempt_at = ?,
    lease_token = NULL, lease_expires_at = NULL
WHERE status = 'dead' AND event_id IN (<<in>>)"""
_PURGE = """DELETE FROM "<<t>>" WHERE status = 'published' AND published_at < ?"""


class OutboxStore(Protocol):
    """Storage port used by the relay and by services that enqueue events."""

    def create_schema(self, conn: sqlite3.Connection) -> None:
        """Create the outbox table and indexes on the caller's connection."""

    def enqueue(
        self,
        conn: sqlite3.Connection,
        topic: str,
        payload: Mapping[str, Any],
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> OutboxEvent:
        """Insert a pending event on the caller's connection without committing."""

    def claim(
        self,
        limit: int,
        now: datetime,
        lease: timedelta,
    ) -> tuple[str, list[OutboxEvent]]:
        """Lease up to ``limit`` eligible events and return the lease token and events."""

    def mark_published(self, lease_token: str, event_id: str) -> None:
        """Mark a leased event as published; raise ``lease_lost`` if the lease is not valid."""

    def mark_failed(
        self,
        lease_token: str,
        event_id: str,
        error: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Record a failed attempt; ``None`` as ``next_attempt_at`` moves the event to dead."""

    def requeue_dead(self, event_ids: Sequence[str] | None = None) -> int:
        """Move dead events back to pending with zero attempts; return how many moved."""

    def purge_published(self, older_than: datetime) -> int:
        """Delete events published before ``older_than``; return how many were removed."""


def _new_id() -> str:
    return uuid.uuid4().hex


@contextmanager
def _immediate(conn: sqlite3.Connection) -> Iterator[None]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def _row_to_event(row: Sequence[Any]) -> OutboxEvent:
    return OutboxEvent(
        event_id=row[1],
        seq=row[0],
        topic=row[2],
        key=row[3],
        payload=json.loads(row[4]),
        headers=json.loads(row[5]),
        created_at=from_micros(row[6]),
        attempts=row[7],
        status=cast(Status, row[8]),
    )


def _tokens_match(stored: str | None, given: object) -> bool:
    if stored is None or not isinstance(given, str):
        return False
    return hmac.compare_digest(
        stored.encode("utf-8", "surrogatepass"),
        given.encode("utf-8", "surrogatepass"),
    )


def _check_lease(
    conn: sqlite3.Connection,
    sql: str,
    lease_token: object,
    event_id: object,
    now_us: int,
) -> None:
    """Check, in order: row exists, status pending, token equal, lease not expired."""
    row = conn.execute(sql, (event_id,)).fetchone() if isinstance(event_id, str) else None
    if row is None:
        raise OutboxError("lease_lost", "event not found")
    status, stored_token, expires_at = row
    if status != "pending":
        raise OutboxError("lease_lost", "event is not pending")
    if not _tokens_match(stored_token, lease_token):
        raise OutboxError("lease_lost", "lease token does not match")
    if expires_at is None or expires_at <= now_us:
        raise OutboxError("lease_lost", "lease expired")


class SqliteOutboxStore:
    """``OutboxStore`` implementation on the standard library ``sqlite3`` module.

    ``enqueue`` and ``create_schema`` run on the caller's connection; every other method
    opens its own short-lived connection to ``path``, so the store is safe to share between
    threads and several stores may point at the same file. Use a file path: in-memory
    databases cannot be shared between connections.

    Times are stored as UTC integer microseconds. ``max_rows`` caps pending plus dead rows;
    published rows are removed only by :meth:`purge_published`.

    Raises:
        OutboxError: ``invalid_table`` if ``table`` is not a safe identifier.
        ValueError: if ``path`` or ``max_rows`` is unusable.
    """

    def __init__(
        self,
        path: str,
        clock: Callable[[], datetime],
        id_factory: Callable[[], str] = _new_id,
        table: str = "outbox",
        max_rows: int = 1_000_000,
    ) -> None:
        if path in ("", ":memory:"):
            raise ValueError("path must be a database file; in-memory databases are not shared")
        if max_rows < 1:
            raise ValueError("max_rows must be at least 1")
        self._table = validate_table(table)
        self._path = path
        self._clock = clock
        self._id_factory = id_factory
        self._max_rows = max_rows

    def _q(self, template: str) -> str:
        return template.replace("<<t>>", self._table)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path, timeout=_BUSY_TIMEOUT_SECONDS, isolation_level=None)

    def create_schema(self, conn: sqlite3.Connection) -> None:
        """Create the table and indexes on ``conn`` (idempotent). Commit if one is open."""
        conn.execute(self._q(_CREATE_TABLE))
        conn.execute(self._q(_CREATE_CLAIM_INDEX))
        conn.execute(self._q(_CREATE_KEY_INDEX))

    def enqueue(
        self,
        conn: sqlite3.Connection,
        topic: str,
        payload: Mapping[str, Any],
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> OutboxEvent:
        """Insert a pending event on ``conn`` without committing.

        Raises:
            OutboxError: ``invalid_topic``, ``payload_invalid``, ``payload_too_large``,
                ``invalid_clock`` or ``outbox_full``.
        """
        validate_topic(topic)
        validate_key(key)
        payload_json = encode_payload(payload)
        headers_json = encode_headers(headers)
        created_us = to_micros(self._clock())
        event_id = self._id_factory()
        cursor = conn.execute(
            self._q(_INSERT),
            (
                event_id,
                topic,
                key,
                payload_json,
                headers_json,
                created_us,
                created_us,
                self._max_rows,
            ),
        )
        if cursor.rowcount != 1:
            raise OutboxError("outbox_full", "the outbox holds the maximum number of rows")
        seq_row = conn.execute(self._q(_SELECT_SEQ), (event_id,)).fetchone()
        return OutboxEvent(
            event_id=event_id,
            seq=int(seq_row[0]),
            topic=topic,
            key=key,
            payload=json.loads(payload_json),
            headers=json.loads(headers_json),
            created_at=from_micros(created_us),
            attempts=0,
            status="pending",
        )

    def claim(
        self,
        limit: int,
        now: datetime,
        lease: timedelta,
    ) -> tuple[str, list[OutboxEvent]]:
        """Lease up to ``limit`` eligible events inside a ``BEGIN IMMEDIATE`` transaction.

        Eligible rows are pending, due (``next_attempt_at <= now``), without a live lease and
        without an older pending row of the same key. Rows are ordered by ``seq``.

        Raises:
            OutboxError: ``invalid_clock`` if ``now`` is naive.
            ValueError: if ``limit`` or ``lease`` is out of range.
        """
        if limit < 1 or limit > MAX_CLAIM_LIMIT:
            raise ValueError("limit must be between 1 and 10000")
        lease_us = delta_to_micros(lease)
        if lease_us < 1:
            raise ValueError("lease must be positive")
        now_us = to_micros(now)
        lease_token = secrets.token_hex(16)
        with closing(self._connect()) as conn, _immediate(conn):
            rows = conn.execute(self._q(_SELECT_CLAIMABLE), (now_us, now_us, limit)).fetchall()
            conn.executemany(
                self._q(_LEASE),
                [(lease_token, now_us + lease_us, row[0]) for row in rows],
            )
        logger.debug("claimed %d outbox events", len(rows))
        return lease_token, [_row_to_event(row) for row in rows]

    def mark_published(self, lease_token: str, event_id: str) -> None:
        """Mark a leased event as published.

        Raises:
            OutboxError: ``lease_lost`` if the row is missing, not pending, leased by another
                token or the lease expired; the row is left unchanged.
        """
        now_us = to_micros(self._clock())
        with closing(self._connect()) as conn, _immediate(conn):
            _check_lease(conn, self._q(_SELECT_LEASE), lease_token, event_id, now_us)
            conn.execute(self._q(_MARK_PUBLISHED), (now_us, event_id))

    def mark_failed(
        self,
        lease_token: str,
        event_id: str,
        error: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Record a failed attempt on a leased event.

        With ``next_attempt_at`` the event stays pending and backs off until then; with
        ``None`` it becomes dead. ``error`` is truncated to 1024 characters.

        Raises:
            OutboxError: ``lease_lost`` (row unchanged) or ``invalid_clock``.
        """
        now_us = to_micros(self._clock())
        next_us = None if next_attempt_at is None else to_micros(next_attempt_at)
        message = str(error)[:_MAX_ERROR_CHARS]
        with closing(self._connect()) as conn, _immediate(conn):
            _check_lease(conn, self._q(_SELECT_LEASE), lease_token, event_id, now_us)
            if next_us is None:
                conn.execute(self._q(_MARK_DEAD), (message, event_id))
            else:
                conn.execute(self._q(_MARK_RETRY), (next_us, message, event_id))

    def requeue_dead(self, event_ids: Sequence[str] | None = None) -> int:
        """Move dead rows back to pending with ``attempts`` 0, eligible now.

        Args:
            event_ids: Restrict to these ids (at most 100000); ``None`` requeues every dead row.

        Returns:
            The number of rows requeued.
        """
        now_us = to_micros(self._clock())
        if event_ids is None:
            with closing(self._connect()) as conn, _immediate(conn):
                everything = conn.execute(self._q(_REQUEUE_ALL), (now_us,)).rowcount
            return everything
        if isinstance(event_ids, str) or len(event_ids) > MAX_REQUEUE_IDS:
            raise ValueError("event_ids must be a sequence of at most 100000 ids")
        if not all(isinstance(item, str) for item in event_ids):
            raise ValueError("event_ids must contain only strings")
        ids = list(dict.fromkeys(event_ids))
        total = 0
        if ids:
            with closing(self._connect()) as conn, _immediate(conn):
                for start in range(0, len(ids), _ID_CHUNK):
                    end = start + _ID_CHUNK
                    chunk = ids[start:end]
                    sql = self._q(_REQUEUE_IDS).replace("<<in>>", ",".join("?" * len(chunk)))
                    total += conn.execute(sql, (now_us, *chunk)).rowcount
        return total

    def purge_published(self, older_than: datetime) -> int:
        """Delete published rows whose publication time is before ``older_than``."""
        cutoff = to_micros(older_than)
        with closing(self._connect()) as conn, _immediate(conn):
            deleted = conn.execute(self._q(_PURGE), (cutoff,)).rowcount
        return deleted
