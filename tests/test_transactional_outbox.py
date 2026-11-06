"""Tests for the transactional outbox."""

from __future__ import annotations

import itertools
import logging
import random
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from transactional_outbox import (
    OutboxError,
    OutboxEvent,
    Relay,
    RelayEvent,
    RelayStats,
    SqliteOutboxStore,
    __version__,
)
from transactional_outbox.testing import FailingPublisher, RecordingPublisher

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
LEASE = timedelta(seconds=30)
MARKER = "do-not-log-this"
ROW_SQL = """SELECT status, attempts, next_attempt_at, last_error, lease_token,
    lease_expires_at, published_at FROM outbox WHERE event_id = ?"""
ROW_COLUMNS = (
    "status",
    "attempts",
    "next_attempt_at",
    "last_error",
    "lease_token",
    "lease_expires_at",
    "published_at",
)


ACCEPTANCE = """
store.enqueue(conn, 'orders', {'id': 1}) inserts on the caller's connection without committing
With max_attempts=2 and RuntimeError('x' * 300), the second failure makes the row 'dead' with last_error 'RuntimeError: ' + 'x' * 200, it is never claimed again, and requeue_dead() returns 1 with the row back to 'pending' and attempts 0
A later event with key 'k' is not claimed while an earlier 'k' event is pending or backing off
Payloads {'a': (1, 2)}, {1: 'x'} and {'a': float('nan')} raise OutboxError code 'payload_invalid'; {'a':
'x' * 262_136} (262_144 JSON bytes) is accepted and {'a': 'x' * 262_137} raises code 'payload_too_large'; topic 'Orders' raises code 'invalid_topic'; with max_rows=2 a third enqueue raises code 'outbox_full'
"""  # noqa: E501


def us(value: datetime) -> int:
    return (value - EPOCH) // timedelta(microseconds=1)


class Clock:
    def __init__(self, start: datetime = START) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


class Ids:
    def __init__(self) -> None:
        self._counter = itertools.count(1)

    def __call__(self) -> str:
        return f"evt-{next(self._counter):04d}"


class MaxRng(random.Random):
    def random(self) -> float:
        return 1.0


class ClockJumpPublisher:
    def __init__(self, clock: Clock, jump: timedelta, fail: bool = False) -> None:
        self.clock = clock
        self.jump = jump
        self.fail = fail
        self.calls: list[str] = []

    def publish(self, event: OutboxEvent) -> None:
        self.calls.append(event.event_id)
        self.clock.advance(self.jump)
        if self.fail:
            raise RuntimeError("late")


class FakeStore:
    def __init__(self, events: list[OutboxEvent], error: OutboxError) -> None:
        self.events = events
        self.error = error

    def claim(self, limit: int, now: datetime, lease: timedelta) -> tuple[str, list[OutboxEvent]]:
        return "fake", list(self.events)

    def mark_published(self, lease_token: str, event_id: str) -> None:
        raise self.error

    def mark_failed(
        self,
        lease_token: str,
        event_id: str,
        error: str,
        next_attempt_at: datetime | None,
    ) -> None:
        raise self.error


def make_event(event_id: str) -> OutboxEvent:
    return OutboxEvent(
        event_id=event_id,
        seq=1,
        topic="orders",
        key=None,
        payload={},
        headers={},
        created_at=START,
        attempts=0,
        status="pending",
    )


def count(conn: sqlite3.Connection) -> int:
    result = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()
    return int(result[0])


def row(conn: sqlite3.Connection, event_id: str) -> dict[str, Any]:
    result = conn.execute(ROW_SQL, (event_id,)).fetchone()
    return dict(zip(ROW_COLUMNS, result, strict=True))


def add(
    store: SqliteOutboxStore,
    conn: sqlite3.Connection,
    payload: dict[str, Any] | None = None,
    key: str | None = None,
) -> OutboxEvent:
    event = store.enqueue(conn, "orders", payload if payload is not None else {"id": 1}, key=key)
    conn.commit()
    return event


def nested(levels: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for _ in range(levels - 1):
        result = {"a": result}
    return result


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "outbox.sqlite3")


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def conn(db_path: str) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(db_path)
    yield connection
    connection.close()


@pytest.fixture
def store(db_path: str, clock: Clock, conn: sqlite3.Connection) -> SqliteOutboxStore:
    created = SqliteOutboxStore(db_path, clock, id_factory=Ids())
    created.create_schema(conn)
    conn.commit()
    return created


# --- enqueue and transactions -------------------------------------------------------------


def test_enqueue_is_part_of_the_callers_transaction(
    store: SqliteOutboxStore, conn: sqlite3.Connection
) -> None:
    event = store.enqueue(conn, "orders", {"id": 1})
    assert event.status == "pending"
    assert event.attempts == 0
    assert event.payload == {"id": 1}
    conn.rollback()
    assert count(conn) == 0
    store.enqueue(conn, "orders", {"id": 1})
    conn.commit()
    assert count(conn) == 1
    assert conn.execute("SELECT status FROM outbox").fetchall() == [("pending",)]


def test_uncommitted_rows_are_invisible_to_other_connections(
    store: SqliteOutboxStore, conn: sqlite3.Connection, db_path: str
) -> None:
    store.enqueue(conn, "orders", {"id": 1})
    with closing(sqlite3.connect(db_path)) as other:
        assert count(other) == 0
    conn.commit()
    with closing(sqlite3.connect(db_path)) as other:
        assert count(other) == 1


def test_create_schema_is_idempotent(store: SqliteOutboxStore, conn: sqlite3.Connection) -> None:
    store.create_schema(conn)
    store.create_schema(conn)
    conn.commit()
    assert count(conn) == 0


def test_custom_table_name_is_isolated(db_path: str, clock: Clock) -> None:
    other = SqliteOutboxStore(db_path, clock, table="events_out")
    with closing(sqlite3.connect(db_path)) as conn:
        other.create_schema(conn)
        conn.commit()
        other.enqueue(conn, "orders", {"id": 1})
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM events_out").fetchone() == (1,)
    assert len(other.claim(10, clock.now, LEASE)[1]) == 1


def test_claimed_event_equals_the_enqueued_one(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    payload = {
        "i": 1,
        "f": 1.5,
        "one": 1.0,
        "big": 2**70,
        "t": True,
        "n": None,
        "l": [1, "a", {"z": 0}],
        "s": "caf" + chr(233),
        "e": 1e300,
    }
    event = store.enqueue(conn, "orders", payload, key="k", headers={"trace": "abc"})
    conn.commit()
    _, claimed = store.claim(10, clock.now, LEASE)
    assert len(claimed) == 1
    got = claimed[0]
    assert got.payload == payload
    assert got.payload is not payload
    assert type(got.payload["one"]) is float
    assert type(got.payload["i"]) is int
    assert got.event_id == event.event_id
    assert got.seq == event.seq
    assert got.key == "k"
    assert got.headers == {"trace": "abc"}
    assert got.created_at == START
    assert got.status == "pending"
    assert got.attempts == 0


def test_hostile_strings_are_stored_literally(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    hostile = "'; DROP TABLE outbox; --"
    store.enqueue(conn, "orders", {"a": hostile}, key=hostile, headers={hostile: hostile})
    conn.commit()
    _, claimed = store.claim(10, clock.now, LEASE)
    assert claimed[0].payload == {"a": hostile}
    assert claimed[0].key == hostile
    assert claimed[0].headers == {hostile: hostile}
    assert count(conn) == 1


# --- validation ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"a": (1, 2)},
        {1: "x"},
        {"a": float("nan")},
        {"a": float("inf")},
        {"a": {1, 2}},
        {"a": b"x"},
        {"a": object()},
        {"a": {"b": (1,)}},
        {"a": [1, (2,)]},
        [1, 2],
        "text",
        None,
    ],
)
def test_invalid_payloads_are_rejected(
    store: SqliteOutboxStore, conn: sqlite3.Connection, payload: Any
) -> None:
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, "orders", payload)
    assert info.value.code == "payload_invalid"
    assert len(str(info.value)) <= 200
    assert count(conn) == 0


def test_payload_size_limit_is_exact(store: SqliteOutboxStore, conn: sqlite3.Connection) -> None:
    accepted = store.enqueue(conn, "orders", {"a": "x" * 262_136})
    assert accepted.payload == {"a": "x" * 262_136}
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, "orders", {"a": "x" * 262_137})
    assert info.value.code == "payload_too_large"
    assert len(str(info.value)) <= 200


def test_payload_depth_limit(store: SqliteOutboxStore, conn: sqlite3.Connection) -> None:
    store.enqueue(conn, "orders", nested(32))
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, "orders", nested(33))
    assert info.value.code == "payload_invalid"


def test_payload_with_too_many_values_is_too_large(
    store: SqliteOutboxStore, conn: sqlite3.Connection
) -> None:
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, "orders", {"a": [0] * 262_144})
    assert info.value.code == "payload_too_large"


def test_huge_integer_is_payload_invalid(
    store: SqliteOutboxStore, conn: sqlite3.Connection
) -> None:
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, "orders", {"a": 10**5000})
    assert info.value.code == "payload_invalid"


@pytest.mark.parametrize(
    "topic",
    ["Orders", "", "orders\n", "has space", "a" * 129, "-bad", "a;b", 5],
)
def test_invalid_topics_are_rejected(
    store: SqliteOutboxStore, conn: sqlite3.Connection, topic: Any
) -> None:
    with pytest.raises(OutboxError) as info:
        store.enqueue(conn, topic, {"id": 1})
    assert info.value.code == "invalid_topic"


@pytest.mark.parametrize("topic", ["a" * 128, "orders.v1-x_y", "0"])
def test_valid_topics_are_accepted(
    store: SqliteOutboxStore, conn: sqlite3.Connection, topic: str
) -> None:
    assert store.enqueue(conn, topic, {"id": 1}).topic == topic


def test_key_limits(store: SqliteOutboxStore, conn: sqlite3.Connection) -> None:
    assert store.enqueue(conn, "orders", {"id": 1}, key="k" * 1024).key == "k" * 1024
    with pytest.raises(OutboxError) as too_long:
        store.enqueue(conn, "orders", {"id": 1}, key="k" * 1025)
    assert too_long.value.code == "payload_invalid"
    with pytest.raises(OutboxError) as not_text:
        store.enqueue(conn, "orders", {"id": 1}, key=5)  # type: ignore[arg-type]
    assert not_text.value.code == "payload_invalid"


def test_header_limits(store: SqliteOutboxStore, conn: sqlite3.Connection) -> None:
    many = {f"h{n}": "v" for n in range(32)}
    assert store.enqueue(conn, "orders", {"id": 1}, headers=many).headers == many
    assert store.enqueue(conn, "orders", {"id": 1}, headers={"h": "v" * 1024}).headers == {
        "h": "v" * 1024
    }
    bad: list[Any] = [
        {f"h{n}": "v" for n in range(33)},
        {"h": "v" * 1025},
        {"h" * 1025: "v"},
        {"h": 1},
        {1: "v"},
        [("h", "v")],
    ]
    for headers in bad:
        with pytest.raises(OutboxError) as info:
            store.enqueue(conn, "orders", {"id": 1}, headers=headers)
        assert info.value.code == "payload_invalid"


@pytest.mark.parametrize(
    "table",
    ["Orders", "1x", "a-b", "", "x" * 64, "a;drop table x", "orders\n", 'a"b', 5],
)
def test_invalid_table_names_are_rejected(db_path: str, clock: Clock, table: Any) -> None:
    with pytest.raises(OutboxError) as info:
        SqliteOutboxStore(db_path, clock, table=table)
    assert info.value.code == "invalid_table"


@pytest.mark.parametrize("table", ["x" * 63, "_a1", "select"])
def test_valid_table_names_work(db_path: str, clock: Clock, table: str) -> None:
    other = SqliteOutboxStore(db_path, clock, table=table)
    with closing(sqlite3.connect(db_path)) as conn:
        other.create_schema(conn)
        other.enqueue(conn, "orders", {"id": 1})
        conn.commit()
    assert len(other.claim(10, clock.now, LEASE)[1]) == 1


def test_store_configuration_errors(db_path: str, clock: Clock) -> None:
    with pytest.raises(ValueError, match="max_rows"):
        SqliteOutboxStore(db_path, clock, max_rows=0)
    with pytest.raises(ValueError, match="path"):
        SqliteOutboxStore(":memory:", clock)
    with pytest.raises(ValueError, match="path"):
        SqliteOutboxStore("", clock)


def test_naive_or_invalid_clock_values(
    db_path: str, clock: Clock, store: SqliteOutboxStore, conn: sqlite3.Connection
) -> None:
    naive = SqliteOutboxStore(db_path, lambda: datetime(2024, 1, 1))
    with pytest.raises(OutboxError) as info:
        naive.enqueue(conn, "orders", {"id": 1})
    assert info.value.code == "invalid_clock"
    text_clock = SqliteOutboxStore(db_path, lambda: "now")  # type: ignore[arg-type,return-value]
    with pytest.raises(OutboxError) as text_info:
        text_clock.enqueue(conn, "orders", {"id": 1})
    assert text_info.value.code == "invalid_clock"
    with pytest.raises(OutboxError) as claim_info:
        store.claim(10, datetime(2024, 1, 1), LEASE)
    assert claim_info.value.code == "invalid_clock"
    with pytest.raises(OutboxError) as purge_info:
        store.purge_published(datetime(2024, 1, 1))
    assert purge_info.value.code == "invalid_clock"
    with pytest.raises(OutboxError) as pub_info:
        naive.mark_published("x", "y")
    assert pub_info.value.code == "invalid_clock"


def test_max_rows_caps_pending_and_dead_rows(
    db_path: str, clock: Clock, conn: sqlite3.Connection
) -> None:
    capped = SqliteOutboxStore(db_path, clock, id_factory=Ids(), max_rows=2)
    capped.create_schema(conn)
    conn.commit()
    capped.enqueue(conn, "orders", {"id": 1})
    capped.enqueue(conn, "orders", {"id": 2})
    with pytest.raises(OutboxError) as info:
        capped.enqueue(conn, "orders", {"id": 3})
    assert info.value.code == "outbox_full"
    conn.commit()
    assert count(conn) == 2
    assert Relay(capped, RecordingPublisher(), clock).relay_once().published == 2
    capped.enqueue(conn, "orders", {"id": 3})
    conn.commit()
    assert count(conn) == 3


def test_dead_rows_count_toward_max_rows(
    db_path: str, clock: Clock, conn: sqlite3.Connection
) -> None:
    capped = SqliteOutboxStore(db_path, clock, id_factory=Ids(), max_rows=1)
    capped.create_schema(conn)
    conn.commit()
    capped.enqueue(conn, "orders", {"id": 1})
    conn.commit()
    relay = Relay(capped, FailingPublisher(fail_times=5), clock, max_attempts=1)
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=1)
    with pytest.raises(OutboxError) as info:
        capped.enqueue(conn, "orders", {"id": 2})
    assert info.value.code == "outbox_full"


def test_purge_published(store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock) -> None:
    add(store, conn)
    Relay(store, RecordingPublisher(), clock).relay_once()
    assert store.purge_published(clock.now) == 0
    assert store.purge_published(clock.now + timedelta(seconds=1)) == 1
    assert count(conn) == 0


# --- relay behaviour ----------------------------------------------------------------------


def test_relay_publishes_in_seq_order(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, {"n": 1})
    second = add(store, conn, {"n": 2})
    publisher = RecordingPublisher()
    relay = Relay(store, publisher, clock)
    assert relay.relay_once() == RelayStats(published=2, failed=0, dead=0)
    assert [event.event_id for event in publisher.events] == [first.event_id, second.event_id]
    assert row(conn, first.event_id)["status"] == "published"
    assert row(conn, second.event_id)["status"] == "published"
    assert row(conn, first.event_id)["lease_token"] is None
    assert row(conn, first.event_id)["published_at"] == us(clock.now)
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)


def test_batch_size_limits_the_claim(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, {"n": 1})
    second = add(store, conn, {"n": 2})
    publisher = RecordingPublisher()
    relay = Relay(store, publisher, clock, batch_size=1)
    assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert [event.event_id for event in publisher.events] == [first.event_id]
    assert row(conn, second.event_id)["status"] == "pending"


def test_jittered_backoff_is_deterministic(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    relay = Relay(
        store,
        FailingPublisher(fail_times=2),
        clock,
        base_delay=timedelta(seconds=1),
        rng=random.Random(0),  # noqa: S311
    )
    first_time = clock.now
    assert relay.relay_once() == RelayStats(published=0, failed=1, dead=0)
    state = row(conn, event.event_id)
    assert state["attempts"] == 1
    assert state["next_attempt_at"] == us(first_time + timedelta(microseconds=844422))
    assert state["lease_token"] is None
    assert "RuntimeError" in state["last_error"]
    clock.now = first_time + timedelta(microseconds=844421)
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)
    clock.now = first_time + timedelta(microseconds=844422)
    second_time = clock.now
    assert relay.relay_once() == RelayStats(published=0, failed=1, dead=0)
    state = row(conn, event.event_id)
    assert state["attempts"] == 2
    assert state["next_attempt_at"] == us(second_time + timedelta(microseconds=1515909))
    clock.now = second_time + timedelta(microseconds=1515909)
    assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert row(conn, event.event_id)["status"] == "published"


def test_backoff_is_capped_by_max_delay(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    relay = Relay(
        store,
        FailingPublisher(fail_times=99),
        clock,
        max_attempts=10,
        base_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=2),
        rng=MaxRng(),
    )
    for expected in (1, 2, 2, 2):
        now = clock.now
        relay.relay_once()
        assert row(conn, event.event_id)["next_attempt_at"] == us(now + timedelta(seconds=expected))
        clock.now = now + timedelta(seconds=expected)


def test_backoff_does_not_overflow_for_many_attempts(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    conn.execute("UPDATE outbox SET attempts = 500 WHERE event_id = ?", (event.event_id,))
    conn.commit()
    relay = Relay(store, FailingPublisher(fail_times=1), clock, max_attempts=1000, rng=MaxRng())
    relay.relay_once()
    state = row(conn, event.event_id)
    assert state["attempts"] == 501
    assert state["next_attempt_at"] == us(clock.now + timedelta(minutes=10))


def test_dead_lettering_and_requeue(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    publisher = FailingPublisher(fail_times=10, error_factory=lambda: RuntimeError("x" * 300))
    relay = Relay(store, publisher, clock, max_attempts=2)
    assert relay.relay_once() == RelayStats(published=0, failed=1, dead=0)
    clock.advance(timedelta(minutes=11))
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=1)
    state = row(conn, event.event_id)
    assert state["status"] == "dead"
    assert state["attempts"] == 2
    assert state["last_error"] == "RuntimeError: " + "x" * 200
    assert state["lease_token"] is None
    clock.advance(timedelta(days=1))
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)
    assert publisher.calls == 2
    assert store.requeue_dead() == 1
    state = row(conn, event.event_id)
    assert state["status"] == "pending"
    assert state["attempts"] == 0
    assert state["next_attempt_at"] == us(clock.now)
    assert store.requeue_dead() == 0


def test_empty_error_message_uses_type_name_only(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    relay = Relay(store, FailingPublisher(fail_times=1, error_factory=lambda: KeyError()), clock)
    relay.relay_once()
    assert row(conn, event.event_id)["last_error"] == "KeyError"


def test_error_whose_str_raises_is_still_recorded(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    class Broken(Exception):
        def __str__(self) -> str:
            raise RuntimeError("no str")

    event = add(store, conn)
    Relay(store, FailingPublisher(fail_times=1, error_factory=Broken), clock).relay_once()
    assert row(conn, event.event_id)["last_error"] == "Broken"


def test_requeue_dead_by_ids(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, {"n": 1})
    second = add(store, conn, {"n": 2})
    lease_token, _ = store.claim(10, clock.now, LEASE)
    store.mark_failed(lease_token, first.event_id, "boom", None)
    store.mark_failed(lease_token, second.event_id, "boom", None)
    assert store.requeue_dead([]) == 0
    assert store.requeue_dead(["unknown"]) == 0
    assert store.requeue_dead([first.event_id, first.event_id]) == 1
    assert row(conn, first.event_id)["status"] == "pending"
    assert row(conn, second.event_id)["status"] == "dead"
    many = [f"missing-{n}" for n in range(1200)] + [second.event_id]
    assert store.requeue_dead(many) == 1


def test_requeue_dead_rejects_bad_arguments(store: SqliteOutboxStore) -> None:
    with pytest.raises(ValueError, match="event_ids"):
        store.requeue_dead("abc")
    with pytest.raises(ValueError, match="event_ids"):
        store.requeue_dead(["a"] * 100_001)
    with pytest.raises(ValueError, match="strings"):
        store.requeue_dead([1])  # type: ignore[list-item]


def test_mark_failed_truncates_long_errors(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    lease_token, _ = store.claim(10, clock.now, LEASE)
    store.mark_failed(lease_token, event.event_id, "e" * 5000, None)
    assert len(row(conn, event.event_id)["last_error"]) == 1024


def test_mark_failed_with_naive_next_attempt_changes_nothing(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    lease_token, _ = store.claim(10, clock.now, LEASE)
    before = row(conn, event.event_id)
    with pytest.raises(OutboxError) as info:
        store.mark_failed(lease_token, event.event_id, "boom", datetime(2024, 1, 1))
    assert info.value.code == "invalid_clock"
    assert row(conn, event.event_id) == before


# --- concurrency and leases -----------------------------------------------------------------


def test_two_relays_never_publish_the_same_row_twice(
    store: SqliteOutboxStore, conn: sqlite3.Connection, db_path: str, clock: Clock
) -> None:
    expected = [store.enqueue(conn, "orders", {"n": n}).event_id for n in range(40)]
    conn.commit()
    publisher = RecordingPublisher()
    errors: list[BaseException] = []

    def work() -> None:
        relay = Relay(SqliteOutboxStore(db_path, clock), publisher, clock, batch_size=5)
        try:
            while relay.relay_once().published:
                pass
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    published_ids = [event.event_id for event in publisher.events]
    assert len(published_ids) == 40
    assert sorted(published_ids) == sorted(expected)
    assert conn.execute("SELECT COUNT(*) FROM outbox WHERE status = 'published'").fetchone() == (
        40,
    )


def test_expired_lease_is_reclaimed_by_the_other_relay(
    store: SqliteOutboxStore, conn: sqlite3.Connection, db_path: str, clock: Clock
) -> None:
    event = add(store, conn)
    other_store = SqliteOutboxStore(db_path, clock)
    first_token, claimed = store.claim(10, clock.now, LEASE)
    assert [item.event_id for item in claimed] == [event.event_id]
    assert other_store.claim(10, clock.now, LEASE)[1] == []
    clock.advance(LEASE)
    publisher = RecordingPublisher()
    assert Relay(other_store, publisher, clock).relay_once() == RelayStats(1, 0, 0)
    with pytest.raises(OutboxError) as info:
        store.mark_published(first_token, event.event_id)
    assert info.value.code == "lease_lost"
    assert row(conn, event.event_id)["status"] == "published"


def test_claim_tokens_are_unique_hex(store: SqliteOutboxStore, clock: Clock) -> None:
    first, events = store.claim(10, clock.now, LEASE)
    second, _ = store.claim(10, clock.now, LEASE)
    assert events == []
    assert first != second
    assert len(first) == 32
    int(first, 16)


def test_claim_rejects_bad_arguments(store: SqliteOutboxStore, clock: Clock) -> None:
    with pytest.raises(ValueError, match="limit"):
        store.claim(0, clock.now, LEASE)
    with pytest.raises(ValueError, match="limit"):
        store.claim(10_001, clock.now, LEASE)
    with pytest.raises(ValueError, match="lease"):
        store.claim(1, clock.now, timedelta(0))


def test_lease_boundaries(store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock) -> None:
    event = add(store, conn)
    lease_token, _ = store.claim(10, clock.now, LEASE)
    clock.advance(LEASE - timedelta(microseconds=1))
    store.mark_published(lease_token, event.event_id)
    assert row(conn, event.event_id)["status"] == "published"


def test_mark_published_rejects_foreign_and_expired_leases(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    lease_token, _ = store.claim(10, clock.now, LEASE)
    before = row(conn, event.event_id)
    assert before["lease_token"] == lease_token
    with pytest.raises(OutboxError) as foreign:
        store.mark_published("foreign", event.event_id)
    assert foreign.value.code == "lease_lost"
    assert row(conn, event.event_id) == before
    clock.advance(LEASE)
    with pytest.raises(OutboxError) as expired:
        store.mark_published(lease_token, event.event_id)
    assert expired.value.code == "lease_lost"
    assert row(conn, event.event_id) == before
    fresh_token, _ = store.claim(10, clock.now, LEASE)
    store.mark_published(fresh_token, event.event_id)
    published = row(conn, event.event_id)
    assert published["status"] == "published"
    with pytest.raises(OutboxError) as again:
        store.mark_published(fresh_token, event.event_id)
    assert again.value.code == "lease_lost"
    assert row(conn, event.event_id) == published
    with pytest.raises(OutboxError) as late_failure:
        store.mark_failed(fresh_token, event.event_id, "late", None)
    assert late_failure.value.code == "lease_lost"
    assert row(conn, event.event_id) == published
    assert published["attempts"] == 0


def test_mark_failed_rejects_bad_leases_and_dead_rows(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    lease_token, _ = store.claim(10, clock.now, LEASE)
    before = row(conn, event.event_id)
    with pytest.raises(OutboxError) as foreign:
        store.mark_failed("foreign", event.event_id, "boom", None)
    assert foreign.value.code == "lease_lost"
    with pytest.raises(OutboxError) as unknown:
        store.mark_failed(lease_token, "unknown", "boom", None)
    assert unknown.value.code == "lease_lost"
    with pytest.raises(OutboxError) as not_text:
        store.mark_published(5, event.event_id)  # type: ignore[arg-type]
    assert not_text.value.code == "lease_lost"
    with pytest.raises(OutboxError) as bad_id:
        store.mark_published(lease_token, 5)  # type: ignore[arg-type]
    assert bad_id.value.code == "lease_lost"
    assert row(conn, event.event_id) == before
    store.mark_failed(lease_token, event.event_id, "boom", None)
    assert row(conn, event.event_id)["status"] == "dead"
    with pytest.raises(OutboxError) as dead:
        store.mark_failed(lease_token, event.event_id, "boom", None)
    assert dead.value.code == "lease_lost"
    assert row(conn, event.event_id)["attempts"] == 1


def test_lease_of_unclaimed_row_is_not_accepted(
    store: SqliteOutboxStore, conn: sqlite3.Connection
) -> None:
    event = add(store, conn)
    with pytest.raises(OutboxError) as info:
        store.mark_published("", event.event_id)
    assert info.value.code == "lease_lost"
    assert row(conn, event.event_id)["status"] == "pending"


# --- ordering -----------------------------------------------------------------------------


def test_per_key_order_holds(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, {"n": 1}, key="k")
    second = add(store, conn, {"n": 2}, key="k")
    free_one = add(store, conn, {"n": 3})
    free_two = add(store, conn, {"n": 4})
    publisher = FailingPublisher(fail_times=1)
    relay = Relay(store, publisher, clock)
    assert relay.relay_once() == RelayStats(published=2, failed=1, dead=0)
    assert [event.event_id for event in publisher.events] == [
        free_one.event_id,
        free_two.event_id,
    ]
    assert row(conn, second.event_id)["attempts"] == 0
    assert row(conn, second.event_id)["lease_token"] is None
    assert store.claim(10, clock.now, LEASE)[1] == []
    clock.advance(timedelta(minutes=11))
    assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert [event.event_id for event in publisher.events] == [
        free_one.event_id,
        free_two.event_id,
        first.event_id,
        second.event_id,
    ]


def test_claim_excludes_later_events_of_a_pending_key(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, key="k")
    add(store, conn, key="k")
    free_one = add(store, conn)
    free_two = add(store, conn)
    _, claimed = store.claim(10, clock.now, LEASE)
    assert [event.event_id for event in claimed] == [
        first.event_id,
        free_one.event_id,
        free_two.event_id,
    ]


def test_events_without_key_are_not_ordered(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    add(store, conn)
    second = add(store, conn)
    publisher = FailingPublisher(fail_times=1)
    assert Relay(store, publisher, clock).relay_once() == RelayStats(1, 1, 0)
    assert [event.event_id for event in publisher.events] == [second.event_id]


def test_dead_rows_do_not_block_their_key(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    first = add(store, conn, key="k")
    second = add(store, conn, key="k")
    lease_token, claimed = store.claim(10, clock.now, LEASE)
    assert [event.event_id for event in claimed] == [first.event_id]
    store.mark_failed(lease_token, first.event_id, "boom", None)
    _, next_claim = store.claim(10, clock.now, LEASE)
    assert [event.event_id for event in next_claim] == [second.event_id]


def test_published_rows_do_not_block_their_key(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    add(store, conn, key="k")
    second = add(store, conn, key="k")
    publisher = RecordingPublisher()
    relay = Relay(store, publisher, clock)
    assert relay.relay_once().published == 1
    assert relay.relay_once().published == 1
    assert publisher.events[-1].event_id == second.event_id


# --- relay edge cases and observability ---------------------------------------------------------


def test_lease_lost_during_publish_means_redelivery(
    store: SqliteOutboxStore,
    conn: sqlite3.Connection,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = add(store, conn)
    publisher = ClockJumpPublisher(clock, timedelta(seconds=60))
    relay = Relay(store, publisher, clock, lease=LEASE)
    with caplog.at_level(logging.WARNING, logger="transactional_outbox"):
        assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)
    assert "redelivered" in caplog.text
    assert row(conn, event.event_id)["status"] == "pending"
    publisher.jump = timedelta(0)
    assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert publisher.calls == [event.event_id, event.event_id]


def test_lease_lost_while_recording_a_failure(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    publisher = ClockJumpPublisher(clock, timedelta(seconds=60), fail=True)
    relay = Relay(store, publisher, clock, lease=LEASE)
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)
    state = row(conn, event.event_id)
    assert state["status"] == "pending"
    assert state["attempts"] == 0


def test_other_store_errors_propagate() -> None:
    clock = Clock()
    fake = FakeStore([make_event("e1")], OutboxError("invalid_clock", "bad"))
    with pytest.raises(OutboxError) as published:
        Relay(fake, RecordingPublisher(), clock).relay_once()  # type: ignore[arg-type]
    assert published.value.code == "invalid_clock"
    with pytest.raises(OutboxError) as failed:
        Relay(fake, FailingPublisher(fail_times=1), clock).relay_once()  # type: ignore[arg-type]
    assert failed.value.code == "invalid_clock"


def test_lease_lost_from_any_store_is_swallowed() -> None:
    clock = Clock()
    fake = FakeStore([make_event("e1")], OutboxError("lease_lost", "gone"))
    relay = Relay(fake, RecordingPublisher(), clock)  # type: ignore[arg-type]
    assert relay.relay_once() == RelayStats(published=0, failed=0, dead=0)


def test_on_event_reports_outcomes_without_payloads(
    store: SqliteOutboxStore,
    conn: sqlite3.Connection,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    first = add(store, conn, {"secret_value": MARKER})
    second = add(store, conn, {"secret_value": MARKER})
    captured: list[RelayEvent] = []
    publisher = FailingPublisher(fail_times=1, error_factory=lambda: RuntimeError(MARKER))
    relay = Relay(store, publisher, clock, max_attempts=2, on_event=captured.append)
    with caplog.at_level(logging.DEBUG, logger="transactional_outbox"):
        relay.relay_once()
        clock.advance(timedelta(minutes=11))
        FailingPublisher(fail_times=0)
        failing_again = Relay(
            store,
            FailingPublisher(fail_times=1),
            clock,
            max_attempts=2,
            on_event=captured.append,
        )
        failing_again.relay_once()
    assert [(item.kind, item.event_id, item.attempts, item.error_type) for item in captured] == [
        ("failed", first.event_id, 1, "RuntimeError"),
        ("published", second.event_id, 0, None),
        ("dead", first.event_id, 2, "RuntimeError"),
    ]
    assert all(item.topic == "orders" for item in captured)
    assert MARKER not in repr(captured)
    assert MARKER not in caplog.text
    assert set(RelayEvent.__dataclass_fields__) == {
        "kind",
        "event_id",
        "topic",
        "attempts",
        "error_type",
    }


def test_failing_on_event_callback_does_not_break_the_relay(
    store: SqliteOutboxStore,
    conn: sqlite3.Connection,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    add(store, conn)

    def broken(_: RelayEvent) -> None:
        raise RuntimeError("callback")

    relay = Relay(store, RecordingPublisher(), clock, on_event=broken)
    with caplog.at_level(logging.WARNING, logger="transactional_outbox"):
        assert relay.relay_once() == RelayStats(published=1, failed=0, dead=0)
    assert "on_event callback failed" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": timedelta(0)},
        {"base_delay": timedelta(seconds=10), "max_delay": timedelta(seconds=1)},
        {"lease": timedelta(0)},
        {"batch_size": 0},
        {"batch_size": 10_001},
    ],
)
def test_relay_rejects_bad_configuration(
    store: SqliteOutboxStore, clock: Clock, kwargs: dict[str, Any]
) -> None:
    with pytest.raises(ValueError):
        Relay(store, RecordingPublisher(), clock, **kwargs)


def test_relay_rejects_naive_clock(store: SqliteOutboxStore) -> None:
    relay = Relay(store, RecordingPublisher(), lambda: datetime(2024, 1, 1))
    with pytest.raises(OutboxError) as info:
        relay.relay_once()
    assert info.value.code == "invalid_clock"


def test_default_rng_is_usable(
    store: SqliteOutboxStore, conn: sqlite3.Connection, clock: Clock
) -> None:
    event = add(store, conn)
    Relay(store, FailingPublisher(fail_times=1), clock).relay_once()
    state = row(conn, event.event_id)
    assert us(clock.now) <= state["next_attempt_at"] <= us(clock.now + timedelta(seconds=1))


# --- errors, logging, testing doubles -----------------------------------------------------------


def test_outbox_error_has_code_and_bounded_message() -> None:
    error = OutboxError("lease_lost", "x" * 500)
    assert error.code == "lease_lost"
    assert len(str(error)) == 200
    assert len(error.message) == 200
    assert isinstance(error, Exception)


def test_package_logger_has_a_null_handler_and_version() -> None:
    handlers = logging.getLogger("transactional_outbox").handlers
    assert any(isinstance(handler, logging.NullHandler) for handler in handlers)
    assert __version__ == "0.1.0"


def test_recording_publisher_is_bounded() -> None:
    publisher = RecordingPublisher(max_events=2)
    for name in ("e0", "e1", "e2"):
        publisher.publish(make_event(name))
    assert [event.event_id for event in publisher.events] == ["e1", "e2"]
    with pytest.raises(ValueError, match="max_events"):
        RecordingPublisher(max_events=0)


def test_failing_publisher_fails_then_records() -> None:
    publisher = FailingPublisher(fail_times=2)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="simulated publish failure"):
            publisher.publish(make_event("e1"))
    publisher.publish(make_event("e2"))
    assert publisher.calls == 3
    assert [event.event_id for event in publisher.events] == ["e2"]
    with pytest.raises(ValueError, match="fail_times"):
        FailingPublisher(fail_times=-1)


def test_failing_publisher_uses_the_error_factory() -> None:
    publisher = FailingPublisher(fail_times=1, error_factory=lambda: KeyError("custom"))
    with pytest.raises(KeyError):
        publisher.publish(make_event("e1"))
