# Lease-token claiming behind a storage port, with a SQLite reference store

Status: accepted (generated, pending human review)

## Context

Services that write to a database and then publish an event lose or duplicate events when the process crashes between the two steps. The outbox pattern writes the event in the same transaction and relays it later, but several relays need a way to avoid publishing the same row concurrently, retry with backoff, preserve per-key order and stop retrying poison events. No runtime dependencies are allowed.

## Decision

Events are enqueued on the caller's own connection so they commit or roll back with the business data. A Relay talks to an OutboxStore protocol. The reference SqliteOutboxStore claims rows inside BEGIN IMMEDIATE, selecting due, unleased pending rows that have no older pending row of the same key, and stamps them with a random lease token and expiry. Outcomes are recorded only if the row exists, is pending, carries the same token and the lease has not expired, otherwise lease_lost is raised and nothing changes. Failures use full-jitter exponential backoff with an injectable random source, and after max_attempts the row becomes dead until requeue_dead. All SQL values are bound parameters and the table name is validated against a strict regex. Times are UTC integer microseconds and clocks, ids and randomness are injected.

## Consequences

Delivery is at least once and consumers must deduplicate on event_id. Writers and claimers serialize on SQLite's write lock, which is simple and correct but not for very high throughput; a PostgreSQL store with FOR UPDATE SKIP LOCKED is documented rather than shipped. Dead rows stop blocking their key, which trades strict per-key order for liveness. A relay whose lease expires mid-publish causes redelivery. Because the protocol is small, other stores can be added without changing the relay.
