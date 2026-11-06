# Architecture

## Purpose

`trysac-transactional-outbox` is a small, dependency-free Python library: Transactional outbox: enqueue events in the caller's database transaction and relay them at least once with leases, jittered backoff and dead-lettering

## Shape

```text
caller code --> transactional_outbox (public API in __init__.py)
                    |
                    +-- pure functions, no I/O, no global state
```

- **Public API**: the names listed in `__all__` of `src/transactional_outbox/__init__.py`. Anything else is
  internal and may change without a major version.
- **Errors**: invalid input raises built-in exceptions (`ValueError`, `TypeError`) with a clear
  message; the library never prints or exits.
- **Side effects**: none. If a feature needs I/O, isolate it in its own module behind a small
  interface and record the choice in an ADR.

## Quality contract

`scripts/check.py` is the single definition of the gates; `fabrica.json` tells the factory and CI
how to run it. See [TESTING.md](TESTING.md).

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
