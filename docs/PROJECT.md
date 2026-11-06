# Project record

## Objective

Transactional outbox: enqueue events in the caller's database transaction and relay them at least once with leases, jittered backoff and dead-lettering

## Scope

- In scope: the public API documented in [README.md](../README.md), typed and tested.
- Out of scope: anything that needs runtime dependencies or I/O without an accepted ADR.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 605235 / 110712 |
| Hours (human) | pending measurement |
| Cost (USD) | 1.0078 |

## Factory run u045 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | claude-sonnet-5-5 | yes | 2673 | 94150 | 0 | 4246 | 0.4738 |
| fix-1 | claude-sonnet-5-5 | no | 6 | 728 | 6596 | 40743 | 0.0279 |
| fix-2 | claude-sonnet-5-5 | no | 8 | 1278 | 25758 | 74322 | 0.0846 |
| fix-3 | claude-sonnet-5-5 | no | 8 | 1113 | 8438 | 59410 | 0.0382 |
| fix-4 | claude-sonnet-5-5 | no | 12 | 3772 | 28787 | 148482 | 0.1246 |
| audit | claude-sonnet-5-5 | no | 12 | 4528 | 49833 | 154061 | 0.1853 |
| docs | claude-haiku-5-5 | no | 121252 | 5143 | 0 | 0 | 0.0735 |

Total: $1.0078. Prices: Anthropic list prices.
