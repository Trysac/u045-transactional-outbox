# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add `transactional_outbox`: transactional enqueue on the caller's connection, a lease-based `Relay` with full-jitter backoff and dead-lettering, an `OutboxStore` protocol with a `SqliteOutboxStore`, and `RecordingPublisher`/`FailingPublisher` test doubles.
- Initial project structure from the factory utility template.
