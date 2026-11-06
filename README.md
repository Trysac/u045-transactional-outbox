# transactional-outbox

[![PyPI version](https://img.shields.io/pypi/v/trysac-transactional-outbox)](https://pypi.org/project/trysac-transactional-outbox/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-transactional-outbox)](LICENSE)
[![CI](https://github.com/Trysac/u045-transactional-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u045-transactional-outbox/actions/workflows/ci.yml)

Transactional outbox: enqueue events in the caller's database transaction and relay them at least once with leases, jittered backoff and dead-lettering

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-transactional-outbox
# or
uv add trysac-transactional-outbox
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import transactional_outbox

# normalize() trims surrounding whitespace and lower-cases the text.
print(transactional_outbox.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    transactional_outbox.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
