"""Internal UTC time helpers: the store keeps times as integer microseconds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .errors import OutboxError

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MICROSECOND = timedelta(microseconds=1)


def require_aware(value: object) -> datetime:
    """Return ``value`` if it is a timezone-aware datetime, else raise ``invalid_clock``."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OutboxError("invalid_clock", "clock values must be timezone-aware datetimes")
    return value


def to_micros(value: object) -> int:
    """Convert an aware datetime to integer microseconds since the Unix epoch (UTC)."""
    return (require_aware(value) - _EPOCH) // MICROSECOND


def from_micros(value: int) -> datetime:
    """Convert integer microseconds since the Unix epoch to an aware UTC datetime."""
    return _EPOCH + timedelta(microseconds=value)


def delta_to_micros(delta: timedelta) -> int:
    """Convert a timedelta to integer microseconds."""
    return delta // MICROSECOND
