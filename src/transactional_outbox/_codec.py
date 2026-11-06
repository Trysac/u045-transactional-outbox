"""Input validation and JSON encoding for outbox rows (internal)."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping

from .errors import OutboxError

MAX_PAYLOAD_BYTES = 262_144
MAX_DEPTH = 32
MAX_NODES = 262_144
MAX_HEADERS = 32
MAX_HEADER_CHARS = 1024
MAX_KEY_CHARS = 1024

_TOPIC_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")
_TABLE_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")
_SCALARS = (str, int, bool)


def validate_table(table: object) -> str:
    """Return ``table`` if it is a safe SQL identifier, else raise ``invalid_table``."""
    if not isinstance(table, str) or _TABLE_RE.fullmatch(table) is None:
        raise OutboxError("invalid_table", "table must match [a-z_][a-z0-9_]{0,62}")
    return table


def validate_topic(topic: object) -> str:
    """Return ``topic`` if it is a valid topic name, else raise ``invalid_topic``."""
    if not isinstance(topic, str) or _TOPIC_RE.fullmatch(topic) is None:
        raise OutboxError("invalid_topic", "topic must match [a-z0-9][a-z0-9._-]{0,127}")
    return topic


def validate_key(key: object) -> str | None:
    """Return ``key`` if it is None or a string of at most 1024 characters."""
    if key is None:
        return None
    if not isinstance(key, str) or len(key) > MAX_KEY_CHARS:
        raise OutboxError("payload_invalid", "key must be None or a string of at most 1024 chars")
    return key


def _check_tree(value: object, depth: int, counter: list[int]) -> None:
    counter[0] += 1
    if counter[0] > MAX_NODES:
        raise OutboxError("payload_too_large", "payload has too many values")
    if value is None or type(value) in _SCALARS:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise OutboxError("payload_invalid", "payload floats must be finite")
        return
    if depth >= MAX_DEPTH:
        raise OutboxError("payload_invalid", "payload is nested too deeply")
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise OutboxError("payload_invalid", "payload dict keys must be strings")
            _check_tree(item, depth + 1, counter)
    elif type(value) is list:
        for item in value:
            _check_tree(item, depth + 1, counter)
    else:
        name = type(value).__name__[:50]
        raise OutboxError("payload_invalid", f"unsupported payload value type: {name}")


def encode_payload(payload: object) -> str:
    """Validate ``payload`` and return its canonical JSON text.

    Raises:
        OutboxError: ``payload_invalid`` or ``payload_too_large``.
    """
    if type(payload) is not dict:
        raise OutboxError("payload_invalid", "payload must be a dict")
    _check_tree(payload, 0, [0])
    try:
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise OutboxError("payload_invalid", "payload is not JSON serializable") from None
    if len(text.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise OutboxError("payload_too_large", "payload exceeds 262144 JSON bytes")
    return text


def encode_headers(headers: object) -> str:
    """Validate ``headers`` (None or at most 32 str entries of 1024 chars) and encode them."""
    if headers is None:
        return "{}"
    if not isinstance(headers, Mapping):
        raise OutboxError("payload_invalid", "headers must be a mapping of strings")
    if len(headers) > MAX_HEADERS:
        raise OutboxError("payload_invalid", "headers hold more than 32 entries")
    checked: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise OutboxError("payload_invalid", "header names and values must be strings")
        if len(name) > MAX_HEADER_CHARS or len(value) > MAX_HEADER_CHARS:
            raise OutboxError(
                "payload_invalid", "header names and values are limited to 1024 chars"
            )
        checked[name] = value
    return json.dumps(checked, sort_keys=True, separators=(",", ":"))
