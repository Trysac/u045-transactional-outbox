"""Typed errors raised by the transactional outbox."""

from __future__ import annotations

from typing import Literal

ErrorCode = Literal[
    "payload_invalid",
    "payload_too_large",
    "invalid_topic",
    "invalid_table",
    "invalid_clock",
    "lease_lost",
    "outbox_full",
]

_MAX_MESSAGE_CHARS = 200


class OutboxError(Exception):
    """Base error of the library.

    Attributes:
        code: Stable machine-readable error code.
        message: Human readable text, at most 200 characters, never a payload.
    """

    def __init__(self, code: ErrorCode, message: str) -> None:
        text = message[:_MAX_MESSAGE_CHARS]
        super().__init__(text)
        self.code: ErrorCode = code
        self.message: str = text
