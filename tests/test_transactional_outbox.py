"""Behavior tests for the public API of transactional_outbox."""

from __future__ import annotations

import pytest

import transactional_outbox


def test_normalize_trims_and_lowercases() -> None:
    assert transactional_outbox.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        transactional_outbox.normalize(value)


def test_version_is_semver() -> None:
    parts = transactional_outbox.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
