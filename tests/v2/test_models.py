"""Tests for raven_bus.models — the ADR-002 address grammar validators."""

from __future__ import annotations

import pytest

from raven_bus.exceptions import InvalidAddressError
from raven_bus.models import (
    format_consumer_id,
    parse_consumer_id,
    validate_atom,
    validate_channel_name,
    validate_tags,
)

TRAILING_NEWLINE_CASES = [
    ("atom", lambda: validate_atom("worker\n", what="role")),
    ("consumer run", lambda: parse_consumer_id("worker@run\n")),
    ("consumer role", lambda: parse_consumer_id("worker\n@run")),
    ("format", lambda: format_consumer_id("worker", "run\n")),
    ("channel", lambda: validate_channel_name("run/x\n")),
    ("channel segment", lambda: validate_channel_name("run/x\n/lane")),
    ("tag", lambda: validate_tags(["tag\n"])),
]


@pytest.mark.parametrize(
    "call", [c for _, c in TRAILING_NEWLINE_CASES], ids=[i for i, _ in TRAILING_NEWLINE_CASES]
)
def test_validators_reject_a_trailing_newline(call) -> None:
    """QA store #5: ``^...$`` with ``re.match`` lets ``$`` match before a
    final "\\n", so "run/x\\n" validated — and was stored as a DISTINCT,
    invisible channel/consumer row that escapes teardown's name scoping.
    The grammar must match the whole string."""
    with pytest.raises(InvalidAddressError):
        call()


def test_validators_still_accept_the_grammar() -> None:
    assert validate_atom("w0.r-k_1", what="role") == "w0.r-k_1"
    assert parse_consumer_id("worker@run-1") == ("worker", "run-1")
    assert validate_channel_name("run/v0-2/lane/3") == "run/v0-2/lane/3"
    assert validate_tags(["Tag.1", "x_y-z"]) == ["Tag.1", "x_y-z"]
