"""Tests for raven_bus.cli._common's shared rendering helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from raven_bus.cli._common import (
    _serialise,
    echo_message_human,
    echo_messages_human,
)
from raven_bus.models import Message


def test_serialise_datetime_to_isoformat() -> None:
    dt = datetime(2026, 1, 1, tzinfo=UTC)
    assert _serialise(dt) == dt.isoformat()


def test_serialise_path_to_str() -> None:
    p = Path("/tmp/bus.db")
    assert _serialise(p) == str(p)


def _message(**overrides) -> Message:
    fields = {
        "id": 1,
        "channel": "run/r1/team",
        "sender": "alice@r1",
        "type": "ping",
        "urgency": "prompt",
        "body": {},
        "tags": [],
        "reply_to": None,
        "thread_id": None,
        "expires_at": None,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    fields.update(overrides)
    return Message(**fields)


def test_echo_message_human_prints_tags(capsys) -> None:
    echo_message_human(_message(tags=["urgent", "prod"]))
    out = capsys.readouterr().out
    assert "tags: urgent, prod" in out


def test_echo_messages_human_empty_prints_placeholder(capsys) -> None:
    echo_messages_human([])
    assert "(no messages)" in capsys.readouterr().out
