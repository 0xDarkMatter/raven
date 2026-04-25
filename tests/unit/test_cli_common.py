"""Cover the helpers in cli/_common.py.

Most of these are reached only through narrow CLI paths (e.g. the
mutually-exclusive --body/--body-file branch, the empty-body case,
the human-readable formatter for messages with tags). Test them
directly so coverage doesn't miss the branches.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import typer

from claude_bus.cli._common import (
    _serialise,
    echo_json,
    echo_message_human,
    echo_messages_human,
    message_to_json,
    parse_address,
    parse_body,
)
from claude_bus.client import Message


# ----- _serialise recursion ----------------------------------------


def test_serialise_datetime_to_iso() -> None:
    ts = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)
    assert _serialise(ts) == ts.isoformat()


def test_serialise_path_to_str() -> None:
    p = Path("/tmp/x.db")
    assert _serialise(p) == str(p)


def test_serialise_dict_recurses() -> None:
    p = Path("/x")
    out = _serialise({"a": p, "b": [p, "raw"]})
    assert out == {"a": str(p), "b": [str(p), "raw"]}


def test_serialise_passes_through_primitives() -> None:
    assert _serialise(42) == 42
    assert _serialise("hi") == "hi"
    assert _serialise(None) is None


# ----- echo helpers ------------------------------------------------


def _make_msg(**overrides) -> Message:
    base = dict(
        id=1,
        session_id="s",
        sender="alice:s",
        recipient="bob:s",
        recipient_role="bob",
        recipient_session="s",
        type="ping",
        body={"k": 1},
        status="unread",
        tags=[],
        created_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC),
    )
    base.update(overrides)
    return Message(**base)


def test_echo_message_human_no_tags(capsys) -> None:
    msg = _make_msg()
    echo_message_human(msg)
    out = capsys.readouterr().out
    assert "#1" in out
    assert "alice:s -> bob:s" in out
    assert "type=ping" in out
    assert "tags:" not in out  # no tags branch suppressed


def test_echo_message_human_with_tags(capsys) -> None:
    msg = _make_msg(tags=["alpha", "beta"])
    echo_message_human(msg)
    out = capsys.readouterr().out
    assert "tags: alpha, beta" in out


def test_echo_messages_human_empty(capsys) -> None:
    echo_messages_human([])
    assert "(no messages)" in capsys.readouterr().out


def test_echo_messages_human_renders_each(capsys) -> None:
    echo_messages_human([_make_msg(id=1), _make_msg(id=2)])
    out = capsys.readouterr().out
    assert "#1" in out
    assert "#2" in out


def test_echo_json_serialises_path_and_datetime(capsys) -> None:
    payload = {"db": Path("/tmp/x"), "when": datetime(2026, 1, 1, tzinfo=UTC)}
    echo_json(payload)
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["db"] == str(Path("/tmp/x"))
    assert parsed["when"].startswith("2026-01-01")


def test_message_to_json_round_trip() -> None:
    msg = _make_msg(tags=["x"])
    payload = message_to_json(msg)
    assert payload["id"] == 1
    assert payload["tags"] == ["x"]
    # JSON-serialisable end-to-end.
    json.dumps(payload)


# ----- parse_body branches -----------------------------------------


def test_parse_body_returns_empty_dict_when_neither_provided() -> None:
    assert parse_body(None, None) == {}


def test_parse_body_rejects_both_supplied(tmp_path: Path) -> None:
    f = tmp_path / "b.json"
    f.write_text("{}")
    with pytest.raises(typer.Exit) as exc:
        parse_body('{"k":1}', f)
    assert exc.value.exit_code == 2  # EXIT_CONFIG


def test_parse_body_reads_from_file(tmp_path: Path) -> None:
    f = tmp_path / "b.json"
    f.write_text('{"a": [1, 2]}', encoding="utf-8")
    assert parse_body(None, f) == {"a": [1, 2]}


def test_parse_body_invalid_json_raises_5() -> None:
    with pytest.raises(typer.Exit) as exc:
        parse_body("not json", None)
    assert exc.value.exit_code == 5


def test_parse_body_non_object_raises_5() -> None:
    """Lists / scalars at the top level are rejected (must be a JSON object)."""
    with pytest.raises(typer.Exit) as exc:
        parse_body("[1, 2, 3]", None)
    assert exc.value.exit_code == 5
    with pytest.raises(typer.Exit) as exc:
        parse_body("42", None)
    assert exc.value.exit_code == 5


# ----- parse_address branches --------------------------------------


def test_parse_address_valid() -> None:
    assert parse_address("alice:s1") == ("alice", "s1")


def test_parse_address_no_colon_exits_2() -> None:
    with pytest.raises(typer.Exit) as exc:
        parse_address("nocolon")
    assert exc.value.exit_code == 2


def test_parse_address_empty_parts_exit_2() -> None:
    for bad in ("alice:", ":session", ":"):
        with pytest.raises(typer.Exit) as exc:
            parse_address(bad)
        assert exc.value.exit_code == 2, f"address {bad!r} should fail"
