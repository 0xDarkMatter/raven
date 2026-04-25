"""Cover the `claude-bus tail` command."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from claude_bus import BusClient
from claude_bus.cli.main import app

runner = CliRunner()


def test_tail_no_follow_prints_existing_messages(db_path: Path) -> None:
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    a.send(to=b.address, type="ping", body={"n": 1})
    a.send(to=b.address, type="ping", body={"n": 2})

    result = runner.invoke(
        app, ["tail", "--db", str(db_path), "--no-follow"]
    )
    assert result.exit_code == 0
    assert "#1" in result.stdout
    assert "#2" in result.stdout
    assert "type=ping" in result.stdout


def test_tail_json_mode(db_path: Path) -> None:
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    a.send(to=b.address, type="ping", body={"n": 7})

    result = runner.invoke(
        app, ["tail", "--db", str(db_path), "--no-follow", "--json"]
    )
    assert result.exit_code == 0
    line = result.stdout.strip().splitlines()[0]
    payload = json.loads(line)
    assert payload["id"] == 1
    assert payload["type"] == "ping"
    assert payload["body"] == {"n": 7}


def test_tail_role_filter(db_path: Path) -> None:
    """--role X:s shows only messages whose recipient resolves to X:s."""
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    c = BusClient(session_id="s", role="charlie", db_path=db_path)
    a.send(to=b.address, type="x", body={"to": "bob"})
    a.send(to=c.address, type="x", body={"to": "charlie"})

    result = runner.invoke(
        app,
        ["tail", "--db", str(db_path), "--no-follow", "-r", "bob:s"],
    )
    assert result.exit_code == 0
    assert "to" in result.stdout
    assert "bob" in result.stdout
    assert "charlie" not in result.stdout


def test_tail_from_id_skips_older(db_path: Path) -> None:
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    a.send(to=b.address, type="x", body={"i": 1})
    a.send(to=b.address, type="x", body={"i": 2})
    a.send(to=b.address, type="x", body={"i": 3})

    result = runner.invoke(
        app,
        ["tail", "--db", str(db_path), "--no-follow", "--from", "1"],
    )
    assert result.exit_code == 0
    assert "#1" not in result.stdout
    assert "#2" in result.stdout
    assert "#3" in result.stdout


def test_tail_invalid_role_exits_2(db_path: Path) -> None:
    result = runner.invoke(
        app,
        ["tail", "--db", str(db_path), "--no-follow", "-r", "no-colon"],
    )
    assert result.exit_code == 2
    assert "<role>:<session>" in result.output


def test_tail_truncates_long_body(db_path: Path) -> None:
    """Body preview is truncated at 80 chars in human mode."""
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    a.send(to=b.address, type="x", body={"long": "x" * 200})

    result = runner.invoke(
        app, ["tail", "--db", str(db_path), "--no-follow"]
    )
    assert result.exit_code == 0
    assert "..." in result.stdout


def test_tail_follow_then_keyboard_interrupt(
    db_path: Path, monkeypatch
) -> None:
    """Simulate Ctrl-C during follow mode — exits cleanly with code 0."""
    import time as time_mod

    # Patch time.sleep so the second call raises KeyboardInterrupt,
    # giving the loop one full poll then a clean exit.
    calls = {"n": 0}
    real_sleep = time_mod.sleep

    def fake_sleep(s):
        calls["n"] += 1
        if calls["n"] >= 1:
            raise KeyboardInterrupt
        real_sleep(s)

    monkeypatch.setattr(time_mod, "sleep", fake_sleep)
    result = runner.invoke(
        app, ["tail", "--db", str(db_path), "--interval", "0.01"]
    )
    assert result.exit_code == 0


# ---------- _core.list_since direct tests ----------------------------


def test_list_since_filters_by_after_id(db_path: Path) -> None:
    from claude_bus import _core
    from claude_bus.db import connection

    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    for i in range(5):
        a.send(to=b.address, type="x", body={"i": i})

    with connection(db_path) as conn:
        msgs = _core.list_since(conn, after_id=2)
    assert [m.id for m in msgs] == [3, 4, 5]


def test_list_since_recipient_filter(db_path: Path) -> None:
    from claude_bus import _core
    from claude_bus.db import connection

    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    c = BusClient(session_id="s", role="charlie", db_path=db_path)
    a.send(to=b.address, type="x", body={})
    a.send(to=c.address, type="x", body={})

    with connection(db_path) as conn:
        only_bob = _core.list_since(conn, 0, recipient=b.alias)
    assert len(only_bob) == 1
    assert only_bob[0].recipient == b.alias


def test_list_since_sender_filter(db_path: Path) -> None:
    from claude_bus import _core
    from claude_bus.db import connection

    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    c = BusClient(session_id="s", role="charlie", db_path=db_path)
    a.send(to=c.address, type="x", body={})
    b.send(to=c.address, type="x", body={})

    with connection(db_path) as conn:
        from_alice = _core.list_since(conn, 0, sender=a.alias)
    assert len(from_alice) == 1
    assert from_alice[0].sender == a.alias


def test_list_since_limit(db_path: Path) -> None:
    from claude_bus import _core
    from claude_bus.db import connection

    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    for _ in range(10):
        a.send(to=b.address, type="x", body={})

    with connection(db_path) as conn:
        msgs = _core.list_since(conn, 0, limit=3)
    assert len(msgs) == 3
