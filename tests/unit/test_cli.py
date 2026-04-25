"""CLI smoke tests via Typer's CliRunner."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from claude_bus.cli.main import app

runner = CliRunner()


def _run(*args: str, db: Path | None = None) -> tuple[int, str]:
    cmd = list(args)
    if db is not None:
        cmd.extend(["--db", str(db)])
    result = runner.invoke(app, cmd)
    return result.exit_code, result.stdout


def test_init_creates_config_and_db(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0
    assert (tmp_path / "claude-bus.yaml").exists()
    assert (tmp_path / "claude-bus.db").exists()


def test_init_refuses_overwrite_without_force(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["init"])
    result = runner.invoke(app, ["init"])
    assert result.exit_code != 0


def test_send_inbox_read_ack_round_trip(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    code, _ = _run("session", "init", "swarm-1", db=db)
    assert code == 0
    code, _ = _run(
        "send", "--from", "a:s1", "--to", "b:s1", "--type", "ping",
        "--body", '{"n": 1}',
        db=db,
    )
    assert code == 0
    code, out = _run("inbox", "--role", "b:s1", "--json", db=db)
    assert code == 0
    payload = json.loads(out)
    assert len(payload["messages"]) == 1
    msg_id = payload["messages"][0]["id"]

    code, _ = _run("read", str(msg_id), db=db)
    assert code == 0
    code, _ = _run("ack", str(msg_id), db=db)
    assert code == 0
    code, out = _run("inbox", "--role", "b:s1", "--json", db=db)
    assert code == 0
    assert json.loads(out)["messages"] == []


def test_read_missing_returns_4(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("session", "init", "x", db=db)
    code, _ = _run("read", "999", db=db)
    assert code == 4


def test_ack_missing_returns_4(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("session", "init", "x", db=db)
    code, _ = _run("ack", "999", db=db)
    assert code == 4


def test_send_invalid_body_json_exits_5(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("session", "init", "x", db=db)
    code, _ = _run(
        "send", "--from", "a:s", "--to", "b:s", "--type", "x",
        "--body", "not-json",
        db=db,
    )
    assert code == 5


def test_send_invalid_address_exits_2(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("session", "init", "x", db=db)
    code, _ = _run(
        "send", "--from", "no-colon", "--to", "b:s", "--type", "x",
        "--body", "{}",
        db=db,
    )
    assert code == 2


def test_doctor_runs(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    code, out = _run("doctor", db=db)
    # exit 0 expected when port 7713 is free; allow non-zero in case it's
    # already in use locally (CI hosts often have nothing on 7713).
    assert code in (0, 1)
    assert "db" in out


def test_read_and_ack_dont_pollute_aliases_table(tmp_path: Path) -> None:
    """Read/ack must not create a spurious '__cli__' / 'reader' alias row."""
    import sqlite3

    db = tmp_path / "bus.db"
    code, _ = _run("send", "--from", "a:s", "--to", "b:s", "--type", "x",
                   "--body", "{}", db=db)
    assert code == 0
    aliases_before = sqlite3.connect(db).execute(
        "SELECT alias, role FROM aliases ORDER BY alias"
    ).fetchall()
    _run("read", "1", db=db)
    _run("ack", "1", db=db)
    aliases_after = sqlite3.connect(db).execute(
        "SELECT alias, role FROM aliases ORDER BY alias"
    ).fetchall()
    assert aliases_before == aliases_after, (
        f"read/ack added spurious aliases: {set(aliases_after) - set(aliases_before)}"
    )


def test_help_lists_eight_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    out = result.stdout
    for cmd in (
        "init", "doctor", "send", "inbox", "read", "ack", "serve", "session",
    ):
        assert cmd in out, f"command {cmd!r} missing from --help"
