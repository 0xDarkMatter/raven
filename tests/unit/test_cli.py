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


def test_serve_preflights_unwritable_db(tmp_path: Path) -> None:
    """`claude-bus serve` must fail fast (exit 3) on an unwritable DB path,
    rather than binding the port and dying on the first request."""
    # Use a path we can't create — point inside a file (not a directory).
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir")
    bad_db = blocker / "claude-bus.db"
    code, _ = _run("serve", "--port", "0", db=bad_db)
    assert code == 3, f"expected EXIT_DB (3), got {code}"


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


def test_version_subcommand() -> None:
    from claude_bus import __version__

    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_short_flags_on_inbox(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("send", "--from", "a:s", "--to", "b:s", "--type", "x",
         "--body", '{"k": 1}', db=db)
    code, out = _run("inbox", "-r", "b:s", "-j", "-m", "10", db=db)
    assert code == 0
    payload = json.loads(out)
    assert len(payload["messages"]) == 1


def test_short_flags_on_send(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    code, _ = _run("send", "--from", "a:s", "--to", "b:s", "-t", "x",
                   "--body", "{}", db=db)
    assert code == 0


def test_send_with_body_file(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    body = tmp_path / "body.json"
    body.write_text('{"k": [1, 2, 3]}', encoding="utf-8")
    code, out = _run(
        "send", "--from", "a:s", "--to", "b:s", "--type", "x",
        "--body-file", str(body),
        db=db,
    )
    assert code == 0
    code, inbox_out = _run("inbox", "--role", "b:s", "--json", db=db)
    payload = json.loads(inbox_out)
    assert payload["messages"][0]["body"] == {"k": [1, 2, 3]}


def test_send_with_correlation_and_reply_to_flags(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("send", "--from", "a:s", "--to", "b:s", "--type", "x",
         "--body", "{}", db=db)
    code, _ = _run(
        "send", "--from", "a:s", "--to", "b:s", "--type", "y",
        "--body", "{}", "--correlation-id", "1", "--reply-to", "1",
        db=db,
    )
    assert code == 0
    code, out = _run("read", "2", "--json", db=db)
    payload = json.loads(out)
    assert payload["correlation_id"] == 1
    assert payload["reply_to"] == 1


def test_inbox_human_output_renders_messages(tmp_path: Path) -> None:
    """Without --json the inbox prints a friendly table; verify it's not empty."""
    db = tmp_path / "bus.db"
    _run("send", "--from", "a:s", "--to", "b:s", "--type", "x",
         "--body", '{"hello": "world"}', db=db)
    code, out = _run("inbox", "--role", "b:s", db=db)
    assert code == 0
    assert "a:s -> b:s" in out
    assert "type=x" in out
    assert "hello" in out


def test_inbox_human_output_empty(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    code, out = _run("inbox", "--role", "b:s", db=db)
    assert code == 0
    assert "(no messages)" in out


def test_read_human_output_renders(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    _run("send", "--from", "a:s", "--to", "b:s", "--type", "x",
         "--body", "{}", db=db)
    code, out = _run("read", "1", db=db)
    assert code == 0
    assert "#1" in out
    assert "a:s -> b:s" in out


def test_init_with_explicit_db_path(tmp_path: Path, monkeypatch) -> None:
    """`init --db /custom/path` should create the DB at that path."""
    monkeypatch.chdir(tmp_path)
    custom_db = tmp_path / "custom.db"
    code, out = _run("init", db=custom_db)
    assert code == 0
    assert custom_db.exists()
    assert (tmp_path / "claude-bus.yaml").exists()


def test_send_schema_validation_error_via_cli(tmp_path: Path) -> None:
    """Register a Pydantic schema, then send a body that doesn't match.
    The CLI must catch SchemaValidationError and exit 5 with a message."""
    from pydantic import BaseModel
    from claude_bus import SchemaRegistry

    class Strict(BaseModel):
        n: int

    SchemaRegistry.register("strict", Strict)
    try:
        db = tmp_path / "bus.db"
        # Use runner directly so we can check `result.output` (stderr+stdout
        # combined). The error message is emitted via typer.echo(..., err=True).
        result = runner.invoke(
            app,
            [
                "send", "--from", "a:s", "--to", "b:s", "--type", "strict",
                "--body", '{"n": "not-an-int"}',
                "--db", str(db),
            ],
        )
        assert result.exit_code == 5
        assert "schema validation failed" in result.output
    finally:
        SchemaRegistry.clear()


def test_global_version_flag() -> None:
    """`claude-bus --version` exits cleanly with the version string."""
    from claude_bus import __version__

    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_send_to_address_with_empty_role_exits_2(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    code, _ = _run(
        "send", "--from", "a:s", "--to", ":s", "--type", "x",
        "--body", "{}", db=db,
    )
    assert code == 2


def test_help_lists_eight_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    out = result.stdout
    for cmd in (
        "init", "doctor", "send", "inbox", "read", "ack", "serve", "session",
    ):
        assert cmd in out, f"command {cmd!r} missing from --help"
