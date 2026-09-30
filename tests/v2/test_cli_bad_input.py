"""Hostile/mistyped CLI input is a one-line ``error:`` with the right exit code.

QA cli #3. The contract (AGENTS.md "CLI surface"): exit 0 ok / 2 usage /
3 not-found / 10 error, a failure is ONE ``error: ...`` line, and a
traceback never reaches a user. Typer's own bound/choice errors keep
typer's usage format; they still exit 2.

Two tests shell out (``python -m raven_bus.cli.main``) because the
last-resort net lives in ``cli_main``, which CliRunner bypasses; the
in-process twin ``test_cli_main_renders_any_exception_as_one_line``
keeps that net inside coverage.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import raven_bus
from raven_bus import db as bus_db
from raven_bus import log
from raven_bus.cli import teardown as teardown_cmd
from raven_bus.cli.main import app, cli_main

runner = CliRunner()

MAX_ID = 2**63 - 1
THIRTY_DAYS_S = 30 * 24 * 3600
SRC_DIR = Path(raven_bus.__file__).resolve().parents[1]


def _send(db: Path, channel: str, *extra: str):
    return runner.invoke(
        app,
        [
            "send", "--db", str(db), "--channel", channel,
            "--from", "a@r1", "-t", "t", "--body", "{}", *extra,
        ],
    )


def _append_many(db: Path, channel_names: list[str]) -> None:
    """One message per entry, in list order (so ids follow the list)."""
    with bus_db.connection(db) as conn:
        for name in channel_names:
            log.append(conn, channel=name, sender="a@r1", type="t", body={})


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """The real console path (cli_main), in a child process that imports
    the same raven_bus source tree as this test process."""
    env = dict(os.environ)
    env.pop("RAVEN_DB", None)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (str(SRC_DIR), env.get("PYTHONPATH")) if p
    )
    return subprocess.run(
        [sys.executable, "-m", "raven_bus.cli.main", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_cli_main_renders_any_exception_as_one_line(capsys) -> None:
    with (
        patch("raven_bus.cli.main.app", side_effect=sqlite3.DatabaseError("file is not a database")),
        pytest.raises(SystemExit) as excinfo,
    ):
        cli_main()
    assert excinfo.value.code == 10
    assert capsys.readouterr().err == "error: DatabaseError: file is not a database\n"


def test_garbage_db_file_is_one_line_error_not_traceback(tmp_path: Path) -> None:
    garbage = tmp_path / "garbage.db"
    garbage.write_text("this is not sqlite\n" * 50)

    proc = _run_cli("read", "--db", str(garbage), "--channel", "run/r1/a", "--as", "a@r1")

    assert proc.returncode == 10
    assert proc.stderr == "error: DatabaseError: file is not a database\n"
    assert "Traceback" not in proc.stdout + proc.stderr


def test_directory_as_db_is_one_line_error_not_traceback(tmp_path: Path) -> None:
    proc = _run_cli("read", "--db", str(tmp_path), "--channel", "run/r1/a", "--as", "a@r1")

    assert proc.returncode == 10
    assert proc.stderr.startswith("error: OperationalError: ")
    assert proc.stderr.count("\n") == 1
    assert "Traceback" not in proc.stdout + proc.stderr


@pytest.mark.parametrize(
    "argv",
    [
        ["ack", "--channel", "run/r1/a", "--as", "a@r1", "--up-to", "-1"],
        ["ack", "--channel", "run/r1/a", "--as", "a@r1", "--up-to", str(MAX_ID + 1)],
        ["done", "--id", "0", "--as", "a@r1"],
        ["done", "--id", str(MAX_ID + 1), "--as", "a@r1"],
        ["release", "--id", "0", "--as", "a@r1"],
        ["release", "--id", str(MAX_ID + 1), "--as", "a@r1"],
        ["claim", "--channel", "run/r1/q", "--as", "a@r1", "--lease", "0"],
        ["claim", "--channel", "run/r1/q", "--as", "a@r1", "--lease", "-5"],
        ["claim", "--channel", "run/r1/q", "--as", "a@r1", "--lease", str(THIRTY_DAYS_S + 1)],
        ["send", "--channel", "run/r1/a", "--from", "a@r1", "-t", "t", "--body", "{}",
         "--expires-in", "0"],
        ["send", "--channel", "run/r1/a", "--from", "a@r1", "-t", "t", "--body", "{}",
         "--expires-in", "-1"],
        ["send", "--channel", "run/r1/a", "--from", "a@r1", "-t", "t", "--body", "{}",
         "--expires-in", "99999999999999"],
        ["send", "--channel", "run/r1/a", "--from", "a@r1", "-t", "t", "--body", "{}",
         "--reply-to", "0"],
        ["send", "--channel", "run/r1/a", "--from", "a@r1", "-t", "t", "--body", "{}",
         "--reply-to", str(MAX_ID + 1)],
    ],
    ids=[
        "ack-negative", "ack-overflow", "done-zero", "done-overflow",
        "release-zero", "release-overflow", "lease-zero", "lease-negative",
        "lease-over-30d", "expires-zero", "expires-negative", "expires-huge",
        "reply-to-zero", "reply-to-overflow",
    ],
)
def test_out_of_range_integers_are_usage_errors(db: Path, argv: list[str]) -> None:
    result = runner.invoke(app, [*argv, "--db", str(db)])
    assert result.exit_code == 2, result.output
    assert "Invalid value for" in result.output


def test_thirty_day_lease_and_ttl_are_accepted(db: Path) -> None:
    sent = _send(db, "run/r1/q", "--kind", "queue", "--expires-in", str(THIRTY_DAYS_S))
    assert sent.exit_code == 0, sent.output

    claimed = runner.invoke(
        app,
        ["claim", "--db", str(db), "--channel", "run/r1/q", "--as", "b@r1",
         "--lease", str(THIRTY_DAYS_S), "-j"],
    )
    assert claimed.exit_code == 0, claimed.output
    assert json.loads(claimed.output)["id"] == 1


@pytest.mark.parametrize("type_", ["", "   "])
def test_send_empty_type_is_a_usage_error(db: Path, type_: str) -> None:
    result = runner.invoke(
        app,
        ["send", "--db", str(db), "--channel", "run/r1/a", "--from", "a@r1",
         "-t", type_, "--body", "{}"],
    )
    assert result.exit_code == 2
    assert result.output == "error: --type must be non-empty\n"


def test_send_body_too_deep_for_json_parser_is_a_usage_error(db: Path) -> None:
    depth = 100_000  # far past json.loads' recursion limit
    body = '{"a":' * depth + "1" + "}" * depth

    result = runner.invoke(
        app,
        ["send", "--db", str(db), "--channel", "run/r1/a", "--from", "a@r1",
         "-t", "t", "--body", body],
    )

    assert result.exit_code == 2
    assert result.output == "error: body is nested too deeply to parse\n"


def test_teardown_without_yes_and_without_a_tty_refuses(db: Path) -> None:
    _append_many(db, ["run/r1/a"])

    result = runner.invoke(app, ["teardown", "--db", str(db), "--run", "r1"])

    assert result.exit_code == 2
    assert result.output == (
        "error: refusing to tear down without confirmation (pass --yes)\n"
    )
    with bus_db.connection(db) as conn:
        assert log.read_after(conn, "run/r1/a", 0) != []


def test_teardown_eof_at_an_interactive_prompt_refuses(db: Path) -> None:
    """Windows NUL claims isatty(): the prompt then hits EOF."""
    _append_many(db, ["run/r1/a"])

    with patch("raven_bus.cli.teardown._stdin_is_interactive", return_value=True):
        result = runner.invoke(app, ["teardown", "--db", str(db), "--run", "r1"], input="")

    assert result.exit_code == 2
    assert result.output.endswith(
        "\nerror: refusing to tear down without confirmation (pass --yes)\n"
    )
    with bus_db.connection(db) as conn:
        assert log.read_after(conn, "run/r1/a", 0) != []


def test_stdin_is_interactive_reads_isatty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "stdin", None)
    assert teardown_cmd._stdin_is_interactive() is False
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(isatty=lambda: True))
    assert teardown_cmd._stdin_is_interactive() is True
