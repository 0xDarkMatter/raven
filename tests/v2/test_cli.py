"""Tests for the raven v2 Typer CLI.  LANE: cli (raven2-p1).

The wrapped modules (log/cursors/claims/channels/db) are parallel
lanes and may still be stub ``NotImplementedError`` bodies in this
worktree, so every store call is mocked here via ``unittest.mock.patch``
on the module attribute the CLI calls through (e.g.
``raven_bus.log.append``). These tests assert argument mapping, output
rendering, and exit-code behaviour — not store correctness, which is
each wrapped module's own lane's job.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from raven_bus.cli._common import EXIT_ERROR
from raven_bus.cli.main import app, cli_main
from raven_bus.exceptions import (
    ClaimDeniedError,
    RavenBusError,
    SchemaMismatchError,
    UnknownChannelError,
    WrongChannelKindError,
)
from raven_bus.models import Channel, Claim, Cursor, Message, SweepResult

runner = CliRunner()


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


def _message(**overrides) -> Message:
    fields = {
        "id": 1,
        "channel": "run/r1/team",
        "sender": "alice@r1",
        "type": "ping",
        "urgency": "prompt",
        "body": {"n": 1},
        "tags": [],
        "reply_to": None,
        "thread_id": None,
        "expires_at": None,
        "created_at": _now(),
    }
    fields.update(overrides)
    return Message(**fields)


def _channel(**overrides) -> Channel:
    fields = {
        "id": 1,
        "name": "run/r1/team",
        "kind": "broadcast",
        "retention_s": None,
        "max_deliveries": 3,
        "created_at": _now(),
    }
    fields.update(overrides)
    return Channel(**fields)


def _cursor(**overrides) -> Cursor:
    fields = {
        "consumer": "alice@r1",
        "channel": "run/r1/team",
        "last_ack_id": 5,
        "updated_at": _now(),
    }
    fields.update(overrides)
    return Cursor(**fields)


def _claim(**overrides) -> Claim:
    fields = {
        "message_id": 1,
        "consumer": "alice@r1",
        "state": "leased",
        "deliveries": 1,
        "lease_until": _now(),
        "updated_at": _now(),
    }
    fields.update(overrides)
    return Claim(**fields)


def _mock_connection(conn: MagicMock) -> MagicMock:
    """A stand-in for ``db.connection(...)`` — a context manager yielding ``conn``."""
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm


# --------------------------------------------------------------------
# send
# --------------------------------------------------------------------


def test_send_happy_path() -> None:
    conn = MagicMock()
    msg = _message(id=7)
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.channels.ensure_channel") as ensure_channel,
        patch("raven_bus.log.append", return_value=msg) as append,
    ):
        result = runner.invoke(
            app,
            [
                "send",
                "--channel", "run/r1/team",
                "--from", "alice@r1",
                "--type", "ping",
                "--body", '{"n": 1}',
                "--tag", "a",
                "--tag", "b",
                "--kind", "broadcast",
            ],
        )
    assert result.exit_code == 0
    assert "sent #7" in result.stdout
    ensure_channel.assert_called_once_with(conn, "run/r1/team", "broadcast")
    append.assert_called_once()
    _, kwargs = append.call_args
    assert kwargs["channel"] == "run/r1/team"
    assert kwargs["sender"] == "alice@r1"
    assert kwargs["type"] == "ping"
    assert kwargs["body"] == {"n": 1}
    assert kwargs["tags"] == ["a", "b"]
    assert kwargs["ensure"] is False


def test_send_invalid_body_json_exits_usage() -> None:
    result = runner.invoke(
        app,
        [
            "send",
            "--channel", "run/r1/team",
            "--from", "alice@r1",
            "--type", "ping",
            "--body", "not-json",
        ],
    )
    assert result.exit_code == 2
    assert "error:" in result.output


def test_send_non_dict_body_exits_usage() -> None:
    result = runner.invoke(
        app,
        [
            "send",
            "--channel", "run/r1/team",
            "--from", "alice@r1",
            "--type", "ping",
            "--body", "[1, 2, 3]",
        ],
    )
    assert result.exit_code == 2
    assert "body must be a JSON object" in result.output


def test_send_bad_address_exits_usage() -> None:
    result = runner.invoke(
        app,
        [
            "send",
            "--channel", "run/r1/team",
            "--from", "no-at-sign",
            "--type", "ping",
            "--body", "{}",
        ],
    )
    assert result.exit_code == 2
    assert "error:" in result.output


def test_send_unknown_channel_exits_not_found() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.log.append",
            side_effect=UnknownChannelError("no such channel"),
        ),
    ):
        result = runner.invoke(
            app,
            [
                "send",
                "--channel", "run/r1/team",
                "--from", "alice@r1",
                "--type", "ping",
                "--body", "{}",
            ],
        )
    assert result.exit_code == 3
    assert "error: no such channel" in result.output


def test_send_wrong_kind_exits_generic_error() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.channels.ensure_channel",
            side_effect=WrongChannelKindError("kind mismatch"),
        ),
    ):
        result = runner.invoke(
            app,
            [
                "send",
                "--channel", "run/r1/team",
                "--from", "alice@r1",
                "--type", "ping",
                "--body", "{}",
                "--kind", "queue",
            ],
        )
    assert result.exit_code == 10
    assert "error: kind mismatch" in result.output


# --------------------------------------------------------------------
# read
# --------------------------------------------------------------------


def test_read_happy_path_human() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.cursors.pending", return_value=[_message()]) as pending,
    ):
        result = runner.invoke(
            app, ["read", "--channel", "run/r1/team", "--as", "alice@r1"]
        )
    assert result.exit_code == 0
    assert "#1" in result.stdout
    pending.assert_called_once_with(conn, "alice@r1", "run/r1/team", limit=100)


def test_read_happy_path_json() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.cursors.pending", return_value=[_message()]),
    ):
        result = runner.invoke(
            app,
            [
                "read",
                "--channel", "run/r1/team",
                "--as", "alice@r1",
                "-m", "5",
                "-j",
            ],
        )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload[0]["id"] == 1


def test_read_bad_address_exits_usage() -> None:
    result = runner.invoke(
        app, ["read", "--channel", "run/r1/team", "--as", "bad"]
    )
    assert result.exit_code == 2


def test_read_unknown_channel_exits_not_found() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.cursors.pending",
            side_effect=UnknownChannelError("gone"),
        ),
    ):
        result = runner.invoke(
            app, ["read", "--channel", "run/r1/team", "--as", "alice@r1"]
        )
    assert result.exit_code == 3


# --------------------------------------------------------------------
# ack
# --------------------------------------------------------------------


def test_ack_happy_path() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.cursors.ack", return_value=_cursor(last_ack_id=9)) as ack,
    ):
        result = runner.invoke(
            app,
            [
                "ack",
                "--channel", "run/r1/team",
                "--as", "alice@r1",
                "--up-to", "9",
            ],
        )
    assert result.exit_code == 0
    assert "up_to=9" in result.stdout
    ack.assert_called_once_with(conn, "alice@r1", "run/r1/team", 9)


def test_ack_bad_address_exits_usage() -> None:
    result = runner.invoke(
        app,
        ["ack", "--channel", "run/r1/team", "--as", "bad", "--up-to", "1"],
    )
    assert result.exit_code == 2


def test_ack_wrong_channel_kind_exits_generic_error() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.cursors.ack",
            side_effect=WrongChannelKindError("not a broadcast channel"),
        ),
    ):
        result = runner.invoke(
            app,
            [
                "ack",
                "--channel", "run/r1/team",
                "--as", "alice@r1",
                "--up-to", "9",
            ],
        )
    assert result.exit_code == 10


# --------------------------------------------------------------------
# claim
# --------------------------------------------------------------------


def test_claim_happy_path_with_message() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.claim_next", return_value=_message()) as claim_next,
    ):
        result = runner.invoke(
            app,
            [
                "claim",
                "--channel", "run/r1/queue",
                "--as", "alice@r1",
                "--lease", "60",
            ],
        )
    assert result.exit_code == 0
    assert "#1" in result.stdout
    claim_next.assert_called_once_with(conn, "alice@r1", "run/r1/queue", lease_s=60)


def test_claim_no_message_human() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.claim_next", return_value=None),
    ):
        result = runner.invoke(
            app, ["claim", "--channel", "run/r1/queue", "--as", "alice@r1"]
        )
    assert result.exit_code == 0
    assert "(no message)" in result.stdout


def test_claim_happy_path_with_message_json() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.claim_next", return_value=_message()),
    ):
        result = runner.invoke(
            app,
            ["claim", "--channel", "run/r1/queue", "--as", "alice@r1", "-j"],
        )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["id"] == 1


def test_claim_no_message_json() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.claim_next", return_value=None),
    ):
        result = runner.invoke(
            app,
            ["claim", "--channel", "run/r1/queue", "--as", "alice@r1", "-j"],
        )
    assert result.exit_code == 0
    assert json.loads(result.stdout) is None


def test_claim_bad_address_exits_usage() -> None:
    result = runner.invoke(
        app, ["claim", "--channel", "run/r1/queue", "--as", "bad"]
    )
    assert result.exit_code == 2


def test_claim_wrong_kind_exits_generic_error() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.claims.claim_next",
            side_effect=WrongChannelKindError("not a queue"),
        ),
    ):
        result = runner.invoke(
            app, ["claim", "--channel", "run/r1/team", "--as", "alice@r1"]
        )
    assert result.exit_code == 10


# --------------------------------------------------------------------
# done
# --------------------------------------------------------------------


def test_done_happy_path() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.complete", return_value=_claim(state="done")) as complete,
    ):
        result = runner.invoke(app, ["done", "--id", "1", "--as", "alice@r1"])
    assert result.exit_code == 0
    assert "done #1" in result.stdout
    complete.assert_called_once_with(conn, 1, "alice@r1")


def test_done_bad_address_exits_usage() -> None:
    result = runner.invoke(app, ["done", "--id", "1", "--as", "bad"])
    assert result.exit_code == 2


def test_done_claim_denied_exits_generic_error() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.claims.complete",
            side_effect=ClaimDeniedError("not your claim"),
        ),
    ):
        result = runner.invoke(app, ["done", "--id", "1", "--as", "alice@r1"])
    assert result.exit_code == 10
    assert "error: not your claim" in result.output


# --------------------------------------------------------------------
# release
# --------------------------------------------------------------------


def test_release_happy_path() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.claims.release", return_value=None) as release,
    ):
        result = runner.invoke(app, ["release", "--id", "1", "--as", "alice@r1"])
    assert result.exit_code == 0
    assert "released #1" in result.stdout
    release.assert_called_once_with(conn, 1, "alice@r1")


def test_release_claim_denied_exits_generic_error() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.claims.release",
            side_effect=ClaimDeniedError("not your claim"),
        ),
    ):
        result = runner.invoke(app, ["release", "--id", "1", "--as", "alice@r1"])
    assert result.exit_code == 10


# --------------------------------------------------------------------
# tail
# --------------------------------------------------------------------


def test_tail_no_follow_single_channel_drains_and_exits() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.log.read_after", return_value=[_message(id=1), _message(id=2)]
        ) as read_after,
        patch("raven_bus.channels.list_channels") as list_channels,
    ):
        result = runner.invoke(
            app, ["tail", "--channel", "run/r1/team", "--no-follow"]
        )
    assert result.exit_code == 0
    assert "#1" in result.stdout
    assert "#2" in result.stdout
    read_after.assert_called_once_with(conn, "run/r1/team", 0, include_expired=True)
    list_channels.assert_not_called()


def test_tail_no_follow_all_channels_json() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.channels.list_channels", return_value=[_channel()]),
        patch("raven_bus.log.read_after", return_value=[_message()]),
    ):
        result = runner.invoke(app, ["tail", "--no-follow", "--json"])
    assert result.exit_code == 0
    line = json.loads(result.stdout.strip().splitlines()[0])
    assert line["id"] == 1


def test_tail_truncates_long_body_preview() -> None:
    conn = MagicMock()
    long_body = {"data": "x" * 100}
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.log.read_after", return_value=[_message(body=long_body)]),
        patch("raven_bus.channels.list_channels"),
    ):
        result = runner.invoke(
            app, ["tail", "--channel", "run/r1/team", "--no-follow"]
        )
    assert result.exit_code == 0
    assert "..." in result.stdout


def test_tail_follow_sleeps_then_stops_on_ctrl_c() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.log.read_after", return_value=[]),
        patch("raven_bus.channels.list_channels", return_value=[]),
        patch("time.sleep", side_effect=KeyboardInterrupt),
    ):
        result = runner.invoke(app, ["tail"])
    assert result.exit_code == 0


# --------------------------------------------------------------------
# channels
# --------------------------------------------------------------------


def test_channels_happy_path_human() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch(
            "raven_bus.channels.list_channels", return_value=[_channel()]
        ) as list_channels,
    ):
        result = runner.invoke(app, ["channels", "--prefix", "run/r1/"])
    assert result.exit_code == 0
    assert "run/r1/team" in result.stdout
    list_channels.assert_called_once_with(conn, prefix="run/r1/")


def test_channels_empty_human() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.channels.list_channels", return_value=[]),
    ):
        result = runner.invoke(app, ["channels"])
    assert result.exit_code == 0
    assert "(no channels)" in result.stdout


def test_channels_json() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.channels.list_channels", return_value=[_channel()]),
    ):
        result = runner.invoke(app, ["channels", "-j"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload[0]["name"] == "run/r1/team"


# --------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------


def test_doctor_all_ok() -> None:
    conn = MagicMock()
    conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=("2",))),
        MagicMock(fetchone=MagicMock(return_value=("wal",))),
    ]
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.db.sweep", return_value=SweepResult()),
    ):
        result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "all checks passed" in result.stdout


def test_doctor_requests_the_expired_count() -> None:
    """sweep's expired COUNT is opt-in (QA store #9); doctor is the one
    caller that reports it, so it must ask."""
    conn = MagicMock()
    conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=("2",))),
        MagicMock(fetchone=MagicMock(return_value=("wal",))),
    ]
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.db.sweep", return_value=SweepResult(expired=4)) as sweep,
    ):
        result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    sweep.assert_called_once_with(conn, count_expired=True)
    assert "expired=4" in result.stdout


def test_doctor_reports_schema_mismatch_as_a_failed_check() -> None:
    """init_db now REFUSES foreign/unstamped files (QA store #8); doctor
    must report that as a failed check, not crash with a traceback."""
    with patch(
        "raven_bus.db.init_db", side_effect=SchemaMismatchError("foreign file here")
    ):
        result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 10
    assert "foreign file here" in result.stdout
    assert "one or more checks failed" in result.stdout


def test_doctor_db_unreachable_exits_generic_error() -> None:
    with patch("raven_bus.db.init_db", side_effect=OSError("disk full")):
        result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 10
    assert "one or more checks failed" in result.stdout


# --------------------------------------------------------------------
# teardown
# --------------------------------------------------------------------


def test_teardown_with_yes_skips_prompt() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.db.teardown_run", return_value=42) as teardown_run,
    ):
        result = runner.invoke(app, ["teardown", "--run", "r1", "--yes"])
    assert result.exit_code == 0
    assert "removed 42 rows" in result.stdout
    teardown_run.assert_called_once_with(conn, "r1")


def test_teardown_confirm_yes() -> None:
    conn = MagicMock()
    with (
        patch("raven_bus.cli.teardown._stdin_is_interactive", return_value=True),
        patch("raven_bus.db.init_db"),
        patch("raven_bus.db.connection", return_value=_mock_connection(conn)),
        patch("raven_bus.db.teardown_run", return_value=3),
    ):
        result = runner.invoke(app, ["teardown", "--run", "r1"], input="y\n")
    assert result.exit_code == 0
    assert "removed 3 rows" in result.stdout


def test_teardown_confirm_no_aborts() -> None:
    with (
        patch("raven_bus.cli.teardown._stdin_is_interactive", return_value=True),
        patch("raven_bus.db.teardown_run") as teardown_run,
    ):
        result = runner.invoke(app, ["teardown", "--run", "r1"], input="n\n")
    assert result.exit_code == 0
    assert "aborted" in result.stdout
    teardown_run.assert_not_called()


def test_teardown_bad_run_exits_usage() -> None:
    result = runner.invoke(app, ["teardown", "--run", "Bad Run!", "--yes"])
    assert result.exit_code == 2


# --------------------------------------------------------------------
# version
# --------------------------------------------------------------------


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "raven" in result.stdout


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "raven" in result.stdout


# --------------------------------------------------------------------
# cli_main() — the console-script entry point's last-resort net
# --------------------------------------------------------------------


def test_cli_main_runs_app() -> None:
    with patch("raven_bus.cli.main.app") as app_mock:
        cli_main()
    app_mock.assert_called_once_with()


def test_cli_main_keyboard_interrupt_exits_130() -> None:
    with (
        patch("raven_bus.cli.main.app", side_effect=KeyboardInterrupt),
        pytest.raises(SystemExit) as excinfo,
    ):
        cli_main()
    assert excinfo.value.code == 130


def test_cli_main_ravenbuserror_exits_error(capsys) -> None:
    with (
        patch("raven_bus.cli.main.app", side_effect=RavenBusError("boom")),
        pytest.raises(SystemExit) as excinfo,
    ):
        cli_main()
    assert excinfo.value.code == EXIT_ERROR
    assert "error: boom" in capsys.readouterr().err


# --------------------------------------------------------------------
# QA store lane: typed store refusals reach the CLI as one-line errors
# --------------------------------------------------------------------


def test_teardown_blocked_by_outside_reply_is_a_one_line_error(tmp_path) -> None:
    """QA store #6/#14 end to end: an outside reply used to crash
    `raven teardown` with a FOREIGN KEY traceback."""
    from raven_bus import db as bus_db
    from raven_bus import log

    target = tmp_path / "bus.db"
    bus_db._reset_init_cache()
    bus_db.init_db(target)
    with bus_db.connection(target) as conn:
        inside = log.append(conn, channel="run/gone/c", sender="a@gone", type="t", body={})
        log.append(
            conn, channel="run/kept/c", sender="b@kept", type="t", body={}, reply_to=inside.id
        )

    result = runner.invoke(app, ["teardown", "--run", "gone", "--yes", "--db", str(target)])

    assert result.exit_code == 10
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "error: cannot tear down run 'gone'" in result.output
    assert "Traceback" not in result.output
