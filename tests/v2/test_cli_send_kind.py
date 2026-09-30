"""`raven send` without --kind must reach an existing queue/stream channel.

QA cli #1: ``--kind`` defaulted to "broadcast" and send always called
``ensure_channel(kind)``, so a plain ``raven send`` to an existing queue
or stream channel failed with WrongChannelKindError (exit 10). Now an
omitted --kind is get-or-create with no kind opinion
(``log.append(ensure=True)``); a given --kind is enforced. Runs against
a real DB — the bug was in how the CLI composed real store calls.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from raven_bus import channels
from raven_bus import db as bus_db
from raven_bus.cli.main import app

runner = CliRunner()


def _send(db: Path, channel: str, *extra: str):
    return runner.invoke(
        app,
        [
            "send", "--db", str(db), "--channel", channel,
            "--from", "a@r1", "-t", "t", "--body", "{}", *extra,
        ],
    )


@pytest.mark.parametrize("kind", ["queue", "stream"])
def test_send_without_kind_appends_to_an_existing_non_broadcast_channel(
    db: Path, kind: str
) -> None:
    assert _send(db, "run/r1/work", "--kind", kind).exit_code == 0

    result = _send(db, "run/r1/work")  # no --kind: used to default to broadcast

    assert result.exit_code == 0, result.output
    assert "sent #2" in result.output
    with bus_db.connection(db) as conn:
        assert channels.get_channel(conn, "run/r1/work").kind == kind


def test_send_without_kind_creates_an_absent_channel_as_broadcast(db: Path) -> None:
    assert _send(db, "run/r1/new").exit_code == 0
    with bus_db.connection(db) as conn:
        assert channels.get_channel(conn, "run/r1/new").kind == "broadcast"


def test_send_with_kind_still_enforces_the_existing_kind(db: Path) -> None:
    assert _send(db, "run/r1/work", "--kind", "queue").exit_code == 0

    result = _send(db, "run/r1/work", "--kind", "broadcast")

    assert result.exit_code == 10
    assert "error: channel 'run/r1/work' is kind 'queue'" in result.output


def test_send_with_kind_creates_an_absent_channel_as_that_kind(db: Path) -> None:
    assert _send(db, "run/r1/jobs", "--kind", "queue").exit_code == 0
    with bus_db.connection(db) as conn:
        assert channels.get_channel(conn, "run/r1/jobs").kind == "queue"


def test_send_bogus_kind_is_rejected_by_the_choice(db: Path) -> None:
    result = _send(db, "run/r1/x", "--kind", "bogus")

    assert result.exit_code == 2
    assert "Invalid value for '--kind'" in result.output
    with bus_db.connection(db) as conn:
        assert channels.list_channels(conn) == []
