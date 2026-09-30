"""`raven tail` drains the whole backlog, in global id order, with typed errors.

QA cli #2: ``--no-follow`` ("drain the backlog and exit") stopped after
one 100-message read per channel; all-channel mode printed channel-name
order, not id order; store errors (schema mismatch) escaped as
tracebacks; a bad/unknown ``--channel`` both exited 10; and a negative
``--interval`` crashed ``time.sleep``. Real DB throughout.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from raven_bus import db as bus_db
from raven_bus import log
from raven_bus.cli.main import app

runner = CliRunner()

MAX_ID = 2**63 - 1


def _append_many(db: Path, channel_names: list[str]) -> None:
    """One message per entry, in list order (so ids follow the list)."""
    with bus_db.connection(db) as conn:
        for name in channel_names:
            log.append(conn, channel=name, sender="a@r1", type="t", body={})


def _tail_ids(output: str) -> list[int]:
    return [json.loads(line)["id"] for line in output.splitlines() if line.strip()]


def test_tail_no_follow_drains_past_one_batch_on_one_channel(db: Path) -> None:
    _append_many(db, ["run/r1/big"] * 250)

    result = runner.invoke(
        app, ["tail", "--db", str(db), "--channel", "run/r1/big", "--no-follow", "--json"]
    )

    assert result.exit_code == 0
    assert _tail_ids(result.output) == list(range(1, 251))


def test_tail_no_follow_drains_past_one_batch_across_channels(db: Path) -> None:
    _append_many(db, ["run/r1/b", "run/r1/a"] * 125)

    result = runner.invoke(app, ["tail", "--db", str(db), "--no-follow", "--json"])

    assert result.exit_code == 0
    assert _tail_ids(result.output) == list(range(1, 251))


def test_tail_all_channels_prints_global_id_order_not_name_order(db: Path) -> None:
    _append_many(db, ["run/r1/zeta", "run/r1/alpha", "run/r1/zeta"])

    result = runner.invoke(app, ["tail", "--db", str(db), "--no-follow", "--json"])

    assert result.exit_code == 0
    assert _tail_ids(result.output) == [1, 2, 3]  # was [2, 1, 3]: alpha first


def test_tail_follow_resumes_after_the_last_id_printed(db: Path) -> None:
    _append_many(db, ["run/r1/a", "run/r1/b"])
    sleeps = iter([lambda: _append_many(db, ["run/r1/b", "run/r1/a"])])

    def fake_sleep(_seconds: float) -> None:
        step = next(sleeps, None)
        if step is None:
            raise KeyboardInterrupt
        step()

    with patch("time.sleep", side_effect=fake_sleep):
        result = runner.invoke(app, ["tail", "--db", str(db), "--json", "--from", "1"])

    assert result.exit_code == 0
    assert _tail_ids(result.output) == [2, 3, 4]


def test_tail_bad_channel_grammar_is_a_usage_error(db: Path) -> None:
    result = runner.invoke(app, ["tail", "--db", str(db), "--channel", "Bad!", "--no-follow"])
    assert result.exit_code == 2
    assert result.output.startswith("error: ")


def test_tail_unknown_channel_is_not_found(db: Path) -> None:
    result = runner.invoke(
        app, ["tail", "--db", str(db), "--channel", "run/r1/nope", "--no-follow"]
    )
    assert result.exit_code == 3
    assert result.output == "error: channel 'run/r1/nope' does not exist\n"


def test_tail_on_a_foreign_db_is_a_one_line_error(tmp_path: Path) -> None:
    foreign = tmp_path / "foreign.db"
    with sqlite3.connect(foreign) as conn:
        conn.execute("CREATE TABLE someone_elses (x)")
    bus_db._reset_init_cache()

    result = runner.invoke(app, ["tail", "--db", str(foreign), "--no-follow"])

    assert result.exit_code == 10
    assert isinstance(result.exception, SystemExit)
    assert result.output.startswith("error: ")
    assert result.output.count("\n") == 1


@pytest.mark.parametrize("interval", ["-1", "0", "nan", "inf", "3601"])
def test_tail_interval_must_be_positive_and_bounded(db: Path, interval: str) -> None:
    result = runner.invoke(
        app, ["tail", "--db", str(db), "--no-follow", "--interval", interval]
    )
    assert result.exit_code == 2
    assert "Invalid value for '--interval'" in result.output


@pytest.mark.parametrize("from_id", ["-1", str(MAX_ID + 1)])
def test_tail_from_out_of_range_is_a_usage_error(db: Path, from_id: str) -> None:
    result = runner.invoke(app, ["tail", "--db", str(db), "--no-follow", "--from", from_id])
    assert result.exit_code == 2
    assert "Invalid value for '--from'" in result.output
