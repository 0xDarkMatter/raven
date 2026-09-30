"""`raven doctor` flags a DB it had to create (a typo'd --db / RAVEN_DB).

QA cli #4: doctor on a path that did not exist silently created the
dirs + an empty DB and reported "all checks passed", hiding the typo.
It still creates (a fresh machine must pass) but now reports a
``[warn]`` line and a "(with warnings)" summary; exit stays 0.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from raven_bus import db as bus_db
from raven_bus.cli.main import app

runner = CliRunner()


def test_doctor_warns_when_it_had_to_create_the_db(tmp_path: Path) -> None:
    typo = tmp_path / "no" / "such" / "bsu.db"
    bus_db._reset_init_cache()

    result = runner.invoke(app, ["doctor", "--db", str(typo)])

    assert result.exit_code == 0
    assert f"[warn]  db       did not exist - created it at {typo.resolve()}" in result.output
    assert result.output.rstrip().endswith("all checks passed (with warnings)")
    assert typo.exists()


def test_doctor_on_an_existing_db_has_no_warning(db: Path) -> None:
    result = runner.invoke(app, ["doctor", "--db", str(db)])

    assert result.exit_code == 0
    assert "[warn]" not in result.output
    assert result.output.rstrip().endswith("all checks passed")
