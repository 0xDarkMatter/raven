"""Coverage for cli/doctor failure paths.

The happy path is exercised by test_cli.py::test_doctor_runs. This
file forces each individual check to fail and asserts the right
report line + non-zero exit code.
"""

from __future__ import annotations

import socket
from pathlib import Path

from typer.testing import CliRunner

from claude_bus.cli.main import app

runner = CliRunner()


def test_doctor_passes_with_writable_db(tmp_path: Path) -> None:
    db = tmp_path / "raven.db"
    result = runner.invoke(app, ["doctor", "--db", str(db)])
    # Port may or may not be free on the test host; tolerate either.
    assert result.exit_code in (0, 1)
    assert "[ok]" in result.stdout
    assert "install" in result.stdout
    assert "db" in result.stdout
    assert "disk" in result.stdout
    assert "port" in result.stdout
    assert "http extra" in result.stdout


def test_doctor_reports_missing_migration(tmp_path: Path, monkeypatch) -> None:
    """Pretend the migration file is missing and confirm doctor catches it."""
    from claude_bus.cli import doctor as doctor_mod

    monkeypatch.setattr(
        doctor_mod, "_INITIAL_MIGRATION", Path("/nope/never/0001_initial.sql")
    )
    result = runner.invoke(app, ["doctor", "--db", str(tmp_path / "bus.db")])
    assert result.exit_code == 1  # at least one fail
    assert "migration file missing" in result.stdout


def test_doctor_reports_unbindable_port(tmp_path: Path) -> None:
    """Bind a port ourselves, then ask doctor to check it."""
    db = tmp_path / "bus.db"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    in_use_port = sock.getsockname()[1]
    try:
        result = runner.invoke(
            app, ["doctor", "--db", str(db), "--port", str(in_use_port)]
        )
    finally:
        sock.close()
    assert result.exit_code == 1
    assert "port" in result.stdout
    assert "not bindable" in result.stdout


def test_doctor_reports_db_oserror(tmp_path: Path, monkeypatch) -> None:
    """If init_db raises OSError, _check_db reports the unreachable DB."""
    from claude_bus.cli import doctor as doctor_mod

    def boom(*_a, **_kw):
        raise OSError("disk gone")

    monkeypatch.setattr(doctor_mod, "init_db", boom)
    result = runner.invoke(app, ["doctor", "--db", str(tmp_path / "bus.db")])
    assert result.exit_code == 1
    assert "unreachable" in result.stdout


def test_doctor_reports_db_missing_schema_version_row(
    tmp_path: Path, monkeypatch
) -> None:
    """If init_db succeeds but bus_meta has no schema_version row, fail."""
    from claude_bus.cli import doctor as doctor_mod

    db = tmp_path / "bus.db"

    def init_then_wipe(*_a, **_kw):
        # Create the schema then delete the version row to simulate it.
        from claude_bus import init_db as real_init

        real_init(db)
        import sqlite3
        with sqlite3.connect(db) as conn:
            conn.execute("DELETE FROM bus_meta WHERE key='schema_version'")
            conn.commit()
        return db

    monkeypatch.setattr(doctor_mod, "init_db", init_then_wipe)
    result = runner.invoke(app, ["doctor", "--db", str(db)])
    assert result.exit_code == 1
    assert "no schema_version row" in result.stdout


def test_doctor_reports_missing_http_extra(tmp_path: Path, monkeypatch) -> None:
    """Stub out starlette/uvicorn imports and confirm doctor flags it."""
    import importlib.util

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, *args, **kwargs):
        if name in ("starlette", "uvicorn"):
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    result = runner.invoke(app, ["doctor", "--db", str(tmp_path / "bus.db")])
    assert result.exit_code == 1
    assert "[fail]" in result.stdout
    assert "http extra" in result.stdout
    assert "[http]" in result.stdout
