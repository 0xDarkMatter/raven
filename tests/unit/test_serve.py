"""Cover cli/serve.py without actually binding a port.

uvicorn.run blocks indefinitely, so we monkey-patch it to capture
kwargs and return immediately. `die()` writes to stderr; in this
Click/Typer version `result.output` already combines stdout+stderr.
"""

from __future__ import annotations

import sys
from pathlib import Path

from typer.testing import CliRunner

from claude_bus.cli.main import app

runner = CliRunner()


def test_serve_invokes_uvicorn_with_correct_kwargs(
    tmp_path: Path, monkeypatch
) -> None:
    """Mock uvicorn.run; assert serve passes through host/port/app correctly."""
    captured: dict = {}

    def fake_run(asgi_app, **kwargs) -> None:
        captured["app"] = asgi_app
        captured.update(kwargs)

    import uvicorn  # available because the [http] extra is installed in dev

    monkeypatch.setattr(uvicorn, "run", fake_run)
    db = tmp_path / "bus.db"
    result = runner.invoke(
        app,
        ["serve", "--db", str(db), "--port", "12345", "--host", "127.0.0.1"],
    )
    assert result.exit_code == 0, result.output
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 12345
    assert captured.get("log_level") == "info"
    assert captured["app"] is not None


def test_serve_reports_oserror_at_bind_time(
    tmp_path: Path, monkeypatch
) -> None:
    """If uvicorn.run raises OSError, serve exits 7 with a clear message."""
    import uvicorn

    def fake_run(*_args, **_kwargs) -> None:
        raise OSError("address already in use")

    monkeypatch.setattr(uvicorn, "run", fake_run)
    db = tmp_path / "bus.db"
    result = runner.invoke(
        app, ["serve", "--db", str(db), "--port", "12345"]
    )
    assert result.exit_code == 7  # EXIT_HTTP_BIND
    assert "failed to bind" in result.output


def test_serve_reports_missing_http_extra(
    tmp_path: Path, monkeypatch
) -> None:
    """When uvicorn isn't importable, serve dies with a 'pip install' hint."""
    # Setting sys.modules[name] = None makes subsequent `import name`
    # raise ImportError (CPython convention for poisoning an import).
    monkeypatch.setitem(sys.modules, "uvicorn", None)
    db = tmp_path / "bus.db"
    result = runner.invoke(app, ["serve", "--db", str(db)])
    assert result.exit_code != 0
    assert "claude-bus[http]" in result.output


def test_serve_preflight_catches_missing_migration(
    tmp_path: Path, monkeypatch
) -> None:
    """If the migration file is missing, preflight raises FileNotFoundError
    which serve catches and reports as exit 3."""
    def boom(*_args, **_kwargs):
        raise FileNotFoundError(2, "no such file", "/missing/0001_initial.sql")

    # cli/serve.py does `from claude_bus import init_db` *inside* cmd_serve,
    # so we have to patch the binding on the package, not on db.py.
    import claude_bus

    monkeypatch.setattr(claude_bus, "init_db", boom)

    result = runner.invoke(
        app, ["serve", "--db", str(tmp_path / "bus.db"), "--port", "0"]
    )
    assert result.exit_code == 3
    assert "missing migration file" in result.output


def test_serve_preflight_catches_generic_oserror(
    tmp_path: Path, monkeypatch
) -> None:
    """A generic OSError from init_db (anything other than FileNotFound /
    Permission) lands as a clean preflight failure too."""
    def boom(*_args, **_kwargs):
        raise OSError(11, "broken")

    import claude_bus

    monkeypatch.setattr(claude_bus, "init_db", boom)

    result = runner.invoke(
        app, ["serve", "--db", str(tmp_path / "bus.db"), "--port", "0"]
    )
    assert result.exit_code == 3
    assert "DB preflight failed" in result.output
