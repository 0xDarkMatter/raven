"""Cover the cli_main() wrapper in cli/main.py.

The Typer app is exercised everywhere via CliRunner.invoke() — but
that goes around the cli_main() function that wraps `app()` in our
exception handlers. These tests call cli_main() directly under
controlled monkey-patches to verify the handler maps each exception
class to the right exit code + message format.
"""

from __future__ import annotations

import sys

import pytest

from claude_bus.cli.main import cli_main
from claude_bus.exceptions import (
    ClaudeBusError,
    SchemaValidationError,
    UnknownMessageError,
)


def _patch_app(monkeypatch, exc: BaseException | None) -> None:
    """Replace the underlying Typer app with a stub that raises ``exc``."""
    from claude_bus.cli import main as main_mod

    def fake_app() -> None:
        if exc is not None:
            raise exc

    monkeypatch.setattr(main_mod, "app", fake_app)


def test_cli_main_clean_exit(monkeypatch) -> None:
    _patch_app(monkeypatch, None)
    cli_main()  # no exception, no sys.exit


def test_cli_main_keyboard_interrupt_exits_130(monkeypatch) -> None:
    _patch_app(monkeypatch, KeyboardInterrupt())
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 130


def test_cli_main_schema_validation_exits_5(monkeypatch, capsys) -> None:
    _patch_app(monkeypatch, SchemaValidationError("body bad"))
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 5
    err = capsys.readouterr().err
    assert "schema validation failed" in err
    assert "body bad" in err


def test_cli_main_unknown_message_exits_4(monkeypatch, capsys) -> None:
    _patch_app(monkeypatch, UnknownMessageError("message id=999 does not exist"))
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 4
    err = capsys.readouterr().err
    assert "999" in err


def test_cli_main_other_claude_bus_error_exits_1(monkeypatch, capsys) -> None:
    _patch_app(monkeypatch, ClaudeBusError("something wrong"))
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "something wrong" in err


def test_cli_main_file_not_found_exits_2(monkeypatch, capsys) -> None:
    _patch_app(monkeypatch, FileNotFoundError(2, "no", "/missing/file"))
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "/missing/file" in err or "no" in err


def test_cli_main_permission_denied_exits_3(monkeypatch, capsys) -> None:
    _patch_app(monkeypatch, PermissionError(13, "denied", "/locked.db"))
    with pytest.raises(SystemExit) as exc:
        cli_main()
    assert exc.value.code == 3
    err = capsys.readouterr().err
    assert "permission denied" in err.lower()


def test_cli_main_unrecognised_exception_propagates(monkeypatch) -> None:
    """Truly unexpected errors should still raise so devs see the trace."""
    _patch_app(monkeypatch, RuntimeError("dev should see this"))
    with pytest.raises(RuntimeError):
        cli_main()
