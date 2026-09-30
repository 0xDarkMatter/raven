"""`raven serve` port bounds and bind failures are one-line errors.

QA cli #5: ``--port 70000`` was an OverflowError traceback from bind();
a port already in use exited 1 through uvicorn's own log. uvicorn,
``create_app`` and ``init_db`` are stubbed so nothing is served; the
only real sockets are ephemeral 127.0.0.1 ones, closed in-test.
"""

from __future__ import annotations

import socket
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from raven_bus.cli import serve as serve_cmd
from raven_bus.cli.main import app

runner = CliRunner()


@pytest.fixture()
def fake_uvicorn(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """uvicorn.run + create_app + init_db stubbed: nothing real is served."""
    from raven_bus.http import app as http_app

    run = Mock()
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=run))
    monkeypatch.setattr(http_app, "create_app", Mock(return_value=object()))
    monkeypatch.setattr(serve_cmd.db, "init_db", Mock())
    return run


@pytest.mark.parametrize("port", ["0", "70000", "-1"])
def test_serve_port_out_of_range_is_a_usage_error(fake_uvicorn: Mock, port: str) -> None:
    result = runner.invoke(app, ["serve", "--port", port])

    assert result.exit_code == 2
    assert "Invalid value for '--port'" in result.output
    fake_uvicorn.assert_not_called()


def test_serve_port_in_use_is_a_one_line_error(fake_uvicorn: Mock) -> None:
    with socket.socket() as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        port = holder.getsockname()[1]

        result = runner.invoke(app, ["serve", "--port", str(port)])

    assert result.exit_code == 10
    assert result.output.startswith(f"error: cannot bind 127.0.0.1:{port}: ")
    assert result.output.count("\n") == 1
    fake_uvicorn.assert_not_called()


def test_probe_bind_accepts_a_free_port() -> None:
    serve_cmd._probe_bind("127.0.0.1", 0)  # ephemeral; bound and closed


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (
            SystemExit(1),
            "error: ravend failed to start on 127.0.0.1:7713 "
            "(uvicorn exit status 1; see its log above)\n",
        ),
        (OSError("address vanished"), "error: ravend failed on 127.0.0.1:7713: address vanished\n"),
    ],
    ids=["uvicorn-sys-exit", "oserror"],
)
def test_serve_uvicorn_startup_failure_is_a_one_line_error(
    fake_uvicorn: Mock, monkeypatch: pytest.MonkeyPatch, raised: BaseException, expected: str
) -> None:
    """The probe can race a peer; uvicorn then logs + sys.exit(1)s."""
    monkeypatch.setattr(serve_cmd, "_probe_bind", lambda host, port: None)
    fake_uvicorn.side_effect = raised

    result = runner.invoke(app, ["serve"])

    assert result.exit_code == 10
    assert result.output == expected
