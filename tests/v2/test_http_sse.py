"""SSE tail and ``raven serve`` contract tests.

Streaming tests run against a REAL uvicorn on an ephemeral loopback
port (daemon thread, torn down per test): sync TestClient.stream over
an infinite SSE generator hangs on context exit — the portal cannot
reliably deliver http.disconnect, so the generator's poll loop never
sees the client go away. A real socket close does. (Test-runner-managed
server; ephemeral port.)
"""

from __future__ import annotations

import asyncio
import builtins
import json
import sys
import time as time_mod
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from unittest.mock import Mock, call

import httpx
import pytest
from starlette.testclient import TestClient
from typer.testing import CliRunner

from raven_bus import db as bus_db
from raven_bus import log
from raven_bus.cli import serve as serve_cmd
from raven_bus.cli.main import app as cli_app
from raven_bus.http import app as http_app
from raven_bus.http import sse

runner = CliRunner()


@pytest.fixture()
def client(db: Path, monkeypatch: pytest.MonkeyPatch):
    """TestClient for NON-streaming endpoints only (pre-stream errors)."""
    monkeypatch.setattr(sse, "POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(sse, "PING_INTERVAL_S", 0.05)
    with TestClient(http_app.create_app(db)) as test_client:
        yield test_client


@pytest.fixture()
def sse_server(db: Path, monkeypatch: pytest.MonkeyPatch):
    """Real uvicorn on 127.0.0.1:<ephemeral>, per test, daemon thread."""
    import uvicorn

    monkeypatch.setattr(sse, "POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(sse, "PING_INTERVAL_S", 0.05)
    config = uvicorn.Config(
        http_app.create_app(db), host="127.0.0.1", port=0, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time_mod.time() + 10.0
    while not server.started:
        if time_mod.time() >= deadline:  # pragma: no cover -- startup failure
            raise RuntimeError("uvicorn did not start within 10s")
        time_mod.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5.0)


def _stream_events(base_url: str, path: str, count: int) -> list[list[str]]:
    """Read ``count`` SSE message events, then close the socket (which
    is what actually signals disconnect to the generator)."""
    events: list[list[str]] = []
    current: list[str] = []
    with (
        httpx.Client(timeout=httpx.Timeout(5.0)) as http,
        http.stream("GET", base_url + path) as response,
    ):
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        for line in response.iter_lines():
            if not line:
                if current:
                    if current[0] == "event: message":
                        events.append(current)
                    current = []
                    if len(events) == count:
                        return events
                continue
            if not line.startswith(":"):
                current.append(line)
    raise AssertionError(f"stream ended after {len(events)} of {count} events")


def _append(
    db_path: Path,
    channel: str,
    number: int,
    *,
    expires_in_s: int | None = None,
):
    with bus_db.connection(db_path) as conn:
        return log.append(
            conn,
            channel=channel,
            sender="writer@test-run",
            type="number",
            body={"number": number},
            expires_in_s=expires_in_s,
        )


def _payload(event: list[str]) -> dict:
    data_line = next(line for line in event if line.startswith("data: "))
    return json.loads(data_line.removeprefix("data: "))


def test_bad_after_is_400_before_stream(client: TestClient) -> None:
    response = client.get("/tail?after=not-an-int")

    assert response.status_code == 400
    assert response.json() == {
        "error": "bad_request",
        "detail": "after must be an integer",
    }


def test_negative_after_is_400_before_stream(client: TestClient) -> None:
    response = client.get("/tail?after=-1")

    assert response.status_code == 400
    assert response.json()["detail"] == "after must be greater than or equal to 0"


def test_unknown_channel_is_404_before_stream(client: TestClient) -> None:
    response = client.get("/tail?channel=run/test-run/missing")

    assert response.status_code == 404
    assert response.json()["error"] == "not_found"


def test_seeded_backlog_streams_in_id_order_with_sse_framing(
    db: Path, sse_server: str
) -> None:
    first = _append(db, "run/test-run/events", 1)
    second = _append(db, "run/test-run/events", 2)

    events = _stream_events(sse_server, "/tail?channel=run/test-run/events", 2)

    assert events[0][0] == "event: message"
    assert events[0][1] == f"id: {first.id}"
    assert events[0][2].startswith("data: {")
    assert events[1][1] == f"id: {second.id}"
    assert [_payload(event)["body"]["number"] for event in events] == [1, 2]
    assert all("\n" not in line for event in events for line in event)


def test_expired_messages_are_included(db: Path, sse_server: str) -> None:
    expired = _append(db, "run/test-run/expired", 1, expires_in_s=-1)

    event = _stream_events(sse_server, "/tail?channel=run/test-run/expired", 1)[0]

    assert event[1] == f"id: {expired.id}"
    assert _payload(event)["expires_at"] is not None


def test_message_appended_after_stream_opens_arrives(
    db: Path, sse_server: str
) -> None:
    with bus_db.connection(db) as conn:
        from raven_bus import channels

        channels.ensure_channel(conn, "run/test-run/live")

    writer = Thread(target=_append, args=(db, "run/test-run/live", 7))
    writer.start()
    try:
        event = _stream_events(sse_server, "/tail?channel=run/test-run/live", 1)[0]
    finally:
        writer.join(timeout=2)

    assert not writer.is_alive()
    assert _payload(event)["body"] == {"number": 7}


def test_all_channels_are_merged_by_global_message_id(
    db: Path, sse_server: str
) -> None:
    messages = [
        _append(db, "run/test-run/z", 1),
        _append(db, "run/test-run/a", 2),
        _append(db, "run/test-run/z", 3),
    ]

    events = _stream_events(sse_server, "/tail", 3)

    assert [int(event[1].removeprefix("id: ")) for event in events] == [
        message.id for message in messages
    ]
    assert [_payload(event)["body"]["number"] for event in events] == [1, 2, 3]


async def test_idle_stream_pings_then_exits_cleanly_on_cancellation(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class CancelAfterFirstPoll:
        calls = 0

        async def is_disconnected(self) -> bool:
            self.calls += 1
            if self.calls == 1:
                return False
            raise asyncio.CancelledError

    monkeypatch.setattr(sse, "POLL_INTERVAL_S", 0.0)
    monkeypatch.setattr(sse, "PING_INTERVAL_S", 0.0)
    events = sse._events(
        CancelAfterFirstPoll(), db_path=db, channel=None, after=0  # type: ignore[arg-type]
    )

    assert await anext(events) == ": ping\n\n"
    with pytest.raises(StopAsyncIteration):
        await anext(events)


def test_serve_command_is_registered() -> None:
    assert "serve" in {command.name for command in cli_app.registered_commands}


def test_serve_missing_http_extra_is_one_line_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def import_without_uvicorn(name, *args, **kwargs):
        if name == "uvicorn":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_uvicorn)
    result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 10
    assert result.output.count("\n") == 1
    assert "error:" in result.output
    assert "[http] extra" in result.output


def _mock_serve_runtime(monkeypatch: pytest.MonkeyPatch):
    timeline = Mock()
    run = Mock()
    create_app = Mock(return_value=object())
    init_db = Mock()
    timeline.attach_mock(init_db, "init_db")
    timeline.attach_mock(create_app, "create_app")
    timeline.attach_mock(run, "run")
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=run))
    monkeypatch.setattr(http_app, "create_app", create_app)
    monkeypatch.setattr(serve_cmd.db, "init_db", init_db)
    return timeline, init_db, create_app, run


def test_serve_preflights_db_before_uvicorn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeline, init_db, create_app, run = _mock_serve_runtime(monkeypatch)
    db_path = tmp_path / "serve.db"

    result = runner.invoke(
        cli_app,
        ["serve", "--db", str(db_path), "--host", "127.0.0.1", "--port", "7788"],
    )

    assert result.exit_code == 0
    init_db.assert_called_once_with(db_path)
    create_app.assert_called_once_with(db_path)
    run.assert_called_once_with(create_app.return_value, host="127.0.0.1", port=7788)
    assert timeline.mock_calls == [
        call.init_db(db_path),
        call.create_app(db_path),
        call.run(create_app.return_value, host="127.0.0.1", port=7788),
    ]


def test_serve_db_preflight_failure_is_one_line_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, init_db, _, run = _mock_serve_runtime(monkeypatch)
    init_db.side_effect = OSError("database is read-only")

    result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 10
    assert result.output == "error: database is read-only\n"
    run.assert_not_called()


def test_serve_warns_for_non_loopback_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_serve_runtime(monkeypatch)

    result = runner.invoke(cli_app, ["serve", "--host", "example.test"])

    assert result.exit_code == 0
    assert "warning:" in result.output
    assert "no in-process authentication" in result.output


def test_serve_recognises_localhost_as_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_serve_runtime(monkeypatch)

    result = runner.invoke(cli_app, ["serve", "--host", "LOCALHOST"])

    assert result.exit_code == 0
    assert "warning:" not in result.output
