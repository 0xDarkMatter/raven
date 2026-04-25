"""HTTP bridge — read endpoints."""

from __future__ import annotations

from pathlib import Path

import pytest
from starlette.testclient import TestClient

from claude_bus import BusClient
from claude_bus.http import create_app


@pytest.fixture
def client(db_path: Path) -> TestClient:
    return TestClient(create_app(db_path))


def test_health(client: TestClient, db_path: Path) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "ok"
    assert payload["db"] == str(db_path)
    assert "version" in payload


def test_inbox_returns_messages(client: TestClient, db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={"n": 1})

    resp = client.get("/inbox", params={"role": "b:s"})
    assert resp.status_code == 200
    payload = resp.json()
    assert len(payload["messages"]) == 1
    assert payload["messages"][0]["id"] == sent.id
    assert payload["messages"][0]["status"] == "unread"


def test_inbox_missing_role_returns_400(client: TestClient) -> None:
    resp = client.get("/inbox")
    assert resp.status_code == 400


def test_inbox_malformed_role_returns_400(client: TestClient) -> None:
    resp = client.get("/inbox", params={"role": "no-colon"})
    assert resp.status_code == 400


def test_inbox_empty_role_part_returns_400(client: TestClient) -> None:
    """`role=architect:` (empty session) should be a 400, not a silent empty list."""
    resp = client.get("/inbox", params={"role": "architect:"})
    assert resp.status_code == 400
    resp = client.get("/inbox", params={"role": ":swarm-1"})
    assert resp.status_code == 400


def test_inbox_non_integer_max_returns_400(client: TestClient) -> None:
    """`max=abc` should be a 400 (ValueError on int())."""
    resp = client.get("/inbox", params={"role": "a:s", "max": "abc"})
    assert resp.status_code == 400
    assert resp.json()["detail"].endswith("must be an integer")


def test_inbox_zero_max_returns_400(client: TestClient) -> None:
    resp = client.get("/inbox", params={"role": "a:s", "max": "0"})
    assert resp.status_code == 400
    resp = client.get("/inbox", params={"role": "a:s", "max": "-3"})
    assert resp.status_code == 400


def test_inbox_max_param(client: TestClient, db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    for i in range(5):
        a.send(to=b.address, type="ping", body={"i": i})
    resp = client.get("/inbox", params={"role": "b:s", "max": "2"})
    assert resp.status_code == 200
    assert len(resp.json()["messages"]) == 2


def test_message_by_id(client: TestClient, db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={"n": 1})

    resp = client.get(f"/message/{sent.id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == sent.id


def test_message_endpoint_does_not_pollute_aliases(
    client: TestClient, db_path: Path
) -> None:
    """GET /message/{id} must not create a '__http__' / 'reader' alias row."""
    import sqlite3

    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={})

    aliases_before = sqlite3.connect(db_path).execute(
        "SELECT alias, role FROM aliases ORDER BY alias"
    ).fetchall()
    client.get(f"/message/{sent.id}")
    client.get(f"/message/{sent.id}")
    aliases_after = sqlite3.connect(db_path).execute(
        "SELECT alias, role FROM aliases ORDER BY alias"
    ).fetchall()
    assert aliases_before == aliases_after


def test_message_not_found_returns_404(client: TestClient) -> None:
    resp = client.get("/message/9999")
    assert resp.status_code == 404
    assert resp.json()["error"] == "message_not_found"


def test_message_non_int_returns_400(client: TestClient) -> None:
    resp = client.get("/message/not-a-number")
    assert resp.status_code == 400
