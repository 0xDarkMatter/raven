"""Cross-lane coverage closure for the raven2-p2 merge (orchestrator).

Each lane hit its own file's paths; the merged tree left a handful of
lines only reachable through cross-cutting setups — exercised here so
the 100% gate stays honest rather than pragma'd away.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from raven_bus import claims as claims_mod
from raven_bus import db as bus_db
from raven_bus import log
from raven_bus.exceptions import UnknownChannelError
from raven_bus.http import app as http_app
from raven_bus.http import read as read_mod


@pytest.fixture()
def client(db: Path):
    with TestClient(http_app.create_app(db)) as test_client:
        yield test_client


def test_map_exception_reraises_foreign_exceptions() -> None:
    """Non-raven, non-ValueError exceptions are NOT swallowed into an
    envelope — they re-raise so genuine bugs surface as 500s."""
    with pytest.raises(KeyError):
        http_app.map_exception(KeyError("not ours"))


def test_health_maps_init_failure(client: TestClient, monkeypatch) -> None:
    def _boom(_path):
        raise UnknownChannelError("probe failed")

    monkeypatch.setattr(read_mod.db, "init_db", _boom)
    response = client.get("/health")
    assert response.status_code == 404
    assert response.json()["error"] == "not_found"


def test_list_channels_maps_store_failure(client: TestClient, monkeypatch) -> None:
    def _boom(_conn, prefix=None):
        raise UnknownChannelError("listing failed")

    monkeypatch.setattr(read_mod.channels, "list_channels", _boom)
    response = client.get("/channels")
    assert response.status_code == 404


def test_messages_rejects_garbage_bool(client: TestClient, db: Path) -> None:
    with bus_db.connection(db) as conn:
        log.append(
            conn, channel="run/g/c", sender="w@g", type="t", body={}
        )
    response = client.get("/channels/run%2Fg%2Fc/messages?include_expired=banana")
    assert response.status_code == 400
    assert response.json()["error"] == "bad_request"


def test_messages_accepts_explicit_false_bool(client: TestClient, db: Path) -> None:
    with bus_db.connection(db) as conn:
        log.append(conn, channel="run/g/f", sender="w@g", type="t", body={})
    response = client.get("/channels/run%2Fg%2Ff/messages?include_expired=false")
    assert response.status_code == 200
    assert len(response.json()["messages"]) == 1


def test_frontier_not_advanced_on_full_batch(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A full candidate batch proves nothing about the ids beyond it —
    _maybe_advance_frontier must leave the watermark alone, however high
    the (pre-scan) ceiling."""
    monkeypatch.setattr(claims_mod, "_FRONTIER", {})
    fake_rows = [object()] * claims_mod._CANDIDATE_BATCH
    claims_mod._maybe_advance_frontier(("k", 1), 0, 99, fake_rows)  # type: ignore[arg-type]
    assert claims_mod._FRONTIER == {}


def test_all_lost_round_advances_frontier_then_returns_none(db: Path) -> None:
    """Every fresh candidate loses its race -> the frontier advance on
    the all-lost path runs, and the next round correctly finds nothing."""
    with bus_db.connection(db) as conn:
        for n in (1, 2):
            log.append(
                conn,
                channel="run/race/q",
                sender="p@race",
                type="task",
                body={"n": n},
                ensure=True,
            )
        conn.execute("UPDATE channels SET kind='queue' WHERE name='run/race/q'")

    class RacingConnection:
        """Delegates to a real connection, but sabotages claim INSERTs
        by slipping a rival claim in first ON THE SAME CONNECTION —
        the claimant's own write txn would deadlock a second writer,
        and the interleaving under test is the conflict, not the lock."""

        def __init__(self, path: Path) -> None:
            self._conn = sqlite3.connect(str(path), timeout=5.0)
            self._conn.row_factory = sqlite3.Row

        def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
            if "INSERT INTO claims" in sql:
                self._conn.execute(
                    "INSERT OR IGNORE INTO claims"
                    "(message_id, consumer, state, deliveries, lease_until) "
                    "VALUES (?, 'rival@race', 'leased', 1, '2999-01-01T00:00:00.000Z')",
                    (params[0],),
                )
            return self._conn.execute(sql, params)

        def commit(self) -> None:
            self._conn.commit()

        def close(self) -> None:
            self._conn.close()

    racing = RacingConnection(db)
    try:
        claims_mod._FRONTIER.clear()
        result = claims_mod.claim_next(racing, "loser@race", "run/race/q")  # type: ignore[arg-type]
        assert result is None
        # The all-lost advance ran: the frontier now sits at the channel max.
        assert any(v >= 2 for v in claims_mod._FRONTIER.values())
    finally:
        racing.close()
        claims_mod._FRONTIER.clear()


# --------------------------------------------------------------------------- #
# Bridge hardening (opus refute-http round) — each test encodes a finding.
# --------------------------------------------------------------------------- #


def test_router_errors_wear_the_envelope(client: TestClient) -> None:
    unknown = client.get("/nope")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "not_found"

    wrong_method = client.get("/send")
    assert wrong_method.status_code == 405
    assert wrong_method.json()["error"] == "method_not_allowed"


def test_claim_rejects_unknown_keys(client: TestClient, db: Path) -> None:
    """The leases_s typo silently applied a default lease before —
    every POST body is now strict, not just /send."""
    resp = client.post(
        "/claim",
        json={"channel": "run/g/q", "consumer": "r@g", "leases_s": 3600},
    )
    assert resp.status_code == 400
    assert "unknown field" in resp.json()["detail"]


@pytest.mark.parametrize("lease", ["x", None, True, 10**30, 0, -5])
def test_claim_rejects_bad_lease(client: TestClient, lease) -> None:
    resp = client.post(
        "/claim", json={"channel": "run/g/q", "consumer": "r@g", "lease_s": lease}
    )
    assert resp.status_code == 400


def test_ack_rejects_null_up_to_id(client: TestClient) -> None:
    resp = client.post(
        "/ack", json={"channel": "run/g/a", "consumer": "r@g", "up_to_id": None}
    )
    assert resp.status_code == 400


def test_send_rejects_string_tags(client: TestClient) -> None:
    """A bare string used to be iterated character-wise into tags."""
    resp = client.post(
        "/send",
        json={
            "channel": "run/g/t", "sender": "r@g", "type": "t",
            "body": {}, "tags": "abc",
        },
    )
    assert resp.status_code == 400
    assert "list of strings" in resp.json()["detail"]


def test_messages_rejects_negative_limit(client: TestClient, db: Path) -> None:
    """SQLite treats LIMIT -1 as unlimited — a GET could dump a channel."""
    with bus_db.connection(db) as conn:
        log.append(conn, channel="run/g/lim", sender="w@g", type="t", body={})
    resp = client.get("/channels/run%2Fg%2Flim/messages?limit=-1")
    assert resp.status_code == 400


def test_sse_burst_larger_than_one_batch_fully_drains(db: Path) -> None:
    """250 messages committed in ONE transaction stranded everything
    past 100 (delivery was gated on data_version changing again)."""
    import threading
    import time as time_mod

    import httpx
    import uvicorn


    with bus_db.connection(db) as conn:
        for n in range(250):
            log.append(
                conn, channel="run/g/burst", sender="w@g", type="t",
                body={"n": n},
            )
    # ONE commit for all 250 (connection context commits on exit).

    app = http_app.create_app(db)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time_mod.time() + 10.0
    while not server.started:
        assert time_mod.time() < deadline, "uvicorn did not start"
        time_mod.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]

    seen = 0
    try:
        with (
            httpx.Client(timeout=httpx.Timeout(10.0)) as http,
            http.stream(
                "GET", f"http://127.0.0.1:{port}/tail?channel=run/g/burst"
            ) as response,
        ):
            for line in response.iter_lines():
                if line.startswith("event: message"):
                    seen += 1
                    if seen == 250:
                        break
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)
    assert seen == 250


def test_send_rejects_non_object_body_field(client: TestClient) -> None:
    resp = client.post(
        "/send",
        json={"channel": "run/g/b", "sender": "r@g", "type": "t", "body": [1, 2]},
    )
    assert resp.status_code == 400
    assert "must be a JSON object" in resp.json()["detail"]


def test_claim_action_path_id_beyond_int64_is_400(client: TestClient) -> None:
    resp = client.post(f"/claims/{2**63}/done", json={"consumer": "r@g"})
    assert resp.status_code == 400


def test_send_rejects_non_string_scalars(client: TestClient) -> None:
    resp = client.post(
        "/send",
        json={"channel": "run/g/s", "sender": "r@g", "type": False, "body": {}},
    )
    assert resp.status_code == 400
    assert "non-empty string" in resp.json()["detail"]

    resp = client.post(
        "/send",
        json={
            "channel": "run/g/s", "sender": "r@g", "type": "t",
            "body": {}, "kind": True,
        },
    )
    assert resp.status_code == 400


def test_send_rejects_nan_in_body(client: TestClient, db: Path) -> None:
    """NaN passed python-json ingest, committed, then blew up in the
    response encoder — the row must never commit now."""
    resp = client.post(
        "/send",
        content='{"channel":"run/g/nan","sender":"r@g","type":"t","body":{"x":NaN}}',
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400
    with bus_db.connection(db) as conn:
        count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
    assert count == 0


def test_405_envelope_keeps_allow_header(client: TestClient) -> None:
    resp = client.get("/send")
    assert resp.status_code == 405
    assert "POST" in resp.headers.get("allow", "")


def test_sse_drain_cap_forces_reread_not_livelock(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hitting _MAX_BATCHES_PER_POLL must yield what was read and force
    an immediate re-read — unbounded draining livelocked against a hot
    producer (re-verify finding). Tiny batch/cap makes 3 messages span
    several capped polls."""
    import threading
    import time as time_mod

    import httpx
    import uvicorn

    from raven_bus.http import sse

    monkeypatch.setattr(sse, "POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(sse, "PING_INTERVAL_S", 5.0)
    monkeypatch.setattr(sse, "_READ_BATCH", 1)
    monkeypatch.setattr(sse, "_MAX_BATCHES_PER_POLL", 1)

    with bus_db.connection(db) as conn:
        for n in range(3):
            log.append(conn, channel="run/g/cap", sender="w@g", type="t", body={"n": n})

    app = http_app.create_app(db)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time_mod.time() + 10.0
    while not server.started:
        assert time_mod.time() < deadline, "uvicorn did not start"
        time_mod.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]

    seen = 0
    try:
        with (
            httpx.Client(timeout=httpx.Timeout(10.0)) as http,
            http.stream(
                "GET", f"http://127.0.0.1:{port}/tail?channel=run/g/cap"
            ) as response,
        ):
            for line in response.iter_lines():
                if line.startswith("event: message"):
                    seen += 1
                    if seen == 3:
                        break
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)
    assert seen == 3
