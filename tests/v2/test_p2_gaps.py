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


def test_frontier_not_advanced_on_full_batch(db: Path) -> None:
    """A full candidate batch proves nothing about the ids beyond it —
    _maybe_advance_frontier must early-return without touching SQL."""
    fake_rows = [object()] * claims_mod._CANDIDATE_BATCH
    poisoned = object()  # would explode if any SQL were attempted
    claims_mod._maybe_advance_frontier(
        poisoned, 1, ("k", 1), 0, fake_rows  # type: ignore[arg-type]
    )


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
