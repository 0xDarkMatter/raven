"""Tests for raven_bus.db — lifecycle + sweep. LANE: store (raven2-p1)."""

from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from raven_bus import db as bus_db
from raven_bus.exceptions import InvalidAddressError


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _insert_channel(
    conn: sqlite3.Connection,
    name: str,
    kind: str = "queue",
    max_deliveries: int = 3,
) -> int:
    cur = conn.execute(
        "INSERT INTO channels (name, kind, max_deliveries) VALUES (?, ?, ?)",
        (name, kind, max_deliveries),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_message(
    conn: sqlite3.Connection,
    channel_id: int,
    *,
    sender: str = "role@run-a",
    expires_at: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO messages (channel_id, sender, type, body, expires_at) "
        "VALUES (?, ?, 'test', '{}', ?)",
        (channel_id, sender, expires_at),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_claim(
    conn: sqlite3.Connection,
    message_id: int,
    *,
    consumer: str = "role@run-a",
    state: str = "leased",
    deliveries: int = 1,
    lease_until: str,
) -> None:
    conn.execute(
        "INSERT INTO claims (message_id, consumer, state, deliveries, lease_until) "
        "VALUES (?, ?, ?, ?, ?)",
        (message_id, consumer, state, deliveries, lease_until),
    )


# --- init_db -----------------------------------------------------------


def test_init_db_creates_parent_dirs_and_schema(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "nested" / "dir" / "bus.db"
    resolved = bus_db.init_db(target)

    assert resolved == target.resolve()
    assert resolved.exists()

    with bus_db.connection(resolved) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {"channels", "messages", "cursors", "claims", "consumers", "bus_meta"} <= tables


def test_init_db_records_schema_version(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    with bus_db.connection(target) as conn:
        row = conn.execute(
            "SELECT value FROM bus_meta WHERE key = 'schema_version'"
        ).fetchone()
    assert row[0] == bus_db.SCHEMA_VERSION


def test_init_db_is_idempotent(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)
    # calling again must not raise (CREATE TABLE IF NOT EXISTS + upsert)
    resolved = bus_db.init_db(target)
    assert resolved == target.resolve()


def test_init_db_process_cache_skips_reinit(tmp_path: Path, monkeypatch) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    calls: list[str] = []
    original = sqlite3.connect

    def spy_connect(*args, **kwargs):
        calls.append("connect")
        return original(*args, **kwargs)

    monkeypatch.setattr(bus_db.sqlite3, "connect", spy_connect)
    bus_db.init_db(target)  # cached — should not touch sqlite3.connect
    assert calls == []


def test_init_db_force_bypasses_cache(tmp_path: Path, monkeypatch) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    calls: list[str] = []
    original = sqlite3.connect

    def spy_connect(*args, **kwargs):
        calls.append("connect")
        return original(*args, **kwargs)

    monkeypatch.setattr(bus_db.sqlite3, "connect", spy_connect)
    bus_db.init_db(target, force=True)
    assert calls == ["connect"]


def test_reset_init_cache_clears(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)
    assert target.resolve() in bus_db._init_cache
    bus_db._reset_init_cache()
    assert bus_db._init_cache == set()


# --- connection ----------------------------------------------------------


def test_connection_sets_wal_and_foreign_keys(db: Path) -> None:
    with bus_db.connection(db) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_connection_row_factory(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/c")
    with bus_db.connection(db) as conn:
        row = conn.execute("SELECT id, name FROM channels WHERE id = ?", (cid,)).fetchone()
        assert row["name"] == "run/t/c"


def test_connection_commits_on_clean_exit(db: Path) -> None:
    with bus_db.connection(db) as conn:
        _insert_channel(conn, "run/t/committed")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/t/committed'"
        ).fetchone()
    assert row is not None


def test_connection_rolls_back_on_exception(db: Path) -> None:
    class Boom(Exception):
        pass

    with pytest.raises(Boom), bus_db.connection(db) as conn:
        _insert_channel(conn, "run/t/rolledback")
        raise Boom("fail")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/t/rolledback'"
        ).fetchone()
    assert row is None


# --- data_version --------------------------------------------------------


def test_data_version_changes_across_connections(db: Path) -> None:
    # PRAGMA data_version snapshots at the start of a connection's read
    # transaction (sqlite docs: "the value returned only changes after
    # the connection has opened a new transaction") — so the poller
    # must end its transaction (rollback, since it did no writes) before
    # re-checking to observe a commit made by another connection.
    poller = sqlite3.connect(str(db), isolation_level="DEFERRED")
    try:
        before = bus_db.data_version(poller)
        poller.rollback()

        with bus_db.connection(db) as conn:
            _insert_channel(conn, "run/t/dv")

        poller.rollback()
        after = bus_db.data_version(poller)
    finally:
        poller.close()

    assert after != before


def test_data_version_returns_int(db: Path) -> None:
    with bus_db.connection(db) as conn:
        assert isinstance(bus_db.data_version(conn), int)


# --- sweep ----------------------------------------------------------------


def test_sweep_noop_on_empty_db(db: Path) -> None:
    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
    assert result.expired == 0
    assert result.requeued == 0
    assert result.dead_lettered == 0


def test_sweep_requeues_lease_below_max_deliveries(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=2, lease_until=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        remaining = conn.execute(
            "SELECT * FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 1
    assert result.dead_lettered == 0
    assert remaining is None


def test_sweep_dead_letters_at_max_deliveries_boundary(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=3, lease_until=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        row = conn.execute(
            "SELECT state FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 0
    assert result.dead_lettered == 1
    assert row["state"] == "dead"


def test_sweep_ignores_non_expired_leases(db: Path) -> None:
    future = _iso(datetime.now(UTC) + timedelta(hours=1))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=1, lease_until=future)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        row = conn.execute(
            "SELECT * FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 0
    assert result.dead_lettered == 0
    assert row is not None
    assert row["state"] == "leased"


def test_sweep_never_touches_messages_table(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=1)
        mid = _insert_message(conn, cid, expires_at=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()

    assert row is not None  # message never deleted by sweep
    assert result.expired == 1


def test_sweep_expired_excludes_terminal_claims(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid, expires_at=past)
        _insert_claim(conn, mid, state="done", deliveries=1, lease_until=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)

    assert result.expired == 0


def test_sweep_cheap_when_nothing_stale(db: Path) -> None:
    # populate a modest number of non-stale rows and ensure sweep doesn't
    # take anything close to O(seconds) — the partial indexes should make
    # this a near-instant no-op even with a few hundred rows present.
    future = _iso(datetime.now(UTC) + timedelta(hours=1))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        for _ in range(200):
            mid = _insert_message(conn, cid)
            _insert_claim(conn, mid, deliveries=1, lease_until=future)

    with bus_db.connection(db) as conn:
        start = time.perf_counter()
        result = bus_db.sweep(conn)
        elapsed = time.perf_counter() - start

    assert result.requeued == 0
    assert result.dead_lettered == 0
    assert elapsed < 0.5


# --- teardown_run ----------------------------------------------------------


def test_teardown_run_deletes_scoped_rows(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid_exact = _insert_channel(conn, "run/target")
        cid_nested = _insert_channel(conn, "run/target/lane/1")
        mid1 = _insert_message(conn, cid_exact)
        mid2 = _insert_message(conn, cid_nested)
        _insert_claim(
            conn,
            mid2,
            deliveries=1,
            lease_until=_iso(datetime.now(UTC) + timedelta(hours=1)),
        )
        conn.execute(
            "INSERT INTO cursors (consumer, channel_id, last_ack_id) VALUES (?, ?, 0)",
            ("role@target", cid_exact),
        )
        conn.execute(
            "INSERT INTO consumers (id, role, run) VALUES (?, ?, ?)",
            ("role@target", "role", "target"),
        )

    with bus_db.connection(db) as conn:
        deleted = bus_db.teardown_run(conn, "target")

    assert deleted > 0

    with bus_db.connection(db) as conn:
        remaining_channels = conn.execute(
            "SELECT COUNT(*) FROM channels WHERE name LIKE 'run/target%'"
        ).fetchone()[0]
        remaining_messages = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE id IN (?, ?)", (mid1, mid2)
        ).fetchone()[0]
        remaining_consumers = conn.execute(
            "SELECT COUNT(*) FROM consumers WHERE run = 'target'"
        ).fetchone()[0]

    assert remaining_channels == 0
    assert remaining_messages == 0
    assert remaining_consumers == 0


def test_teardown_run_leaves_other_runs_untouched(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid_target = _insert_channel(conn, "run/target")
        cid_other = _insert_channel(conn, "run/other")
        _insert_message(conn, cid_target)
        mid_other = _insert_message(conn, cid_other)
        conn.execute(
            "INSERT INTO consumers (id, role, run) VALUES (?, ?, ?)",
            ("role@other", "role", "other"),
        )

    with bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    with bus_db.connection(db) as conn:
        other_channel = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/other'"
        ).fetchone()
        other_message = conn.execute(
            "SELECT 1 FROM messages WHERE id = ?", (mid_other,)
        ).fetchone()
        other_consumer = conn.execute(
            "SELECT 1 FROM consumers WHERE run = 'other'"
        ).fetchone()

    assert other_channel is not None
    assert other_message is not None
    assert other_consumer is not None


def test_teardown_run_does_not_match_prefix_lookalikes(db: Path) -> None:
    # "run/targetx" must not be swept up by teardown_run("target")
    with bus_db.connection(db) as conn:
        _insert_channel(conn, "run/targetx")

    with bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/targetx'"
        ).fetchone()
    assert row is not None


def test_teardown_run_rejects_invalid_atom(db: Path) -> None:
    with bus_db.connection(db) as conn, pytest.raises(InvalidAddressError):
        bus_db.teardown_run(conn, "Bad Run!")


def test_teardown_run_returns_zero_for_unknown_run(db: Path) -> None:
    with bus_db.connection(db) as conn:
        deleted = bus_db.teardown_run(conn, "nonexistent")
    assert deleted == 0
