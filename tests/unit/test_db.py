"""Schema apply + WAL setup."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from claude_bus.db import connection, init_db


def test_init_db_is_idempotent(tmp_path: Path) -> None:
    p = tmp_path / "raven.db"
    init_db(p)
    init_db(p)
    init_db(p)
    assert p.exists()


def test_init_db_caches_per_process(tmp_path: Path, monkeypatch) -> None:
    """Second call should not re-execute the migration script."""
    from claude_bus import db as db_mod

    p = tmp_path / "raven.db"
    calls: list[int] = []

    real_load = db_mod._load_schema_sql

    def counting_load() -> str:
        calls.append(1)
        return real_load()

    monkeypatch.setattr(db_mod, "_load_schema_sql", counting_load)
    db_mod._reset_init_cache()
    init_db(p)
    init_db(p)
    init_db(p)
    assert sum(calls) == 1, f"_load_schema_sql ran {sum(calls)} times, expected 1"

    # `force=True` bypasses the cache.
    init_db(p, force=True)
    assert sum(calls) == 2


def test_init_db_records_schema_version(tmp_path: Path) -> None:
    p = tmp_path / "raven.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        row = conn.execute(
            "SELECT value FROM bus_meta WHERE key='schema_version'"
        ).fetchone()
        assert row is not None
        assert row[0] == "1"


def test_connection_uses_wal(tmp_path: Path) -> None:
    p = tmp_path / "raven.db"
    init_db(p)
    with connection(p) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"


def test_teardown_session_deletes_session_rows(tmp_path: Path) -> None:
    """teardown_session removes only the messages + aliases tagged with
    ``session_id``, leaves other sessions and the bus_meta row intact."""
    from claude_bus import _core, aliases as alias_mod
    from claude_bus.db import connection, teardown_session

    db = tmp_path / "raven.db"
    init_db(db)
    with connection(db) as conn:
        a = alias_mod.register(conn, "alice", "doomed")
        b = alias_mod.register(conn, "bob", "doomed")
        # Sender + recipient both register so cross-session alias persists.
        alias_mod.register(conn, "alice", "kept")
        alias_mod.register(conn, "bob", "kept")
        _core.send(conn, sender=a, recipient=b, type="x", body={},
                   validate=False)
        # In session "kept" — should survive.
        kept_a = alias_mod.register(conn, "alice", "kept")
        kept_b = alias_mod.register(conn, "bob", "kept")
        _core.send(conn, sender=kept_a, recipient=kept_b, type="x", body={},
                   validate=False)

        removed = teardown_session(conn, "doomed")
        assert removed >= 3  # 1 message + 2 aliases at minimum

        msg_count = conn.execute(
            "SELECT count(*) FROM messages WHERE session_id='doomed'"
        ).fetchone()[0]
        assert msg_count == 0
        kept_msgs = conn.execute(
            "SELECT count(*) FROM messages WHERE session_id='kept'"
        ).fetchone()[0]
        assert kept_msgs == 1
        meta_row = conn.execute(
            "SELECT value FROM bus_meta WHERE key='schema_version'"
        ).fetchone()
        assert meta_row is not None  # schema_version untouched


def test_teardown_empty_session_returns_zero(tmp_path: Path) -> None:
    """Tearing down a session with no rows is a safe no-op."""
    from claude_bus.db import connection, teardown_session

    db = tmp_path / "raven.db"
    init_db(db)
    with connection(db) as conn:
        assert teardown_session(conn, "never-existed") == 0


def test_connection_rolls_back_on_exception(tmp_path: Path) -> None:
    p = tmp_path / "raven.db"
    init_db(p)
    try:
        with connection(p) as conn:
            conn.execute(
                "INSERT INTO aliases(alias, role, session_id) VALUES (?, ?, ?)",
                ("alpha", "scout", "s1"),
            )
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    with connection(p) as conn:
        row = conn.execute(
            "SELECT count(*) FROM aliases WHERE alias='alpha'"
        ).fetchone()
        assert row[0] == 0
