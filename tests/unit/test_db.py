"""Schema apply + WAL setup."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from claude_bus.db import connection, init_db


def test_init_db_is_idempotent(tmp_path: Path) -> None:
    p = tmp_path / "claude-bus.db"
    init_db(p)
    init_db(p)
    init_db(p)
    assert p.exists()


def test_init_db_caches_per_process(tmp_path: Path, monkeypatch) -> None:
    """Second call should not re-execute the migration script."""
    from claude_bus import db as db_mod

    p = tmp_path / "claude-bus.db"
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
    p = tmp_path / "claude-bus.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        row = conn.execute(
            "SELECT value FROM bus_meta WHERE key='schema_version'"
        ).fetchone()
        assert row is not None
        assert row[0] == "1"


def test_connection_uses_wal(tmp_path: Path) -> None:
    p = tmp_path / "claude-bus.db"
    init_db(p)
    with connection(p) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"


def test_connection_rolls_back_on_exception(tmp_path: Path) -> None:
    p = tmp_path / "claude-bus.db"
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
