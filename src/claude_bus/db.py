"""SQLite lifecycle helpers for claude-bus.

The DB is opened in WAL mode with a 5-second busy timeout so concurrent
producers and consumers can safely share a single SQLite file.

Public surface:

- :func:`init_db` — idempotent schema apply
- :func:`connection` — context manager yielding a ``sqlite3.Connection``
- :func:`teardown_session` — optional row-scoped cleanup per session_id
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import structlog

from claude_bus.paths import resolve_db_path

log = structlog.get_logger(__name__)

SCHEMA_VERSION = "1"
"""Schema version recorded in ``bus_meta`` after init."""

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_INITIAL_MIGRATION = _MIGRATIONS_DIR / "0001_initial.sql"

DEFAULT_BUSY_TIMEOUT_S: float = 5.0

# DB paths whose schema this process has already applied. init_db() is
# idempotent at the SQL layer; this cache short-circuits the redundant
# round-trips that long-running subscribers + bulk CLI usage would
# otherwise cause (and reduces write-lock contention with peers).
_init_cache: set[Path] = set()


def _load_schema_sql() -> str:
    """Read the initial migration once per call."""
    return _INITIAL_MIGRATION.read_text(encoding="utf-8")


def init_db(db_path: str | Path | None = None, *, force: bool = False) -> Path:
    """Create the claude-bus DB if missing and apply the schema.

    Idempotent and process-cached: re-calls within the same process
    return immediately after the first apply. Pass ``force=True`` to
    re-run the migration regardless (useful for tests).

    Parent directories are created if missing. Returns the resolved
    absolute DB path.
    """
    path = resolve_db_path(db_path)
    if not force and path in _init_cache:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = _load_schema_sql()

    with sqlite3.connect(path, timeout=DEFAULT_BUSY_TIMEOUT_S) as conn:
        conn.executescript(schema_sql)
        conn.execute(
            "INSERT INTO bus_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("schema_version", SCHEMA_VERSION),
        )
        conn.commit()

    _init_cache.add(path)
    log.debug("claude_bus.db.init", path=str(path), version=SCHEMA_VERSION)
    return path


def _reset_init_cache() -> None:
    """Test helper — drop the per-process init cache."""
    _init_cache.clear()


@contextmanager
def connection(db_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    """Yield a short-lived ``sqlite3.Connection`` against ``db_path``.

    - Row factory is ``sqlite3.Row`` so callers can index by column name.
    - ``journal_mode=WAL`` is set every open (cheap; idempotent).
    - Commit on clean exit; rollback + re-raise on exception.
    - Connection is closed unconditionally.
    """
    path = resolve_db_path(db_path)
    conn = sqlite3.connect(
        path,
        timeout=DEFAULT_BUSY_TIMEOUT_S,
        isolation_level="DEFERRED",
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def teardown_session(conn: sqlite3.Connection, session_id: str) -> int:
    """Row-scoped cleanup for one session.

    Deletes all messages and aliases tagged with ``session_id``. Returns
    the total number of rows removed. Safe to call on a session that
    has no rows. Does **not** drop the ``bus_meta`` entry (global).
    """
    cursor = conn.execute(
        "DELETE FROM messages WHERE session_id = ?",
        (session_id,),
    )
    msgs_deleted = cursor.rowcount
    cursor = conn.execute(
        "DELETE FROM aliases WHERE session_id = ?",
        (session_id,),
    )
    aliases_deleted = cursor.rowcount
    conn.commit()
    total = msgs_deleted + aliases_deleted
    log.info(
        "claude_bus.db.teardown_session",
        session_id=session_id,
        messages=msgs_deleted,
        aliases=aliases_deleted,
    )
    return total


__all__ = [
    "DEFAULT_BUSY_TIMEOUT_S",
    "SCHEMA_VERSION",
    "connection",
    "init_db",
    "teardown_session",
]
