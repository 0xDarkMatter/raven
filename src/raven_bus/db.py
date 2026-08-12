"""SQLite lifecycle + the opportunistic sweep.  LANE: store (raven2-p1).

Contract (frozen):

- WAL mode, 5s busy timeout, ``sqlite3.Row`` factory — v1's proven setup.
- ``init_db`` is idempotent and process-cached (``force=True`` bypasses,
  tests use :func:`_reset_init_cache`).
- ``sweep`` is THE enforcement point for expiry and lease reaping
  (ADR-001): v1 shipped ``sweep_expired`` and never called it — v2 read
  paths call :func:`sweep` opportunistically (cursors/claims lanes call
  it; this module owns its correctness).
- ``data_version`` powers cheap change-detection polling
  (design §9 Q2: ~250ms fast-poll, full query only on change).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from raven_bus.models import SweepResult
from raven_bus.paths import resolve_db_path

SCHEMA_VERSION = "2"
DEFAULT_BUSY_TIMEOUT_S: float = 5.0

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_V2_MIGRATION = _MIGRATIONS_DIR / "0002_v2_schema.sql"

_init_cache: set[Path] = set()


def _reset_init_cache() -> None:
    """Test helper — drop the per-process init cache."""
    _init_cache.clear()


def init_db(db_path: str | Path | None = None, *, force: bool = False) -> Path:
    """Create the DB if missing, apply the v2 schema, record
    ``schema_version`` in ``bus_meta``. Idempotent, process-cached.
    Creates parent directories. Returns the resolved absolute path."""
    resolved = resolve_db_path(db_path)
    if not force and resolved in _init_cache:
        return resolved

    resolved.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = _V2_MIGRATION.read_text(encoding="utf-8")

    conn = sqlite3.connect(str(resolved), timeout=DEFAULT_BUSY_TIMEOUT_S)
    try:
        conn.executescript(schema_sql)
        conn.execute(
            "INSERT INTO bus_meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SCHEMA_VERSION,),
        )
        conn.commit()
    finally:
        conn.close()

    _init_cache.add(resolved)
    return resolved


@contextmanager
def connection(db_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    """Short-lived connection: WAL + foreign_keys pragmas, Row factory,
    commit on clean exit, rollback + re-raise on exception, always
    closed. DEFERRED isolation."""
    resolved = resolve_db_path(db_path)
    conn = sqlite3.connect(
        str(resolved),
        timeout=DEFAULT_BUSY_TIMEOUT_S,
        isolation_level="DEFERRED",
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def data_version(conn: sqlite3.Connection) -> int:
    """Return ``PRAGMA data_version`` — changes whenever another
    connection commits. The cheap poll primitive."""
    row = conn.execute("PRAGMA data_version").fetchone()
    return int(row[0])


def sweep(conn: sqlite3.Connection) -> SweepResult:
    """Enforce time-based state, in one pass (ADR-001):

    1. **Lease reaping**: DELETE claims where ``state='leased' AND
       lease_until < now AND deliveries < channel.max_deliveries``
       (message becomes claimable again → counts as ``requeued``);
       claims at/over max_deliveries flip to ``state='dead'`` instead
       (→ ``dead_lettered``).
    2. **Expiry**: messages past ``expires_at`` are *not* deleted
       (append-only log) — read paths must filter them. ``expired``
       counts messages newly past expiry with no terminal claim, for
       observability only.

    Cheap when nothing is stale; safe to call on every read."""
    now = _now_iso()

    stale = conn.execute(
        """
        SELECT c.message_id AS message_id, c.deliveries AS deliveries,
               ch.max_deliveries AS max_deliveries
        FROM claims AS c
        JOIN messages AS m ON m.id = c.message_id
        JOIN channels AS ch ON ch.id = m.channel_id
        WHERE c.state = 'leased' AND c.lease_until < ?
        """,
        (now,),
    ).fetchall()

    requeued = 0
    dead_lettered = 0
    for row in stale:
        if row["deliveries"] < row["max_deliveries"]:
            conn.execute(
                "DELETE FROM claims WHERE message_id = ?", (row["message_id"],)
            )
            requeued += 1
        else:
            conn.execute(
                "UPDATE claims SET state = 'dead', updated_at = ? "
                "WHERE message_id = ?",
                (now, row["message_id"]),
            )
            dead_lettered += 1

    expired_row = conn.execute(
        """
        SELECT COUNT(*) FROM messages AS m
        WHERE m.expires_at IS NOT NULL AND m.expires_at < ?
        AND NOT EXISTS (
            SELECT 1 FROM claims AS c
            WHERE c.message_id = m.id AND c.state IN ('done', 'dead')
        )
        """,
        (now,),
    ).fetchone()
    expired = int(expired_row[0])

    return SweepResult(expired=expired, requeued=requeued, dead_lettered=dead_lettered)


def teardown_run(conn: sqlite3.Connection, run: str) -> int:
    """Delete all rows for channels under ``run/<run>/`` (messages,
    cursors, claims, the channels themselves) plus ``consumers`` rows
    with this run. Returns total rows removed. The ONLY sanctioned
    DELETE on messages besides retention (ADR-001/002)."""
    from raven_bus.models import validate_atom

    validate_atom(run, what="run")

    channel_ids = [
        row[0]
        for row in conn.execute(
            "SELECT id FROM channels WHERE name = ? OR name LIKE ? ESCAPE '\\'",
            (f"run/{run}", f"run/{_escape_like(run)}/%"),
        ).fetchall()
    ]

    total = 0
    if channel_ids:
        placeholders = ",".join("?" for _ in channel_ids)
        cur = conn.execute(
            f"DELETE FROM claims WHERE message_id IN "
            f"(SELECT id FROM messages WHERE channel_id IN ({placeholders}))",
            channel_ids,
        )
        total += cur.rowcount
        cur = conn.execute(
            f"DELETE FROM cursors WHERE channel_id IN ({placeholders})",
            channel_ids,
        )
        total += cur.rowcount
        cur = conn.execute(
            f"DELETE FROM messages WHERE channel_id IN ({placeholders})",
            channel_ids,
        )
        total += cur.rowcount
        cur = conn.execute(
            f"DELETE FROM channels WHERE id IN ({placeholders})",
            channel_ids,
        )
        total += cur.rowcount

    cur = conn.execute("DELETE FROM consumers WHERE run = ?", (run,))
    total += cur.rowcount

    return total


def _now_iso() -> str:
    """UTC now formatted to match ``strftime('%Y-%m-%dT%H:%M:%fZ')``."""
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _escape_like(value: str) -> str:
    """Escape ``%``/``_``/``\\`` for a ``LIKE ... ESCAPE '\\'`` clause."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


__all__ = [
    "DEFAULT_BUSY_TIMEOUT_S",
    "SCHEMA_VERSION",
    "_reset_init_cache",
    "connection",
    "data_version",
    "init_db",
    "sweep",
    "teardown_run",
]
