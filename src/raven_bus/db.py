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
import time
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

    # Fast path: an already-migrated DB needs no executescript at all.
    # This is a concurrency fix, not an optimisation — re-running the
    # script (incl. its WAL pragma) on a file other processes are
    # actively writing raced into 'database is locked' at process
    # startup (finding verify-004). Only genuine first-creation runs
    # the script, and that residual race gets a bounded retry.
    stored = _stored_schema_version(resolved)
    if not force and stored == SCHEMA_VERSION:
        _init_cache.add(resolved)
        return resolved
    # A DB stamped with a DIFFERENT version must never be silently
    # re-stamped: CREATE IF NOT EXISTS would leave the old tables in
    # place while marking the file current (re-verify wave finding).
    # v2 does not migrate foreign schemas — refuse loudly.
    if stored is not None and stored != SCHEMA_VERSION:
        raise RuntimeError(
            f"{resolved} was created by schema version {stored!r}; this "
            f"raven_bus expects version {SCHEMA_VERSION!r} and does not "
            "migrate old files — point RAVEN_DB/db_path at a fresh path"
        )

    schema_sql = _V2_MIGRATION.read_text(encoding="utf-8")
    last_error: sqlite3.OperationalError | None = None
    for _attempt in range(5):
        try:
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
        except sqlite3.OperationalError as exc:
            # Another process is initialising the same file right now.
            # If it finished the job, the fast path accepts its work.
            last_error = exc
            time.sleep(0.1)
            if not force and _schema_current(resolved):
                _init_cache.add(resolved)
                return resolved
            continue
        _init_cache.add(resolved)
        return resolved

    raise last_error if last_error is not None else RuntimeError(
        "init_db retry loop exited without an error"
    )  # pragma: no cover -- loop always sets last_error before exhausting


def _stored_schema_version(resolved: Path) -> str | None:
    """The schema version recorded in ``resolved``, or None.

    Read-only probe; never creates the file (sqlite3.connect would, so
    check existence first) and treats any error as "not initialised".
    """
    if not resolved.exists():
        return None
    try:
        conn = sqlite3.connect(str(resolved), timeout=DEFAULT_BUSY_TIMEOUT_S)
        try:
            row = conn.execute(
                "SELECT value FROM bus_meta WHERE key = 'schema_version'"
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    return None if row is None else str(row[0])


def _schema_current(resolved: Path) -> bool:
    """True if ``resolved`` already carries the current schema version."""
    return _stored_schema_version(resolved) == SCHEMA_VERSION


@contextmanager
def connection(
    db_path: str | Path | None = None, *, cross_thread: bool = False
) -> Iterator[sqlite3.Connection]:
    """Short-lived connection: WAL + foreign_keys pragmas, Row factory,
    commit on clean exit, rollback + re-raise on exception, always
    closed. DEFERRED isolation.

    ``cross_thread=True`` disables sqlite3's same-thread check for
    callers that hold the connection on one task but run each blocking
    call in a worker thread (ravend's SSE tail). The CALLER must
    guarantee no two threads use it concurrently — awaiting each call
    before the next satisfies that."""
    resolved = resolve_db_path(db_path)
    conn = sqlite3.connect(
        str(resolved),
        timeout=DEFAULT_BUSY_TIMEOUT_S,
        isolation_level="DEFERRED",
        check_same_thread=not cross_thread,
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

    1. **Lease reaping** — two single set-based UPDATEs whose WHERE
       clauses re-check every predicate at write time (a SELECT-then-
       write gap here clobbered concurrently-completed claims and could
       double-lease — finding verify-000):
       ``leased AND lease_until < now AND deliveries >= max_deliveries``
       → ``dead`` (→ ``dead_lettered``); same but ``< max_deliveries``
       → ``lapsed`` (→ ``requeued``). A lapsed claim keeps its
       ``deliveries`` count durably and is re-claimed (count+1) by
       ``claims.claim_next`` — never deleted, so no caller-side
       snapshot is needed and dead-lettering cannot be evaded by other
       sweep call sites (finding verify-001).
    2. **Expiry**: messages past ``expires_at`` are *not* deleted
       (append-only log) — read paths must filter them. ``expired`` is
       a RUNNING TOTAL of live-expired messages lacking a terminal
       claim (observability only; it is not "newly expired this
       sweep").

    Cheap when nothing is stale; safe to call on every read."""
    now = _now_iso()

    _MAX_SUBQ = (
        "(SELECT ch.max_deliveries FROM messages AS m "
        " JOIN channels AS ch ON ch.id = m.channel_id "
        " WHERE m.id = claims.message_id)"
    )
    dead_cur = conn.execute(
        "UPDATE claims SET state = 'dead', updated_at = ? "
        f"WHERE state = 'leased' AND lease_until < ? AND deliveries >= {_MAX_SUBQ}",
        (now, now),
    )
    dead_lettered = dead_cur.rowcount if dead_cur.rowcount != -1 else 0

    requeue_cur = conn.execute(
        "UPDATE claims SET state = 'lapsed', updated_at = ? "
        f"WHERE state = 'leased' AND lease_until < ? AND deliveries < {_MAX_SUBQ}",
        (now, now),
    )
    requeued = requeue_cur.rowcount if requeue_cur.rowcount != -1 else 0

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
