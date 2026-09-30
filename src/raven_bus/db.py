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

from raven_bus.exceptions import SchemaMismatchError, TeardownBlockedError
from raven_bus.models import SweepResult
from raven_bus.paths import resolve_db_path

SCHEMA_VERSION = "2"
DEFAULT_BUSY_TIMEOUT_S: float = 5.0

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_V2_MIGRATION = _MIGRATIONS_DIR / "0002_v2_schema.sql"

# The tables 0002_v2_schema.sql creates. An UNSTAMPED file holding only
# these (plus SQLite's internal sqlite_* tables) is a peer's first-create
# in flight, or one that crashed before stamping — finishing it is safe.
# Any other table means a foreign file, which init_db refuses rather than
# stamping it '2' or half-migrating it (QA store #8).
_V2_TABLES = frozenset(
    {"bus_meta", "channels", "claims", "consumers", "cursors", "messages"}
)

# Primary result codes worth a retry (extended codes carry them in the
# low byte, e.g. SQLITE_BUSY_SNAPSHOT = 517 -> 5).
_BUSY_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})

_init_cache: set[Path] = set()


def _reset_init_cache() -> None:
    """Test helper — drop the per-process init cache."""
    _init_cache.clear()


def init_db(db_path: str | Path | None = None, *, force: bool = False) -> Path:
    """Create the DB if missing, apply the v2 schema, record
    ``schema_version`` in ``bus_meta``. Idempotent, process-cached.
    Creates parent directories. Returns the resolved absolute path.

    Raises :class:`SchemaMismatchError` — leaving the file untouched —
    for a file stamped with another schema version, an unstamped SQLite
    file holding tables raven did not create, or unstamped raven-named
    tables the v2 script cannot apply over. Only busy/locked errors are
    retried; any other sqlite error surfaces at once."""
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
    probe = _probe(resolved)
    stored = None if probe is None else probe[0]
    if not force and stored == SCHEMA_VERSION:
        _init_cache.add(resolved)
        return resolved
    # A DB stamped with a DIFFERENT version must never be silently
    # re-stamped: CREATE IF NOT EXISTS would leave the old tables in
    # place while marking the file current (re-verify wave finding).
    # v2 does not migrate foreign schemas — refuse loudly.
    if stored is not None and stored != SCHEMA_VERSION:
        raise SchemaMismatchError(
            f"{resolved} was created by schema version {stored!r}; this "
            f"raven_bus expects version {SCHEMA_VERSION!r} and does not "
            "migrate old files — point RAVEN_DB/db_path at a fresh path"
        )
    # Unstamped but readable: only raven's own tables may be adopted.
    adopting = False
    if probe is not None and stored is None:
        tables = {name for name in probe[1] if not name.startswith("sqlite_")}
        foreign = sorted(tables - _V2_TABLES)
        if foreign:
            raise SchemaMismatchError(
                f"{resolved} has no raven schema stamp but holds tables raven "
                f"did not create ({', '.join(foreign)}); refusing to adopt it "
                "— point RAVEN_DB/db_path at a fresh path"
            )
        adopting = bool(tables)

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
            if not _is_busy(exc):
                # Deterministic: five retries would only delay the same
                # failure. Over pre-existing raven-named tables it means
                # they are not v2's shape (e.g. 'no such column').
                if adopting:
                    raise SchemaMismatchError(
                        f"{resolved} holds raven-named tables but no schema "
                        f"stamp, and the v2 schema does not apply over them "
                        f"({exc}) — point RAVEN_DB/db_path at a fresh path"
                    ) from exc
                raise
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


def _probe(resolved: Path) -> tuple[str | None, frozenset[str]] | None:
    """``(schema_version stamp or None, table names)`` of ``resolved``,
    or None when the file is missing or unreadable.

    Read-only; never creates the file (sqlite3.connect would, so check
    existence first). Any sqlite error means "unreadable", never "no
    stamp": only a SUCCESSFUL read that finds tables without a stamp may
    lead init_db to refuse a file, so a transient error can't get a live
    store refused as foreign."""
    if not resolved.exists():
        return None
    try:
        conn = sqlite3.connect(str(resolved), timeout=DEFAULT_BUSY_TIMEOUT_S)
        try:
            tables = frozenset(
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            )
            row = None
            if "bus_meta" in tables:
                row = conn.execute(
                    "SELECT value FROM bus_meta WHERE key = 'schema_version'"
                ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    return (None if row is None else str(row[0])), tables


def _stored_schema_version(resolved: Path) -> str | None:
    """The schema version recorded in ``resolved``, or None (missing,
    unreadable, or unstamped)."""
    probe = _probe(resolved)
    return None if probe is None else probe[0]


def _is_busy(exc: sqlite3.OperationalError) -> bool:
    """True for the transient lock errors worth retrying (SQLITE_BUSY /
    SQLITE_LOCKED, incl. extended codes). Everything else — 'no such
    column' from a foreign schema, disk I/O, read-only — is
    deterministic. Hand-built errors carry no ``sqlite_errorcode``
    (sqlite3 sets it only on errors it raises), so fall back to
    sqlite's message wording."""
    code = getattr(exc, "sqlite_errorcode", None)
    if code is not None:
        return (code & 0xFF) in _BUSY_CODES
    message = str(exc).lower()
    return "locked" in message or "busy" in message


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


_TEARDOWN_BLOCKERS_SHOWN = 10


def _teardown_blockers(
    conn: sqlite3.Connection, channel_ids: list[int]
) -> tuple[list[tuple[int, str]], int]:
    """Messages OUTSIDE ``channel_ids`` whose reply_to/thread_id points at
    a message INSIDE them: the first ``_TEARDOWN_BLOCKERS_SHOWN`` as
    ``(id, channel_name)`` in id order, plus the full count (one scan —
    ``COUNT(*) OVER ()`` is computed before LIMIT)."""
    placeholders = ",".join("?" for _ in channel_ids)
    rows = conn.execute(
        f"""
        SELECT o.id, ch.name, COUNT(*) OVER () AS total
        FROM messages AS o
        JOIN channels AS ch ON ch.id = o.channel_id
        WHERE o.channel_id NOT IN ({placeholders})
          AND EXISTS (
              SELECT 1 FROM messages AS p
              WHERE p.id IN (o.reply_to, o.thread_id)
                AND p.channel_id IN ({placeholders})
          )
        ORDER BY o.id
        LIMIT {_TEARDOWN_BLOCKERS_SHOWN}
        """,
        [*channel_ids, *channel_ids],
    ).fetchall()
    blockers = [(int(row[0]), str(row[1])) for row in rows]
    return blockers, (int(rows[0][2]) if rows else 0)


def _teardown_blocked(
    run: str, blockers: list[tuple[int, str]], total: int
) -> TeardownBlockedError:
    shown = ", ".join(f"#{mid} in {name!r}" for mid, name in blockers)
    more = f" and {total - len(blockers)} more" if total > len(blockers) else ""
    return TeardownBlockedError(
        f"cannot tear down run {run!r}: {total} message(s) outside it reply to "
        f"or thread under its messages ({shown}{more}); deleting would orphan "
        "them (the schema's foreign keys forbid it) — nothing was deleted",
        blockers=blockers,
        total=total,
    )


def teardown_run(conn: sqlite3.Connection, run: str) -> int:
    """Delete all rows for channels under ``run/<run>/`` (messages,
    cursors, claims, the channels themselves) plus ``consumers`` rows
    with this run. Returns total rows removed. The ONLY sanctioned
    DELETE on messages besides retention (ADR-001/002).

    Raises :class:`TeardownBlockedError` — having deleted NOTHING — when
    a message outside the run references (reply_to/thread_id) one inside
    it: the schema's self-referencing foreign keys forbid orphaning it,
    and that used to surface as a raw IntegrityError mid-teardown (QA
    store #6). There is no migration chain, so existing DBs keep those
    FKs; the refusal, not a schema change, is the fix."""
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
        # Pre-check BEFORE the first DELETE, so a blocked teardown leaves
        # the caller's transaction untouched.
        blockers, count = _teardown_blockers(conn, channel_ids)
        if count:
            raise _teardown_blocked(run, blockers, count)
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
        try:
            cur = conn.execute(
                f"DELETE FROM messages WHERE channel_id IN ({placeholders})",
                channel_ids,
            )
        except sqlite3.IntegrityError:
            # Race backstop: the pre-check ran before this transaction held
            # the write lock, so a peer can add a referencing message in
            # between. Same typed refusal; the claims/cursors deletes above
            # are undone by the caller's rollback (db.connection does it).
            blockers, count = _teardown_blockers(conn, channel_ids)
            if not count:
                raise
            raise _teardown_blocked(run, blockers, count) from None
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
