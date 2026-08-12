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
    raise NotImplementedError


@contextmanager
def connection(db_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    """Short-lived connection: WAL + foreign_keys pragmas, Row factory,
    commit on clean exit, rollback + re-raise on exception, always
    closed. DEFERRED isolation."""
    raise NotImplementedError


def data_version(conn: sqlite3.Connection) -> int:
    """Return ``PRAGMA data_version`` — changes whenever another
    connection commits. The cheap poll primitive."""
    raise NotImplementedError


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
    raise NotImplementedError


def teardown_run(conn: sqlite3.Connection, run: str) -> int:
    """Delete all rows for channels under ``run/<run>/`` (messages,
    cursors, claims, the channels themselves) plus ``consumers`` rows
    with this run. Returns total rows removed. The ONLY sanctioned
    DELETE on messages besides retention (ADR-001/002)."""
    raise NotImplementedError


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
