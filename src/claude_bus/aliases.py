"""Alias registry — deterministic display names for a (role, session) pair.

The alias is a primary-key ``TEXT`` derived from
``<role>-<first-6-of-sha1(role+session_id)>``. This is stable: the
same (role, session_id) always produces the same alias, and repeat
registrations are idempotent (INSERT OR IGNORE).
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class Alias(BaseModel):
    """Row-shaped alias returned by :func:`resolve` / :func:`list_by_role`."""

    model_config = ConfigDict(frozen=True)

    alias: str
    role: str
    session_id: str
    created_at: datetime


def compute_alias(role: str, session_id: str) -> str:
    """Deterministic ``<role>-<6hex>`` form.

    Public because callers (e.g. :mod:`claude_bus.client`) need to
    derive the alias for a known ``(role, session_id)`` pair without
    going through a DB round-trip.
    """
    digest = hashlib.sha1(f"{role}:{session_id}".encode(), usedforsecurity=False)
    return f"{role}-{digest.hexdigest()[:6]}"


def register(conn: sqlite3.Connection, role: str, session_id: str) -> str:
    """Register the deterministic alias for (role, session_id).

    Idempotent: re-registering an existing (role, session_id) returns
    the same alias without error. The first registration records
    ``created_at`` at DB time; subsequent calls leave it unchanged.
    """
    alias = compute_alias(role, session_id)
    conn.execute(
        "INSERT OR IGNORE INTO aliases(alias, role, session_id) "
        "VALUES (?, ?, ?)",
        (alias, role, session_id),
    )
    return alias


def resolve(conn: sqlite3.Connection, alias: str) -> Alias | None:
    """Return the :class:`Alias` row for ``alias``, or ``None`` if absent."""
    row = conn.execute(
        "SELECT alias, role, session_id, created_at FROM aliases WHERE alias = ?",
        (alias,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_alias(row)


def list_by_role(
    conn: sqlite3.Connection, role: str, session_id: str
) -> list[Alias]:
    """Return all aliases matching ``role`` within ``session_id``.

    Results are sorted by ``created_at ASC`` so a stable iteration order
    across multiple registrations within the same second is preserved
    (SQLite tiebreaks on insertion order when timestamps collide).
    """
    rows = conn.execute(
        "SELECT alias, role, session_id, created_at FROM aliases "
        "WHERE role = ? AND session_id = ? "
        "ORDER BY created_at ASC, alias ASC",
        (role, session_id),
    ).fetchall()
    return [_row_to_alias(r) for r in rows]


def list_by_session(
    conn: sqlite3.Connection, session_id: str
) -> list[Alias]:
    """Return every alias registered within ``session_id``."""
    rows = conn.execute(
        "SELECT alias, role, session_id, created_at FROM aliases "
        "WHERE session_id = ? "
        "ORDER BY created_at ASC, alias ASC",
        (session_id,),
    ).fetchall()
    return [_row_to_alias(r) for r in rows]


def _row_to_alias(row: sqlite3.Row) -> Alias:
    return Alias(
        alias=row["alias"],
        role=row["role"],
        session_id=row["session_id"],
        created_at=_parse_sqlite_ts(row["created_at"]),
    )


def _parse_sqlite_ts(raw: str) -> datetime:
    """Parse SQLite's ``strftime('%Y-%m-%dT%H:%M:%fZ')`` into UTC datetime."""
    # SQLite emits e.g. "2026-04-22T12:34:56.123Z". datetime.fromisoformat
    # since 3.11 accepts the trailing "Z" via a minor adjustment.
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


__all__ = [
    "Alias",
    "compute_alias",
    "list_by_role",
    "list_by_session",
    "register",
    "resolve",
]
