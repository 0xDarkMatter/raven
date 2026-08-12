"""Consumer registry — the one writer of the ``consumers`` table.

Extracted in the raven2-p2 verify round: log/cursors/claims each carried
a private copy of this upsert, and the HTTP heartbeat endpoint ran raw
SQL in the bridge layer (ADR-005 drift). One public contract, four
call sites.
"""

from __future__ import annotations

import sqlite3

from raven_bus.models import parse_consumer_id

_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def touch(conn: sqlite3.Connection, consumer: str) -> None:
    """Validate ``consumer`` (ADR-002 grammar), upsert its row, and bump
    ``last_seen_at`` — the liveness signal `raven`'s fleet tooling reads.
    Idempotent; raises :class:`InvalidAddressError` on bad grammar."""
    role, run = parse_consumer_id(consumer)
    conn.execute(
        f"""
        INSERT INTO consumers(id, role, run, last_seen_at)
        VALUES (?, ?, ?, {_NOW_SQL})
        ON CONFLICT(id) DO UPDATE SET last_seen_at = {_NOW_SQL}
        """,
        (consumer, role, run),
    )


__all__ = ["touch"]
