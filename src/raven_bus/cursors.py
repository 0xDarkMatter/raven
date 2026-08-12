"""Broadcast read-state: per-consumer cursors.  LANE: cursors (raven2-p1).

ADR-001: ack is **cursor-jump only** — ``ack(up_to_id)`` advances the
cursor monotonically; there is no per-message ack and no gap tracking.
Valid ONLY on ``broadcast`` channels (:class:`WrongChannelKindError`
otherwise). Read paths call ``db.sweep`` opportunistically and must
filter expired messages (delegate reads to ``log.read_after``).
"""

from __future__ import annotations

import sqlite3

from raven_bus.models import Cursor, Message


def pending(
    conn: sqlite3.Connection,
    consumer: str,
    channel: str,
    *,
    limit: int = 100,
) -> list[Message]:
    """Unseen live messages for ``consumer`` on ``channel``: id >
    cursor's last_ack_id, expired filtered, id-ordered ascending.
    Registers the consumer (last_seen_at bump). Does NOT move the
    cursor — reading is not acking."""
    raise NotImplementedError


def ack(
    conn: sqlite3.Connection,
    consumer: str,
    channel: str,
    up_to_id: int,
) -> Cursor:
    """Advance the cursor to ``max(current, up_to_id)`` (monotonic —
    acking backwards is a no-op, never an error). Returns the resulting
    cursor. Creates the cursor row if absent."""
    raise NotImplementedError


def get_cursor(
    conn: sqlite3.Connection, consumer: str, channel: str
) -> Cursor | None:
    """Current cursor, or None if the consumer never acked here."""
    raise NotImplementedError


__all__ = ["ack", "get_cursor", "pending"]
