"""Broadcast read-state: per-consumer cursors.  LANE: cursors (raven2-p1).

ADR-001: ack is **cursor-jump only** — ``ack(up_to_id)`` advances the
cursor monotonically; there is no per-message ack and no gap tracking.
Valid ONLY on ``broadcast`` channels (:class:`WrongChannelKindError`
otherwise). Read paths call ``db.sweep`` opportunistically and must
filter expired messages (delegate reads to ``log.read_after``).
"""

from __future__ import annotations

import sqlite3

from raven_bus import channels, db, log
from raven_bus.exceptions import WrongChannelKindError
from raven_bus.models import Channel, Cursor, Message, parse_consumer_id

# WHY centralise the format string: it MUST match the schema's column
# DEFAULT (migrations/0002_v2_schema.sql) so cursors never fights the
# clock source SQLite uses for created_at/updated_at defaults.
_TS_NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def _require_broadcast(conn: sqlite3.Connection, channel: str) -> Channel:
    """Resolve ``channel`` and assert it is ``broadcast``.

    WHY: both ``pending`` and ``ack`` share this guard — broadcast
    cursors are meaningless on queue/stream channels (ADR-001).
    ``channels.get_channel`` raises :class:`UnknownChannelError` for a
    name that has no row.
    """
    ch = channels.get_channel(conn, channel)
    if ch.kind != "broadcast":
        raise WrongChannelKindError(
            f"cursor ops require a 'broadcast' channel; {channel!r} is kind={ch.kind!r}"
        )
    return ch


def _upsert_consumer(conn: sqlite3.Connection, consumer: str, role: str, run: str) -> None:
    """Register ``consumer`` and bump ``last_seen_at`` (idempotent).

    WHY: ADR-002 — the consumer id is the full validated string; the
    role/run atoms are parsed once upstream and reused here. On conflict
    we touch only ``last_seen_at`` — role/run/kind are immutable facts
    about an existing consumer, not per-read state.
    """
    conn.execute(
        f"""
        INSERT INTO consumers (id, role, run, last_seen_at)
        VALUES (?, ?, ?, {_TS_NOW})
        ON CONFLICT(id) DO UPDATE SET last_seen_at = {_TS_NOW}
        """,
        (consumer, role, run),
    )


def _last_ack_id(conn: sqlite3.Connection, consumer: str, channel_id: int) -> int:
    """Current cursor position, or 0 if the consumer never acked here."""
    row = conn.execute(
        "SELECT last_ack_id FROM cursors WHERE consumer = ? AND channel_id = ?",
        (consumer, channel_id),
    ).fetchone()
    return int(row["last_ack_id"]) if row is not None else 0


def _load_cursor(conn: sqlite3.Connection, consumer: str, channel: Channel) -> Cursor | None:
    """Hydrate a :class:`Cursor` from the row, or None if absent."""
    row = conn.execute(
        """
        SELECT last_ack_id, updated_at
        FROM cursors
        WHERE consumer = ? AND channel_id = ?
        """,
        (consumer, channel.id),
    ).fetchone()
    if row is None:
        return None
    return Cursor(
        consumer=consumer,
        channel=channel.name,
        last_ack_id=int(row["last_ack_id"]),
        updated_at=row["updated_at"],
    )


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
    # WHY sweep first: opportunistic expiry/lease enforcement (ADR-001)
    # so a read never serves state a lapsed lease should have released.
    db.sweep(conn)

    # ADR-002: validate the full <role>@<run> string via the frozen helper.
    role, run = parse_consumer_id(consumer)

    ch = _require_broadcast(conn, channel)
    _upsert_consumer(conn, consumer, role, run)

    # WHY read_after over a hand-rolled query: it is the canonical
    # expiry-filtering, id-ascending read (log lane contract). Reading
    # does NOT move the cursor — only ack advances last_ack_id.
    after_id = _last_ack_id(conn, consumer, ch.id)
    return log.read_after(conn, channel, after_id, limit=limit, include_expired=False)


def ack(
    conn: sqlite3.Connection,
    consumer: str,
    channel: str,
    up_to_id: int,
) -> Cursor:
    """Advance the cursor to ``max(current, up_to_id)`` (monotonic —
    acking backwards is a no-op, never an error). Returns the resulting
    cursor. Creates the cursor row if absent."""
    role, run = parse_consumer_id(consumer)
    ch = _require_broadcast(conn, channel)
    _upsert_consumer(conn, consumer, role, run)

    # WHY a single UPSERT with MAX + a guarded updated_at: it is atomic
    # monotonic advancement with no read/modify/write race. The CASE
    # makes a backwards ack a TRUE no-op — last_ack_id AND updated_at
    # are both left untouched, so observers don't see a phantom touch.
    conn.execute(
        f"""
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES (:consumer, :channel_id, :up_to_id, {_TS_NOW})
        ON CONFLICT(consumer, channel_id) DO UPDATE SET
            last_ack_id = MAX(excluded.last_ack_id, cursors.last_ack_id),
            updated_at = CASE
                WHEN excluded.last_ack_id > cursors.last_ack_id
                THEN {_TS_NOW}
                ELSE cursors.updated_at
            END
        """,
        {"consumer": consumer, "channel_id": ch.id, "up_to_id": up_to_id},
    )

    result = _load_cursor(conn, consumer, ch)
    # WHY assert: the UPSERT above guarantees the row exists; None here
    # would mean a broken constraint or a swept-away channel, not a
    # normal condition — fail loudly rather than mask it with None.
    assert result is not None
    return result


def get_cursor(conn: sqlite3.Connection, consumer: str, channel: str) -> Cursor | None:
    """Current cursor, or None if the consumer never acked here."""
    # WHY a pure join with no validation/sweep: this is a side-effect-free
    # lookup. An unknown channel simply has no matching row -> None,
    # matching "row or None" without raising on a name that may be
    # legitimately absent.
    row = conn.execute(
        """
        SELECT c.last_ack_id, c.updated_at
        FROM cursors c
        JOIN channels ch ON ch.id = c.channel_id
        WHERE c.consumer = ? AND ch.name = ?
        """,
        (consumer, channel),
    ).fetchone()
    if row is None:
        return None
    return Cursor(
        consumer=consumer,
        channel=channel,
        last_ack_id=int(row["last_ack_id"]),
        updated_at=row["updated_at"],
    )


__all__ = ["ack", "get_cursor", "pending"]
