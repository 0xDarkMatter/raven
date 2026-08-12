"""Queue read-state: atomic claims with leases.  LANE: claims (raven2-p1).

ADR-001: exactly-one-winner claiming uses
``INSERT INTO claims ... ON CONFLICT DO NOTHING``; rowcount 1 wins. Queue
state never mutates the append-only message log.

The frozen sweep contract deletes a sub-threshold lapsed claim. To retain
its attempt count without adding state to ``messages``, :func:`claim_next`
snapshots lapsed claims for its channel immediately before calling
``db.sweep``. A winning re-claim inserts ``deliveries=prior+1``. Once the
stored count reaches ``channel.max_deliveries``, the next sweep changes the
claim to ``dead`` instead of deleting it. A voluntary release is not in the
snapshot and its later claim therefore starts at one. This bookkeeping is
deliberately scoped to the same claim operation and transaction as the
sweep; the claims row remains the sole durable queue state.

Every public operation invokes ``db.sweep`` before observing actionable
claim state. Valid only on ``queue`` channels
(:class:`WrongChannelKindError` otherwise).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from raven_bus import db
from raven_bus.exceptions import (
    ClaimDeniedError,
    UnknownChannelError,
    WrongChannelKindError,
)
from raven_bus.models import Claim, Message, parse_consumer_id, validate_channel_name

DEFAULT_LEASE_S = 300

_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def _upsert_consumer(conn: sqlite3.Connection, consumer: str) -> None:
    role, run = parse_consumer_id(consumer)
    conn.execute(
        f"""
        INSERT INTO consumers(id, role, run, last_seen_at)
        VALUES (?, ?, ?, {_NOW_SQL})
        ON CONFLICT(id) DO UPDATE SET
            role = excluded.role,
            run = excluded.run,
            last_seen_at = excluded.last_seen_at
        """,
        (consumer, role, run),
    )


def _queue_channel_id(conn: sqlite3.Connection, channel: str) -> int:
    validate_channel_name(channel)
    row = conn.execute(
        "SELECT id, kind FROM channels WHERE name = ?",
        (channel,),
    ).fetchone()
    if row is None:
        raise UnknownChannelError(f"channel {channel!r} does not exist")
    if row[1] != "queue":
        raise WrongChannelKindError(
            f"channel {channel!r} has kind {row[1]!r}, expected 'queue'"
        )
    return int(row[0])


def _lapsed_deliveries(conn: sqlite3.Connection, channel: str) -> dict[int, int]:
    """Capture counts that the frozen sweep is about to delete."""
    validate_channel_name(channel)
    rows = conn.execute(
        f"""
        SELECT cl.message_id, cl.deliveries
        FROM claims AS cl
        JOIN messages AS m ON m.id = cl.message_id
        JOIN channels AS ch ON ch.id = m.channel_id
        WHERE ch.name = ?
          AND cl.state = 'leased'
          AND cl.lease_until < {_NOW_SQL}
          AND cl.deliveries < ch.max_deliveries
        """,
        (channel,),
    ).fetchall()
    return {int(row[0]): int(row[1]) for row in rows}


def _lease_until(lease_s: int) -> str:
    value = datetime.now(tz=UTC) + timedelta(seconds=lease_s)
    milliseconds = f"{value.microsecond // 1000:03d}"
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{milliseconds}Z"


def _claim_from_row(row: sqlite3.Row | tuple[Any, ...]) -> Claim:
    return Claim(
        message_id=row[0],
        consumer=row[1],
        state=row[2],
        deliveries=row[3],
        lease_until=row[4],
        updated_at=row[5],
    )


def _message_from_row(row: sqlite3.Row | tuple[Any, ...]) -> Message:
    raw_body = row[5]
    body = json.loads(raw_body) if raw_body else {}
    return Message(
        id=row[0],
        channel=row[1],
        sender=row[2],
        type=row[3],
        urgency=row[4],
        body=body,
        tags=[tag for tag in str(row[6] or "").split(",") if tag],
        reply_to=row[7],
        thread_id=row[8],
        expires_at=row[9],
        created_at=row[10],
    )


def _read_claim(conn: sqlite3.Connection, message_id: int) -> Claim | None:
    row = conn.execute(
        """
        SELECT message_id, consumer, state, deliveries, lease_until, updated_at
        FROM claims
        WHERE message_id = ?
        """,
        (message_id,),
    ).fetchone()
    return None if row is None else _claim_from_row(row)


def _denied(message_id: int, consumer: str) -> ClaimDeniedError:
    return ClaimDeniedError(
        f"consumer {consumer!r} does not hold a leased claim for message "
        f"id={message_id}"
    )


def claim_next(
    conn: sqlite3.Connection,
    consumer: str,
    channel: str,
    *,
    lease_s: int = DEFAULT_LEASE_S,
) -> Message | None:
    """Claim the oldest live unclaimed message on ``channel``, or None.

    Skips expired messages and messages with any claim row (leased,
    done, or dead). On a lost race (another consumer inserted first),
    retries the next candidate rather than returning None early.
    Re-claiming after a lease lapse increments ``deliveries``."""
    prior_deliveries = _lapsed_deliveries(conn, channel)
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    channel_id = _queue_channel_id(conn, channel)

    candidates = conn.execute(
        f"""
        SELECT m.id, ch.name, m.sender, m.type, m.urgency, m.body, m.tags,
               m.reply_to, m.thread_id, m.expires_at, m.created_at
        FROM messages AS m
        JOIN channels AS ch ON ch.id = m.channel_id
        LEFT JOIN claims AS cl ON cl.message_id = m.id
        WHERE m.channel_id = ?
          AND (m.expires_at IS NULL OR m.expires_at > {_NOW_SQL})
          AND cl.message_id IS NULL
        ORDER BY m.id
        """,
        (channel_id,),
    ).fetchall()

    lease_until = _lease_until(lease_s)
    for row in candidates:
        message_id = int(row[0])
        attempts = prior_deliveries.get(message_id, 0) + 1
        cursor = conn.execute(
            """
            INSERT INTO claims(
                message_id, consumer, state, deliveries, lease_until
            ) VALUES (?, ?, 'leased', ?, ?)
            ON CONFLICT(message_id) DO NOTHING
            """,
            (message_id, consumer, attempts, lease_until),
        )
        if cursor.rowcount == 1:
            return _message_from_row(row)
    return None


def renew(
    conn: sqlite3.Connection,
    message_id: int,
    consumer: str,
    *,
    lease_s: int = DEFAULT_LEASE_S,
) -> Claim:
    """Extend a lease this consumer holds. Raises
    :class:`ClaimDeniedError` if the claim is absent, held by another
    consumer, or not in state 'leased' (a lapsed-and-reaped lease is
    indistinguishable from never-claimed — by design)."""
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    cursor = conn.execute(
        """
        UPDATE claims
        SET lease_until = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE message_id = ? AND consumer = ? AND state = 'leased'
        """,
        (_lease_until(lease_s), message_id, consumer),
    )
    if cursor.rowcount != 1:
        raise _denied(message_id, consumer)
    claim = _read_claim(conn, message_id)
    if claim is None:  # pragma: no cover - protected by the write lock above
        raise _denied(message_id, consumer)
    return claim


def complete(conn: sqlite3.Connection, message_id: int, consumer: str) -> Claim:
    """Mark this consumer's leased claim 'done' (terminal). Same denial
    rules as :func:`renew`. Idempotent for the same consumer: completing
    an already-'done' claim you own returns it unchanged."""
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    cursor = conn.execute(
        f"""
        UPDATE claims
        SET state = 'done', updated_at = {_NOW_SQL}
        WHERE message_id = ? AND consumer = ? AND state = 'leased'
        """,
        (message_id, consumer),
    )
    claim = _read_claim(conn, message_id)
    if cursor.rowcount == 1:
        if claim is None:  # pragma: no cover - protected by the write lock above
            raise _denied(message_id, consumer)
        return claim
    if claim is not None and claim.consumer == consumer and claim.state == "done":
        return claim
    raise _denied(message_id, consumer)


def release(conn: sqlite3.Connection, message_id: int, consumer: str) -> None:
    """Voluntarily give a leased message back: deletes the claim row so
    the message is immediately claimable. The next claim starts fresh at
    deliveries=1 — deliberate: voluntary release is cooperative, not a
    failure signal, so it never counts toward dead-lettering. Same
    denial rules as :func:`renew`."""
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    cursor = conn.execute(
        """
        DELETE FROM claims
        WHERE message_id = ? AND consumer = ? AND state = 'leased'
        """,
        (message_id, consumer),
    )
    if cursor.rowcount != 1:
        raise _denied(message_id, consumer)


def get_claim(conn: sqlite3.Connection, message_id: int) -> Claim | None:
    """Current claim row for a message, or None."""
    db.sweep(conn)
    return _read_claim(conn, message_id)


__all__ = [
    "DEFAULT_LEASE_S",
    "claim_next",
    "complete",
    "get_claim",
    "release",
    "renew",
]