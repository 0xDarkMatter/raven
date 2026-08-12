"""Queue read-state: atomic claims with leases.  LANE: claims (raven2-p1).

ADR-001: exactly-one-winner claiming uses
``INSERT INTO claims ... ON CONFLICT DO NOTHING``; rowcount 1 wins. Queue
state never mutates the append-only message log.

``db.sweep`` flips a lapsed lease to state='lapsed' (verify-000/001 fix
round), so the ``deliveries`` count survives requeue DURABLY in the
claims row. A lapsed claim is re-won atomically by
``UPDATE ... SET state='leased', consumer=?, deliveries=deliveries+1
WHERE message_id=? AND state='lapsed'`` (rowcount 1 wins); a
never-claimed message by the INSERT above. There is no caller-side
snapshot, so any sweep call site (cursors, doctor, other channels) is
harmless to dead-letter accounting. A voluntary ``release`` deletes the
row, deliberately forgetting attempts — cooperative hand-back is not a
failure signal.

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

# Candidate batch bound: keeps claim_next's scan O(batch) per round trip
# instead of loading an entire backlog (with bodies) under the write
# lock (finding verify-002). Losing a full batch to racing claimants
# just advances the id watermark and re-queries.
_CANDIDATE_BATCH = 32

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

    Skips expired messages and messages whose claim row is terminal or
    live (done, dead, leased). Claimable = no claim row (won via the
    atomic INSERT) or a 'lapsed' row (won via the guarded UPDATE, which
    increments ``deliveries`` — durable dead-letter accounting). On a
    lost race, falls through to the next candidate; only returns None
    when no candidate remains. Candidates are fetched in bounded
    batches so a large backlog never runs an unbounded scan under the
    write lock (finding verify-002)."""
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    channel_id = _queue_channel_id(conn, channel)

    after_id = 0
    while True:
        # Lease deadline is computed per batch, not once up front: a
        # short lease plus a slow scan over a large terminal backlog
        # could otherwise write an already-expired lease_until
        # (re-verify wave finding).
        lease_until = _lease_until(lease_s)
        candidates = conn.execute(
            f"""
            SELECT m.id, ch.name, m.sender, m.type, m.urgency, m.body, m.tags,
                   m.reply_to, m.thread_id, m.expires_at, m.created_at,
                   cl.state AS claim_state
            FROM messages AS m
            JOIN channels AS ch ON ch.id = m.channel_id
            LEFT JOIN claims AS cl ON cl.message_id = m.id
            WHERE m.channel_id = ?
              AND m.id > ?
              AND (m.expires_at IS NULL OR m.expires_at > {_NOW_SQL})
              AND (cl.message_id IS NULL OR cl.state = 'lapsed')
            ORDER BY m.id
            LIMIT {_CANDIDATE_BATCH}
            """,
            (channel_id, after_id),
        ).fetchall()
        if not candidates:
            return None

        for row in candidates:
            message_id = int(row[0])
            after_id = message_id
            if row["claim_state"] == "lapsed":
                # Re-claim: the guarded UPDATE is the atomic winner test
                # AND the durable attempt increment in one statement.
                cursor = conn.execute(
                    f"""
                    UPDATE claims
                    SET consumer = ?, state = 'leased',
                        deliveries = deliveries + 1,
                        lease_until = ?, updated_at = {_NOW_SQL}
                    WHERE message_id = ? AND state = 'lapsed'
                    """,
                    (consumer, lease_until, message_id),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO claims(
                        message_id, consumer, state, deliveries, lease_until
                    ) VALUES (?, ?, 'leased', 1, ?)
                    ON CONFLICT(message_id) DO NOTHING
                    """,
                    (message_id, consumer, lease_until),
                )
            if cursor.rowcount == 1:
                return _message_from_row(row)


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