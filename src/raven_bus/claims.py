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

CLAIM FRONTIER (verify-002 waived-P1 fix): a cold ``claim_next`` scans
from message id 0, so a channel with a large terminal (done/dead)
backlog pays an index scan proportional to that backlog on *every*
call, not just the first. ``_FRONTIER`` is a per-process, in-memory
``{(db_path, channel_id): id}`` watermark below which no NEVER-CLAIMED
message can exist -- once a fresh-candidate scan proves it reached the
true end of the messages table for a channel without filling a batch,
every id up to that table's current max is known to already carry a
claims row (or was just about to receive one), so the next call's
fresh scan starts there instead of at 0. Lapsed rows (leases that
expired) are NOT covered by the frontier -- they can and do exist
below it, since a message can be claimed-then-lapse long after the
frontier passed its id -- so they are always found via a direct
``claims JOIN messages`` scan on ``state = 'lapsed'``, independent of
the watermark. This keeps the sanctioned two-part shape: "have I ever
been claimed" (frontier-bounded) is a different question from "am I
lapsed right now" (small-table scan).

Crash behaviour: the dict is pure cache, never persisted. A fresh
process (restart, crash, new worker) starts every channel at
frontier=0 and re-derives the watermark from the same query that
serves the request -- one full backlog scan to warm up, same cost as
today's unoptimised path, then O(batch) after. No schema change, no
public-signature change, no correctness dependency on the dict ever
being populated.
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

# Per-process claim-frontier cache -- see the module docstring
# ("CLAIM FRONTIER") for the invariant, the two-part scan shape it
# enables, and cold-process (empty-dict) crash behaviour.
_FRONTIER: dict[tuple[str, int], int] = {}

_CANDIDATE_COLUMNS = """
    m.id, ch.name, m.sender, m.type, m.urgency, m.body, m.tags,
    m.reply_to, m.thread_id, m.expires_at, m.created_at
"""

# Part (a): lapsed rows -- always scanned from after_id, never bounded
# by the frontier, since a message can lapse long after its id fell
# below the watermark. Cheap because the claims table is small
# relative to messages (no index on state is added -- that would be a
# schema change, out of scope for this lane).
_LAPSED_CANDIDATES_SQL = f"""
    SELECT {_CANDIDATE_COLUMNS}, 'lapsed' AS claim_state
    FROM claims AS cl
    JOIN messages AS m ON m.id = cl.message_id
    JOIN channels AS ch ON ch.id = m.channel_id
    WHERE cl.state = 'lapsed'
      AND m.channel_id = ?
      AND m.id > ?
      AND (m.expires_at IS NULL OR m.expires_at > {_NOW_SQL})
    ORDER BY m.id
    LIMIT {_CANDIDATE_BATCH}
"""

# Part (b): never-claimed rows -- bounded below by max(after_id,
# frontier), so a warm frontier skips straight past any terminal
# backlog instead of re-walking it every call.
_FRESH_CANDIDATES_SQL = f"""
    SELECT {_CANDIDATE_COLUMNS}, NULL AS claim_state
    FROM messages AS m
    JOIN channels AS ch ON ch.id = m.channel_id
    LEFT JOIN claims AS cl ON cl.message_id = m.id
    WHERE m.channel_id = ?
      AND m.id > ?
      AND (m.expires_at IS NULL OR m.expires_at > {_NOW_SQL})
      AND cl.message_id IS NULL
    ORDER BY m.id
    LIMIT {_CANDIDATE_BATCH}
"""


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


def _frontier_db_key(conn: sqlite3.Connection) -> str:
    """Identify the attached DB file for the ``_FRONTIER`` cache key.

    ``PRAGMA database_list`` reflects the same read snapshot as any
    other statement on this connection -- cheap (single row) and
    avoids threading db_path through every caller."""
    row = conn.execute("PRAGMA database_list").fetchone()
    return "" if row is None else str(row[2])


def _maybe_advance_frontier(
    conn: sqlite3.Connection,
    channel_id: int,
    frontier_key: tuple[str, int],
    frontier: int,
    fresh_rows: list[sqlite3.Row],
) -> None:
    """Advance the cached frontier once a fresh-candidate scan proves
    it reached the true end of the messages table for this channel.

    Only call this once every row in ``fresh_rows`` has actually been
    attempted (INSERTed) this round -- returning early with a winner
    mid-batch would leave later fresh rows genuinely never-claimed,
    and advancing the watermark past them would starve them forever.
    ``len(fresh_rows) < _CANDIDATE_BATCH`` is what proves the scan hit
    table end rather than stopping on LIMIT: SQLite only returns fewer
    than the batch when there was nothing left to examine. Because
    ``db.sweep``/``_upsert_consumer`` already opened a write
    transaction earlier in this call, this MAX(id) read shares that
    transaction's snapshot with the fresh-candidate scan -- no
    concurrently-committed message can land at or below the ceiling
    without also having been visible to the scan."""
    if len(fresh_rows) >= _CANDIDATE_BATCH:
        return
    ceiling_row = conn.execute(
        "SELECT MAX(id) FROM messages WHERE channel_id = ?", (channel_id,)
    ).fetchone()
    ceiling = ceiling_row[0]
    if ceiling is not None and ceiling > frontier:
        _FRONTIER[frontier_key] = int(ceiling)


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
    when no candidate remains. Candidates are fetched as two bounded
    batches — lapsed rows (always scanned) and fresh rows (scanned
    from the cached claim frontier, see the module docstring) — so a
    large terminal backlog never runs an unbounded scan under the
    write lock (finding verify-002), and a warm frontier skips it
    entirely on repeat calls."""
    db.sweep(conn)
    _upsert_consumer(conn, consumer)
    channel_id = _queue_channel_id(conn, channel)
    frontier_key = (_frontier_db_key(conn), channel_id)

    after_id = 0
    while True:
        # Lease deadline is computed per batch, not once up front: a
        # short lease plus a slow scan over a large terminal backlog
        # could otherwise write an already-expired lease_until
        # (re-verify wave finding).
        lease_until = _lease_until(lease_s)
        frontier = _FRONTIER.get(frontier_key, 0)
        fresh_after = max(after_id, frontier)

        lapsed_rows = conn.execute(
            _LAPSED_CANDIDATES_SQL, (channel_id, after_id)
        ).fetchall()
        fresh_rows = conn.execute(
            _FRESH_CANDIDATES_SQL, (channel_id, fresh_after)
        ).fetchall()

        candidates = sorted([*lapsed_rows, *fresh_rows], key=lambda r: r["id"])
        if not candidates:
            # Vacuously "every fresh row was attempted" (there were
            # none) — safe to advance before returning.
            _maybe_advance_frontier(
                conn, channel_id, frontier_key, frontier, fresh_rows
            )
            return None

        for row in candidates:
            message_id = int(row["id"])
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

        # Reached only when every candidate this round (including
        # every fresh row) lost its race — each now genuinely carries
        # a claims row, so it is safe to advance the frontier past
        # them before the next iteration re-queries.
        _maybe_advance_frontier(conn, channel_id, frontier_key, frontier, fresh_rows)


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