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
harmless to dead-letter accounting. A voluntary ``release`` flips the
row to lapsed and undoes only its OWN delivery (``deliveries - 1``,
floor 0), keeping earlier involuntary lapses on the count (never
delete: a deleted row becomes a never-claimed candidate below every
process's frontier — see release()).

Every public operation invokes ``db.sweep`` before observing actionable
claim state. Valid only on ``queue`` channels
(:class:`WrongChannelKindError` otherwise).

CLAIM FRONTIER (verify-002 waived-P1 fix): a cold ``claim_next`` scans
from message id 0, so a channel with a large terminal (done/dead)
backlog pays an index scan proportional to that backlog on *every*
call, not just the first. ``_FRONTIER`` is a per-process, in-memory
``{(db_file_key, channel_id): id}`` watermark below which no
NEVER-CLAIMED message can exist -- once a fresh-candidate scan proves
it reached the true end of the messages table for a channel without
filling a batch, every id up to the channel's max id READ BEFORE THAT
SCAN is known to carry a claims row (or be expired), so the next
call's fresh scan starts there instead of at 0. ``db_file_key`` names
the FILE, not just its path: see ``_frontier_db_key`` (a durable
``bus_meta`` instance id; no id, no cache). Lapsed rows (leases that
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
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from raven_bus import consumers, db
from raven_bus.exceptions import (
    ClaimDeniedError,
    UnknownChannelError,
    WrongChannelKindError,
)
from raven_bus.models import Claim, Message, validate_channel_name

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


def _frontier_db_key(conn: sqlite3.Connection) -> str | None:
    """Identify the attached DB FILE (not just its path) for the
    ``_FRONTIER`` cache key, or None to mean "don't cache".

    A REPLACED file at the same path must get a cold frontier: a stale
    watermark over a fresh file hides every message at or below it
    (raven2-p2 refute-frontier finding). The inode alone can't tell —
    ext4 reuses a replaced file's inode deterministically (QA store #11)
    — so the key carries ``db.instance_id``, a durable id stored IN the
    file. No readable id means no cache (never fall back to trusting
    the inode). The (st_dev, st_ino) stamp stays as a cheap extra
    discriminator (it does catch a backup restored by rename, which
    carries the old id); stat failure just drops it."""
    identity = db.instance_id(conn)
    if identity is None:
        return None
    row = conn.execute("PRAGMA database_list").fetchone()
    path = "" if row is None else str(row[2])
    try:
        stat = os.stat(path)
    except OSError:
        return f"{path}|{identity}"
    return f"{path}|{identity}|{stat.st_dev}:{stat.st_ino}"


def _channel_ceiling(conn: sqlite3.Connection, channel_id: int) -> int:
    """The channel's current max message id (0 if empty)."""
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM messages WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()
    return int(row[0])


def _maybe_advance_frontier(
    frontier_key: tuple[str, int] | None,
    frontier: int,
    ceiling: int,
    fresh_rows: list[sqlite3.Row],
) -> None:
    """Advance the cached frontier to ``ceiling`` once a fresh-candidate
    scan proves it reached the true end of this channel's messages.

    Only call this once every row in ``fresh_rows`` has actually been
    attempted (INSERTed) this round -- returning early with a winner
    mid-batch would leave later fresh rows genuinely never-claimed,
    and advancing the watermark past them would starve them forever.
    ``len(fresh_rows) < _CANDIDATE_BATCH`` is what proves the scan hit
    table end rather than stopping on LIMIT.

    ``ceiling`` MUST be read BEFORE the fresh scan (QA store #10): every
    id at or below it was committed before the scan began (one writer
    at a time allocates ids), so the scan saw all of them. Read after
    the scan -- as this used to be -- an autocommit connection (no
    write transaction pinning a snapshot) could see a message committed
    in between: above everything the scan returned, at/below the
    ceiling, and so skipped by this process forever."""
    if frontier_key is None or len(fresh_rows) >= _CANDIDATE_BATCH:
        return
    if ceiling > frontier:
        _FRONTIER[frontier_key] = ceiling


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
    consumers.touch(conn, consumer)
    channel_id = _queue_channel_id(conn, channel)
    db_key = _frontier_db_key(conn)
    frontier_key = None if db_key is None else (db_key, channel_id)

    # One resume point PER SCAN, never a shared one: after a round lost
    # in full, a shared "last candidate id" could be a lapsed row far
    # beyond the fresh batch's end, restarting the fresh scan past fresh
    # rows it never examined -- which the next short scan then buried
    # under the frontier (found while fixing QA store #10).
    lapsed_after = 0
    fresh_after = 0
    while True:
        # Lease deadline is computed per batch, not once up front: a
        # short lease plus a slow scan over a large terminal backlog
        # could otherwise write an already-expired lease_until
        # (re-verify wave finding).
        lease_until = _lease_until(lease_s)
        frontier = 0 if frontier_key is None else _FRONTIER.get(frontier_key, 0)
        fresh_after = max(fresh_after, frontier)
        ceiling = _channel_ceiling(conn, channel_id)  # BEFORE the scan -- see
        # _maybe_advance_frontier for why the order is load-bearing.

        lapsed_rows = conn.execute(
            _LAPSED_CANDIDATES_SQL, (channel_id, lapsed_after)
        ).fetchall()
        fresh_rows = conn.execute(
            _FRESH_CANDIDATES_SQL, (channel_id, fresh_after)
        ).fetchall()

        candidates = sorted([*lapsed_rows, *fresh_rows], key=lambda r: r["id"])
        if not candidates:
            # Vacuously "every fresh row was attempted" (there were
            # none) — safe to advance before returning.
            _maybe_advance_frontier(frontier_key, frontier, ceiling, fresh_rows)
            return None

        for row in candidates:
            message_id = int(row["id"])
            if row["claim_state"] == "lapsed":
                lapsed_after = message_id
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
                fresh_after = message_id
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
        _maybe_advance_frontier(frontier_key, frontier, ceiling, fresh_rows)


def renew(
    conn: sqlite3.Connection,
    message_id: int,
    consumer: str,
    *,
    lease_s: int = DEFAULT_LEASE_S,
) -> Claim:
    """Extend a lease this consumer holds. Raises
    :class:`ClaimDeniedError` if the claim is absent, held by another
    consumer, or not in state 'leased'. A lease that already lapsed
    cannot be revived — its row persists as 'lapsed' (open to any
    claimant, deliveries kept) and renew is denied, the same as for a
    claim this consumer never held."""
    db.sweep(conn)
    consumers.touch(conn, consumer)
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
    consumers.touch(conn, consumer)
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
    """Voluntarily give a leased message back: flips the claim row to
    'lapsed' with ``deliveries - 1`` (floor 0), so the message is
    immediately re-claimable and the release itself never counts toward
    dead-lettering — voluntary release is cooperative, not a failure
    signal. Same denial rules as :func:`renew`.

    WHY decrement, not reset (QA store #7): ``deliveries=0`` also erased
    every EARLIER involuntary lapse, so a poison message never reached
    ``max_deliveries`` as long as someone released it in between. The
    release undoes exactly the one delivery its own claim added:
    claim → release → reclaim still yields deliveries=1.

    WHY flip-not-delete (raven2-p2 refute-frontier finding): deleting
    the row turned the message back into a NEVER-CLAIMED candidate at
    an old id — below every process's claim frontier, hence permanently
    invisible to frontier-bounded fresh scans (and no in-process cache
    repair can help OTHER processes). As 'lapsed' it travels the lapsed
    scan, which ignores the frontier by design, in every process."""
    db.sweep(conn)
    consumers.touch(conn, consumer)
    cursor = conn.execute(
        f"""
        UPDATE claims
        SET state = 'lapsed', deliveries = MAX(deliveries - 1, 0),
            updated_at = {_NOW_SQL}
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