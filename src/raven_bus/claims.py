"""Queue read-state: atomic claims with leases.  LANE: claims (raven2-p1).

ADR-001: exactly-one-winner claiming via
``INSERT INTO claims ... ON CONFLICT DO NOTHING`` (rowcount 1 wins —
v1's proven atomic-UPDATE pattern relocated to an insert). Lease expiry
requeues (db.sweep deletes the stale claim); ``deliveries`` counts
attempts; at/over the channel's ``max_deliveries`` the sweep flips the
claim to ``dead`` instead. Valid ONLY on ``queue`` channels
(:class:`WrongChannelKindError` otherwise).

Every function here calls ``db.sweep`` first — a queue read must never
see (or fail to see) state a lapsed lease should have released.
"""

from __future__ import annotations

import sqlite3

from raven_bus.models import Claim, Message

DEFAULT_LEASE_S = 300


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
    raise NotImplementedError


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
    raise NotImplementedError


def complete(conn: sqlite3.Connection, message_id: int, consumer: str) -> Claim:
    """Mark this consumer's leased claim 'done' (terminal). Same denial
    rules as :func:`renew`. Idempotent for the same consumer: completing
    an already-'done' claim you own returns it unchanged."""
    raise NotImplementedError


def release(conn: sqlite3.Connection, message_id: int, consumer: str) -> None:
    """Voluntarily give a leased message back: deletes the claim row so
    the message is immediately claimable. The next claim starts fresh at
    deliveries=1 — deliberate: voluntary release is cooperative, not a
    failure signal, so it never counts toward dead-lettering. Same
    denial rules as :func:`renew`."""
    raise NotImplementedError


def get_claim(conn: sqlite3.Connection, message_id: int) -> Claim | None:
    """Current claim row for a message, or None."""
    raise NotImplementedError


__all__ = [
    "DEFAULT_LEASE_S",
    "claim_next",
    "complete",
    "get_claim",
    "release",
    "renew",
]
