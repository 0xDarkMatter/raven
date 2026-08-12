"""Append + read primitives over the message log.  LANE: log (raven2-p1).

ADR-001 invariants enforced here:

- ``append`` is the ONLY writer of messages rows; there is no update
  path, ever.
- Every read filters ``expires_at`` (an expired message must never be
  returned by :func:`read_after` / :func:`read_by_id` callers relying
  on liveness — ``include_expired=True`` exists for `tail`/forensics
  only).
"""

from __future__ import annotations

import sqlite3
from typing import Any

from raven_bus.models import Message, Urgency


def append(
    conn: sqlite3.Connection,
    *,
    channel: str,
    sender: str,
    type: str,
    body: dict[str, Any],
    urgency: Urgency = "prompt",
    tags: list[str] | None = None,
    reply_to: int | None = None,
    thread_id: int | None = None,
    expires_in_s: int | None = None,
    ensure: bool = True,
) -> Message:
    """Append one message and return it.

    - ``sender`` is validated as a consumer id and upserted into
      ``consumers`` (last_seen_at bumped).
    - ``channel`` must exist unless ``ensure`` (then created as
      ``broadcast``).
    - ``thread_id`` inherits from the ``reply_to`` parent when unset
      (parent's thread_id, else the parent id itself) — v1's proven
      conversation rule.
    - body is JSON-serialised ``sort_keys=True, ensure_ascii=False``.
    """
    raise NotImplementedError


def read_after(
    conn: sqlite3.Connection,
    channel: str,
    after_id: int,
    *,
    limit: int = 100,
    sender: str | None = None,
    include_expired: bool = False,
) -> list[Message]:
    """Messages in ``channel`` with id > after_id, id-ordered ascending.
    The tail/cursor read primitive. Filters expired unless asked."""
    raise NotImplementedError


def read_by_id(conn: sqlite3.Connection, message_id: int) -> Message:
    """Fetch one message (no liveness filter — forensic read).
    Raises :class:`UnknownMessageError`."""
    raise NotImplementedError


def read_thread(conn: sqlite3.Connection, thread_id: int) -> list[Message]:
    """Thread root + all replies, oldest first (no liveness filter)."""
    raise NotImplementedError


__all__ = ["append", "read_after", "read_by_id", "read_thread"]
