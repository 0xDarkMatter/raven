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

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from raven_bus import channels, consumers
from raven_bus.exceptions import (
    InvalidAddressError,
    UnknownChannelError,
    UnknownMessageError,
)
from raven_bus.models import URGENCY_RANK, Message, Urgency, parse_consumer_id, validate_tags

_SELECT = "SELECT * FROM messages"
_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def _format_ts(dt: datetime) -> str:
    """UTC ISO-8601 with millisecond precision + 'Z', matching
    ``strftime('%Y-%m-%dT%H:%M:%fZ','now')`` (sqlite's %f is SS.SSS)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _row_to_message(row: sqlite3.Row, *, channel_name: str) -> Message:
    tags_raw = row["tags"]
    tags = [t for t in tags_raw.split(",") if t] if tags_raw else []
    return Message(
        id=row["id"],
        channel=channel_name,
        sender=row["sender"],
        type=row["type"],
        urgency=row["urgency"],
        body=json.loads(row["body"]),
        tags=tags,
        reply_to=row["reply_to"],
        thread_id=row["thread_id"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
    )


def _channel_name(conn: sqlite3.Connection, channel_id: int) -> str:
    row = conn.execute(
        "SELECT name FROM channels WHERE id = ?", (channel_id,)
    ).fetchone()
    return row["name"]


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
    - ``ensure=True`` is get-or-create WITHOUT a kind opinion: an
      existing channel of ANY kind is appended to as-is; only an absent
      one is created, as ``broadcast``. (It used to re-ensure with the
      default kind, so appending to an existing queue/stream raised
      WrongChannelKindError — QA store #2.) Callers that must enforce a
      kind call ``channels.ensure_channel(conn, name, kind)`` themselves
      and pass ``ensure=False``. ``ensure=False``: the channel must
      exist (:class:`UnknownChannelError`).
    - ``thread_id`` inherits from the ``reply_to`` parent when unset
      (parent's thread_id, else the parent id itself) — v1's proven
      conversation rule.
    - body is JSON-serialised ``sort_keys=True, ensure_ascii=False``.
    """
    parse_consumer_id(sender)  # early ADR-002 validation
    if urgency not in URGENCY_RANK:
        raise InvalidAddressError(
            f"urgency {urgency!r} must be one of {sorted(URGENCY_RANK)}"
        )
    clean_tags = validate_tags(tags)

    consumers.touch(conn, sender)

    try:
        chan = channels.get_channel(conn, channel)
    except UnknownChannelError:
        if not ensure:
            raise
        # consumers.touch above already took this transaction's write
        # lock, so no peer can create the name between the miss and here;
        # ensure_channel is race-safe on its own regardless.
        chan = channels.ensure_channel(conn, channel)

    resolved_thread_id = thread_id
    if reply_to is not None and thread_id is None:
        parent = conn.execute(
            "SELECT id, thread_id FROM messages WHERE id = ?", (reply_to,)
        ).fetchone()
        if parent is not None:
            resolved_thread_id = (
                parent["thread_id"] if parent["thread_id"] is not None else parent["id"]
            )

    expires_at = None
    if expires_in_s is not None:
        expires_at = _format_ts(
            datetime.now(UTC) + timedelta(seconds=expires_in_s)
        )

    body_json = json.dumps(body, sort_keys=True, ensure_ascii=False)
    tags_str = ",".join(clean_tags)

    cur = conn.execute(
        "INSERT INTO messages "
        "(channel_id, sender, type, urgency, body, tags, reply_to, thread_id, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            chan.id,
            sender,
            type,
            urgency,
            body_json,
            tags_str,
            reply_to,
            resolved_thread_id,
            expires_at,
        ),
    )
    row = conn.execute(f"{_SELECT} WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _row_to_message(row, channel_name=chan.name)


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
    chan = channels.get_channel(conn, channel)
    clauses = ["channel_id = ?", "id > ?"]
    params: list[Any] = [chan.id, after_id]
    if sender is not None:
        clauses.append("sender = ?")
        params.append(sender)
    if not include_expired:
        clauses.append(f"(expires_at IS NULL OR expires_at > {_NOW_SQL})")
    params.append(limit)
    rows = conn.execute(
        f"{_SELECT} WHERE {' AND '.join(clauses)} ORDER BY id ASC LIMIT ?",
        params,
    ).fetchall()
    return [_row_to_message(row, channel_name=chan.name) for row in rows]


def read_by_id(conn: sqlite3.Connection, message_id: int) -> Message:
    """Fetch one message (no liveness filter — forensic read).
    Raises :class:`UnknownMessageError`."""
    row = conn.execute(f"{_SELECT} WHERE id = ?", (message_id,)).fetchone()
    if row is None:
        raise UnknownMessageError(f"message {message_id!r} does not exist")
    return _row_to_message(row, channel_name=_channel_name(conn, row["channel_id"]))


def read_thread(conn: sqlite3.Connection, thread_id: int) -> list[Message]:
    """Thread root + all replies, oldest first (no liveness filter)."""
    rows = conn.execute(
        f"{_SELECT} WHERE id = ? OR thread_id = ? ORDER BY id ASC",
        (thread_id, thread_id),
    ).fetchall()
    names: dict[int, str] = {}
    messages = []
    for row in rows:
        cid = row["channel_id"]
        if cid not in names:
            names[cid] = _channel_name(conn, cid)
        messages.append(_row_to_message(row, channel_name=names[cid]))
    return messages


__all__ = ["append", "read_after", "read_by_id", "read_thread"]
