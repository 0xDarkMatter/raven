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
    InvalidBodyError,
    UnknownChannelError,
    UnknownMessageError,
)
from raven_bus.models import URGENCY_RANK, Message, Urgency, parse_consumer_id, validate_tags

_SELECT = "SELECT * FROM messages"
_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
_MAX_SQLITE_INT = 2**63 - 1

MAX_BODY_DEPTH = 64
"""Deepest container nesting ``append`` accepts (the body dict itself is
depth 1). WHY a cap at the single writer (QA store #12): ``json.dumps``
takes ~1000 levels, but pydantic's JSON serializer — every HTTP reader,
and the HTTP claim response — gives up at ~100 (the Message wrapper
adds one more), so a deeper body was storable yet unreadable, and a
claim leased a message it could not return. 64 leaves headroom under
that limit; raising it past ~99 re-opens the bug."""


def _check_body_depth(body: Any) -> None:
    """Raise :class:`InvalidBodyError` if any dict/list/tuple in ``body``
    sits deeper than :data:`MAX_BODY_DEPTH`. Iterative on purpose: a
    recursive walk (or json.dumps) dies with RecursionError on the very
    bodies this guards against."""
    stack: list[tuple[Any, int]] = [(body, 1)]
    while stack:
        value, depth = stack.pop()
        if isinstance(value, dict):
            children: Any = value.values()
        elif isinstance(value, list | tuple):
            children = value
        else:
            continue
        if depth > MAX_BODY_DEPTH:
            raise InvalidBodyError(
                f"message body is nested deeper than {MAX_BODY_DEPTH} levels"
            )
        stack.extend((child, depth + 1) for child in children)


def _require_message(conn: sqlite3.Connection, message_id: int, what: str) -> sqlite3.Row:
    """The referenced message's ``(id, thread_id)`` row, or
    :class:`UnknownMessageError` naming ``what`` (reply_to / thread_id).
    Ids outside SQLite's positive INTEGER range cannot exist; checking
    that in Python keeps them from raising OverflowError on bind."""
    row = None
    if 0 < message_id <= _MAX_SQLITE_INT:
        row = conn.execute(
            "SELECT id, thread_id FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
    if row is None:
        raise UnknownMessageError(f"{what} {message_id!r}: message does not exist")
    return row


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
    - ``reply_to`` / explicit ``thread_id`` must name existing messages
      (:class:`UnknownMessageError`; previously a raw FK IntegrityError —
      QA store #4). Cross-channel references are allowed.
    - body is JSON-serialised ``sort_keys=True, ensure_ascii=False``;
      nesting deeper than :data:`MAX_BODY_DEPTH` raises
      :class:`InvalidBodyError` before anything is written.
    """
    parse_consumer_id(sender)  # early ADR-002 validation
    if urgency not in URGENCY_RANK:
        raise InvalidAddressError(
            f"urgency {urgency!r} must be one of {sorted(URGENCY_RANK)}"
        )
    clean_tags = validate_tags(tags)
    _check_body_depth(body)

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

    # Checked under touch()'s write lock, so a referenced message can't be
    # torn down between this check and the INSERT's FK check.
    resolved_thread_id = thread_id
    if thread_id is not None:
        _require_message(conn, thread_id, "thread_id")
    if reply_to is not None:
        parent = _require_message(conn, reply_to, "reply_to")
        if thread_id is None:
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


__all__ = ["MAX_BODY_DEPTH", "append", "read_after", "read_by_id", "read_thread"]
