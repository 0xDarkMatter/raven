"""Low-level send / read / query primitives for the claude-bus store.

This module is the functional core. Callers typically work against a
connection obtained from :func:`claude_bus.db.connection`. The
ergonomic public API lives in :class:`claude_bus.client.BusClient`,
which wraps these primitives with role-addressable semantics.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field

from claude_bus import aliases as alias_mod
from claude_bus.exceptions import (
    InvalidTagError,
    SchemaValidationError,
    UnknownMessageError,
    UnknownRoleError,
)
from claude_bus.schemas import SchemaRegistry

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

_TAG_RE = re.compile(r"^[a-zA-Z0-9._-]{1,32}$")

_URGENCY_RANK: dict[str, int] = {"blocking": 0, "prompt": 1, "fyi": 2}

_ROLE_WILDCARD = ":*"
_ALL_WILDCARD = "all:*"


class Message(BaseModel):
    """Row-shaped message returned by reads."""

    model_config = ConfigDict(frozen=False, extra="ignore")

    id: int
    session_id: str
    sender: str
    recipient: str
    type: str
    urgency: str
    body: dict[str, Any]
    tags: list[str] = Field(default_factory=list)
    in_reply_to: int | None = None
    conversation_id: int | None = None
    ref_id: int | None = None
    task_id: str | None = None
    status: str
    expires_at: datetime | None = None
    created_at: datetime
    delivered_at: datetime | None = None
    resolved_at: datetime | None = None


class SendResult(BaseModel):
    """Result of a send; includes broadcast ids for fan-out."""

    model_config = ConfigDict(frozen=True)

    id: int
    """The first inserted row's id. For single-recipient sends, the only id."""

    message_ids: list[int]
    """All inserted row ids. Length == 1 for direct sends, >=1 for broadcast."""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _iso(ts: datetime) -> str:
    """ISO-8601 with milliseconds + Z suffix, matching SQLite strftime output."""
    utc = ts.astimezone(UTC)
    ms = f"{utc.microsecond // 1000:03d}"
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms}Z"


def _parse_sqlite_ts(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _validate_tags(tags: list[str] | None) -> list[str]:
    if not tags:
        return []
    cleaned: list[str] = []
    for tag in tags:
        if not _TAG_RE.match(tag):
            raise InvalidTagError(
                f"tag {tag!r} does not match ^[a-zA-Z0-9._-]{{1,32}}$"
            )
        cleaned.append(tag)
    return cleaned


def _serialise_tags(tags: list[str]) -> str:
    return ",".join(tags)


def _parse_tags(raw: str) -> list[str]:
    if not raw:
        return []
    return [t for t in raw.split(",") if t]


def _require_urgency(urgency: str) -> None:
    if urgency not in _URGENCY_RANK:
        raise SchemaValidationError(
            f"urgency {urgency!r} not one of {sorted(_URGENCY_RANK)}"
        )


def _lookup_sender_session(
    conn: sqlite3.Connection, sender: str
) -> str:
    row = conn.execute(
        "SELECT session_id FROM aliases WHERE alias = ?",
        (sender,),
    ).fetchone()
    if row is None:
        raise UnknownRoleError(
            f"sender alias {sender!r} is not registered"
        )
    return cast(str, row["session_id"])


def _expand_recipients(
    conn: sqlite3.Connection,
    recipient: str,
    sender_session_id: str,
) -> list[str]:
    """Return the concrete alias list a ``recipient`` string targets.

    - ``alias``            → [alias] if present; raise UnknownRoleError.
    - ``role:*``           → list_by_role(role, sender_session).
    - ``all:*``            → list_by_session(sender_session).
    """
    if recipient == _ALL_WILDCARD:
        rows = alias_mod.list_by_session(conn, sender_session_id)
        if not rows:
            raise UnknownRoleError(
                f"no aliases registered in session {sender_session_id!r}"
            )
        return [r.alias for r in rows]

    if recipient.endswith(_ROLE_WILDCARD) and not recipient.startswith("all:"):
        role = recipient[: -len(_ROLE_WILDCARD)]
        rows = alias_mod.list_by_role(conn, role, sender_session_id)
        if not rows:
            raise UnknownRoleError(
                f"no aliases for role {role!r} in session {sender_session_id!r}"
            )
        return [r.alias for r in rows]

    # Direct alias — must exist in this session.
    row = conn.execute(
        "SELECT alias FROM aliases WHERE alias = ? AND session_id = ?",
        (recipient, sender_session_id),
    ).fetchone()
    if row is None:
        raise UnknownRoleError(
            f"recipient alias {recipient!r} is not registered in session "
            f"{sender_session_id!r}"
        )
    return [recipient]


def _row_to_message(row: sqlite3.Row) -> Message:
    raw_body = row["body"]
    body: dict[str, Any]
    try:
        body = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        body = {"_raw": raw_body}

    created = _parse_sqlite_ts(row["created_at"])
    if created is None:
        # Messages always carry a default; treat as defensive programming.
        created = _utc_now()

    return Message(
        id=row["id"],
        session_id=row["session_id"],
        sender=row["sender"],
        recipient=row["recipient"],
        type=row["type"],
        urgency=row["urgency"],
        body=body,
        tags=_parse_tags(row["tags"] or ""),
        in_reply_to=row["in_reply_to"],
        conversation_id=row["conversation_id"],
        ref_id=row["ref_id"],
        task_id=row["task_id"],
        status=row["status"],
        expires_at=_parse_sqlite_ts(row["expires_at"]),
        created_at=created,
        delivered_at=_parse_sqlite_ts(row["delivered_at"]),
        resolved_at=_parse_sqlite_ts(row["resolved_at"]),
    )


# ---------------------------------------------------------------------
# send / reply
# ---------------------------------------------------------------------


def send(
    conn: sqlite3.Connection,
    *,
    sender: str,
    recipient: str,
    type: str,
    body: dict[str, Any],
    urgency: str = "prompt",
    tags: list[str] | None = None,
    in_reply_to: int | None = None,
    conversation_id: int | None = None,
    ref_id: int | None = None,
    task_id: str | None = None,
    expires_in_s: int | None = None,
    validate: bool = True,
) -> SendResult:
    """Write one or more message rows and return the inserted ids.

    Body validation runs through :class:`SchemaRegistry`. With no
    schemas registered, validation is permissive and accepts any
    JSON-serialisable dict. ``validate=False`` skips registry
    validation entirely (useful when the caller has already validated
    upstream).
    """
    _require_urgency(urgency)

    if validate:
        body = SchemaRegistry.validate(type, body)

    clean_tags = _validate_tags(tags)

    session_id = _lookup_sender_session(conn, sender)

    # Conversation id resolution: inherit from parent if unset + in_reply_to.
    if conversation_id is None and in_reply_to is not None:
        parent = conn.execute(
            "SELECT conversation_id FROM messages WHERE id = ?",
            (in_reply_to,),
        ).fetchone()
        if parent is not None:
            # If the parent itself had no conversation_id, treat the parent
            # itself as the root of the thread.
            conversation_id = parent["conversation_id"] or in_reply_to

    expires_at: str | None = None
    if expires_in_s is not None:
        expires_at = _iso(_utc_now() + timedelta(seconds=expires_in_s))

    recipients = _expand_recipients(conn, recipient, session_id)

    body_json = json.dumps(body, ensure_ascii=False, sort_keys=True)
    tags_csv = _serialise_tags(clean_tags)

    inserted: list[int] = []
    for concrete in recipients:
        cursor = conn.execute(
            """
            INSERT INTO messages(
                session_id, sender, recipient, type, urgency, body, tags,
                in_reply_to, conversation_id, ref_id, task_id, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                sender,
                concrete,
                type,
                urgency,
                body_json,
                tags_csv,
                in_reply_to,
                conversation_id,
                ref_id,
                task_id,
                expires_at,
            ),
        )
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("sqlite lastrowid was None after INSERT")
        inserted.append(int(row_id))

    conn.commit()
    return SendResult(id=inserted[0], message_ids=inserted)


def reply(
    conn: sqlite3.Connection,
    to_id: int,
    *,
    sender: str,
    type: str,
    body: dict[str, Any],
    urgency: str = "prompt",
    tags: list[str] | None = None,
    resolve_parent: bool = True,
    validate: bool = True,
) -> SendResult:
    """Reply to message ``to_id``.

    Looks up the parent's ``sender`` (the reply's recipient) and
    ``conversation_id``. If ``resolve_parent`` is True, the parent row's
    status is transitioned to ``resolved`` in the same transaction as
    the reply insert.
    """
    parent = conn.execute(
        "SELECT sender, conversation_id, status FROM messages WHERE id = ?",
        (to_id,),
    ).fetchone()
    if parent is None:
        raise UnknownRoleError(f"parent message id={to_id} does not exist")

    reply_recipient = cast(str, parent["sender"])
    conversation_id = parent["conversation_id"] or to_id

    result = send(
        conn,
        sender=sender,
        recipient=reply_recipient,
        type=type,
        body=body,
        urgency=urgency,
        tags=tags,
        in_reply_to=to_id,
        conversation_id=conversation_id,
        validate=validate,
    )

    if resolve_parent and parent["status"] not in {"resolved", "expired"}:
        now = _iso(_utc_now())
        conn.execute(
            "UPDATE messages SET status = 'resolved', resolved_at = ? WHERE id = ?",
            (now, to_id),
        )
        conn.commit()

    return result


# ---------------------------------------------------------------------
# Read / query
# ---------------------------------------------------------------------


_INBOX_ORDER = """
    CASE urgency
        WHEN 'blocking' THEN 0
        WHEN 'prompt'   THEN 1
        WHEN 'fyi'      THEN 2
        ELSE 3
    END ASC,
    created_at ASC,
    id ASC
"""


def read_next(
    conn: sqlite3.Connection,
    recipient: str,
    *,
    mark_delivered: bool = True,
    urgency_min: str | None = None,
    statuses: tuple[str, ...] = ("sent", "delivered"),
) -> Message | None:
    """Return the next unread message for ``recipient``, or ``None``.

    ``statuses`` controls which lifecycle states qualify as "unread".
    The default (``sent`` + ``delivered``) matches the design-doc
    semantics — deliver-and-resolve flow. Callers that want
    consume-on-read (P2's BusClient shim) pass ``("sent",)`` so an
    already-delivered row is skipped.

    If ``mark_delivered`` is True and the row's status is ``sent``,
    transition it to ``delivered`` and set ``delivered_at``.
    ``urgency_min`` filters to messages at or above that urgency.
    """
    query, params = _inbox_query(
        recipient, limit=1, urgency_min=urgency_min, statuses=statuses
    )
    row = conn.execute(query, params).fetchone()
    if row is None:
        return None

    message = _row_to_message(row)
    if mark_delivered and message.status == "sent":
        now = _iso(_utc_now())
        conn.execute(
            "UPDATE messages SET status = 'delivered', delivered_at = ? "
            "WHERE id = ? AND status = 'sent'",
            (now, message.id),
        )
        conn.commit()
        # Re-read status so the returned object reflects the transition.
        message = message.model_copy(
            update={
                "status": "delivered",
                "delivered_at": _parse_sqlite_ts(now),
            }
        )
    return message


def list_unread(
    conn: sqlite3.Connection,
    recipient: str,
    *,
    limit: int = 50,
    mark_delivered: bool = False,
    urgency: str | None = None,
    statuses: tuple[str, ...] = ("sent", "delivered"),
) -> list[Message]:
    """Return up to ``limit`` unread messages for ``recipient``.

    ``urgency`` (exact match) narrows the bucket if supplied. Ordering
    matches :func:`read_next`'s inbox sort. ``statuses`` has the same
    meaning as in :func:`read_next`.
    """
    query, params = _inbox_query(
        recipient,
        limit=limit,
        urgency_exact=urgency,
        statuses=statuses,
    )
    rows = conn.execute(query, params).fetchall()
    messages = [_row_to_message(r) for r in rows]

    if mark_delivered and messages:
        ids = [m.id for m in messages if m.status == "sent"]
        if ids:
            now = _iso(_utc_now())
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE messages SET status = 'delivered', delivered_at = ? "
                f"WHERE id IN ({placeholders}) AND status = 'sent'",
                (now, *ids),
            )
            conn.commit()
            messages = [
                m.model_copy(
                    update={
                        "status": "delivered",
                        "delivered_at": _parse_sqlite_ts(now),
                    }
                )
                if m.status == "sent"
                else m
                for m in messages
            ]
    return messages


def unread_count(
    conn: sqlite3.Connection,
    recipient: str,
    *,
    urgency: str | None = None,
) -> int:
    """Return the number of unread messages addressed to ``recipient``.

    "Unread" = status in (``sent``, ``delivered``). An explicit
    ``urgency`` (exact match) narrows the count.
    """
    params: list[Any] = [recipient]
    sql = (
        "SELECT COUNT(*) FROM messages "
        "WHERE recipient = ? AND status IN ('sent', 'delivered')"
    )
    if urgency is not None:
        sql += " AND urgency = ?"
        params.append(urgency)
    row = conn.execute(sql, tuple(params)).fetchone()
    return int(row[0])


def _inbox_query(
    recipient: str,
    *,
    limit: int,
    urgency_min: str | None = None,
    urgency_exact: str | None = None,
    statuses: tuple[str, ...] = ("sent", "delivered"),
) -> tuple[str, tuple[Any, ...]]:
    if not statuses:
        raise SchemaValidationError("statuses must be a non-empty tuple")
    placeholders = ",".join("?" for _ in statuses)
    params: list[Any] = [recipient, *statuses]
    where = f"recipient = ? AND status IN ({placeholders})"
    if urgency_exact is not None:
        where += " AND urgency = ?"
        params.append(urgency_exact)
    elif urgency_min is not None:
        rank = _URGENCY_RANK.get(urgency_min)
        if rank is None:
            raise SchemaValidationError(
                f"urgency {urgency_min!r} is not one of {sorted(_URGENCY_RANK)}"
            )
        # SQLite doesn't have native enum rank; re-create inline.
        where += (
            " AND CASE urgency WHEN 'blocking' THEN 0 WHEN 'prompt' THEN 1 "
            "WHEN 'fyi' THEN 2 ELSE 3 END <= ?"
        )
        params.append(rank)

    sql = (
        "SELECT id, session_id, sender, recipient, type, urgency, body, tags, "
        "in_reply_to, conversation_id, ref_id, task_id, status, expires_at, "
        "created_at, delivered_at, resolved_at "
        f"FROM messages WHERE {where} ORDER BY {_INBOX_ORDER} LIMIT ?"
    )
    params.append(limit)
    return sql, tuple(params)


def list_conversation(
    conn: sqlite3.Connection, conversation_id: int
) -> list[Message]:
    """Return every message in a thread (thread root + replies), oldest first."""
    rows = conn.execute(
        "SELECT id, session_id, sender, recipient, type, urgency, body, tags, "
        "in_reply_to, conversation_id, ref_id, task_id, status, expires_at, "
        "created_at, delivered_at, resolved_at "
        "FROM messages "
        "WHERE id = ? OR conversation_id = ? "
        "ORDER BY created_at ASC, id ASC",
        (conversation_id, conversation_id),
    ).fetchall()
    return [_row_to_message(r) for r in rows]


def list_by_task(
    conn: sqlite3.Connection, task_id: str
) -> list[Message]:
    """Return every message bearing ``task_id``, oldest first."""
    rows = conn.execute(
        "SELECT id, session_id, sender, recipient, type, urgency, body, tags, "
        "in_reply_to, conversation_id, ref_id, task_id, status, expires_at, "
        "created_at, delivered_at, resolved_at "
        "FROM messages WHERE task_id = ? "
        "ORDER BY created_at ASC, id ASC",
        (task_id,),
    ).fetchall()
    return [_row_to_message(r) for r in rows]


def resolve(conn: sqlite3.Connection, message_id: int) -> None:
    """Manually mark ``message_id`` as resolved.

    Idempotent: re-resolving a resolved row is a no-op. Does not
    touch the ``expired`` status (an expired message stays expired).
    """
    now = _iso(_utc_now())
    conn.execute(
        "UPDATE messages SET status = 'resolved', resolved_at = ? "
        "WHERE id = ? AND status NOT IN ('resolved', 'expired')",
        (now, message_id),
    )
    conn.commit()


def sweep_expired(conn: sqlite3.Connection) -> int:
    """Transition stale rows to ``expired`` status; return count."""
    now = _iso(_utc_now())
    cursor = conn.execute(
        "UPDATE messages SET status = 'expired' "
        "WHERE expires_at IS NOT NULL "
        "  AND expires_at < ? "
        "  AND status NOT IN ('resolved', 'expired')",
        (now,),
    )
    conn.commit()
    return cursor.rowcount if cursor.rowcount != -1 else 0


__all__ = [
    "Message",
    "SendResult",
    "list_by_task",
    "list_conversation",
    "list_unread",
    "read_next",
    "reply",
    "resolve",
    "send",
    "sweep_expired",
    "unread_count",
]
