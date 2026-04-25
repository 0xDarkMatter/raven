"""Public BusClient — role-addressable wrapper over the SQLite store.

A :class:`BusClient` is identified by ``(session_id, role)``. It sends
messages to other identities addressed as ``"<role>:<session_id>"``,
reads its own inbox, acknowledges, and (optionally) subscribes to live
incoming traffic via an async iterator.

The wire status surface is reduced to two states for the public API:

- ``"unread"`` — message is waiting in the recipient's inbox
- ``"read"`` — message has been acknowledged

Internally the store uses a four-state lifecycle (sent → delivered →
resolved + expired); the mapping is done here so consumers see a
simple two-state model.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

from claude_bus import _core, aliases as alias_mod
from claude_bus.db import connection, init_db
from claude_bus.exceptions import UnknownMessageError

log = structlog.get_logger(__name__)

# Map internal Raven-flavoured states to the public two-state surface.
_PUBLIC_STATUS = {
    "sent": "unread",
    "delivered": "unread",
    "resolved": "read",
    "expired": "read",
}


class Message(BaseModel):
    """Public message shape returned by reads.

    Addresses are exposed both as the canonical ``"<role>:<session>"``
    form (``recipient`` / ``sender``) and split out (``recipient_role``,
    ``recipient_session``) for convenience.

    Note: ``id`` is an integer in v0.1.x — this is a pragmatic
    deviation from the original spec's UUID intention. Integers play
    nicer with shells and humans (``claude-bus read 42``).
    """

    model_config = ConfigDict(extra="ignore")

    id: int
    session_id: str
    sender: str
    recipient: str
    recipient_role: str
    recipient_session: str
    type: str
    body: dict[str, Any]
    status: str = Field(description="'unread' | 'read'")
    tags: list[str] = Field(default_factory=list)
    correlation_id: int | None = None
    reply_to: int | None = None
    task_id: str | None = None
    created_at: datetime
    read_at: datetime | None = None


def _parse_address(addr: str) -> tuple[str, str]:
    if ":" not in addr:
        raise ValueError(
            f"address {addr!r} must be of the form '<role>:<session_id>'"
        )
    role, session_id = addr.split(":", 1)
    if not role or not session_id:
        raise ValueError(
            f"address {addr!r} has empty role or session_id"
        )
    return role, session_id


def _format_address(role: str, session_id: str) -> str:
    return f"{role}:{session_id}"


def _to_public(internal: _core.Message, conn: sqlite3.Connection) -> Message:
    """Convert internal Raven Message → public Message.

    Looks up the sender + recipient roles by their aliases so the
    public message can carry split (role, session_id) pairs as well as
    the canonical "<role>:<session>" form.
    """
    sender_alias = alias_mod.resolve(conn, internal.sender)
    recipient_alias = alias_mod.resolve(conn, internal.recipient)

    if sender_alias is not None:
        sender_str = _format_address(sender_alias.role, sender_alias.session_id)
    else:
        sender_str = internal.sender

    if recipient_alias is not None:
        recipient_role = recipient_alias.role
        recipient_session = recipient_alias.session_id
        recipient_str = _format_address(recipient_role, recipient_session)
    else:
        recipient_role = ""
        recipient_session = ""
        recipient_str = internal.recipient

    public_status = _PUBLIC_STATUS.get(internal.status, internal.status)

    return Message(
        id=internal.id,
        session_id=internal.session_id,
        sender=sender_str,
        recipient=recipient_str,
        recipient_role=recipient_role,
        recipient_session=recipient_session,
        type=internal.type,
        body=internal.body,
        status=public_status,
        tags=internal.tags,
        correlation_id=internal.conversation_id,
        reply_to=internal.in_reply_to,
        task_id=internal.task_id,
        created_at=internal.created_at,
        read_at=internal.delivered_at or internal.resolved_at,
    )


class BusClient:
    """Identity-bound client for a single (session, role) pair."""

    def __init__(
        self,
        session_id: str,
        role: str,
        db_path: str | Path | None = None,
    ) -> None:
        self.session_id = session_id
        self.role = role
        self.db_path = Path(db_path) if db_path is not None else None
        # Ensure the schema exists. Idempotent.
        init_db(self.db_path)
        # Register our own identity so we can be addressed as a recipient.
        with connection(self.db_path) as conn:
            self._alias = alias_mod.register(conn, role, session_id)

    # ----- properties -------------------------------------------------

    @property
    def address(self) -> str:
        """Canonical ``"<role>:<session_id>"`` form for this client."""
        return _format_address(self.role, self.session_id)

    @property
    def alias(self) -> str:
        """Internal alias (``<role>-<6hex>``) used as a row identifier."""
        return self._alias

    # ----- send -------------------------------------------------------

    def send(
        self,
        to: str,
        type: str,
        body: dict[str, Any],
        *,
        urgency: str = "prompt",
        tags: list[str] | None = None,
        correlation_id: int | None = None,
        reply_to: int | None = None,
        task_id: str | None = None,
        expires_in_s: int | None = None,
    ) -> Message:
        """Send a message to ``to`` (an ``"<role>:<session_id>"`` address)."""
        recipient_role, recipient_session = _parse_address(to)
        with connection(self.db_path) as conn:
            # Auto-register the recipient identity. claude-bus addresses
            # are deterministic given (role, session), so a producer can
            # name a recipient that hasn't yet booted.
            recipient_alias = alias_mod.register(
                conn, recipient_role, recipient_session
            )
            result = _core.send(
                conn,
                sender=self._alias,
                recipient=recipient_alias,
                type=type,
                body=body,
                urgency=urgency,
                tags=tags,
                in_reply_to=reply_to,
                conversation_id=correlation_id,
                task_id=task_id,
                expires_in_s=expires_in_s,
            )
            row = conn.execute(
                "SELECT id, session_id, sender, recipient, type, urgency, body, "
                "tags, in_reply_to, conversation_id, ref_id, task_id, status, "
                "expires_at, created_at, delivered_at, resolved_at "
                "FROM messages WHERE id = ?",
                (result.id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(
                    f"message id={result.id} disappeared between insert and read"
                )
            internal = _core._row_to_message(row)
            return _to_public(internal, conn)

    # ----- read / inbox / ack -----------------------------------------

    def inbox(
        self,
        role: str | None = None,
        max: int = 100,
    ) -> list[Message]:
        """Return unread messages addressed to this client.

        ``role`` is reserved for a future where one client can read
        another role's inbox (e.g. supervisor view). For now it must
        match this client's own role or be omitted.
        """
        if role is not None and role != self.address and role != self.role:
            raise ValueError(
                f"BusClient.inbox(role=...) currently only supports this client's "
                f"own role; got {role!r}, expected {self.address!r} or {self.role!r}"
            )
        with connection(self.db_path) as conn:
            internals = _core.list_unread(
                conn,
                self._alias,
                limit=max,
                mark_delivered=False,
            )
            return [_to_public(m, conn) for m in internals]

    def read(self, message_id: int) -> Message:
        """Return a single message by id without changing its status."""
        with connection(self.db_path) as conn:
            internal = _core.read_by_id(conn, message_id)
            return _to_public(internal, conn)

    def ack(self, message_id: int) -> None:
        """Mark a message as read. Idempotent."""
        with connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE id = ?", (message_id,)
            ).fetchone()
            if row is None:
                raise UnknownMessageError(f"message id={message_id} does not exist")
            _core.resolve(conn, message_id)

    # ----- subscribe (async polling iterator) -------------------------

    async def subscribe(
        self,
        role: str | None = None,
        poll_interval_s: float = 1.0,
        max_per_poll: int = 50,
    ) -> AsyncIterator[Message]:
        """Yield each new unread message exactly once, at-most-once.

        Acks each yielded message *before* yielding it. If the consumer
        crashes mid-handle the message is gone — for at-least-once
        semantics, use :meth:`inbox` and ack manually after processing.

        Polls every ``poll_interval_s`` seconds. Cancellation
        propagates cleanly via :class:`asyncio.CancelledError`.
        """
        if role is not None and role != self.address and role != self.role:
            raise ValueError(
                f"BusClient.subscribe(role=...) currently only supports this "
                f"client's own role; got {role!r}"
            )
        try:
            while True:
                msgs = self.inbox(max=max_per_poll)
                for msg in msgs:
                    self.ack(msg.id)
                    # Reflect the ack in the yielded copy.
                    yielded = msg.model_copy(update={"status": "read"})
                    yield yielded
                if not msgs:
                    await asyncio.sleep(poll_interval_s)
        except asyncio.CancelledError:
            log.debug("claude_bus.subscribe.cancelled", role=self.address)
            raise


__all__ = ["BusClient", "Message"]
