"""v1 compatibility shim (ADR-004).  LANE: compat (raven2-p1).

Keeps the v1 public surface importable for one release so the four
worked examples and their integration tests survive as the rewrite's
regression net. Deprecated from day one; removed after one minor
release.

Mapping (ADR-002/004):

- v1 ``BusClient(session_id, role)``  → consumer ``<role>@<session_id>``
  (v1 "session" IS the v2 "run"; validate with the v2 grammar — v1
  allowed uppercase, v2 does not: document the tightening in the
  docstring, raise ``ValueError`` like v1 did for bad input).
- v1 address ``"<role>:<session>"``   → broadcast channel
  ``compat/<session>/<role>`` (one channel per v1 recipient identity —
  a 2-consumer DM equivalent).
- v1 ``send``                          → ``log.append`` on the recipient's
  compat channel.
- v1 ``inbox``                         → ``cursors.pending`` (status
  'unread').
- v1 ``ack``                           → ``cursors.ack(up_to_id=id)`` —
  NOTE the semantic narrowing: v1 acked single messages; cursor-jump
  acks everything up to id. Acceptable for the examples (they ack in
  order); documented loudly here.
- v1 ``subscribe``                     → poll ``claims``-free loop over
  ``pending`` + per-message ``cursors.ack`` — at-most-once is
  approximated by ack-before-yield, matching v1's documented crash
  semantics.
- v1 ``SchemaRegistry``                → permissive no-op stub
  (register/unregister/strict_mode accepted; validate returns body
  unchanged). v2 core has no schema registry (kept out of P1 scope).

Tightening vs v1 (all four examples are lowercase, so they keep running):
- role AND session_id are normalised with ``.lower()`` before mapping to
  the v2 grammar, which is lowercase-only (ADR-002). v1 was
  case-sensitive; this shim folds case so the v1 address forms survive.
- ``task_id`` has no v2 column: it is carried INSIDE ``body`` under
  ``"__task_id__"`` on send and stripped back out on read.
- ``ack`` is cursor-jump (see above), not single-message.
"""

from __future__ import annotations

import asyncio
import sqlite3  # noqa: F401  (siblings take a sqlite3.Connection)
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# v2-native imports only (ADR-004: NEVER import claude_bus from here).
# models is frozen + implemented → safe to use its validators directly.
# log/cursors/db/channels may be stubs in a parallel worktree; we call
# them only through their documented signatures.
from raven_bus import channels, cursors, db, log
from raven_bus import models as v2models
from raven_bus.models import (
    format_consumer_id,
    parse_consumer_id,
    validate_channel_name,
)

# Body key used to smuggle v1 ``task_id`` through the v2 store, which has
# no dedicated column. Stripped on read so the caller sees a clean body.
_TASK_ID_KEY = "__task_id__"

# v1 read-state is reduced to two states; v2 has none on the message row
# (read-state lives in cursors — ADR-001). These are the v1 public names.
_UNREAD = "unread"
_READ = "read"

# Broadcast channel prefix; one channel per v1 recipient identity.
_CHANNEL_PREFIX = "compat"


def _compat_channel(session_id: str, role: str) -> str:
    """The broadcast channel for a v1 recipient identity.

    ``compat/<session>/<role>`` — both atoms validated by the v2 grammar
    (ADR-002). Order is session-then-role so all roles in a session share
    a prefix, mirroring v1's "session groups its roles" mental model.
    """
    return validate_channel_name(f"{_CHANNEL_PREFIX}/{session_id}/{role}")


def _consumer_id(role: str, session_id: str) -> str:
    """v2 consumer id ``<role>@<run>`` for a v1 (role, session) pair."""
    return format_consumer_id(role, session_id)


def _parse_v1_address(addr: str) -> tuple[str, str]:
    """Split a v1 ``"<role>:<session>"`` address, raising ValueError.

    Mirrors v1's :func:`claude_bus.client._parse_address`: a missing
    separator or an empty part is a ``ValueError``. The returned atoms
    are NOT lowercased here — the caller normalises, so a single place
    owns the case-folding tightening.
    """
    if not isinstance(addr, str) or ":" not in addr:
        raise ValueError(
            f"address {addr!r} must be of the form '<role>:<session_id>'"
        )
    role, session_id = addr.split(":", 1)
    if not role or not session_id:
        raise ValueError(f"address {addr!r} has empty role or session_id")
    return role, session_id


def _format_v1_address(role: str, session_id: str) -> str:
    return f"{role}:{session_id}"


def _strip_task_id(body: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    """Pull a smuggled ``__task_id__`` back out of a body read from the
    store. Returns ``(body_without_task_id, task_id_or_None)`` without
    mutating the caller's dict."""
    if not isinstance(body, dict) or _TASK_ID_KEY not in body:
        return body, None
    cleaned = {k: v for k, v in body.items() if k != _TASK_ID_KEY}
    return cleaned, body[_TASK_ID_KEY]


class Message(BaseModel):
    """v1-shaped public message (see claude_bus.client.Message)."""

    model_config = ConfigDict(extra="ignore")

    id: int
    session_id: str
    sender: str                      # '<role>:<session>' v1 form
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


def _to_public(
    v2: v2models.Message,
    *,
    own_consumer: str,
    own_role: str,
    own_session: str,
    status: str,
    read_at: datetime | None = None,
) -> Message:
    """Reconstruct a v1-shaped Message from a v2 log row.

    The compat channel encodes the *recipient* identity
    (``compat/<session>/<role>``); the sender is a v2 consumer id
    (``<role>@<run>``). Both are parsed back into v1's
    ``"<role>:<session>"`` forms. ``task_id`` is un-smuggled from body.
    """
    # Recipient identity comes from the channel name: compat/<session>/<role>.
    segments = v2.channel.split("/")
    # Defensive: a message not on a compat/<sess>/<role> channel has no
    # recoverable recipient identity — fall back to the owner's identity.
    if len(segments) == 3 and segments[0] == _CHANNEL_PREFIX:
        recipient_session, recipient_role = segments[1], segments[2]
    else:  # pragma: no cover - only compat channels are ever read here
        recipient_role, recipient_session = own_role, own_session
    recipient = _format_v1_address(recipient_role, recipient_session)

    # Sender is a v2 consumer id '<role>@<run>'; fall back to the raw
    # string if it isn't one (e.g. a foreign producer) so we never drop
    # information.
    try:
        sender_role, sender_session = parse_consumer_id(v2.sender)
        sender = _format_v1_address(sender_role, sender_session)
    except ValueError:
        sender = v2.sender

    body, task_id = _strip_task_id(v2.body)

    return Message(
        id=v2.id,
        # v1 exposed the sender's session; the v2 store has no per-row
        # session, so use the sender's run (== v1 session) derived above.
        session_id=sender_session,
        sender=sender,
        recipient=recipient,
        recipient_role=recipient_role,
        recipient_session=recipient_session,
        type=v2.type,
        body=body,
        status=status,
        tags=list(v2.tags),
        correlation_id=v2.thread_id,
        reply_to=v2.reply_to,
        task_id=task_id,
        created_at=v2.created_at,
        read_at=read_at,
    )


class SchemaRegistry:
    """Permissive no-op stand-in for v1's SchemaRegistry.

    v2 core ships no schema registry (out of P1 scope). The examples
    register Pydantic models against it but rely on permissive
    behaviour, so every method is a no-op and :meth:`validate` returns
    the body unchanged — keeping the calls sites working without
    enforcing anything.
    """

    @classmethod
    def register(cls, type_name: str, model: type) -> None:
        """No-op: store nothing, enforce nothing."""
        return

    @classmethod
    def unregister(cls, type_name: str) -> None:
        """No-op."""
        return

    @classmethod
    def strict_mode(cls, enabled: bool) -> None:
        """No-op: strict mode is not enforced by the shim."""
        return

    @classmethod
    def validate(cls, type_name: str, body: dict[str, Any]) -> dict[str, Any]:
        """Permissive: return ``body`` unchanged regardless of registration."""
        return body


class BusClient:
    """v1-compatible client over the v2 store. See module docstring for
    the mapping; keep v1's constructor validation errors (``ValueError``
    on empty/':'-bearing role, empty session)."""

    def __init__(
        self,
        session_id: str,
        role: str,
        db_path: str | Path | None = None,
    ) -> None:
        # v1 parity: type + emptiness checks first, so callers get the
        # familiar ValueError before any v2 grammar validation runs.
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError(
                f"session_id must be a non-empty string, got {session_id!r}"
            )
        if not isinstance(role, str) or not role.strip():
            raise ValueError(f"role must be a non-empty string, got {role!r}")
        if ":" in role:
            raise ValueError(
                f"role {role!r} must not contain ':' (it's the address separator)"
            )

        # TIGHTENING vs v1: v2's grammar is lowercase-only (ADR-002), so
        # fold case BEFORE mapping. All four v1 examples use lowercase
        # identities, so this keeps them running unchanged.
        role = role.lower()
        session_id = session_id.lower()

        # ADR-002 grammar check via the frozen models validators. This
        # raises InvalidAddressError — a ValueError subclass — so the v1
        # "raises ValueError on bad input" contract holds while still
        # surfacing the real reason (bad atom).
        self._consumer = _consumer_id(role, session_id)
        self._channel = _compat_channel(session_id, role)

        self.role = role
        self.session_id = session_id
        self.db_path = Path(db_path) if db_path is not None else None

        # Ensure the store + own channel exist. init_db is idempotent
        # (process-cached); ensure_channel is get-or-create broadcast.
        db.init_db(self.db_path)
        with db.connection(self.db_path) as conn:
            channels.ensure_channel(conn, self._channel, kind="broadcast")

    # ----- properties -------------------------------------------------

    @property
    def address(self) -> str:
        """v1 canonical ``"<role>:<session_id>"``."""
        return _format_v1_address(self.role, self.session_id)

    @property
    def consumer_id(self) -> str:
        """v2 consumer id this client acts as (``<role>@<session>``)."""
        return self._consumer

    @property
    def channel(self) -> str:
        """v2 broadcast channel backing this client's inbox."""
        return self._channel

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
        """Send to a v1 ``"<role>:<session>"`` address via the v2 log.

        urgency passes through verbatim (v1 used blocking/prompt/fyi,
        same literals as v2 — a bad value is rejected by ``log.append``'s
        Urgency validation). correlation_id→thread_id, reply_to→reply_to.
        task_id is smuggled in body (no v2 column).
        """
        recipient_role, recipient_session = _parse_v1_address(to)
        # Same case-folding tightening as the constructor.
        recipient_role = recipient_role.lower()
        recipient_session = recipient_session.lower()
        recipient_channel = _compat_channel(recipient_session, recipient_role)

        # Smuggle task_id into the body — v2 has no task_id column.
        if task_id is not None:
            body = {**body, _TASK_ID_KEY: task_id}

        with db.connection(self.db_path) as conn:
            appended = log.append(
                conn,
                channel=recipient_channel,
                sender=self._consumer,
                type=type,
                body=body,
                urgency=urgency,  # type: ignore[arg-type]
                tags=tags,
                reply_to=reply_to,
                thread_id=correlation_id,
                expires_in_s=expires_in_s,
            )
        # send() returns the just-sent message as 'unread' (v1 behaviour).
        return _to_public(
            appended,
            own_consumer=self._consumer,
            own_role=self.role,
            own_session=self.session_id,
            status=_UNREAD,
        )

    # ----- read / inbox / ack -----------------------------------------

    def inbox(
        self,
        role: str | None = None,
        max: int = 100,
    ) -> list[Message]:
        """Unseen messages on this client's compat channel (``pending``).

        ``role`` is accepted for v1 signature parity; only this client's
        own role is supported (matching v1's behaviour).
        """
        if role is not None and role != self.address and role != self.role:
            raise ValueError(
                f"BusClient.inbox(role=...) currently only supports this client's "
                f"own role; got {role!r}, expected {self.address!r} or {self.role!r}"
            )
        with db.connection(self.db_path) as conn:
            pending = cursors.pending(
                conn, self._consumer, self._channel, limit=max
            )
        return [
            _to_public(
                m,
                own_consumer=self._consumer,
                own_role=self.role,
                own_session=self.session_id,
                # pending() returns id > cursor's last_ack_id by contract,
                # i.e. un-acked → unread.
                status=_UNREAD,
            )
            for m in pending
        ]

    def read(self, message_id: int) -> Message:
        """One message by id; status derived from this client's cursor.

        id <= own cursor's last_ack_id → 'read', else 'unread' (ADR-001:
        read-state is cursor-derived, never stored on the row).
        """
        with db.connection(self.db_path) as conn:
            v2 = log.read_by_id(conn, message_id)
            cursor = cursors.get_cursor(conn, self._consumer, self._channel)
        last_ack = cursor.last_ack_id if cursor is not None else 0
        status = _READ if message_id <= last_ack else _UNREAD
        return _to_public(
            v2,
            own_consumer=self._consumer,
            own_role=self.role,
            own_session=self.session_id,
            status=status,
            read_at=cursor.updated_at if status == _READ and cursor else None,
        )

    def ack(self, message_id: int) -> None:
        """Ack by advancing this client's cursor to ``message_id``.

        NARROWING vs v1: v1 acked a single message; the v2 cursor is
        monotonic, so this acks everything up to ``message_id``. Fine for
        the examples, which ack in ascending id order.
        """
        with db.connection(self.db_path) as conn:
            # v1 raised UnknownMessageError on a missing id; preserve that
            # by probing the log before advancing the cursor. read_by_id
            # raises on absence — no need to re-wrap.
            log.read_by_id(conn, message_id)
            cursors.ack(
                conn, self._consumer, self._channel, up_to_id=message_id
            )

    # ----- subscribe (async polling iterator) -------------------------

    async def subscribe(
        self,
        role: str | None = None,
        poll_interval_s: float = 1.0,
        max_per_poll: int = 50,
    ) -> AsyncIterator[Message]:
        """Yield each new message exactly once, at-most-once.

        Acks each message *before* yielding it (ack-before-yield): if the
        consumer crashes mid-handle the message is gone, matching v1's
        documented crash semantics. Polls every ``poll_interval_s``;
        cancellation propagates as :class:`asyncio.CancelledError`.
        """
        if role is not None and role != self.address and role != self.role:
            raise ValueError(
                f"BusClient.subscribe(role=...) currently only supports this "
                f"client's own role; got {role!r}"
            )
        # No try/except around the loop: asyncio.CancelledError raised at
        # the await point propagates straight out — clean cancellation,
        # matching v1's contract. Wrapping it just to re-raise adds nothing.
        while True:
            msgs = self.inbox(max=max_per_poll)
            for msg in msgs:
                # Ack-before-yield: at-most-once, even across a crash that
                # strikes between this ack and the consumer's handler.
                self.ack(msg.id)
                yield msg.model_copy(update={"status": _READ})
            # Empty (or partial) poll: back off to the runtime.
            if len(msgs) < max_per_poll:
                await asyncio.sleep(poll_interval_s)


__all__ = ["BusClient", "Message", "SchemaRegistry"]
