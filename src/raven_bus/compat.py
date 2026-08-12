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
"""

from __future__ import annotations

import sqlite3  # noqa: F401  (implementation will use via raven_bus.db)
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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


class SchemaRegistry:
    """Permissive no-op stand-in for v1's SchemaRegistry."""

    @classmethod
    def register(cls, type_name: str, model: type) -> None:
        raise NotImplementedError

    @classmethod
    def unregister(cls, type_name: str) -> None:
        raise NotImplementedError

    @classmethod
    def strict_mode(cls, enabled: bool) -> None:
        raise NotImplementedError

    @classmethod
    def validate(cls, type_name: str, body: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


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
        raise NotImplementedError

    @property
    def address(self) -> str:
        """v1 canonical ``"<role>:<session_id>"``."""
        raise NotImplementedError

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
        raise NotImplementedError

    def inbox(self, role: str | None = None, max: int = 100) -> list[Message]:
        raise NotImplementedError

    def read(self, message_id: int) -> Message:
        raise NotImplementedError

    def ack(self, message_id: int) -> None:
        raise NotImplementedError

    async def subscribe(
        self,
        role: str | None = None,
        poll_interval_s: float = 1.0,
        max_per_poll: int = 50,
    ) -> AsyncIterator[Message]:
        raise NotImplementedError
        yield  # pragma: no cover -- makes this an async generator


__all__ = ["BusClient", "Message", "SchemaRegistry"]
