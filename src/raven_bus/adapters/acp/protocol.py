"""Minimal ACP client over a child's stdio.  LANE: acp-protocol (raven2-p3).

FROZEN signatures (wave-0). Speaks the Agent Client Protocol's client
side, minimal subset only (ADR-006): JSON-RPC 2.0, newline-delimited
JSON messages over the child process's stdin/stdout (one JSON object
per line — the ACP stdio transport). Supported calls:

- ``initialize``      (client → agent; advertise no fs/terminal caps)
- ``session/new``     (client → agent)
- ``session/prompt``  (client → agent; text content blocks)
- ``session/update``  (agent → client notification; streamed chunks)
- ``session/cancel``  (client → agent notification)
- agent → client REQUESTS (permission/fs/terminal) are answered with a
  JSON-RPC error (method not supported) — we granted no capabilities.

Blocking, synchronous, single-threaded by design: the harness drives
one call at a time (dumb pipe). No respawn logic here (ADR-006).
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AcpError(Exception):
    """Protocol-level failure: malformed frame, JSON-RPC error response,
    unexpected EOF (child died), or timeout."""


class PromptResult(BaseModel):
    """Outcome of one session/prompt round trip."""

    model_config = ConfigDict(frozen=True)

    stop_reason: str
    """ACP stopReason (end_turn, max_tokens, refusal, cancelled, ...)."""

    text: str
    """Concatenated agent text chunks from session/update notifications."""

    raw_updates: list[dict[str, Any]] = Field(default_factory=list)
    """Every session/update params dict, in arrival order (telemetry)."""


class AcpClient:
    """Client for ONE agent child process. Not thread-safe; call from
    one thread. The caller owns the child's lifecycle (spawn args,
    reaping) — this class only speaks the protocol over its pipes."""

    def __init__(
        self,
        child: subprocess.Popen,
        *,
        timeout_s: float = 600.0,
        on_update: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        """``child`` must have stdin/stdout as pipes (text mode handled
        internally either way — implementer's choice, document it).
        ``on_update`` fires for every session/update as it arrives
        (live telemetry hook for the harness)."""
        raise NotImplementedError

    def initialize(self) -> dict[str, Any]:
        """initialize handshake; returns the agent's capabilities dict.
        Advertise a client with NO fs/terminal capabilities."""
        raise NotImplementedError

    def new_session(self, *, cwd: str) -> str:
        """session/new → session id."""
        raise NotImplementedError

    def prompt(self, session_id: str, text: str) -> PromptResult:
        """session/prompt with one text content block; pumps
        notifications (and rejects agent-initiated requests) until the
        prompt's response arrives; returns the collected result.
        Raises AcpError on child EOF / JSON-RPC error / timeout."""
        raise NotImplementedError

    def cancel(self, session_id: str) -> None:
        """session/cancel notification (fire and forget)."""
        raise NotImplementedError


def build_agent_command(argv: Sequence[str]) -> list[str]:
    """Validate/normalise the agent command line passed after ``--`` on
    `raven acp` (non-empty, returned as list)."""
    raise NotImplementedError


__all__ = ["AcpClient", "AcpError", "PromptResult", "build_agent_command"]
