"""The bus↔ACP loop.  LANE: acp-harness (raven2-p3).

FROZEN signatures (wave-0). A dumb pipe (ADR-006): spawn nothing,
respawn nothing, exit when the child exits or the loop is stopped.

Per boundary (the loop, roughly):

1. read ``cursors.pending`` for the consumer on its channels
   (``run/<run>/lane/<id>``-style names come from the caller);
2. ``policy.plan`` → nothing to deliver? sleep ``poll_interval_s``
   (data_version fast-poll allowed) and re-check;
3. deliver: each ``interrupt`` alone via ``AcpClient.prompt``, then the
   batch+digest render as one prompt;
4. AFTER each successful prompt submission, ``cursors.ack`` up to that
   delivery's ids (never past deferred — the plan's ack_up_to already
   guarantees the final position; intermediate acks per interrupt are
   fine and crash-safer);
5. post the agent's reply text and stop_reason to the bus as telemetry
   (``log.append`` type='acp-reply' on the reply_channel), and each
   session/update batch count as type='acp-activity' heartbeats.

Crash semantics: if the child dies (AcpError EOF), stop cleanly and
return; undelivered/deferred messages stay pending for the next process
(that is WHY ack happens only after submission).
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from raven_bus.adapters.acp.protocol import AcpClient


class HarnessConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    consumer: str
    """`<role>@<run>` — the identity whose channels we deliver."""

    channels: tuple[str, ...]
    """Broadcast channels to watch (pending+ack per ADR-001)."""

    reply_channel: str | None = None
    """Where agent output/telemetry is posted (None = no posting)."""

    db_path: Path | None = None
    poll_interval_s: float = 1.0
    token_budget: int = 2000
    cwd: str = "."
    """cwd passed to session/new (the agent's working directory)."""


def run_harness(
    config: HarnessConfig,
    child: subprocess.Popen,
    *,
    client: AcpClient | None = None,
    max_boundaries: int | None = None,
) -> int:
    """Drive the loop until the child exits, AcpError, or
    ``max_boundaries`` deliveries (tests). Returns an exit code:
    0 = child exited cleanly / boundary cap reached, 10 = protocol
    error. ``client`` injection exists for tests; default constructs
    an AcpClient over ``child``."""
    raise NotImplementedError


def parse_channels(raw: Sequence[str]) -> tuple[str, ...]:
    """Validate channel names (models grammar) for the CLI."""
    raise NotImplementedError


__all__ = ["HarnessConfig", "parse_channels", "run_harness"]
