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
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from raven_bus import cursors, log, policy
from raven_bus.adapters.acp.protocol import AcpClient, AcpError, PromptResult
from raven_bus.db import connection as db_connection
from raven_bus.models import Message, validate_channel_name


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
    acp = client if client is not None else AcpClient(child)

    try:
        acp.initialize()
        session_id = acp.new_session(cwd=config.cwd)
    except AcpError:
        return 10

    boundary = 0
    while True:
        if child.poll() is not None:
            return 0

        pending = _gather_pending(config)
        plan_ = policy.plan(pending, now=datetime.now(UTC), token_budget=config.token_budget)

        if not plan_.interrupt and not plan_.batch and not plan_.digest_source:
            time.sleep(config.poll_interval_s)
            continue

        boundary += 1
        try:
            _deliver(config, acp, session_id, plan_, boundary)
        except AcpError:
            return 10

        if max_boundaries is not None and boundary >= max_boundaries:
            return 0


def _gather_pending(config: HarnessConfig) -> list[Message]:
    """Pending messages across ``config.channels``, id-ascending merge."""
    messages: list[Message] = []
    with db_connection(config.db_path) as conn:
        for channel in config.channels:
            messages.extend(cursors.pending(conn, config.consumer, channel))
    messages.sort(key=lambda m: m.id)
    return messages


def _deliver(
    config: HarnessConfig,
    acp: AcpClient,
    session_id: str,
    plan_: policy.InjectionPlan,
    boundary: int,
) -> None:
    """Interrupts alone, first (one prompt each); then one prompt for
    batch+digest if either is non-empty. Ack + reply-post happen after
    each individual prompt succeeds — never before.

    Every ack is capped at ``plan_.ack_up_to`` — the plan's own
    id-ceiling that already stops before the first deferred message
    (ADR-001 cursor-jump semantics: a single per-channel cursor can't
    skip over an older, still-deferred message just because a newer
    one on the same channel was delivered)."""
    for msg in plan_.interrupt:
        solo = policy.InjectionPlan(interrupt=[msg])
        result = acp.prompt(session_id, policy.render(solo))
        with db_connection(config.db_path) as conn:
            _ack_covered(conn, config.consumer, [msg], plan_.ack_up_to)
        _post_telemetry(config, result, boundary)

    if plan_.batch or plan_.digest_source:
        solo = policy.InjectionPlan(batch=plan_.batch, digest_source=plan_.digest_source)
        result = acp.prompt(session_id, policy.render(solo))
        with db_connection(config.db_path) as conn:
            _ack_covered(
                conn, config.consumer, list(plan_.batch) + list(plan_.digest_source), plan_.ack_up_to
            )
        _post_telemetry(config, result, boundary)


def _ack_covered(
    conn: object, consumer: str, messages: list[Message], ack_up_to: int
) -> None:
    """Ack each channel represented in ``messages`` up to the highest
    covered id on that channel, never past ``ack_up_to`` (a message's
    channel is on the Message; cursors are per-channel, so a
    mixed-channel batch acks each once)."""
    by_channel: dict[str, int] = {}
    for msg in messages:
        if msg.id > ack_up_to:
            continue
        by_channel[msg.channel] = max(by_channel.get(msg.channel, 0), msg.id)
    for channel, up_to_id in by_channel.items():
        cursors.ack(conn, consumer, channel, up_to_id)  # type: ignore[arg-type]


def _post_telemetry(config: HarnessConfig, result: PromptResult, boundary: int) -> None:
    """acp-reply (text + stop_reason) and an acp-activity heartbeat for
    the session/update batch behind this prompt, on ``reply_channel``
    (no-op when unset)."""
    if config.reply_channel is None:
        return
    with db_connection(config.db_path) as conn:
        log.append(
            conn,
            channel=config.reply_channel,
            sender=config.consumer,
            type="acp-reply",
            urgency="fyi",
            body={"text": result.text, "stop_reason": result.stop_reason, "boundary": boundary},
        )
        log.append(
            conn,
            channel=config.reply_channel,
            sender=config.consumer,
            type="acp-activity",
            urgency="fyi",
            body={"updates": len(result.raw_updates), "boundary": boundary},
        )


def parse_channels(raw: Sequence[str]) -> tuple[str, ...]:
    """Validate channel names (models grammar) for the CLI."""
    channels = tuple(validate_channel_name(c) for c in raw)
    if not channels:
        from raven_bus.exceptions import InvalidAddressError

        raise InvalidAddressError("at least one --channel is required")
    return channels


__all__ = ["HarnessConfig", "parse_channels", "run_harness"]
