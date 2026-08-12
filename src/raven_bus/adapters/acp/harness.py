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
4. AFTER the WHOLE boundary succeeds, one ``cursors.ack`` capped at the
   plan's ack_up_to (per-prompt acking LOST messages — see _deliver);
   an in-memory delivered watermark stops same-session redelivery
   where a deferred fyi pins the cursor;
5. post the agent's reply text and stop_reason to the bus as telemetry
   (``log.append`` type='acp-reply' on the reply_channel), and each
   session/update batch count as type='acp-activity' heartbeats.

Crash semantics: if the child dies (AcpError EOF), stop cleanly and
return; anything not yet acked stays pending and redelivers
at-least-once in the next process (ack only after a completed
boundary is WHY loss cannot happen).
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, model_validator

from raven_bus import cursors, log, policy
from raven_bus.adapters.acp.protocol import AcpClient, AcpError, PromptResult
from raven_bus.exceptions import RavenBusError
from raven_bus.db import connection as db_connection
from raven_bus.models import Message, validate_channel_name


class HarnessConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    consumer: str
    """`<role>@<run>` — the identity whose channels we deliver."""

    channels: tuple[str, ...]
    """Broadcast channels to watch (pending+ack per ADR-001)."""

    reply_channel: str | None = None
    """Where agent output/telemetry is posted (None = no posting).
    MUST NOT be one of ``channels`` — the harness would digest its own
    acp-reply/acp-activity messages back into the agent (feedback
    loop; verify finding). Enforced below."""

    db_path: Path | None = None
    poll_interval_s: float = 1.0
    token_budget: int = 2000
    cwd: str = "."
    """cwd passed to session/new (the agent's working directory)."""

    mode: str | None = None
    """Session mode selected via ``session/set_mode`` right after
    ``session/new`` (None = never sent). Needed for headless lanes:
    this harness refuses ``session/request_permission`` (no
    capabilities granted), so an agent left in a prompting permission
    mode cannot use tools — the spawner selects a non-prompting mode
    (e.g. ``bypassPermissions``, ``dontAsk``) it considers safe for the
    lane's cage. The mode string is agent-defined; raven does not
    validate it beyond non-emptiness at the CLI."""

    @model_validator(mode="after")
    def _no_reply_feedback_loop(self) -> HarnessConfig:
        if self.reply_channel is not None and self.reply_channel in self.channels:
            raise ValueError(
                f"reply_channel {self.reply_channel!r} is also a watched "
                "channel — the harness would inject its own telemetry back "
                "into the agent"
            )
        return self


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
        if config.mode is not None:
            acp.set_mode(session_id, config.mode)
    except AcpError:
        return 10

    # In-memory delivered watermark per channel: a message delivered this
    # SESSION but not (yet) coverable by the cursor (a lower-id deferred
    # fyi pins ack_up_to) must not be re-planned every tick — that was a
    # full-CPU byte-identical redelivery storm (verify finding). Crash
    # semantics unchanged: the dict dies with the process, the cursor is
    # the durable truth, redelivery after a crash is at-least-once.
    delivered_watermark: dict[str, int] = {}

    boundary = 0
    while True:
        if child.poll() is not None:
            return 0

        try:
            pending = [
                m
                for m in _gather_pending(config)
                if m.id > delivered_watermark.get(m.channel, 0)
            ]
            plan_ = policy.plan(
                pending, now=datetime.now(UTC), token_budget=config.token_budget
            )

            if not plan_.interrupt and not plan_.batch and not plan_.digest_source:
                time.sleep(config.poll_interval_s)
                continue

            boundary += 1
            _deliver(config, acp, session_id, plan_, boundary, delivered_watermark)
        except (AcpError, RavenBusError, sqlite3.Error) as exc:
            # Store errors (a 5s busy timeout under writer load is an
            # sqlite3.OperationalError) must exit like protocol errors —
            # a traceback-crash was the GLM verify finding. One stderr
            # breadcrumb; undelivered messages stay pending (cursor is
            # the durable truth).
            print(f"raven-acp: {type(exc).__name__}: {exc}", file=sys.stderr)
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
    delivered_watermark: dict[str, int],
) -> None:
    """Interrupts alone, first (one prompt each); then one prompt for
    batch+digest if either is non-empty.

    The cursor ack happens ONCE, after EVERY prompt of the boundary has
    succeeded, capped at ``plan_.ack_up_to``. Per-prompt acking looked
    crash-safer but caused MESSAGE LOSS (verify finding): an interrupt's
    ack at ack_up_to could cover lower-id batch messages the failed
    batch prompt never delivered. A crash mid-boundary now redelivers
    already-prompted interrupts — at-least-once, the survivable
    failure mode. The delivered watermark advances even where the
    cursor cannot (deferred fyi pinning ack_up_to), so this session
    never re-delivers what it already injected."""
    for msg in plan_.interrupt:
        solo = policy.InjectionPlan(interrupt=[msg])
        result = acp.prompt(session_id, policy.render(solo))
        _post_telemetry(config, result, boundary)

    if plan_.batch or plan_.digest_source:
        solo = policy.InjectionPlan(batch=plan_.batch, digest_source=plan_.digest_source)
        result = acp.prompt(session_id, policy.render(solo))
        _post_telemetry(config, result, boundary)

    delivered = (
        list(plan_.interrupt) + list(plan_.batch) + list(plan_.digest_source)
    )
    with db_connection(config.db_path) as conn:
        _ack_covered(conn, config.consumer, delivered, plan_.ack_up_to)
    for msg in delivered:
        delivered_watermark[msg.channel] = max(
            delivered_watermark.get(msg.channel, 0), msg.id
        )


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
    """Validate channel names (models grammar) for the CLI; dedupe
    preserving first-seen order (a repeated --channel would double-
    deliver every message and halve digest thresholds — verify
    finding)."""
    seen: dict[str, None] = {}
    for c in raw:
        seen.setdefault(validate_channel_name(c), None)
    channels = tuple(seen)
    if not channels:
        from raven_bus.exceptions import InvalidAddressError

        raise InvalidAddressError("at least one --channel is required")
    return channels


__all__ = ["HarnessConfig", "parse_channels", "run_harness"]
