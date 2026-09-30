"""The bus↔ACP loop.  LANE: acp-harness (raven2-p3).

FROZEN signatures (wave-0). A dumb pipe (ADR-006): spawn nothing,
respawn nothing, exit when the child exits or the loop is stopped.

Per boundary (the loop, roughly):

1. read ``cursors.pending`` for the consumer on its channels
   (``run/<run>/lane/<id>``-style names come from the caller), paging
   past ids this session already delivered (see _gather_pending);
2. ``policy.plan`` over the NOT-yet-delivered ones → nothing to deliver?
   ack any newly-coverable delivered prefix, sleep ``poll_interval_s``
   and re-check;
3. deliver: each ``interrupt`` alone via ``AcpClient.prompt``, then the
   batch+digest render as one prompt;
4. AFTER the WHOLE boundary succeeds, ack each channel up to the longest
   prefix of its pending ids this session has delivered (per-prompt
   acking LOST messages — see _deliver; the prefix rule is
   _ack_delivered_prefixes). An in-memory per-channel set of delivered
   ids stops same-session redelivery where an undelivered message pins
   a channel's cursor;
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

from raven_bus import channels, cursors, log, policy
from raven_bus.adapters.acp.protocol import AcpClient, AcpError, PromptResult
from raven_bus.exceptions import RavenBusError, UnknownChannelError
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

    timeout_s: float | None = None
    """ACP inactivity limit (``raven acp --timeout``): a request fails —
    exit 10 — when the agent sends no frame for this long. None = no
    limit; why that is the default is on ``AcpClient.__init__``."""

    mode: str | None = None
    """Session mode selected via ``session/set_mode`` right after
    ``session/new`` (None = never sent). Needed for headless lanes:
    this harness refuses ``session/request_permission`` (no
    capabilities granted), so an agent left in a prompting permission
    mode cannot use tools — the spawner selects a non-prompting mode
    (e.g. ``bypassPermissions``, ``dontAsk``) it considers safe for the
    lane's cage. The mode string is agent-defined; raven does not
    validate it beyond non-emptiness at the CLI."""

    initial_prompt: str | None = None
    """Sent VERBATIM as the session's first prompt (boundary 0), before
    the bus loop starts. This is the lane's task packet — TRUSTED
    spawner input, deliberately NOT run through ``policy.render``: the
    data framing exists to stop *bus messages* acting as instructions
    (ADR-003), and framing the task itself as data makes a well-behaved
    agent refuse its own assignment (observed live: a claude lane
    declined a task delivered as a data-framed bus message, citing
    injection hygiene). Trust boundary: initial_prompt comes from the
    process that spawned the harness; everything arriving via the bus
    stays data-framed."""

    @model_validator(mode="after")
    def _no_reply_feedback_loop(self) -> HarnessConfig:
        if self.reply_channel is not None and self.reply_channel in self.channels:
            raise ValueError(
                f"reply_channel {self.reply_channel!r} is also a watched "
                "channel - the harness would inject its own telemetry back "
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
    0 = the child exited with status 0 / boundary cap reached;
    10 = protocol or store error, or the child exited NON-zero (a crash
    while idle used to return 0 — QA finding A13 — telling the spawner
    "clean exit" about an OOM-killed agent). Every 10 leaves one
    ``raven-acp:`` line on stderr. ``client`` injection exists for
    tests; default constructs an AcpClient over ``child``."""
    acp = client if client is not None else AcpClient(child, timeout_s=config.timeout_s)

    try:
        acp.initialize()
        session_id = acp.new_session(cwd=config.cwd)
        if config.mode is not None:
            acp.set_mode(session_id, config.mode)
        if config.initial_prompt is not None:
            # Boundary 0: the task packet, verbatim (see the field's
            # trust-boundary note). Telemetry is posted like any other
            # boundary so the orchestrator sees the lane accept its task.
            result = acp.prompt(session_id, config.initial_prompt)
            _post_telemetry(config, result, 0)
    except (AcpError, RavenBusError, sqlite3.Error) as exc:
        # Store errors too: boundary 0's telemetry write escaped as a
        # traceback (QA finding A13), and a handshake/set_mode failure
        # exited 10 silently.
        return _fail("session setup failed", exc)

    # Ids delivered this SESSION but not (yet) coverable by the cursor (a
    # lower-id undelivered message pins that channel) must not be
    # re-planned every tick — that was a full-CPU byte-identical
    # redelivery storm (verify finding). Per channel, and exact ids, never
    # a per-channel max: a max hides lower-id DEFERRED messages from
    # plan(), and the cursor then jumps over messages injected zero times
    # (issue #3 — message loss). Pruned by _prune_delivered. Crash
    # semantics unchanged: the sets die with the process, the cursor is
    # the durable truth, redelivery after a crash is at-least-once.
    delivered: dict[str, set[int]] = {}

    boundary = 0
    while True:
        status = child.poll()
        if status is not None:
            if status != 0:
                print(f"raven-acp: agent exited with status {status}", file=sys.stderr)
                return 10
            return 0

        try:
            gathered = _gather_pending(config, delivered)
            _prune_delivered(delivered, gathered)
            pending = sorted(
                (
                    m
                    for channel, msgs in gathered.items()
                    for m in msgs
                    if m.id not in delivered.get(channel, ())
                ),
                key=lambda m: m.id,
            )
            plan_ = policy.plan(
                pending, now=datetime.now(UTC), token_budget=config.token_budget
            )

            if not plan_.interrupt and not plan_.batch and not plan_.digest_source:
                # Catch-up: a pin can clear without a delivery (the
                # undelivered message expired, or another process acked
                # past it); its channel's delivered prefix is ackable now.
                # Opens a write only when there is such a prefix.
                _ack_delivered_prefixes(config, gathered, delivered)
                time.sleep(config.poll_interval_s)
                continue

            boundary += 1
            _deliver(config, acp, session_id, plan_, boundary, gathered, delivered)
        except (AcpError, RavenBusError, sqlite3.Error) as exc:
            # Store errors (a 5s busy timeout under writer load is an
            # sqlite3.OperationalError) must exit like protocol errors —
            # a traceback-crash was the GLM verify finding. Undelivered
            # messages stay pending (cursor is the durable truth).
            return _fail("delivery failed", exc)

        if max_boundaries is not None and boundary >= max_boundaries:
            return 0


_GATHER_PAGE = 100
"""Rows per pending read — cursors.pending's own default window."""

_GATHER_MAX_PAGES = 10
"""Per channel per tick. Bounds the extra reads while a pin holds many
delivered-but-unackable ids below newer messages; past it, newer messages
wait until the pin clears (a deferred fyi is due within
DEFAULT_DIGEST_MAX_AGE_S, a budget-shed prompt goes next boundary), so the
bound can delay, never stall."""


def _fail(what: str, exc: BaseException) -> int:
    """One stderr breadcrumb, then the harness's error exit code."""
    print(f"raven-acp: {what}: {type(exc).__name__}: {exc}", file=sys.stderr)
    return 10


def _gather_pending(
    config: HarnessConfig, delivered: dict[str, set[int]]
) -> dict[str, list[Message]]:
    """Pending messages per channel, each id-ascending and CONTIGUOUS from
    the cursor (the prefix-ack walk relies on that).

    ``cursors.pending`` returns a 100-row window. When this session has
    already delivered most of it (ids pinned behind an undelivered one),
    the window alone hid everything newer — a later BLOCKING message
    stayed invisible to plan() until the pin cleared, and forever when
    the window was entirely delivered (QA findings A1/A6). So keep paging
    with ``log.read_after`` (expiry-filtered, like pending) until a
    page-worth of NOT-yet-delivered messages is in hand, the channel is
    drained, or _GATHER_MAX_PAGES is hit. With nothing delivered this
    stops after the first page, exactly as before."""
    gathered: dict[str, list[Message]] = {}
    with db_connection(config.db_path) as conn:
        for channel in config.channels:
            seen = delivered.get(channel, set())
            page = cursors.pending(conn, config.consumer, channel, limit=_GATHER_PAGE)
            msgs = list(page)
            pages = 1
            while (
                len(page) == _GATHER_PAGE
                and pages < _GATHER_MAX_PAGES
                and sum(m.id not in seen for m in msgs) < _GATHER_PAGE
            ):
                page = log.read_after(conn, channel, msgs[-1].id, limit=_GATHER_PAGE)
                msgs.extend(page)
                pages += 1
            gathered[channel] = msgs
    return gathered


def _prune_delivered(
    delivered: dict[str, set[int]], gathered: dict[str, list[Message]]
) -> None:
    """Forget delivered ids that can never be pending again: those below
    their channel's lowest pending id (acked — ack is monotonic — or
    expired; expiry never reverses). Ids above the gathered window are
    KEPT: they may still be pending beyond _GATHER_MAX_PAGES, and
    forgetting them would re-inject them once the pin clears."""
    for channel, ids in delivered.items():
        msgs = gathered.get(channel)
        if not msgs:
            ids.clear()
            continue
        low = msgs[0].id
        ids.difference_update([i for i in ids if i < low])


def _ack_delivered_prefixes(
    config: HarnessConfig,
    gathered: dict[str, list[Message]],
    delivered: dict[str, set[int]],
) -> None:
    """Ack each channel up to the LONGEST PREFIX of its pending ids that
    this session has delivered (in completed boundaries only — ids enter
    ``delivered`` after their boundary's prompts all succeeded).

    Walks the channel's gathered ids ascending from the cursor and stops
    at the first one NOT delivered, so the cursor-jump (ADR-001) can
    never pass an undelivered or deferred id — the real invariant. It
    replaced "ack only this boundary's ids, capped at the plan's GLOBAL
    ack_up_to" (QA finding A1), which (a) left ids delivered under a pin
    unacked even after the pin cleared, until the 100-row pending window
    filled with them and the channel stalled for the session, (b) re-
    injected them on every clean restart, and (c) let a deferred fyi on
    one channel pin acks on every other channel."""
    acks: dict[str, int] = {}
    for channel, msgs in gathered.items():
        ids = delivered.get(channel, set())
        last: int | None = None
        for m in msgs:
            if m.id not in ids:
                break
            last = m.id
        if last is not None:
            acks[channel] = last
    if not acks:
        return
    with db_connection(config.db_path) as conn:
        for channel, up_to_id in acks.items():
            cursors.ack(conn, config.consumer, channel, up_to_id)
    for channel, up_to_id in acks.items():
        delivered[channel] = {i for i in delivered[channel] if i > up_to_id}


def _deliver(
    config: HarnessConfig,
    acp: AcpClient,
    session_id: str,
    plan_: policy.InjectionPlan,
    boundary: int,
    gathered: dict[str, list[Message]],
    delivered: dict[str, set[int]],
) -> None:
    """Interrupts alone, first (one prompt each); then one prompt for
    batch+digest if either is non-empty.

    Acking happens ONCE, after EVERY prompt of the boundary has
    succeeded. Per-prompt acking looked crash-safer but caused MESSAGE
    LOSS (verify finding): an interrupt's ack could cover lower-id batch
    messages the failed batch prompt never delivered. A crash
    mid-boundary now redelivers already-prompted interrupts —
    at-least-once, the survivable failure mode. Every injected id is
    recorded in ``delivered`` (per channel) even where its cursor cannot
    advance yet, so this session never re-delivers it — while deferred
    ids stay OUT of it, so plan() keeps seeing them."""
    for msg in plan_.interrupt:
        solo = policy.InjectionPlan(interrupt=[msg])
        result = acp.prompt(session_id, policy.render(solo))
        _post_telemetry(config, result, boundary)

    if plan_.batch or plan_.digest_source:
        solo = policy.InjectionPlan(batch=plan_.batch, digest_source=plan_.digest_source)
        result = acp.prompt(session_id, policy.render(solo))
        _post_telemetry(config, result, boundary)

    for msg in (*plan_.interrupt, *plan_.batch, *plan_.digest_source):
        delivered.setdefault(msg.channel, set()).add(msg.id)
    _ack_delivered_prefixes(config, gathered, delivered)


def _post_telemetry(config: HarnessConfig, result: PromptResult, boundary: int) -> None:
    """acp-reply (text + stop_reason) and an acp-activity heartbeat for
    the session/update batch behind this prompt, on ``reply_channel``
    (no-op when unset)."""
    if config.reply_channel is None:
        return
    with db_connection(config.db_path) as conn:
        # ensure=False after an absent-only create: append's own ensure
        # re-ensures as broadcast and raised WrongChannelKindError on a
        # reply channel that exists as a `stream` (the design's
        # run/<run>/telemetry). Posting accepts any kind.
        ensure_absent_as_broadcast(conn, config.reply_channel)
        log.append(
            conn,
            channel=config.reply_channel,
            sender=config.consumer,
            type="acp-reply",
            urgency="fyi",
            body={"text": result.text, "stop_reason": result.stop_reason, "boundary": boundary},
            ensure=False,
        )
        log.append(
            conn,
            channel=config.reply_channel,
            sender=config.consumer,
            type="acp-activity",
            urgency="fyi",
            body={"updates": len(result.raw_updates), "boundary": boundary},
            ensure=False,
        )


def ensure_absent_as_broadcast(conn: sqlite3.Connection, name: str) -> str:
    """Create channel ``name`` as ``broadcast`` only if it doesn't exist;
    return its kind. An EXISTING channel of any kind is left alone.

    Deliberately not ``channels.ensure_channel(kind="broadcast")``, which
    raises WrongChannelKindError on an existing channel of another kind —
    that killed `raven acp` startup when the reply channel was a `stream`
    (store-lane finding). Callers that need broadcast (watched channels:
    the loop reads cursors) check the returned kind themselves."""
    try:
        return channels.get_channel(conn, name).kind
    except UnknownChannelError:
        return channels.ensure_channel(conn, name, kind="broadcast").kind


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


__all__ = ["HarnessConfig", "ensure_absent_as_broadcast", "parse_channels", "run_harness"]
