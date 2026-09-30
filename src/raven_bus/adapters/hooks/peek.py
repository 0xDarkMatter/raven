"""PreToolUse inbox peek.  LANE: hook (raven2-p3).

Runnable as ``python -m raven_bus.adapters.hooks.peek``. ADR-006: this
hook PEEKS ONLY — it reads the consumer's pending messages and emits
:func:`raven_bus.policy.render_hint`'s bounded PULL notice (counts, ids,
senders, the ``raven read`` command — never bodies) when anything is due;
it NEVER acks and NEVER composes its own text. A notice, not the full
``policy.render`` block, because this fires on EVERY tool call and never
acks: re-pushing whole messages repeated the entire backlog each call
(issue #1). The notice travels as PreToolUse
``hookSpecificOutput.additionalContext`` JSON — the only PreToolUse stdout
Claude Code shows the model (see ``_emit``). Silent (empty stdout) when
nothing is due. Always exits 0 — a broken
hook must never block a tool call, so EVERY failure path (missing/locked
DB, bad config, a still-stubbed policy) is swallowed and leaves only a
single stderr breadcrumb.

Config is environment-only (no argparse — the hook fires on every tool
call and must stay cheap and dependency-light):

- ``RAVEN_CONSUMER``  absent → silent no-op (the hook is inactive).
- ``RAVEN_CHANNELS``  comma-separated channel names; REQUIRED when a
  consumer is set (the hook performs NO ``run/<run>/lane/<role>``
  derivation — ADR-006 explicit only). Missing/empty → silent exit 0.
- ``RAVEN_DB``        optional; the store's normal resolution otherwise.

Per ADR-001, reading does not advance the cursor; per ADR-006 the hook
never acks, so it may run beside a harness serving the same consumer.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime


def _parse_channels(raw: str) -> list[str]:
    """Split a comma-separated channel list, dropping blanks/dupes.

    Order is preserved (the merge below is stable across channels); a
    blank-only list yields ``[]`` so the caller treats it as "no
    channels configured" and stays silent.
    """
    seen: set[str] = set()
    out: list[str] = []
    for token in raw.split(","):
        name = token.strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def peek() -> int:
    """Read env, validate, peek pending, print banner+render. Exit 0.

    The whole body is wrapped in ``except BaseException`` so that a
    missing or locked DB, a stubbed policy, or any store error leaves
    stdout empty and the process at exit 0 — ADR-006's "a broken hook
    must never block a tool call". stderr gets ONE breadcrumb line (the
    wrapper discards stderr anyway, but it aids debugging when run by
    hand).
    """
    try:
        return _peek_inner()
    except BaseException as exc:  # noqa: BLE001 — ADR-006: swallow ALL.
        # Never on stdout; one stderr line (the .sh sends stderr to /dev/null).
        # ``exc`` is rendered defensively — name + str, never the raw repr.
        print(f"raven-inbox-hook: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0


def _peek_inner() -> int:
    """The real path; ``peek`` wraps it to guarantee exit 0."""
    consumer = os.environ.get("RAVEN_CONSUMER", "").strip()
    if not consumer:
        # No consumer configured → the hook is inactive. Not an error.
        return 0

    channels = _parse_channels(os.environ.get("RAVEN_CHANNELS", ""))
    if not channels:
        # Consumer set but no channels — explicit-only (ADR-006: no
        # derivation). Treat as "nothing to watch", stay silent.
        print(
            "raven-inbox-hook: RAVEN_CONSUMER set but RAVEN_CHANNELS empty",
            file=sys.stderr,
        )
        return 0

    db_path = os.environ.get("RAVEN_DB") or None

    # Validate the consumer id + every channel name with the frozen
    # models helpers BEFORE touching the store (ADR-002 grammar). A bad
    # grammar is a configuration mistake, not a runtime error — stay
    # silent on stdout, one stderr breadcrumb, exit 0.
    from raven_bus.exceptions import RavenBusError
    from raven_bus.models import parse_consumer_id, validate_channel_name

    try:
        parse_consumer_id(consumer)
        for ch in channels:
            validate_channel_name(ch)
    except RavenBusError as exc:
        print(f"raven-inbox-hook: bad config: {exc}", file=sys.stderr)
        return 0

    pending = _read_pending(consumer, channels, db_path)
    if not pending:
        # Silent when nothing pending (ADR-006 BLUF).
        return 0

    # policy owns ALL text (ADR-003/006) — the hook never composes any.
    # Nothing due (e.g. only fyi still held below digest thresholds)
    # renders "" and the hook stays quiet this call.
    from raven_bus import policy

    notice = policy.render_hint(pending, consumer=consumer, now=datetime.now(UTC))
    if not notice:
        return 0

    _emit(notice)
    return 0


def _read_pending(consumer: str, channels: list[str], db_path: str | None) -> list:
    """Open the store read-only-ish, gather pending across channels.

    Uses :func:`raven_bus.db.connection` (the sanctioned WAL connection)
    and :func:`cursors.pending` per channel, merging id-ascending.
    ``cursors.pending``'s own consumer upsert is the module's one
    sanctioned write (ADR-006) — the hook adds nothing else and NEVER
    calls ``cursors.ack``.
    """
    from raven_bus import cursors, db

    db.init_db(db_path)
    merged: list = []
    with db.connection(db_path) as conn:
        for channel in channels:
            merged.extend(cursors.pending(conn, consumer, channel))

    # Stable id-ascending merge across channels: pending() returns each
    # channel already id-sorted; a final sort by id gives a global order
    # that is deterministic and matches ADR-001's ascending contract.
    merged.sort(key=lambda m: m.id)
    return merged


def _emit(context: str) -> None:
    """Emit ``context`` VERBATIM as ONE line of PreToolUse hook JSON.

    Transport, not framing: for PreToolUse, Claude Code writes PLAIN
    stdout to its debug log and never adds it to the model's context —
    only ``hookSpecificOutput.additionalContext`` reaches the model
    (code.claude.com/docs/en/hooks, "Exit code 0" + PreToolUse decision
    control). Printing the text bare was issue #2: the hook fired and
    rendered, the session never saw a byte. ``ensure_ascii`` (json's
    default) keeps stdout pure ASCII so text outside the console
    codepage (cp1252 on Windows pipes) can't raise inside peek's
    catch-all and silently drop the delivery. The text itself is
    policy's (``render_hint``); the hook adds nothing to it.
    """
    envelope = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": context,
        }
    }
    print(json.dumps(envelope), file=sys.stdout)


def main() -> None:
    """Module entry point — exits 0 in every path (ADR-006)."""
    sys.exit(peek())


if __name__ == "__main__":
    main()
