"""PreToolUse inbox peek.  LANE: hook (raven2-p3).

Runnable as ``python -m raven_bus.adapters.hooks.peek``. ADR-006: this
hook PEEKS ONLY — it reads the consumer's pending messages and emits
:func:`raven_bus.policy.render_hint`'s bounded PULL notice (counts, ids,
senders, the ``raven read --framed`` command — never bodies) when anything
is due; it NEVER acks and NEVER composes its own text. A notice, not the
full ``policy.render`` block, because this fires on EVERY tool call and
never acks: re-pushing whole messages repeated the entire backlog each call
(issue #1). The notice travels as PreToolUse
``hookSpecificOutput.additionalContext`` JSON — the only PreToolUse stdout
Claude Code shows the model (see ``_emit``). Silent (empty stdout) when
nothing is due. Always exits 0 — a broken hook must never block a tool
call, so EVERY failure path (missing/locked DB, bad config, a still-stubbed
policy) is swallowed and leaves only a single stderr breadcrumb.

The read path WRITES NOTHING (see ``_read_pending``): no ``init_db``, no
sweep, no presence upsert, on a read-only connection with a short busy
timeout. It used to go through ``cursors.pending``, whose sweep UPDATEs
and consumer upsert made every tool call a writer — under another
process's write lock each call stalled ~5 s and then dropped the notice
(QA finding A10).

Config is environment-only (no argparse — the hook fires on every tool
call and must stay cheap and dependency-light):

- ``RAVEN_CONSUMER``  absent → silent no-op (the hook is inactive).
- ``RAVEN_CHANNELS``  comma-separated channel names; REQUIRED when a
  consumer is set (the hook performs NO ``run/<run>/lane/<role>``
  derivation — ADR-006 explicit only). Missing/empty → silent exit 0.
  A listed channel that doesn't exist or isn't ``broadcast`` is skipped
  (one stderr breadcrumb) — it must not silence the valid ones.
- ``RAVEN_DB``        optional; the store's normal resolution otherwise.
  A missing DB file means silence — the hook never creates one.
- ``RAVEN_ACP_CONSUMER`` / ``RAVEN_ACP_CHANNELS`` are set by ``raven acp``
  in the agent it spawns: channels that harness already delivers for the
  same consumer are skipped (see ``_harness_served``).

Per ADR-001, reading does not advance the cursor; per ADR-006 the hook
never acks, so it may run beside a harness serving the same consumer.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

HOOK_BUSY_TIMEOUT_S = 0.25
"""The hook's SQLite busy timeout. Tool calls wait on the hook, so a
contended DB must cost at most a quarter second, never the store's 5 s
default; a notice missed now repeats on the next tool call."""

HOOK_PAGE = 100
"""Pending rows read per channel — cursors.pending's window."""

ENV_ACP_CONSUMER = "RAVEN_ACP_CONSUMER"
ENV_ACP_CHANNELS = "RAVEN_ACP_CHANNELS"


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


def _harness_served(consumer: str) -> set[str]:
    """Channels a ``raven acp`` harness already delivers for ``consumer``
    in this process tree (its env markers; empty when not under one).

    The harness injects those messages itself and acks only after the
    turn ends, so a hook inside the harness-driven agent re-announced
    every message during the very turn that delivered it (QA finding
    A11). Skipping them is exact: the markers name the consumer and
    channels, so a hook watching OTHER channels, or as another consumer,
    is unaffected."""
    if os.environ.get(ENV_ACP_CONSUMER, "").strip() != consumer:
        return set()
    return set(_parse_channels(os.environ.get(ENV_ACP_CHANNELS, "")))


def peek() -> int:
    """Read env, validate, peek pending, emit render_hint as JSON. Exit 0.

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

    served = _harness_served(consumer)
    channels = [ch for ch in channels if ch not in served]
    if not channels:
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


def _connect(path: Path) -> sqlite3.Connection:
    """Open ``path`` READ-ONLY (``mode=ro`` URI): SQLite refuses every
    write on the connection and never creates a missing file. (SQLite
    >= 3.22 opens a WAL database read-only even when no -shm/-wal exist
    yet.) No ``journal_mode`` pragma — WAL is persistent in the file, and
    switching modes needs a lock. ``sqlite3.Row`` because the store's
    read helpers index rows by column name."""
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=HOOK_BUSY_TIMEOUT_S)
    conn.row_factory = sqlite3.Row
    return conn


def _read_pending(consumer: str, channels: list[str], db_path: str | None) -> list:
    """Pending messages across ``channels``, merged id-ascending — a pure
    read (QA finding A10: this path used to write on every tool call).

    Mirrors ``cursors.pending`` minus its side effects: the cursor row
    (``cursors.get_cursor``, a pure SELECT) and ``log.read_after``, which
    filters expired messages itself. Skipped: ``init_db`` (a missing file
    means silence, never creation), ``db.sweep`` (lease reaping belongs to
    queue readers, and broadcast expiry is already filtered) and the
    ``consumers.touch`` presence upsert (a hook firing on every tool call
    is not a heartbeat). NEVER calls ``cursors.ack``.

    A channel that doesn't exist or isn't ``broadcast`` is skipped with
    one stderr breadcrumb — it used to raise and silence the notice for
    every other channel, blocking messages included (QA finding A8). The
    hook stays read-only: it never creates the missing channel.
    """
    from raven_bus import channels as channels_mod
    from raven_bus import cursors, log
    from raven_bus.exceptions import UnknownChannelError
    from raven_bus.paths import resolve_db_path

    path = resolve_db_path(db_path)
    if not path.is_file():
        return []

    merged: list = []
    skipped: list[str] = []
    conn = _connect(path)
    try:
        for name in channels:
            try:
                channel = channels_mod.get_channel(conn, name)
            except UnknownChannelError:
                skipped.append(name)
                continue
            if channel.kind != "broadcast":
                skipped.append(name)
                continue
            cursor = cursors.get_cursor(conn, consumer, name)
            after_id = cursor.last_ack_id if cursor is not None else 0
            merged.extend(log.read_after(conn, name, after_id, limit=HOOK_PAGE))
    finally:
        conn.close()

    if skipped:
        print(
            "raven-inbox-hook: skipped channel(s) that are missing or not "
            f"broadcast: {', '.join(skipped)}",
            file=sys.stderr,
        )

    # Stable id-ascending merge across channels: each read is already
    # id-sorted; a final sort by id gives a global order that is
    # deterministic and matches ADR-001's ascending contract.
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
