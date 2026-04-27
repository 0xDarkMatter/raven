"""``raven tail`` — stream new messages as they arrive.

Identity-free observer. Does not consume messages — multiple tailers
can run side-by-side without stealing from each other. Useful for
watching live bus traffic during demos, debugging cross-process
coordination, or sanity-checking what an agent is actually sending.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import typer

from claude_bus import _core
from claude_bus.cli._common import EXIT_OK
from claude_bus.db import connection, init_db


def cmd_tail(
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
    role: str | None = typer.Option(
        None,
        "--role",
        "-r",
        help='Only show messages addressed to "<role>:<session>" (otherwise: all).',
    ),
    from_id: int = typer.Option(
        0, "--from", help="Resume from this message id (default 0 = beginning)."
    ),
    follow: bool = typer.Option(
        True,
        "--follow/--no-follow",
        "-f",
        help="Stay attached and stream new messages (default). "
        "--no-follow prints the backlog and exits.",
    ),
    poll_interval_s: float = typer.Option(
        0.2, "--interval", help="Poll cadence in seconds (default 0.2s)."
    ),
    json_out: bool = typer.Option(
        False, "--json", "-j",
        help="One JSON object per line (newline-delimited).",
    ),
) -> None:
    """Tail the message log; pipes to stdout, exits cleanly on Ctrl-C."""
    init_db(db)

    recipient_filter: str | None = None
    if role is not None:
        # Translate role:session → the deterministic alias used in the DB.
        from claude_bus import aliases as alias_mod

        if ":" not in role:
            typer.echo(
                f"error: --role must be '<role>:<session>', got {role!r}",
                err=True,
            )
            raise typer.Exit(code=2)
        rname, sid = role.split(":", 1)
        recipient_filter = alias_mod.compute_alias(rname, sid)

    last_id = from_id
    started = time.monotonic()

    try:
        while True:
            with connection(db) as conn:
                msgs = _core.list_since(
                    conn, last_id, limit=200, recipient=recipient_filter
                )
            for m in msgs:
                _print_message(m, started, json_out=json_out)
                last_id = m.id
            if not follow:
                return
            time.sleep(poll_interval_s)
    except KeyboardInterrupt:
        # Print a newline so the shell prompt isn't glued to the last message.
        sys.stdout.write("\n")
        sys.stdout.flush()
        raise typer.Exit(code=EXIT_OK) from None


def _print_message(msg, started: float, *, json_out: bool) -> None:
    if json_out:
        payload = {
            "id": msg.id,
            "session_id": msg.session_id,
            "sender": msg.sender,
            "recipient": msg.recipient,
            "type": msg.type,
            "body": msg.body,
            "status": msg.status,
            "created_at": msg.created_at.isoformat(),
        }
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()
        return

    elapsed_ms = (time.monotonic() - started) * 1000
    body_preview = json.dumps(msg.body, sort_keys=True)
    if len(body_preview) > 80:
        body_preview = body_preview[:77] + "..."
    typer.echo(
        f"#{msg.id:<4}  +{elapsed_ms:6.0f}ms  "
        f"{msg.sender} -> {msg.recipient}  "
        f"type={msg.type}  body={body_preview}"
    )
