"""``raven tail`` — identity-free forensic stream of raw log messages.

Includes expired messages (``include_expired=True``) — this is the
forensic surface, not a liveness-filtered consumer read. Does not
touch cursors or claims: multiple tailers never steal from each other.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import typer

from raven_bus import channels, db, log
from raven_bus.cli._common import EXIT_OK, message_to_json


def cmd_tail(
    channel: str | None = typer.Option(
        None, "--channel", help="Restrict to one channel (default: all channels)."
    ),
    from_id: int = typer.Option(
        0, "--from", help="Resume from this message id (default 0 = beginning)."
    ),
    follow: bool = typer.Option(
        True,
        "--follow/--no-follow",
        help="Stay attached and stream new messages (default). "
        "--no-follow drains the backlog and exits.",
    ),
    json_out: bool = typer.Option(
        False, "--json", help="One JSON object per line (newline-delimited)."
    ),
    poll_interval_s: float = typer.Option(
        0.2, "--interval", help="Poll cadence in seconds."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Stream raw log messages; pipes to stdout, exits cleanly on Ctrl-C."""
    db.init_db(db_path)
    last_id: dict[str, int] = {}
    if channel is not None:
        last_id[channel] = from_id

    try:
        while True:
            with db.connection(db_path) as conn:
                names = (
                    [channel]
                    if channel is not None
                    else [c.name for c in channels.list_channels(conn)]
                )
                for name in names:
                    after = last_id.setdefault(name, from_id)
                    msgs = log.read_after(conn, name, after, include_expired=True)
                    for m in msgs:
                        _print_message(m, json_out=json_out)
                        last_id[name] = m.id
            if not follow:
                raise typer.Exit(code=EXIT_OK)
            time.sleep(poll_interval_s)
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        sys.stdout.flush()
        raise typer.Exit(code=EXIT_OK) from None


def _print_message(msg, *, json_out: bool) -> None:
    if json_out:
        sys.stdout.write(json.dumps(message_to_json(msg)) + "\n")
        sys.stdout.flush()
        return
    body_preview = json.dumps(msg.body, sort_keys=True)
    if len(body_preview) > 80:
        body_preview = body_preview[:77] + "..."
    typer.echo(
        f"#{msg.id:<4}  {msg.channel}  {msg.sender}  "
        f"type={msg.type}  body={body_preview}"
    )
