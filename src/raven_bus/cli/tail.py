"""``raven tail`` — identity-free forensic stream of raw log messages.

Includes expired messages (``include_expired=True``) — this is the
forensic surface, not a liveness-filtered consumer read. Does not
touch cursors or claims: multiple tailers never steal from each other.

Ordering + draining (QA cli #2):

- Output is ALWAYS id-ordered. With no ``--channel`` every poll reads
  through ``log.read_all_after`` — one global ``ORDER BY id`` window —
  so resuming from the last id printed can never skip a lower id on
  another channel. Per-channel windows stitched in channel-name order
  printed out of id order and could skip messages.
- Each poll DRAINS: store reads cap at a batch (``_BATCH``), so a poll
  loops batches until one comes back short. A single read used to make
  ``--no-follow`` exit after 100 messages per channel.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import typer

from raven_bus import db, log, models
from raven_bus.cli._common import EXIT_OK, MAX_ID, handle_errors, message_to_json

_BATCH = 100
"""Messages per store read. A drain loops until a batch is short, so
this bounds one query's size, never how much a poll prints."""

_MAX_INTERVAL_S = 3600.0
"""Upper bound for --interval: ``time.sleep`` raises OverflowError on
huge/infinite values, and an hour-long poll is already useless."""


def _check_interval(value: float) -> float:
    """--interval must be in (0, 3600]; also rejects NaN (every
    comparison with NaN is False) — ``time.sleep`` raises on negatives."""
    if not 0 < value <= _MAX_INTERVAL_S:
        raise typer.BadParameter(f"must be > 0 and <= {_MAX_INTERVAL_S:g} seconds")
    return value


def cmd_tail(
    channel: str | None = typer.Option(
        None, "--channel", help="Restrict to one channel (default: all channels)."
    ),
    from_id: int = typer.Option(
        0, "--from", min=0, max=MAX_ID,
        help="Resume after this message id (default 0 = beginning).",
    ),
    follow: bool = typer.Option(
        True,
        "--follow/--no-follow",
        help="Stay attached and stream new messages (default). "
        "--no-follow drains the whole backlog and exits.",
    ),
    json_out: bool = typer.Option(
        False, "--json", help="One JSON object per line (newline-delimited)."
    ),
    poll_interval_s: float = typer.Option(
        0.2, "--interval", callback=_check_interval,
        help="Poll cadence in seconds (> 0, at most 3600).",
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Stream raw log messages in id order; exits cleanly on Ctrl-C."""
    last_id = from_id
    try:
        with handle_errors():
            if channel is not None:
                models.validate_channel_name(channel)
            db.init_db(db_path)
        while True:
            with handle_errors():
                last_id = _drain(db_path, channel, last_id, json_out=json_out)
            if not follow:
                raise typer.Exit(code=EXIT_OK)
            time.sleep(poll_interval_s)
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        sys.stdout.flush()
        raise typer.Exit(code=EXIT_OK) from None


def _drain(
    db_path: Path | None, channel: str | None, after_id: int, *, json_out: bool
) -> int:
    """Print every message with id > ``after_id`` (one channel, or all
    channels globally id-ordered); return the last id printed. An
    unknown ``channel`` raises UnknownChannelError (exit 3)."""
    with db.connection(db_path) as conn:
        while True:
            if channel is None:
                batch = log.read_all_after(
                    conn, after_id, limit=_BATCH, include_expired=True
                )
            else:
                batch = log.read_after(
                    conn, channel, after_id, limit=_BATCH, include_expired=True
                )
            for msg in batch:
                _print_message(msg, json_out=json_out)
                after_id = msg.id
            if len(batch) < _BATCH:
                return after_id


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
