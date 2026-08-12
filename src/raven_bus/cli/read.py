"""``raven read`` — list a broadcast channel's unseen live messages."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import cursors, db, models
from raven_bus.cli._common import (
    EXIT_OK,
    echo_json,
    echo_messages_human,
    handle_errors,
    message_to_json,
)


def cmd_read(
    channel: str = typer.Option(..., "--channel", help="Channel to read."),
    as_: str = typer.Option(
        ..., "--as", help="Reader consumer id '<role>@<run>'."
    ),
    max_: int = typer.Option(100, "-m", "--max", help="Maximum messages to return."),
    json_out: bool = typer.Option(
        False, "-j", "--json", help="Emit JSON instead of text."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """List unseen live messages on a broadcast channel; does not ack."""
    with handle_errors():
        models.parse_consumer_id(as_)
        models.validate_channel_name(channel)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            msgs = cursors.pending(conn, as_, channel, limit=max_)
    if json_out:
        echo_json([message_to_json(m) for m in msgs])
    else:
        echo_messages_human(msgs)
    raise typer.Exit(code=EXIT_OK)
