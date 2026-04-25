"""``claude-bus read`` — fetch one message by id."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import BusClient
from claude_bus.cli._common import (
    EXIT_MESSAGE_NOT_FOUND,
    EXIT_OK,
    echo_json,
    echo_message_human,
    message_to_json,
)
from claude_bus.exceptions import UnknownMessageError


def cmd_read(
    message_id: int = typer.Argument(..., help="Message id to fetch."),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of text."),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """Read a single message without changing its status."""
    # Reading is identity-agnostic at the CLI layer — use a sentinel client.
    # The DB doesn't enforce ACLs in P1 so any client can read by id.
    client = BusClient(session_id="__cli__", role="reader", db_path=db)
    try:
        msg = client.read(message_id)
    except UnknownMessageError:
        typer.echo(f"error: message id={message_id} not found", err=True)
        raise typer.Exit(code=EXIT_MESSAGE_NOT_FOUND) from None
    if json_out:
        echo_json(message_to_json(msg))
    else:
        echo_message_human(msg)
    raise typer.Exit(code=EXIT_OK)
