"""``claude-bus ack`` — mark a message as read."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import BusClient
from claude_bus.cli._common import EXIT_MESSAGE_NOT_FOUND, EXIT_OK
from claude_bus.exceptions import UnknownMessageError


def cmd_ack(
    message_id: int = typer.Argument(..., help="Message id to acknowledge."),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """Mark a message as read. Idempotent."""
    client = BusClient(session_id="__cli__", role="reader", db_path=db)
    try:
        client.ack(message_id)
    except UnknownMessageError:
        typer.echo(f"error: message id={message_id} not found", err=True)
        raise typer.Exit(code=EXIT_MESSAGE_NOT_FOUND) from None
    typer.echo(f"acked #{message_id}")
    raise typer.Exit(code=EXIT_OK)
