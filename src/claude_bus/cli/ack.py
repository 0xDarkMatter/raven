"""``raven ack`` — mark a message as read."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import _core
from claude_bus.cli._common import EXIT_MESSAGE_NOT_FOUND, EXIT_OK
from claude_bus.db import connection, init_db
from claude_bus.exceptions import UnknownMessageError


def cmd_ack(
    message_id: int = typer.Argument(..., help="Message id to acknowledge."),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """Mark a message as read. Idempotent.

    Identity-free: doesn't register any alias just to flip status.
    """
    init_db(db)
    with connection(db) as conn:
        try:
            _core.read_by_id(conn, message_id)  # existence check
        except UnknownMessageError:
            typer.echo(f"error: message id={message_id} not found", err=True)
            raise typer.Exit(code=EXIT_MESSAGE_NOT_FOUND) from None
        _core.resolve(conn, message_id)
    typer.echo(f"acked #{message_id}")
    raise typer.Exit(code=EXIT_OK)
