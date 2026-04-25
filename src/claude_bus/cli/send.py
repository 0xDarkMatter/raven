"""``claude-bus send`` — send a typed message between two role addresses."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import BusClient
from claude_bus.cli._common import (
    EXIT_OK,
    EXIT_SCHEMA_VALIDATION,
    parse_address,
    parse_body,
)
from claude_bus.exceptions import SchemaValidationError


def cmd_send(
    from_: str = typer.Option(..., "--from", help='Sender address "<role>:<session>".'),
    to: str = typer.Option(..., "--to", help='Recipient address "<role>:<session>".'),
    type: str = typer.Option(..., "--type", "-t", help="Message type / kind."),
    body: str | None = typer.Option(
        None, "--body", help="JSON body string (mutually exclusive with --body-file)."
    ),
    body_file: Path | None = typer.Option(
        None,
        "--body-file",
        help="Path to a JSON file containing the body.",
        exists=True,
        readable=True,
    ),
    correlation_id: int | None = typer.Option(
        None, "--correlation-id", help="Group this message with a conversation id."
    ),
    reply_to: int | None = typer.Option(
        None, "--reply-to", help="The id of the message this is replying to."
    ),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """Send a message and print the new id."""
    payload = parse_body(body, body_file)
    sender_role, sender_session = parse_address(from_)
    parse_address(to)  # validate format

    client = BusClient(session_id=sender_session, role=sender_role, db_path=db)
    try:
        msg = client.send(
            to=to,
            type=type,
            body=payload,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )
    except SchemaValidationError as exc:
        typer.echo(f"error: schema validation failed: {exc}", err=True)
        raise typer.Exit(code=EXIT_SCHEMA_VALIDATION) from exc

    typer.echo(f"sent #{msg.id} {msg.sender} -> {msg.recipient} type={msg.type}")
    raise typer.Exit(code=EXIT_OK)
