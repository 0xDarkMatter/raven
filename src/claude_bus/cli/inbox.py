"""``claude-bus inbox`` — list unread messages for a role."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import BusClient
from claude_bus.cli._common import (
    EXIT_OK,
    echo_json,
    echo_messages_human,
    message_to_json,
    parse_address,
)


def cmd_inbox(
    role: str = typer.Option(
        ...,
        "--role",
        "-r",
        help='Role address "<role>:<session>" whose inbox to read.',
    ),
    max: int = typer.Option(
        100, "--max", "-m", help="Maximum messages to return."
    ),
    json_out: bool = typer.Option(
        False, "--json", "-j", help="Emit JSON instead of text."
    ),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """List unread messages for ``--role``."""
    role_name, session_id = parse_address(role)
    client = BusClient(session_id=session_id, role=role_name, db_path=db)
    msgs = client.inbox(max=max)
    if json_out:
        echo_json({"messages": [message_to_json(m) for m in msgs]})
    else:
        echo_messages_human(msgs)
    raise typer.Exit(code=EXIT_OK)
