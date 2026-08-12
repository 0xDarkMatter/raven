"""``raven ack`` — advance a broadcast cursor (jump-ack, ADR-001)."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import cursors, db, models
from raven_bus.cli._common import EXIT_OK, handle_errors


def cmd_ack(
    channel: str = typer.Option(..., "--channel", help="Channel to ack on."),
    as_: str = typer.Option(
        ..., "--as", help="Acking consumer id '<role>@<run>'."
    ),
    up_to: int = typer.Option(
        ..., "--up-to", help="Advance the cursor to (at least) this message id."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Advance a consumer's cursor on a broadcast channel."""
    with handle_errors():
        models.parse_consumer_id(as_)
        models.validate_channel_name(channel)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            cursor = cursors.ack(conn, as_, channel, up_to)
    typer.echo(f"acked {as_} on {channel} up_to={cursor.last_ack_id}")
    raise typer.Exit(code=EXIT_OK)
