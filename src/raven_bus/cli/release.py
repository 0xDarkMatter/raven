"""``raven release`` — voluntarily give back a held claim."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import claims, db, models
from raven_bus.cli._common import EXIT_OK, MAX_ID, handle_errors


def cmd_release(
    id_: int = typer.Option(
        ..., "--id", min=1, max=MAX_ID, help="Message id to release."
    ),
    as_: str = typer.Option(
        ..., "--as", help="Claim-holding consumer id '<role>@<run>'."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Release a leased message back to the queue, deliveries reset to 0 (does not count toward dead-lettering)."""
    with handle_errors():
        models.parse_consumer_id(as_)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            claims.release(conn, id_, as_)
    typer.echo(f"released #{id_} as {as_}")
    raise typer.Exit(code=EXIT_OK)
