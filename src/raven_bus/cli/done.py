"""``raven done`` — mark a held claim complete (terminal)."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import claims, db, models
from raven_bus.cli._common import EXIT_OK, handle_errors


def cmd_done(
    id_: int = typer.Option(..., "--id", help="Message id to complete."),
    as_: str = typer.Option(
        ..., "--as", help="Claim-holding consumer id '<role>@<run>'."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Mark this consumer's leased claim done. Idempotent."""
    with handle_errors():
        models.parse_consumer_id(as_)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            claims.complete(conn, id_, as_)
    typer.echo(f"done #{id_} as {as_}")
    raise typer.Exit(code=EXIT_OK)
