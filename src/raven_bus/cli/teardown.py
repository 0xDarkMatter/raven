"""``raven teardown`` — delete all data for a run (ADR-001's one sanctioned
DELETE besides retention)."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import db, models
from raven_bus.cli._common import EXIT_OK, handle_errors


def cmd_teardown(
    run: str = typer.Option(
        ..., "--run", help="Run id whose channels/messages/claims/cursors to delete."
    ),
    yes: bool = typer.Option(
        False, "--yes", help="Skip the confirmation prompt."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Irreversibly delete every row belonging to ``run``."""
    with handle_errors():
        models.validate_atom(run, what="run")
    if not yes and not typer.confirm(
        f"Delete all data for run {run!r}? This cannot be undone."
    ):
        typer.echo("aborted")
        raise typer.Exit(code=EXIT_OK)
    with handle_errors():
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            removed = db.teardown_run(conn, run)
    typer.echo(f"removed {removed} rows for run {run!r}")
    raise typer.Exit(code=EXIT_OK)
