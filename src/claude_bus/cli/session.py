"""``claude-bus session ...`` — Phase-1 has only ``init``."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import init_db
from claude_bus.cli._common import EXIT_OK

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command("init", help="Initialise the DB and reserve a session id.")
def cmd_session_init(
    session_id: str = typer.Argument(..., help="Opaque session identifier."),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    resolved = init_db(db)
    typer.echo(f"initialised {resolved}")
    typer.echo(f"session id: {session_id} (opaque string; no row tracking in v0.1)")
    raise typer.Exit(code=EXIT_OK)
