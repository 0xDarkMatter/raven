"""``raven teardown`` — delete all data for a run (ADR-001's one sanctioned
DELETE besides retention).

Without ``--yes`` it asks for confirmation ONLY when a human can answer
(stdin is a TTY). Non-interactive (a script, a pipe, CI) it refuses with
a usage error instead: ``typer.confirm`` on a closed stdin hit EOF and
click printed a bare "Aborted." with exit 1 (QA cli #3), which a script
could not tell apart from a real failure. EOF/Ctrl-C AT the prompt maps
to the same refusal: on Windows ``NUL`` reports ``isatty() == True``, so
``raven teardown --run R < NUL`` still reaches the prompt and hits EOF.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
import typer

from raven_bus import db, models
from raven_bus.cli._common import EXIT_OK, EXIT_USAGE, die, handle_errors

_REFUSAL = "refusing to tear down without confirmation (pass --yes)"


def _stdin_is_interactive() -> bool:
    """True when a human can answer the confirmation prompt."""
    stdin = sys.stdin
    return stdin is not None and stdin.isatty()


def cmd_teardown(
    run: str = typer.Option(
        ..., "--run", help="Run id whose channels/messages/claims/cursors to delete."
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Skip the confirmation prompt (required when stdin is not a terminal).",
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Irreversibly delete every row belonging to ``run``."""
    with handle_errors():
        models.validate_atom(run, what="run")
    if not yes:
        if not _stdin_is_interactive():
            die(_REFUSAL, EXIT_USAGE)
        try:
            confirmed = typer.confirm(
                f"Delete all data for run {run!r}? This cannot be undone."
            )
        except click.exceptions.Abort:  # EOF or Ctrl-C at the prompt
            typer.echo("")  # end the dangling prompt line
            die(_REFUSAL, EXIT_USAGE)
            return  # pragma: no cover - die() always raises typer.Exit
        if not confirmed:
            typer.echo("aborted")
            raise typer.Exit(code=EXIT_OK)
    with handle_errors():
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            removed = db.teardown_run(conn, run)
    typer.echo(f"removed {removed} rows for run {run!r}")
    raise typer.Exit(code=EXIT_OK)
