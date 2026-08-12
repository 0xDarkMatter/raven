"""``raven channels`` — list the channel registry."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import channels, db
from raven_bus.cli._common import EXIT_OK, echo_json, handle_errors, model_to_json


def cmd_channels(
    prefix: str | None = typer.Option(
        None, "--prefix", help="Only list channels whose name starts with this."
    ),
    json_out: bool = typer.Option(
        False, "-j", "--json", help="Emit JSON instead of text."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """List channels, name-ordered."""
    with handle_errors():
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            chans = channels.list_channels(conn, prefix=prefix)
    if json_out:
        echo_json([model_to_json(c) for c in chans])
        raise typer.Exit(code=EXIT_OK)
    if not chans:
        typer.echo("(no channels)")
        raise typer.Exit(code=EXIT_OK)
    for c in chans:
        typer.echo(f"{c.name}  kind={c.kind}  max_deliveries={c.max_deliveries}")
    raise typer.Exit(code=EXIT_OK)
