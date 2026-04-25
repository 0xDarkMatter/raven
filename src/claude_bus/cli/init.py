"""``claude-bus init`` — scaffold a project directory."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus import init_db
from claude_bus.cli._common import EXIT_OK, die
from claude_bus.paths import DB_FILENAME

CONFIG_FILENAME = "claude-bus.yaml"

DEFAULT_CONFIG = """\
# claude-bus configuration file.
# Override values here or via environment variables (CLAUDE_BUS_DB, etc.).

db_path: ./claude-bus.db

http:
  enabled: false
  host: 127.0.0.1
  port: 7713

schema:
  strict_mode: false      # reject unknown message types

log_level: INFO           # DEBUG | INFO | WARN | ERROR
"""


def cmd_init(
    force: bool = typer.Option(
        False, "--force", "-f", help="Overwrite an existing config file."
    ),
    db: Path | None = typer.Option(
        None, "--db", help="Override db path; defaults to ./claude-bus.db."
    ),
) -> None:
    """Scaffold ``claude-bus.yaml`` and the SQLite DB in the current directory."""
    config_path = Path(CONFIG_FILENAME)
    if config_path.exists() and not force:
        die(
            f"{CONFIG_FILENAME} already exists; pass --force to overwrite",
        )
    config_path.write_text(DEFAULT_CONFIG, encoding="utf-8")

    db_path = db if db is not None else Path(DB_FILENAME)
    resolved = init_db(db_path)

    typer.echo(f"wrote {config_path}")
    typer.echo(f"initialised {resolved}")
    typer.echo("ready. try: claude-bus doctor")
    raise typer.Exit(code=EXIT_OK)
