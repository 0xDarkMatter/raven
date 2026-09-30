"""``raven doctor`` — db reachable, schema version, WAL, runs one real sweep (reaps lapsed leases) and reports it."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import typer

from raven_bus import db
from raven_bus.cli._common import EXIT_ERROR, EXIT_OK
from raven_bus.paths import resolve_db_path


def cmd_doctor(
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Run a small battery of operational checks."""
    resolved = resolve_db_path(db_path)
    checks: list[tuple[str, bool, str]] = []
    try:
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM bus_meta WHERE key = 'schema_version'"
            ).fetchone()
            version = row[0] if row is not None else None
            checks.append(
                ("db", version is not None, f"reachable at {resolved} (schema_version={version})")
            )
            mode_row = conn.execute("PRAGMA journal_mode").fetchone()
            mode = mode_row[0] if mode_row is not None else "unknown"
            checks.append(("wal", str(mode).lower() == "wal", f"journal_mode={mode}"))
            result = db.sweep(conn)
            sweep_detail = (
                f"expired={result.expired} requeued={result.requeued} "
                f"dead_lettered={result.dead_lettered}"
            )
            checks.append(("sweep", True, sweep_detail))
    except (OSError, sqlite3.DatabaseError) as exc:
        checks.append(("db", False, f"unreachable: {exc}"))

    all_ok = True
    for name, ok, detail in checks:
        marker = "[ok]" if ok else "[fail]"
        if not ok:
            all_ok = False
        typer.echo(f"  {marker:<7} {name:<8} {detail}")

    typer.echo("")
    if all_ok:
        typer.echo("all checks passed")
        raise typer.Exit(code=EXIT_OK)
    typer.echo("one or more checks failed")
    raise typer.Exit(code=EXIT_ERROR)
