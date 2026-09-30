"""``raven doctor`` — db reachable, schema version, WAL, runs one real sweep (reaps lapsed leases) and reports it.

Checks are ok / warn / fail; only a fail exits non-zero. The one warn
today: the DB file did not exist, so ``init_db`` just created it (and
any missing parent dirs). It still creates — doctor on a fresh machine
must pass — but a typo'd ``--db``/RAVEN_DB used to report "all checks
passed" against a brand-new empty DB, hiding the typo (QA cli #4).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import typer

from raven_bus import db
from raven_bus.cli._common import EXIT_ERROR, EXIT_OK, handle_errors
from raven_bus.exceptions import SchemaMismatchError
from raven_bus.paths import resolve_db_path

_MARKERS = {"ok": "[ok]", "warn": "[warn]", "fail": "[fail]"}


def cmd_doctor(
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Run a small battery of operational checks."""
    with handle_errors():  # a relative RAVEN_DB is a usage error, not a failed check
        resolved = resolve_db_path(db_path)
    existed = resolved.exists()
    checks: list[tuple[str, str, str]] = []  # (name, "ok"|"warn"|"fail", detail)
    try:
        db.init_db(db_path)
        if not existed:
            # ASCII on purpose: piped stdout on Windows is cp1252/OEM, where an
            # em dash turns into mojibake in logs.
            checks.append(("db", "warn", f"did not exist - created it at {resolved}"))
        with db.connection(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM bus_meta WHERE key = 'schema_version'"
            ).fetchone()
            version = row[0] if row is not None else None
            checks.append(
                (
                    "db",
                    "ok" if version is not None else "fail",
                    f"reachable at {resolved} (schema_version={version})",
                )
            )
            mode_row = conn.execute("PRAGMA journal_mode").fetchone()
            mode = mode_row[0] if mode_row is not None else "unknown"
            checks.append(
                ("wal", "ok" if str(mode).lower() == "wal" else "fail", f"journal_mode={mode}")
            )
            result = db.sweep(conn, count_expired=True)
            sweep_detail = (
                f"expired={result.expired} requeued={result.requeued} "
                f"dead_lettered={result.dead_lettered}"
            )
            checks.append(("sweep", "ok", sweep_detail))
    except (OSError, sqlite3.DatabaseError) as exc:
        checks.append(("db", "fail", f"unreachable: {exc}"))
    except SchemaMismatchError as exc:
        # init_db refuses foreign/unstamped files; a check failure, not a crash.
        checks.append(("db", "fail", f"schema mismatch: {exc}"))

    for name, status, detail in checks:
        typer.echo(f"  {_MARKERS[status]:<7} {name:<8} {detail}")

    typer.echo("")
    statuses = [status for _, status, _ in checks]
    if "fail" in statuses:
        typer.echo("one or more checks failed")
        raise typer.Exit(code=EXIT_ERROR)
    typer.echo("all checks passed (with warnings)" if "warn" in statuses else "all checks passed")
    raise typer.Exit(code=EXIT_OK)
