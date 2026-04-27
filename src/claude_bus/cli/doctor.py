"""``raven doctor`` — environment health checks."""

from __future__ import annotations

import importlib.util
import shutil
import socket
import sqlite3
from pathlib import Path

import typer

from claude_bus import init_db
from claude_bus.cli._common import EXIT_GENERIC, EXIT_OK
from claude_bus.db import _INITIAL_MIGRATION
from claude_bus.paths import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, resolve_db_path


def _check_migration_bundle() -> tuple[bool, str]:
    """The 0001 migration must ship with the package install."""
    if not _INITIAL_MIGRATION.exists():
        return (
            False,
            f"migration file missing at {_INITIAL_MIGRATION} — "
            "the package install is incomplete; reinstall raven",
        )
    return True, f"migration bundled at {_INITIAL_MIGRATION.name}"


def _check_db(path: Path) -> tuple[bool, str]:
    try:
        init_db(path)
        with sqlite3.connect(path) as conn:
            row = conn.execute(
                "SELECT value FROM bus_meta WHERE key = 'schema_version'"
            ).fetchone()
        if row is None:
            return False, f"db at {path} has no schema_version row"
        return True, f"db at {path} ok (schema_version={row[0]})"
    except (OSError, sqlite3.DatabaseError) as exc:
        return False, f"db at {path} unreachable: {exc}"


def _check_port(host: str, port: int) -> tuple[bool, str]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            sock.bind((host, port))
        return True, f"port {host}:{port} free"
    except OSError as exc:
        return False, f"port {host}:{port} not bindable: {exc}"


def _check_http_extra() -> tuple[bool, str]:
    starlette = importlib.util.find_spec("starlette")
    uvicorn = importlib.util.find_spec("uvicorn")
    if starlette is None or uvicorn is None:
        return (
            False,
            "starlette + uvicorn not installed (install with `pip install "
            "raven[http]` to use `raven serve`)",
        )
    return True, "[http] extra installed"


def _check_disk(path: Path) -> tuple[bool, str]:
    parent = path.parent if path.parent.exists() else Path.cwd()
    usage = shutil.disk_usage(parent)
    free_mb = usage.free / (1024 * 1024)
    if free_mb < 100:  # pragma: no cover -- assertable only with a near-full disk
        return False, f"only {free_mb:.0f}MiB free at {parent}"
    return True, f"{free_mb:.0f}MiB free at {parent}"


def cmd_doctor(
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
    port: int = typer.Option(
        DEFAULT_HTTP_PORT, "--port", help="HTTP port to check availability of."
    ),
    host: str = typer.Option(DEFAULT_HTTP_HOST, "--host", help="HTTP host."),
) -> None:
    """Run a small battery of operational checks."""
    db_path = resolve_db_path(db)

    checks = [
        ("install", _check_migration_bundle()),
        ("db", _check_db(db_path)),
        ("disk", _check_disk(db_path)),
        ("port", _check_port(host, port)),
        ("http extra", _check_http_extra()),
    ]

    all_ok = True
    for name, (ok, detail) in checks:
        marker = "[ok]" if ok else "[fail]"
        if not ok:
            all_ok = False
        typer.echo(f"  {marker:<7} {name:<14} {detail}")

    typer.echo("")
    if all_ok:
        typer.echo("all checks passed")
        raise typer.Exit(code=EXIT_OK)
    typer.echo("one or more checks failed")
    raise typer.Exit(code=EXIT_GENERIC)
