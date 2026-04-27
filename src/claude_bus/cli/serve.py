"""``raven serve`` — run the optional HTTP bridge."""

from __future__ import annotations

from pathlib import Path

import typer

from claude_bus.cli._common import EXIT_HTTP_BIND, die
from claude_bus.paths import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, resolve_db_path


def cmd_serve(
    port: int = typer.Option(DEFAULT_HTTP_PORT, "--port", help="HTTP port."),
    host: str = typer.Option(DEFAULT_HTTP_HOST, "--host", help="Bind host."),
    db: Path | None = typer.Option(None, "--db", help="DB path override."),
) -> None:
    """Start the read-focused HTTP bridge (requires the ``[http]`` extra)."""
    try:
        import uvicorn  # noqa: F401  -- imported for availability check
    except ImportError:
        die(
            "starlette + uvicorn are required. install with: "
            "pip install 'raven[http]'",
        )
        return  # pragma: no cover

    db_path = resolve_db_path(db)

    # Preflight: prove the DB is reachable + writable BEFORE binding the
    # port. Otherwise we'd happily start serving and fail on the first
    # request — confusing for ops.
    from claude_bus import init_db
    from claude_bus.cli._common import EXIT_DB

    try:
        init_db(db_path)
    except FileNotFoundError as exc:
        die(
            f"DB preflight failed: missing migration file ({exc.filename or exc}). "
            "Reinstall raven.",
            code=EXIT_DB,
        )
        return  # pragma: no cover
    except PermissionError as exc:  # pragma: no cover -- platform-specific; covered indirectly via test_serve_preflights_unwritable_db
        die(
            f"DB preflight failed: cannot write to {db_path} ({exc}). "
            "Check directory permissions.",
            code=EXIT_DB,
        )
        return
    except OSError as exc:
        die(f"DB preflight failed at {db_path}: {exc}", code=EXIT_DB)
        return  # pragma: no cover

    from claude_bus.http import create_app

    app = create_app(db_path)
    typer.echo(f"raven serve -> {host}:{port} db={db_path}")
    try:
        import uvicorn

        uvicorn.run(app, host=host, port=port, log_level="info")
    except OSError as exc:
        die(f"failed to bind {host}:{port}: {exc}", code=EXIT_HTTP_BIND)
