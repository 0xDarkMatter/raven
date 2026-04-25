"""``claude-bus serve`` — run the optional HTTP bridge."""

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
    except ImportError as exc:
        die(
            "starlette + uvicorn are required. install with: "
            "pip install 'claude-bus[http]'",
        )
        return  # pragma: no cover

    from claude_bus.http import create_app

    db_path = resolve_db_path(db)
    app = create_app(db_path)
    typer.echo(f"claude-bus serve -> {host}:{port} db={db_path}")
    try:
        import uvicorn

        uvicorn.run(app, host=host, port=port, log_level="info")
    except OSError as exc:
        die(f"failed to bind {host}:{port}: {exc}", code=EXIT_HTTP_BIND)
