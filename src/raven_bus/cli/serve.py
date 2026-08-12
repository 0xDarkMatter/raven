"""`raven serve` — run ravend under uvicorn.  LANE: http-sse (raven2-p2).

    raven serve [--host 127.0.0.1] [--port 7713] [--db PATH]

- Missing [http] extra → one-line `error: ...` + exit 10 (match the
  CLI error conventions; do not traceback).
- Preflight: db.init_db(db) before binding, so a bad path fails fast
  with the CLI's normal error rendering rather than mid-request.
- Loopback default per ADR-005; a non-loopback --host is allowed but
  prints a one-line warning to stderr (no auth in-process).
- Registration wiring: add exactly one `app.command()` registration for
  this command in cli/main.py (the ONLY edit this lane makes there).
"""

from __future__ import annotations

from ipaddress import ip_address
from pathlib import Path

import typer

from raven_bus import db
from raven_bus.cli._common import EXIT_ERROR, die


def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Address to bind."),
    port: int = typer.Option(7713, "--port", help="TCP port to bind."),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Run the loopback HTTP bridge under uvicorn."""
    try:
        import uvicorn

        from raven_bus.http.app import create_app
    except ImportError:
        die(
            "raven serve requires the [http] extra: pip install -e '.[http]'",
            EXIT_ERROR,
        )
        return  # pragma: no cover -- die always raises

    try:
        db.init_db(db_path)
    except Exception as exc:
        die(str(exc), EXIT_ERROR)
        return  # pragma: no cover -- die always raises

    if not _is_loopback(host):
        typer.echo(
            f"warning: serving on non-loopback host {host!r}; "
            "ravend has no in-process authentication",
            err=True,
        )

    uvicorn.run(create_app(db_path), host=host, port=port)


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = ["serve"]
