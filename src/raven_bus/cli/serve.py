"""`raven serve` — run ravend under uvicorn.  LANE: http-sse (raven2-p2).

    raven serve [--host 127.0.0.1] [--port 7713] [--db PATH]
                [--yes-expose]

- Missing [http] extra → one-line `error: ...` + exit 10 (match the
  CLI error conventions; do not traceback).
- Preflight: db.init_db(db) before binding, so a bad path fails fast
  with the CLI's normal error rendering rather than mid-request.
- Non-loopback --host is REFUSED (usage error, exit 2) unless
  --yes-expose is also given — ADR-005's "loopback only, no auth"
  posture must not be defeatable by one mistyped flag (opus verify
  finding). With --yes-expose it binds and prints the no-auth warning.
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
    yes_expose: bool = typer.Option(
        False,
        "--yes-expose",
        help="Required to bind a non-loopback host (ravend has no auth).",
    ),
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

    if not _is_loopback(host):
        if not yes_expose:
            die(
                f"refusing to bind non-loopback host {host!r}: ravend has no "
                "in-process authentication (ADR-005). Pass --yes-expose to "
                "override, and terminate TLS/auth at a reverse proxy.",
                2,
            )
            return  # pragma: no cover -- die always raises
        typer.echo(
            f"warning: serving on non-loopback host {host!r}; "
            "ravend has no in-process authentication",
            err=True,
        )

    try:
        db.init_db(db_path)
    except Exception as exc:  # noqa: BLE001 -- any preflight failure renders as the one-line CLI error
        die(str(exc), EXIT_ERROR)
        return  # pragma: no cover -- die always raises

    uvicorn.run(create_app(db_path), host=host, port=port)


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = ["serve"]
