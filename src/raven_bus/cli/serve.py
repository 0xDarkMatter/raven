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
- --port is bounded 1..65535 by typer (70000 was an OverflowError
  traceback from bind()).
- Bind failures are ONE line + exit 10 (QA cli #5). ``_probe_bind``
  binds-and-closes the address first, so a port in use or an
  unresolvable host fails before uvicorn logs anything. The probe can
  race a peer grabbing the port; uvicorn then logs the OSError itself
  and ``sys.exit(1)``s, which the SystemExit arm below still renders as
  the one-line error (after uvicorn's own log line).
"""

from __future__ import annotations

import os
import socket
from ipaddress import ip_address
from pathlib import Path

import typer

from raven_bus import db
from raven_bus.cli._common import EXIT_ERROR, EXIT_USAGE, die
from raven_bus.exceptions import InvalidDbPathError


def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Address to bind."),
    port: int = typer.Option(
        7713, "--port", min=1, max=65535, help="TCP port to bind (1-65535)."
    ),
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
    except InvalidDbPathError as exc:  # a relative RAVEN_DB: usage, like handle_errors maps it
        die(str(exc), EXIT_USAGE)
        return  # pragma: no cover -- die always raises
    except Exception as exc:  # noqa: BLE001 -- any preflight failure renders as the one-line CLI error
        die(str(exc), EXIT_ERROR)
        return  # pragma: no cover -- die always raises

    try:
        _probe_bind(host, port)
    except OSError as exc:
        die(f"cannot bind {host}:{port}: {exc}", EXIT_ERROR)
        return  # pragma: no cover -- die always raises

    try:
        uvicorn.run(create_app(db_path), host=host, port=port)
    except OSError as exc:
        die(f"ravend failed on {host}:{port}: {exc}", EXIT_ERROR)
    except SystemExit as exc:
        # uvicorn reports a startup failure by logging it, then sys.exit().
        die(
            f"ravend failed to start on {host}:{port} "
            f"(uvicorn exit status {exc.code}; see its log above)",
            EXIT_ERROR,
        )


def _probe_bind(host: str, port: int) -> None:
    """Bind-and-close every address ``host`` resolves to, as uvicorn's
    ``asyncio.create_server`` is about to. Raises OSError (incl.
    ``socket.gaierror``) on a port in use / unresolvable host.

    SO_REUSEADDR mirrors asyncio: set on POSIX (so a restart onto a port
    in TIME_WAIT is not refused here but accepted by uvicorn), NOT on
    Windows, where it would let the probe share a port another process
    is listening on — the very case this must catch."""
    infos = socket.getaddrinfo(
        host, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE
    )
    for family, socktype, proto, _canonname, sockaddr in infos:
        with socket.socket(family, socktype, proto) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, int(os.name != "nt"))
            sock.bind(sockaddr)


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = ["serve"]
