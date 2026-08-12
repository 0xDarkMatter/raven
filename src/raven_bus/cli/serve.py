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


def serve() -> None:  # signature completed by the lane to Typer conventions
    """Implemented by the http-sse lane per the module docstring."""
    raise NotImplementedError


__all__ = ["serve"]
