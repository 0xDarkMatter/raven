"""`raven acp` — run an agent under the ACP harness.  LANE: acp-harness.

    raven acp --as lane-3@v0-2 --channel run/v0-2/lane/3 \
              [--channel run/v0-2/control]... [--reply-to run/v0-2/telemetry] \
              [--db PATH] [--poll-interval 1.0] [--budget 2000] [--cwd .] \
              -- <agent command...>

Spawns the agent command (everything after ``--``) with piped stdio,
runs the harness loop, exits with the harness's code. Registration:
one `app.command` line in cli/main.py (the ONLY edit there). CLI error
conventions as the rest of the app (one-line error:, exit 2 usage /
10 error).
"""

from __future__ import annotations


def acp() -> None:  # Typer signature completed by the lane
    """Implemented by the acp-harness lane per the module docstring."""
    raise NotImplementedError


__all__ = ["acp"]
