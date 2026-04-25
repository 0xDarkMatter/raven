"""Database path resolver for claude-bus.

Resolution order:

1. Explicit ``db_path`` argument (always wins).
2. ``CLAUDE_BUS_DB`` environment variable.
3. ``./claude-bus.db`` in the current working directory.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_DB_PATH = "CLAUDE_BUS_DB"
"""Environment variable callers may set to force a specific DB path."""

DB_FILENAME = "claude-bus.db"
"""Default database filename in the current working directory."""

DEFAULT_HTTP_PORT = 7713
"""Default port for the optional ``claude-bus serve`` HTTP bridge."""

DEFAULT_HTTP_HOST = "127.0.0.1"
"""Default host (loopback only — no auth assumed on the bridge)."""


def resolve_db_path(db_path: str | os.PathLike[str] | None = None) -> Path:
    """Return the path to the claude-bus SQLite file.

    Returns an absolute path. The parent directory may not yet exist —
    callers responsible for creating it should use :func:`claude_bus.db.init_db`,
    which handles ``mkdir(parents=True, exist_ok=True)`` for them.
    """
    if db_path is not None:
        return Path(db_path).expanduser().resolve()

    env_path = os.environ.get(ENV_DB_PATH)
    if env_path:
        return Path(env_path).expanduser().resolve()

    return (Path.cwd() / DB_FILENAME).resolve()


__all__ = [
    "DB_FILENAME",
    "DEFAULT_HTTP_HOST",
    "DEFAULT_HTTP_PORT",
    "ENV_DB_PATH",
    "resolve_db_path",
]
