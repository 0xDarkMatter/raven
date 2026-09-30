"""DB path resolution — ONE host DB (ADR-002).

Resolution order:

1. Explicit ``db_path`` argument (always wins — tests use tmp_path).
   Relative values resolve against the cwd, like any file argument.
2. ``RAVEN_DB`` environment variable — MUST be absolute after ``~``
   expansion, or :class:`InvalidDbPathError` (ADR-002 amendment, QA S13).
3. ``~/.raven/bus.db``  (v2 default — deliberately NOT the v1 per-cwd
   ``./raven.db``; runs share one DB and are namespaced by channel
   prefix, see ADR-002).

This module is the single enforcement point: every store entry (``db``,
``doctor``, ravend's startup, the hook) resolves through it.
"""

from __future__ import annotations

import os
from pathlib import Path

from raven_bus.exceptions import InvalidDbPathError

ENV_DB_PATH = "RAVEN_DB"
DEFAULT_DB = Path.home() / ".raven" / "bus.db"


def resolve_db_path(db_path: str | os.PathLike[str] | None = None) -> Path:
    """Return the absolute path to the raven_bus SQLite file.

    Parent directories may not exist; ``db.init_db`` creates them.

    Raises :class:`InvalidDbPathError` when ``db_path`` is None and
    ``RAVEN_DB`` is relative. An explicit relative ``db_path`` is fine.
    """
    if db_path is not None:
        return Path(db_path).expanduser().resolve()
    env_path = os.environ.get(ENV_DB_PATH)
    if env_path:
        expanded = Path(env_path).expanduser()
        # Guard: do not "helpfully" resolve a relative RAVEN_DB. The variable
        # is inherited by every child process (lanes in different worktrees),
        # so resolving it per cwd silently gave one run several DBs - the v1
        # per-cwd split ADR-002 exists to prevent. Windows "C:x" and "\x" are
        # not absolute either (they depend on the current drive/dir).
        if not expanded.is_absolute():
            raise InvalidDbPathError(
                f"RAVEN_DB must be an absolute path (got {env_path!r}); a relative "
                "one resolves against each process's working directory, giving "
                "one run several DBs"
            )
        return expanded.resolve()
    return DEFAULT_DB


__all__ = ["DEFAULT_DB", "ENV_DB_PATH", "resolve_db_path"]
