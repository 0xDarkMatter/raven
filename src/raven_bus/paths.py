"""DB path resolution — ONE host DB (ADR-002).

Resolution order:

1. Explicit ``db_path`` argument (always wins — tests use tmp_path).
2. ``RAVEN_DB`` environment variable.
3. ``~/.raven/bus.db``  (v2 default — deliberately NOT the v1 per-cwd
   ``./raven.db``; runs share one DB and are namespaced by channel
   prefix, see ADR-002).
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_DB_PATH = "RAVEN_DB"
DEFAULT_DB = Path.home() / ".raven" / "bus.db"


def resolve_db_path(db_path: str | os.PathLike[str] | None = None) -> Path:
    """Return the absolute path to the raven_bus SQLite file.

    Parent directories may not exist; ``db.init_db`` creates them.
    """
    if db_path is not None:
        return Path(db_path).expanduser().resolve()
    env_path = os.environ.get(ENV_DB_PATH)
    if env_path:
        return Path(env_path).expanduser().resolve()
    return DEFAULT_DB


__all__ = ["DEFAULT_DB", "ENV_DB_PATH", "resolve_db_path"]
