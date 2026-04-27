"""Session helpers for raven.

Phase 1 keeps the session model deliberately minimal: a session is
just an opaque string used as part of the address (``"<role>:<sid>"``).
There's no per-session lifecycle row; the DB itself is the only piece
of state that needs initialising.

The Phase 2 plan promotes sessions to first-class entities (tracked
in a ``sessions`` table with ``open_at`` / ``closed_at`` timestamps)
behind the same ``init_session`` / ``teardown_session`` names.
"""

from __future__ import annotations

from pathlib import Path

import structlog

from claude_bus import aliases as alias_mod
from claude_bus.db import connection, init_db

log = structlog.get_logger(__name__)


def register_role_alias(
    session_id: str,
    role: str,
    db_path: str | Path | None = None,
) -> str:
    """Persist the deterministic alias for ``(role, session_id)``.

    Idempotent: re-registering the same pair returns the same alias.
    Returns the internal alias string (``"<role>-<6hex>"``).

    In Phase 1 this is the only "alias" concept available; the
    short-name → canonical-role aliasing described in the spec
    (e.g. ``a`` → ``alice``) is deferred to Phase 2.
    """
    init_db(db_path)
    with connection(db_path) as conn:
        alias = alias_mod.register(conn, role, session_id)
    log.debug(
        "claude_bus.session.register_role_alias",
        session_id=session_id,
        role=role,
        alias=alias,
    )
    return alias


__all__ = ["register_role_alias"]
