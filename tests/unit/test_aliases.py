"""Alias registry — deterministic identities."""

from __future__ import annotations

from pathlib import Path

from claude_bus import aliases as alias_mod
from claude_bus import register_role_alias
from claude_bus.db import connection


def test_compute_alias_is_deterministic() -> None:
    a = alias_mod.compute_alias("scout", "sess-1")
    b = alias_mod.compute_alias("scout", "sess-1")
    assert a == b
    assert a.startswith("scout-")
    assert len(a) == len("scout-") + 6


def test_compute_alias_distinguishes_sessions() -> None:
    a = alias_mod.compute_alias("scout", "sess-1")
    b = alias_mod.compute_alias("scout", "sess-2")
    assert a != b


def test_register_role_alias_is_idempotent(db_path: Path) -> None:
    first = register_role_alias("sess-1", "scout", db_path)
    second = register_role_alias("sess-1", "scout", db_path)
    assert first == second

    with connection(db_path) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM aliases WHERE role='scout' AND session_id='sess-1'"
        ).fetchone()
        assert rows[0] == 1


def test_register_role_alias_resolvable(db_path: Path) -> None:
    alias = register_role_alias("sess-1", "scout", db_path)
    with connection(db_path) as conn:
        resolved = alias_mod.resolve(conn, alias)
        assert resolved is not None
        assert resolved.role == "scout"
        assert resolved.session_id == "sess-1"


def test_resolve_unknown_alias_returns_none(db_path: Path) -> None:
    with connection(db_path) as conn:
        assert alias_mod.resolve(conn, "never-registered") is None


def test_list_by_role_within_session(db_path: Path) -> None:
    """list_by_role returns aliases of a role scoped to a single session."""
    with connection(db_path) as conn:
        alias_mod.register(conn, "consumer", "sA")
        # Same role in a different session must be excluded.
        alias_mod.register(conn, "consumer", "sB")
        rows = alias_mod.list_by_role(conn, "consumer", "sA")
        assert len(rows) == 1
        assert rows[0].session_id == "sA"
        assert rows[0].role == "consumer"


def test_list_by_role_unknown_role_empty(db_path: Path) -> None:
    with connection(db_path) as conn:
        rows = alias_mod.list_by_role(conn, "ghost", "sA")
        assert rows == []


def test_list_by_session_returns_all_roles_in_session(db_path: Path) -> None:
    with connection(db_path) as conn:
        alias_mod.register(conn, "alice", "sA")
        alias_mod.register(conn, "bob", "sA")
        alias_mod.register(conn, "charlie", "sA")
        alias_mod.register(conn, "alice", "other")  # excluded
        rows = alias_mod.list_by_session(conn, "sA")
        assert len(rows) == 3
        roles = {r.role for r in rows}
        assert roles == {"alice", "bob", "charlie"}


def test_list_by_session_unknown_session_empty(db_path: Path) -> None:
    with connection(db_path) as conn:
        assert alias_mod.list_by_session(conn, "never-registered") == []
