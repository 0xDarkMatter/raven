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
