"""Tests for raven_bus.paths.resolve_db_path's resolution order."""

from __future__ import annotations

from pathlib import Path

import pytest

from raven_bus.paths import DEFAULT_DB, ENV_DB_PATH, resolve_db_path


def test_explicit_db_path_wins(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit.db"
    assert resolve_db_path(explicit) == explicit.expanduser().resolve()


def test_env_var_used_when_no_explicit_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_path = tmp_path / "from-env.db"
    monkeypatch.setenv(ENV_DB_PATH, str(env_path))
    assert resolve_db_path() == env_path.expanduser().resolve()


def test_default_used_when_no_path_or_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_DB_PATH, raising=False)
    assert resolve_db_path() == DEFAULT_DB
