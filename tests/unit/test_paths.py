"""Path resolution + environment variable plumbing."""

from __future__ import annotations

from pathlib import Path

from claude_bus.paths import (
    DB_FILENAME,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    ENV_DB_PATH,
    resolve_db_path,
)


def test_explicit_argument_wins(monkeypatch, tmp_path: Path) -> None:
    """Explicit db_path always takes priority over env + cwd default."""
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "from-env.db"))
    explicit = tmp_path / "explicit.db"
    assert resolve_db_path(explicit) == explicit.resolve()


def test_env_var_overrides_default(monkeypatch, tmp_path: Path) -> None:
    """RAVEN_DB picks the path when no explicit argument given."""
    target = tmp_path / "from-env.db"
    monkeypatch.setenv(ENV_DB_PATH, str(target))
    assert resolve_db_path() == target.resolve()


def test_cwd_default_when_no_arg_no_env(monkeypatch, tmp_path: Path) -> None:
    """Default is `<cwd>/raven.db` when neither arg nor env set."""
    monkeypatch.delenv(ENV_DB_PATH, raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_db_path() == (tmp_path / DB_FILENAME).resolve()


def test_env_var_expands_user_home(monkeypatch, tmp_path: Path) -> None:
    """`~/foo.db` in the env var should expand."""
    monkeypatch.setenv(ENV_DB_PATH, "~/cb-test.db")
    resolved = resolve_db_path()
    assert "~" not in str(resolved)
    assert resolved.name == "cb-test.db"


def test_default_http_constants() -> None:
    """Lock down the documented defaults so accidental changes break a test."""
    assert DEFAULT_HTTP_PORT == 7713
    assert DEFAULT_HTTP_HOST == "127.0.0.1"
