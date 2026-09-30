"""Tests for raven_bus.paths.resolve_db_path's resolution order."""

from __future__ import annotations

from pathlib import Path

import pytest

from raven_bus.exceptions import InvalidDbPathError
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


# "C:bus.db" is drive-relative and "\bus.db" root-relative on Windows (both
# depend on the process's current drive/dir); on POSIX both are plain relative
# filenames. Either way they must be rejected.
@pytest.mark.parametrize("relative", ["bus.db", "runs/bus.db", "./bus.db", "C:bus.db", r"\bus.db"])
def test_relative_raven_db_env_is_rejected_not_resolved_per_cwd(
    relative: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """QA S13: RAVEN_DB is inherited by every child process, and a relative
    value resolved against each child's cwd - lanes in different worktrees
    silently wrote to different DBs (the v1 per-cwd split ADR-002 removed)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(ENV_DB_PATH, relative)
    with pytest.raises(InvalidDbPathError, match="RAVEN_DB must be an absolute path"):
        resolve_db_path()


def test_home_relative_raven_db_env_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """``~`` is expanded BEFORE the absolute check - "~/x" names one file
    whatever the cwd."""
    monkeypatch.setenv(ENV_DB_PATH, "~/raven-s13/bus.db")
    assert resolve_db_path() == (Path.home() / "raven-s13" / "bus.db").resolve()


def test_relative_explicit_db_path_stays_cwd_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--db`` / ``db_path`` is a per-invocation file argument (documented:
    ``raven tail --db incident.db``), so it keeps normal cwd-relative
    meaning - and, winning, it never consults a bad RAVEN_DB."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(ENV_DB_PATH, "not-absolute.db")
    assert resolve_db_path("incident.db") == (tmp_path / "incident.db").resolve()
