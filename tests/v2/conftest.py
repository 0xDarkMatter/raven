"""Shared fixtures for raven_bus (v2) tests.  Wave-0 artifact, frozen.

Every test gets an isolated DB under tmp_path with process caches
reset. Lanes add module-specific fixtures in their own test files, not
here.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    """Fresh, initialised v2 DB path, isolated per test."""
    from raven_bus.db import _reset_init_cache, init_db

    _reset_init_cache()
    path = tmp_path / "bus.db"
    init_db(path, force=True)
    return path
