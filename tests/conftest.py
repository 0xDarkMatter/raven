"""Shared test fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from claude_bus import init_db
from claude_bus.schemas import SchemaRegistry


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Initialised, empty claude-bus DB at a per-test path."""
    target = tmp_path / "claude-bus.db"
    init_db(target)
    return target


@pytest.fixture(autouse=True)
def _reset_schema_registry() -> Iterator[None]:
    """Keep tests independent — clear registered schemas between tests."""
    yield
    SchemaRegistry.clear()
    SchemaRegistry.strict_mode(False)
