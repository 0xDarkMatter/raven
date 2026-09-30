"""Packaging guards for pyproject.toml (ADR-004).

Mechanical gates for two things that drifted before: the version was
hard-coded in pyproject (``0.1.1``) while the package said ``0.2.0.dev0``,
so pip/uv/importlib.metadata reported the wrong version; and runtime
dependencies outlived the v1 code that imported them.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import raven_bus

_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_version_is_single_sourced_from_package() -> None:
    project = _PYPROJECT["project"]
    assert "version" not in project, "hard-coded version: use dynamic + [tool.hatch.version]"
    assert "version" in project["dynamic"]

    path = _PYPROJECT["tool"]["hatch"]["version"]["path"]
    source = (_ROOT / path).read_text(encoding="utf-8")
    # hatchling's default regex source: `__version__ = "<version>"`.
    match = re.search(r'^__version__ = "(?P<v>[^"]+)"$', source, re.MULTILINE)
    assert match is not None
    assert match["v"] == raven_bus.__version__


def test_runtime_dependencies_are_imported_by_src() -> None:
    # A runtime dep nothing in src/ imports is dead weight on every install
    # (structlog/pyyaml were v1-only). Map dist name -> import name.
    import_names = {"pydantic": "pydantic", "typer": "typer"}
    src = "\n".join(
        p.read_text(encoding="utf-8") for p in (_ROOT / "src" / "raven_bus").rglob("*.py")
    )
    for dep in _PYPROJECT["project"]["dependencies"]:
        dist = re.split(r"[<>=!~\[; ]", dep, maxsplit=1)[0]
        mod = import_names.get(dist)
        assert mod is not None, f"new runtime dep {dist!r}: add it to import_names"
        assert re.search(rf"^\s*(import|from) {mod}\b", src, re.MULTILINE), (
            f"runtime dependency {dist!r} is not imported anywhere in src/raven_bus"
        )
