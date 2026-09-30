"""Mechanical gates for AGENTS.md's landmines — prose made enforceable.

Each test pins one invariant AGENTS.md calls a "build-breaker" so a cold
agent that violates it fails the suite instead of shipping. They read the
source, not the runtime: SQL writers are found by scanning NON-docstring
string literals (a docstring may quote ``INSERT INTO claims`` harmlessly),
and purity rules by walking the AST. When one of these fails, the fix is
almost never to widen the allowlist — read the landmine first.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "raven_bus"


def _docstring_nodes(tree: ast.Module) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def _code_strings(path: Path) -> str:
    """Every string literal in ``path`` except docstrings, joined."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    skip = _docstring_nodes(tree)
    parts = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip
    ]
    return "\n".join(parts)


def _writers(pattern: str) -> set[str]:
    """Module paths (relative to raven_bus/) whose SQL matches ``pattern``."""
    rx = re.compile(pattern, re.IGNORECASE)
    return {
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if rx.search(_code_strings(path))
    }


# --------------------------------------------------------------------------- #
# Store single-writer rules (AGENTS.md "Landmines (ADR-001)").
# --------------------------------------------------------------------------- #
def test_messages_is_append_only():
    """log.append is the sole INSERT; only teardown_run deletes; nothing updates."""
    assert _writers(r"\bINSERT\s+INTO\s+messages\b") == {"log.py"}
    assert _writers(r"\bUPDATE\s+messages\b") == set()
    assert _writers(r"\bDELETE\s+FROM\s+messages\b") == {"db.py"}


def test_consumers_has_one_writer():
    assert _writers(r"\bINSERT\s+INTO\s+consumers\b|\bUPDATE\s+consumers\b") == {"consumers.py"}
    assert _writers(r"\bDELETE\s+FROM\s+consumers\b") == {"db.py"}


def test_cursors_written_only_by_cursors_and_teardown():
    assert _writers(r"\bINSERT\s+INTO\s+cursors\b|\bUPDATE\s+cursors\b") == {"cursors.py"}
    assert _writers(r"\bDELETE\s+FROM\s+cursors\b") == {"db.py"}


def test_claims_written_only_by_claims_and_db():
    """claims.py owns claim/renew/complete/release; db.py owns sweep + teardown."""
    assert _writers(r"\bINSERT\s+INTO\s+claims\b") == {"claims.py"}
    assert _writers(r"\bUPDATE\s+claims\b") == {"claims.py", "db.py"}
    assert _writers(r"\bDELETE\s+FROM\s+claims\b") == {"db.py"}


# --------------------------------------------------------------------------- #
# Adapter rules (AGENTS.md "Landmines — adapters").
# --------------------------------------------------------------------------- #
_IMPURE_MODULES = {"os", "random", "secrets", "sqlite3", "subprocess", "time", "pathlib"}
_IMPURE_CALLS = {"now", "utcnow", "today", "open", "time", "urandom"}


def test_policy_is_pure():
    """No clock reads, I/O or randomness in policy.py — `now` is an input."""
    tree = ast.parse((SRC / "policy.py").read_text(encoding="utf-8"))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            offenders += [a.name for a in node.names if a.name.split(".")[0] in _IMPURE_MODULES]
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] in _IMPURE_MODULES:
                offenders.append(node.module)
        elif isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            if name in _IMPURE_CALLS:
                offenders.append(f"{name}() at line {node.lineno}")
    assert offenders == []


def test_hook_never_acks():
    """The hook peeks only; an .ack( call anywhere under adapters/hooks/ is a defect."""
    calls = [
        f"{path.name}:{node.lineno}"
        for path in (SRC / "adapters" / "hooks").glob("*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "ack"
    ]
    assert calls == []
