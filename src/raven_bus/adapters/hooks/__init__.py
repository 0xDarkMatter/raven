"""Claude Code PreToolUse inbox hook.  LANE: hook (raven2-p3).

A peek-only adapter (ADR-006): on every tool call it reads the consumer's
pending messages, renders them via :mod:`raven_bus.policy`, and prints a
compact block when any are deliverable. Silent when the inbox is empty;
always exits 0. The real logic lives in
:mod:`raven_bus.adapters.hooks.peek` (runnable as
``python -m raven_bus.adapters.hooks.peek``); the shell wrapper in this
directory execs that module with stderr discarded so even an interpreter
failure can't block a tool call.

This package intentionally re-exports nothing: ``peek`` is both the
submodule name and a function within it, so re-exporting the function
here would shadow the submodule on ``import …hooks.peek``. Import the
submodule directly when you need the callable.
"""

from __future__ import annotations

__all__: list[str] = []
