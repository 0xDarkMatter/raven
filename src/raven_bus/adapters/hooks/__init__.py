"""Claude Code PreToolUse inbox hook.  LANE: hook (raven2-p3).

A peek-only adapter (ADR-006): on every tool call it reads the consumer's
pending messages and emits :func:`raven_bus.policy.render_hint`'s bounded
pull notice as PreToolUse ``additionalContext`` JSON when any are due —
never the message bodies (the agent pulls those with ``raven read``).
Silent when nothing is due; always exits 0. The real
logic lives in :mod:`raven_bus.adapters.hooks.peek` (runnable as
``python -m raven_bus.adapters.hooks.peek``); the shell wrapper in this
directory runs that module with stderr discarded and forces exit 0, so
even a missing interpreter can't error a tool call.

This package intentionally re-exports nothing: ``peek`` is both the
submodule name and a function within it, so re-exporting the function
here would shadow the submodule on ``import …hooks.peek``. Import the
submodule directly when you need the callable.
"""

from __future__ import annotations

__all__: list[str] = []
