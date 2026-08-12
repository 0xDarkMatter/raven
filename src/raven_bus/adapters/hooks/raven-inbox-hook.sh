#!/usr/bin/env bash
# raven inbox hook (Claude Code PreToolUse) — LANE: hook (raven2-p3)
#
# Trivial wrapper (ADR-006): exec the real logic in
# raven_bus.adapters.hooks.peek, discard stderr, and coerce any
# failure (even a missing interpreter) to exit 0. A broken hook must
# NEVER block a tool call. Git Bash on Windows is POSIX + exec only —
# no bashisms beyond those.
exec python -m raven_bus.adapters.hooks.peek 2>/dev/null || true
