#!/usr/bin/env bash
# raven inbox hook (Claude Code PreToolUse) — LANE: hook (raven2-p3)
#
# Trivial wrapper (ADR-006): run the real logic in
# raven_bus.adapters.hooks.peek, discard stderr, and force exit 0 on
# every path. A broken hook must NEVER error a tool call.
#
# Deliberately NOT `exec python ... || true`: exec replaces this shell,
# so the `|| true` never ran — a missing interpreter exited 127 and
# Claude Code showed a hook-error notice on every tool call. peek's
# stdout (the additionalContext JSON) passes through untouched.
#
# RAVEN_PYTHON picks the interpreter (default: `python` on PATH). It must
# be one raven_bus is installed into — with a `uv tool` install, plain
# `python` usually isn't, and the hook would stay silently quiet.
# Git Bash on Windows is POSIX only — no bashisms.
"${RAVEN_PYTHON:-python}" -m raven_bus.adapters.hooks.peek 2>/dev/null
exit 0
