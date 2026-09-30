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
# Interpreter: $RAVEN_PYTHON if set (only it — an explicit choice is not
# second-guessed), else python3, then python. The first that RUNS peek
# wins. That can't print twice: peek always exits 0 once imported and
# prints its single JSON line last, while a missing interpreter (127),
# the Windows Store python3 stub, or a python without raven_bus (module
# import error, exit 1) exits non-zero having written nothing to stdout.
# Bare `python` alone was often not the raven interpreter, and doesn't
# exist at all on many Linux boxes (QA finding A14).
#
# CRLF guard: every command line ends in ` #`, and the command block has
# no blank lines. In a copy saved with CRLF endings the stray \r then
# lands in a comment; otherwise `exit 0\r` makes bash exit 2 — PreToolUse's
# BLOCKING code, refusing every tool call. (The shebang can't be guarded:
# invoke the file as `bash raven-inbox-hook.sh`.)
# Git Bash on Windows is POSIX only — no bashisms.
if [ -n "${RAVEN_PYTHON:-}" ]; then #
    "$RAVEN_PYTHON" -m raven_bus.adapters.hooks.peek 2>/dev/null #
    exit 0 #
fi #
python3 -m raven_bus.adapters.hooks.peek 2>/dev/null && exit 0 #
python -m raven_bus.adapters.hooks.peek 2>/dev/null #
exit 0 #
