"""Tests for the Claude Code PreToolUse inbox hook.  LANE: hook (raven2-p3).

The hook is a peek-only adapter (ADR-006): it reads pending, emits
``raven_bus.policy.render_hint``'s bounded pull notice (issue #1) as
PreToolUse additionalContext JSON (issue #2) when anything is due, never
acks, and exits 0 in every path. These tests exercise that through
a real subprocess (env injected) so the quality bar's guarantees hold at
the actual process boundary, plus in-process unit tests for the parts
that are awkward to assert over stdout.

``raven_bus.policy`` ships stubbed in this lane (``plan``/``render`` raise
``NotImplementedError``) — it is a PARALLEL lane. Per the run plan we
stand it in with deterministic doubles for the render path (a driver
shim patches ``raven_bus.policy`` in the subprocess before invoking the
REAL peek code; the store read is 100% real). Every other path is tested
against the real stubbed policy, which also validates ADR-006's guarantee
that a broken/stubbed policy never blocks a tool call.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HOOK_MODULE = "raven_bus.adapters.hooks.peek"
WRAPPER = (
    Path(__file__).resolve().parents[2]  # repo root: tests/v2 -> tests -> root
    / "src"
    / "raven_bus"
    / "adapters"
    / "hooks"
    / "raven-inbox-hook.sh"
)
# Deterministic render marker the policy double emits; lets the tests
# assert the rendered block reached stdout without depending on the
# policy lane's (still-stubbed) framing.
RENDER_MARKER = "RAVEN-RENDERED-BLOCK"


# --------------------------------------------------------------------------- #
# Subprocess harness.
# --------------------------------------------------------------------------- #
def _env(**overrides: str) -> dict[str, str]:
    """A copy of the live env with the given RAVEN_* overrides applied.

    Inherits PATH (so ``python`` resolves) and PYTHONPATH-free import of
    the installed package; only the hook's own vars are set/cleared."""
    env = dict(os.environ)
    for key in ("RAVEN_CONSUMER", "RAVEN_CHANNELS", "RAVEN_DB"):
        env.pop(key, None)
    env.update({k: str(v) for k, v in overrides.items() if v is not None})
    return env


def run_peek(
    *args: str,
    env: dict[str, str] | None = None,
    python: str = sys.executable,
    timeout: float = 30.0,
) -> subprocess.CompletedProcess[str]:
    """Run ``python -m raven_bus.adapters.hooks.peek`` and return the result.

    Captures stdout/stderr as text. The hook must exit 0 in every path
    (ADR-006); callers assert that explicitly rather than via check=."""
    return subprocess.run(
        [python, "-m", HOOK_MODULE, *args],
        env=env if env is not None else _env(),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


# A driver shim that patches the (stubbed) policy with deterministic
# doubles, then invokes the REAL peek main(). Running peek through this
# shim keeps the process boundary + real store read while making the
# render path assertable. The doubles record what plan() received and
# emit a fixed render marker.
_DRIVER = """\
import raven_bus.policy as policy

# A deterministic, assertable notice double: echoes the pending ids and
# consumer the REAL peek code handed to render_hint(). The hook's job is
# only to wrap policy's text verbatim (ADR-003/006), so this is all the
# test needs to assert the notice reaches stdout unaltered.


def _render_hint(pending, *, consumer, now, **kw):
    ids = ",".join(str(m.id) for m in pending)
    return f"RAVEN-RENDERED-BLOCK ids=[{ids}] consumer={consumer}"


policy.render_hint = _render_hint

from raven_bus.adapters.hooks.peek import main
main()
"""


def additional_context(stdout: str) -> str:
    """Unwrap the hook's stdout: exactly one JSON object whose
    ``hookSpecificOutput.additionalContext`` carries the text. That
    envelope is the ONLY PreToolUse output Claude Code feeds to the model
    — plain stdout goes to its debug log (issue #2) — so asserting on
    a raw substring of stdout would pass while the model saw nothing."""
    envelope = json.loads(stdout)
    specific = envelope["hookSpecificOutput"]
    assert specific["hookEventName"] == "PreToolUse"
    return specific["additionalContext"]


def write_driver(tmp_path: Path) -> Path:
    """Write the policy-double driver shim; return its path."""
    driver = tmp_path / "_raven_hook_driver.py"
    driver.write_text(_DRIVER, encoding="utf-8")
    return driver


def run_peek_with_policy_double(
    tmp_path: Path,
    *,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run the REAL peek code in a subprocess with policy patched to doubles."""
    driver = write_driver(tmp_path)
    return subprocess.run(
        [sys.executable, str(driver)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )


# --------------------------------------------------------------------------- #
# Store seeding via the REAL public API (channels.ensure_channel + log.append).
# --------------------------------------------------------------------------- #
def seed_channel(
    db_path: Path,
    channel: str,
    messages: list[tuple[str, str, dict]],
    *,
    sender: str = "sender@run-x",
) -> list[int]:
    """Append messages to ``channel`` via the real store; return their ids.

    Each entry is ``(type, urgency, body)``. Uses ``ensure=True`` so the
    channel is created broadcast on first send (ADR-001/002)."""
    from raven_bus import channels, db, log
    from raven_bus.db import _reset_init_cache

    _reset_init_cache()
    db.init_db(db_path, force=True)
    ids: list[int] = []
    with db.connection(db_path) as conn:
        channels.ensure_channel(conn, channel, "broadcast")
        for msg_type, urgency, body in messages:
            msg = log.append(
                conn,
                channel=channel,
                sender=sender,
                type=msg_type,
                body=body,
                urgency=urgency,  # type: ignore[arg-type]
            )
            ids.append(msg.id)
    return ids


def pending_ids(db_path: Path, consumer: str, channel: str) -> list[int]:
    """Read pending ids via the real store (the never-acks oracle)."""
    from raven_bus import cursors, db
    from raven_bus.db import _reset_init_cache

    _reset_init_cache()
    db.init_db(db_path)
    with db.connection(db_path) as conn:
        return [m.id for m in cursors.pending(conn, consumer, channel)]


def cursor_exists(db_path: Path, consumer: str, channel: str) -> bool:
    """True iff a cursor row exists for consumer/channel (it must NOT after a peek)."""
    from raven_bus import cursors, db
    from raven_bus.db import _reset_init_cache

    _reset_init_cache()
    db.init_db(db_path)
    with db.connection(db_path) as conn:
        return cursors.get_cursor(conn, consumer, channel) is not None


CONSUMER = "worker@run-a"
CHANNEL = "run/v0-2/broadcast"


# --------------------------------------------------------------------------- #
# ADR-006 BLUF: silent + exit 0 in every path.
# --------------------------------------------------------------------------- #
def test_no_consumer_is_silent_noop_exit_zero(tmp_path):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("note", "prompt", {"x": 1})])

    result = run_peek(env=_env(RAVEN_DB=str(db_path)))  # no RAVEN_CONSUMER

    assert result.returncode == 0
    assert result.stdout == ""


def test_consumer_without_channels_is_silent_exit_zero(tmp_path):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("note", "prompt", {"x": 1})])

    result = run_peek(env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_DB=str(db_path)))

    # RAVEN_CONSUMER set but RAVEN_CHANNELS empty -> explicit-only, silent.
    assert result.returncode == 0
    assert result.stdout == ""


def test_empty_inbox_is_silent_exit_zero(tmp_path):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [])  # channel exists, no messages

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    )

    assert result.returncode == 0
    assert result.stdout == ""


@pytest.mark.parametrize("bad", ["no-at-sign", "a@b@c", "Role@run", "role@Run", "@run", "role@"])
def test_bad_consumer_grammar_is_silent_exit_zero(tmp_path, bad):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("note", "prompt", {})])

    result = run_peek(
        env=_env(RAVEN_CONSUMER=bad, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    )

    # Bad grammar is a config mistake, never a tool-blocking error.
    assert result.returncode == 0
    assert result.stdout == ""


def test_bad_channel_grammar_is_silent_exit_zero(tmp_path):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("note", "prompt", {})])

    result = run_peek(
        env=_env(
            RAVEN_CONSUMER=CONSUMER,
            RAVEN_CHANNELS="Bad/Channel",  # uppercase atom violates ADR-002
            RAVEN_DB=str(db_path),
        )
    )

    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_db_path_is_exit_zero(tmp_path):
    # A genuinely absent DB in a creatable dir: silent, exit 0 — and the
    # hook must NOT create it (it is read-only; QA finding A10 — it used
    # to run init_db on every tool call).
    missing = tmp_path / "nested" / "does-not-exist.db"

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(missing))
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert not missing.exists()
    assert not missing.parent.exists()


def test_unopenable_db_is_caught_exit_zero(tmp_path):
    # RAVEN_DB whose parent is a *file* -> init_db's mkdir raises; ADR-006
    # requires the catch-all to swallow it and stay exit 0.
    blocker = tmp_path / "iamafile"
    blocker.write_text("x", encoding="utf-8")
    bad_db = blocker / "bus.db"

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(bad_db))
    )

    assert result.returncode == 0
    assert result.stdout == ""


# --------------------------------------------------------------------------- #
# The never-acks guarantee (ADR-006): a peek must not move the cursor.
# --------------------------------------------------------------------------- #
def test_peek_never_acks_cursor_does_not_move(tmp_path):
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {}), ("b", "prompt", {})])
    before = pending_ids(db_path, CONSUMER, CHANNEL)
    assert before  # sanity: there IS something pending

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    )

    assert result.returncode == 0
    # Reading is not acking: the same messages are still pending...
    assert pending_ids(db_path, CONSUMER, CHANNEL) == before
    # ...and no cursor row was created (only ack writes cursors).
    assert cursor_exists(db_path, CONSUMER, CHANNEL) is False


def test_peek_with_raising_policy_is_silent_exit_zero(tmp_path):
    """A broken policy never blocks a tool call (ADR-006): patch
    policy.plan to raise and require silence + exit 0. (Originally
    written against the sibling lane's NotImplementedError stubs; the
    guarantee outlived the stubs, so the raise is now explicit.)"""
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {})])

    driver = tmp_path / "raising_driver.py"
    driver.write_text(
        "from raven_bus import policy\n"
        "def _boom(*a, **k):\n"
        "    raise RuntimeError('policy exploded')\n"
        "policy.render_hint = _boom\n"
        "from raven_bus.adapters.hooks import peek\n"
        "raise SystemExit(peek.main())\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(driver)],
        env=_env(
            RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path)
        ),
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""


# --------------------------------------------------------------------------- #
# The render path (policy patched to doubles; real peek code + real store).
# --------------------------------------------------------------------------- #
def test_pending_messages_emit_policy_notice_verbatim(tmp_path):
    db_path = tmp_path / "bus.db"
    ids = seed_channel(
        db_path,
        CHANNEL,
        [("first", "prompt", {"n": 1}), ("second", "prompt", {"n": 2})],
    )

    result = run_peek_with_policy_double(
        tmp_path,
        env=_env(
            RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path)
        ),
    )

    assert result.returncode == 0
    # policy.render_hint's text arrives EXACTLY — the hook adds nothing
    # (all wording is policy's — ADR-003/006).
    assert additional_context(result.stdout) == (
        f"{RENDER_MARKER} ids=[{','.join(str(i) for i in ids)}] consumer={CONSUMER}"
    )


def test_real_policy_notice_names_the_pull_command_and_omits_bodies(tmp_path):
    """End to end with the REAL policy: the notice says what is waiting
    and how to pull it, and never carries a body (issue #1 — the content
    is pulled, not re-pushed every tool call)."""
    db_path = tmp_path / "bus.db"
    seed_channel(
        db_path,
        CHANNEL,
        [("steer", "prompt", {"secret": "BODY-MUST-NOT-APPEAR"})],
        sender="orchestrator@run-a",
    )

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    )

    assert result.returncode == 0
    out = additional_context(result.stdout)
    assert f"raven read --framed --channel {CHANNEL} --as {CONSUMER}" in out
    assert "orchestrator@run-a" in out
    assert "BODY-MUST-NOT-APPEAR" not in out


def test_real_policy_held_fyi_is_silent(tmp_path):
    """A lone fresh fyi is held by ADR-003's digest rule: nothing is due,
    so the hook stays silent rather than nagging every tool call."""
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("note", "fyi", {})])

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    )

    assert result.returncode == 0
    assert result.stdout == ""


def test_render_path_still_never_acks(tmp_path):
    """Even when the hook renders and prints, it must not move the cursor."""
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {}), ("b", "prompt", {})])
    before = pending_ids(db_path, CONSUMER, CHANNEL)

    result = run_peek_with_policy_double(
        tmp_path,
        env=_env(
            RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path)
        ),
    )

    assert result.returncode == 0
    assert RENDER_MARKER in result.stdout
    assert pending_ids(db_path, CONSUMER, CHANNEL) == before
    assert cursor_exists(db_path, CONSUMER, CHANNEL) is False


# --------------------------------------------------------------------------- #
# Multi-channel merge + id-ascending hand-off to policy.plan.
# --------------------------------------------------------------------------- #
def test_pending_merged_across_channels_id_ascending(tmp_path):
    db_path = tmp_path / "bus.db"
    ch_a = "run/v0-2/lane/a"
    ch_b = "run/v0-2/lane/b"
    # Interleave sends across channels so global id order differs from
    # per-channel order; the merge must surface strictly id-ascending.
    seed_channel(db_path, ch_a, [("a1", "prompt", {})])
    seed_channel(db_path, ch_b, [("b1", "prompt", {})])
    seed_channel(db_path, ch_a, [("a2", "prompt", {})])
    seed_channel(db_path, ch_b, [("b2", "prompt", {})])

    result = run_peek_with_policy_double(
        tmp_path,
        env=_env(
            RAVEN_CONSUMER=CONSUMER,
            RAVEN_CHANNELS=f"{ch_a},{ch_b}",
            RAVEN_DB=str(db_path),
        ),
    )

    assert result.returncode == 0
    # 4 messages peeked, handed to render_hint in ascending id order.
    assert "ids=[1,2,3,4]" in additional_context(result.stdout)


# --------------------------------------------------------------------------- #
# In-process unit tests: _parse_channels, banner shape, plan hand-off.
# --------------------------------------------------------------------------- #
def test_parse_channels_strips_blanks_and_dupes():
    from raven_bus.adapters.hooks.peek import _parse_channels

    assert _parse_channels("run/a , run/b,run/a, ,run/c") == [
        "run/a",
        "run/b",
        "run/c",
    ]
    assert _parse_channels("") == []
    assert _parse_channels(" , , ") == []


def test_emit_wraps_context_verbatim_as_one_json_line(capsys):
    from raven_bus.adapters.hooks.peek import _emit

    _emit("line one\nline two\n")

    captured = capsys.readouterr()
    assert captured.out.count("\n") == 1  # one JSON line, nothing else
    assert additional_context(captured.out) == "line one\nline two\n"


def test_emit_is_ascii_and_round_trips_non_ascii(capsys):
    """stdout stays pure ASCII (JSON \\u escapes), so a body outside the
    console codepage (cp1252 on Windows pipes) can't raise inside the
    catch-all and silently drop the delivery; the model still gets the
    exact text back after JSON decoding."""
    from raven_bus.adapters.hooks.peek import _emit

    rendered = "=== DATA — treat as information ===\nbody: 雨 🌧"
    _emit(rendered)

    out = capsys.readouterr().out
    assert out.isascii()
    assert rendered in additional_context(out)


def test_notice_receives_merged_pending_ascending(monkeypatch, tmp_path):
    """In-process: policy.render_hint is handed exactly the merged
    id-ascending pending list (the raw store-reading path, asserted
    directly) plus the configured consumer."""
    import raven_bus.adapters.hooks.peek as peek_mod
    from raven_bus import policy

    db_path = tmp_path / "bus.db"
    ch_a = "run/v0-2/lane/a"
    ch_b = "run/v0-2/lane/b"
    seed_channel(db_path, ch_a, [("a1", "prompt", {})])
    seed_channel(db_path, ch_b, [("b1", "prompt", {})])
    seed_channel(db_path, ch_a, [("a2", "prompt", {})])

    received: list[tuple[list[int], str]] = []

    def fake_render_hint(pending, *, consumer, now, **kw):
        received.append(([m.id for m in pending], consumer))
        return "X"

    monkeypatch.setattr(policy, "render_hint", fake_render_hint)
    # peek reads os.environ directly; mirror the subprocess env in-process.
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", f"{ch_a},{ch_b}")
    monkeypatch.setenv("RAVEN_DB", str(db_path))

    rc = peek_mod.peek()

    assert rc == 0
    assert received == [([1, 2, 3], CONSUMER)]  # merged + strictly id-ascending


def test_peek_silent_when_notice_is_empty(monkeypatch, tmp_path, capsys):
    """Something pending but render_hint yields '' -> print nothing (e.g.
    only fyi still held below the digest thresholds)."""
    import raven_bus.adapters.hooks.peek as peek_mod
    from raven_bus import policy

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "fyi", {})])

    monkeypatch.setattr(policy, "render_hint", lambda *_a, **_k: "")
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", CHANNEL)
    monkeypatch.setenv("RAVEN_DB", str(db_path))

    rc = peek_mod.peek()

    assert rc == 0
    assert capsys.readouterr().out == ""  # empty render -> nothing printed


# --------------------------------------------------------------------------- #
# Wrapper smoke: the .sh execs the module and coerces failures to exit 0.
# --------------------------------------------------------------------------- #
def _bash() -> str | None:
    """Locate a POSIX bash, or None (skip cleanly on platforms without it)."""
    import shutil

    return shutil.which("bash")


def test_wrapper_smoke_no_consumer_silent_exit_zero(tmp_path):
    bash = _bash()
    if bash is None:
        pytest.skip("bash not available — wrapper smoke is Git Bash/POSIX only")

    # Run the wrapper under bash with the venv's python on PATH so the
    # ``exec python -m ...`` inside resolves to the installed package.
    env = _env(RAVEN_DB=str(tmp_path / "bus.db"))
    venv_scripts = str(Path(sys.executable).parent)
    env["PATH"] = venv_scripts + os.pathsep + env.get("PATH", "")

    result = subprocess.run(
        [bash, str(WRAPPER)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    # No RAVEN_CONSUMER -> silent no-op.
    assert result.returncode == 0
    assert result.stdout == ""


def test_wrapper_missing_interpreter_still_exits_zero(tmp_path):
    """No ``python`` on PATH must still exit 0 (ADR-006: a broken hook
    never errors). The original wrapper ``exec``'d python, so its
    ``|| true`` never ran and this exited 127 — Claude Code then shows a
    hook-error notice on every tool call (issue #2 note)."""
    bash = _bash()
    if bash is None:
        pytest.skip("bash not available — wrapper smoke is Git Bash/POSIX only")

    empty_bin = tmp_path / "empty-bin"
    empty_bin.mkdir()
    env = _env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL)
    env["PATH"] = str(empty_bin)

    result = subprocess.run(
        [bash, str(WRAPPER)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""


def test_wrapper_honours_raven_python(tmp_path):
    """RAVEN_PYTHON selects the interpreter: with NO python on PATH (the
    uv-tool case, where PATH's python can't import raven_bus) the hook
    still delivers when RAVEN_PYTHON points at the right one."""
    bash = _bash()
    if bash is None:
        pytest.skip("bash not available — wrapper smoke is Git Bash/POSIX only")

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("steer", "prompt", {})])
    empty_bin = tmp_path / "empty-bin"
    empty_bin.mkdir()
    env = _env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    env["PATH"] = str(empty_bin)
    env["RAVEN_PYTHON"] = Path(sys.executable).as_posix()

    result = subprocess.run(
        [bash, str(WRAPPER)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    assert result.returncode == 0
    assert f"raven read --framed --channel {CHANNEL} --as {CONSUMER}" in additional_context(result.stdout)


# --------------------------------------------------------------------------- #
# In-process coverage of peek's guard paths (the subprocess drivers above
# exercise them for real, but a child process is invisible to coverage).
# --------------------------------------------------------------------------- #
def test_inprocess_no_consumer_is_inactive(monkeypatch):
    from raven_bus.adapters.hooks import peek as peek_mod

    monkeypatch.delenv("RAVEN_CONSUMER", raising=False)
    assert peek_mod.peek() == 0


def test_inprocess_consumer_without_channels_is_silent(monkeypatch, capsys):
    from raven_bus.adapters.hooks import peek as peek_mod

    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.delenv("RAVEN_CHANNELS", raising=False)
    assert peek_mod.peek() == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "RAVEN_CHANNELS empty" in captured.err


def test_inprocess_bad_grammar_is_silent(monkeypatch, capsys):
    from raven_bus.adapters.hooks import peek as peek_mod

    monkeypatch.setenv("RAVEN_CONSUMER", "NOT VALID")
    monkeypatch.setenv("RAVEN_CHANNELS", CHANNEL)
    assert peek_mod.peek() == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "bad config" in captured.err


def test_inprocess_catchall_swallows_everything(monkeypatch, capsys, tmp_path):
    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {})])
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", CHANNEL)
    monkeypatch.setenv("RAVEN_DB", str(db_path))

    def _boom(*_a, **_k):
        raise RuntimeError("policy exploded")

    # peek imports policy lazily inside the function body — patch the
    # source module, not a (nonexistent) peek-module attribute.
    from raven_bus import policy as policy_mod

    monkeypatch.setattr(policy_mod, "render_hint", _boom)
    assert peek_mod.peek() == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "RuntimeError" in captured.err


def test_inprocess_main_exits_zero(monkeypatch):
    from raven_bus.adapters.hooks import peek as peek_mod

    monkeypatch.delenv("RAVEN_CONSUMER", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        peek_mod.main()
    assert excinfo.value.code == 0


def test_dunder_main_delegates(monkeypatch):
    import runpy

    monkeypatch.delenv("RAVEN_CONSUMER", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("raven_bus.adapters.hooks", run_name="__main__")
    assert excinfo.value.code == 0


def test_inprocess_empty_inbox_is_silent(monkeypatch, capsys, tmp_path):
    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [])  # channel exists, nothing pending
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", CHANNEL)
    monkeypatch.setenv("RAVEN_DB", str(db_path))
    assert peek_mod.peek() == 0
    captured = capsys.readouterr()
    assert captured.out == ""


# --------------------------------------------------------------------------- #
# QA finding A8 — one bad watched channel silenced the whole notice.
# --------------------------------------------------------------------------- #
def _inprocess_env(monkeypatch, db_path: Path, channels: str) -> None:
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", channels)
    monkeypatch.setenv("RAVEN_DB", str(db_path))
    for key in ("RAVEN_ACP_CONSUMER", "RAVEN_ACP_CHANNELS"):
        monkeypatch.delenv(key, raising=False)


@pytest.mark.parametrize("order", ["bad-first", "bad-last"])
def test_missing_or_non_broadcast_channel_is_skipped_not_fatal(
    monkeypatch, capsys, tmp_path, order
):
    """probe_hook_unknown: a watched channel that doesn't exist (or is a
    queue) raised inside the catch-all and dropped the notice for the
    valid channel — a BLOCKING message included. Now it's skipped, with
    one breadcrumb, and never created."""
    from raven_bus import channels, db
    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("stop", "blocking", {})], sender="boss@run-a")
    with db.connection(db_path) as conn:
        channels.ensure_channel(conn, "run/v0-2/jobs", "queue")
    bad = "run/v0-2/nope,run/v0-2/jobs"
    _inprocess_env(
        monkeypatch, db_path, f"{bad},{CHANNEL}" if order == "bad-first" else f"{CHANNEL},{bad}"
    )

    assert peek_mod.peek() == 0

    captured = capsys.readouterr()
    notice = additional_context(captured.out)
    assert "highest urgency: blocking" in notice
    assert f"--channel {CHANNEL} " in notice
    assert captured.err == (
        "raven-inbox-hook: skipped channel(s) that are missing or not broadcast: "
        "run/v0-2/nope, run/v0-2/jobs\n"
    )
    with db.connection(db_path) as conn:
        names = [c.name for c in channels.list_channels(conn)]
    assert "run/v0-2/nope" not in names  # the hook stays read-only


# --------------------------------------------------------------------------- #
# QA finding A10 — the hook wrote on every tool call (init_db, sweep,
# consumers.touch) and stalled ~5 s under another writer's lock.
# --------------------------------------------------------------------------- #
def test_hook_read_path_issues_no_writes(monkeypatch, capsys, tmp_path):
    """Every statement the hook runs is a SELECT, on a connection SQLite
    itself makes read-only; no other connection sees a commit
    (data_version unchanged) and no presence row appears."""
    import sqlite3

    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {}), ("b", "fyi", {})])
    _inprocess_env(monkeypatch, db_path, CHANNEL)

    statements: list[str] = []
    real_connect = peek_mod._connect

    def _traced(path):
        conn = real_connect(path)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(peek_mod, "_connect", _traced)
    observer = sqlite3.connect(db_path)
    before = observer.execute("PRAGMA data_version").fetchone()[0]

    assert peek_mod.peek() == 0

    assert "raven read --framed" in additional_context(capsys.readouterr().out)
    assert statements and all(s.lstrip().upper().startswith("SELECT") for s in statements)
    assert observer.execute("PRAGMA data_version").fetchone()[0] == before
    assert observer.execute(
        "SELECT COUNT(*) FROM consumers WHERE id = ?", (CONSUMER,)
    ).fetchone()[0] == 0
    observer.close()


def test_hook_connection_refuses_writes(tmp_path):
    import sqlite3

    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [])
    conn = peek_mod._connect(db_path)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM channels")
    finally:
        conn.close()


def test_hook_is_not_stalled_by_another_writers_lock(monkeypatch, capsys, tmp_path):
    """probe_hooklock: with another process mid-transaction (BEGIN
    IMMEDIATE), the old hook blocked ~5 s on its sweep UPDATE and then
    dropped the notice. A pure WAL reader is not blocked at all."""
    import sqlite3
    import time

    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("stop", "blocking", {})])
    _inprocess_env(monkeypatch, db_path, CHANNEL)
    writer = sqlite3.connect(db_path, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    try:
        started = time.monotonic()
        assert peek_mod.peek() == 0
        elapsed = time.monotonic() - started
    finally:
        writer.execute("ROLLBACK")
        writer.close()

    assert elapsed < 1.5
    assert "highest urgency: blocking" in additional_context(capsys.readouterr().out)


def test_inprocess_missing_db_is_silent_and_not_created(monkeypatch, capsys, tmp_path):
    from raven_bus.adapters.hooks import peek as peek_mod

    missing = tmp_path / "absent.db"
    _inprocess_env(monkeypatch, missing, CHANNEL)
    assert peek_mod.peek() == 0
    assert capsys.readouterr().out == ""
    assert not missing.exists()


# --------------------------------------------------------------------------- #
# QA finding A11 (mitigation) — a hook inside a harness-driven agent.
# --------------------------------------------------------------------------- #
def test_hook_skips_channels_its_own_harness_delivers(monkeypatch, capsys, tmp_path):
    """`raven acp` marks its child's env with the consumer + channels it
    serves; the hook skips exactly those (the harness injects them and
    acks after the turn, so announcing them mid-turn is a double
    delivery). Another consumer, or another channel, still gets its
    notice."""
    from raven_bus.adapters.hooks import peek as peek_mod

    db_path = tmp_path / "bus.db"
    other = "run/v0-2/other"
    seed_channel(db_path, CHANNEL, [("a", "prompt", {})])
    seed_channel(db_path, other, [("b", "prompt", {})])
    _inprocess_env(monkeypatch, db_path, f"{CHANNEL},{other}")
    monkeypatch.setenv("RAVEN_ACP_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_ACP_CHANNELS", CHANNEL)

    assert peek_mod.peek() == 0
    notice = additional_context(capsys.readouterr().out)
    assert f"--channel {other} " in notice
    assert f"--channel {CHANNEL} " not in notice

    monkeypatch.setenv("RAVEN_ACP_CHANNELS", f"{CHANNEL},{other}")
    assert peek_mod.peek() == 0
    assert capsys.readouterr().out == ""  # everything is the harness's

    monkeypatch.setenv("RAVEN_ACP_CONSUMER", "someone-else@run-a")
    assert peek_mod.peek() == 0
    assert f"--channel {CHANNEL} " in additional_context(capsys.readouterr().out)


# --------------------------------------------------------------------------- #
# QA finding A14 — the wrapper's interpreter choice and CRLF copies.
# --------------------------------------------------------------------------- #
def _shim(bin_dir: Path, name: str, body: str) -> None:
    """A fake interpreter on PATH: a POSIX sh script (Git Bash runs a
    shebang file without an extension)."""
    shim = bin_dir / name
    shim.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8", newline="\n")
    shim.chmod(0o755)


def _run_wrapper(tmp_path: Path, env: dict[str, str], wrapper: Path = WRAPPER):
    bash = _bash()
    if bash is None:
        pytest.skip("bash not available — wrapper smoke is Git Bash/POSIX only")
    return subprocess.run(
        [bash, str(wrapper)], env=env, capture_output=True, text=True, timeout=60.0,
        check=False,
    )


_REAL = Path(sys.executable).as_posix()


def _wrapper_env(tmp_path: Path, bin_dir: Path) -> dict[str, str]:
    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("steer", "prompt", {})])
    env = _env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(db_path))
    env.pop("RAVEN_PYTHON", None)
    for key in ("RAVEN_ACP_CONSUMER", "RAVEN_ACP_CHANNELS"):
        env.pop(key, None)
    env["PATH"] = str(bin_dir)
    return env


def _one_notice(stdout: str) -> str:
    assert stdout.count("\n") == 1, stdout  # exactly one JSON line — never two
    return additional_context(stdout)


@pytest.mark.parametrize(
    "broken_python3",
    [
        "exit 49",  # the Windows Store stub: stderr-only, non-zero
        f'exec "{_REAL}" -m raven_bus_no_such_module "$@"',  # no raven_bus there
    ],
    ids=["store-stub", "import-fails"],
)
def test_wrapper_falls_back_from_python3_to_python(tmp_path, broken_python3):
    """No RAVEN_PYTHON: python3 first, then python. A python3 that can't
    run peek writes nothing to stdout, so the fallback prints once."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _shim(bin_dir, "python3", broken_python3)
    _shim(bin_dir, "python", f'exec "{_REAL}" "$@"')

    result = _run_wrapper(tmp_path, _wrapper_env(tmp_path, bin_dir))

    assert result.returncode == 0
    assert f"--channel {CHANNEL} --as {CONSUMER}" in _one_notice(result.stdout)


def test_wrapper_prefers_python3_and_never_runs_a_second_interpreter(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _shim(bin_dir, "python3", f'exec "{_REAL}" "$@"')
    _shim(bin_dir, "python", 'echo "SECOND-INTERPRETER-RAN"')

    result = _run_wrapper(tmp_path, _wrapper_env(tmp_path, bin_dir))

    assert result.returncode == 0
    assert "SECOND-INTERPRETER-RAN" not in result.stdout
    _one_notice(result.stdout)


def test_wrapper_does_not_second_guess_an_explicit_raven_python(tmp_path):
    """RAVEN_PYTHON names THE interpreter: when it is wrong the hook stays
    silent (exit 0) rather than quietly using some other python."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _shim(bin_dir, "python3", f'exec "{_REAL}" "$@"')
    env = _wrapper_env(tmp_path, bin_dir)
    env["RAVEN_PYTHON"] = (tmp_path / "no-such-python").as_posix()

    result = _run_wrapper(tmp_path, env)

    assert result.returncode == 0
    assert result.stdout == ""


def test_wrapper_command_lines_are_crlf_safe():
    """Every command line ends in ' #' and the command block has no blank
    line, so a CRLF copy's stray \\r always lands in a comment. (Linux
    bash exits 2 — PreToolUse's BLOCKING code — on `exit 0\\r`; Git
    Bash hides this by stripping CRs, hence a structural check too.)"""
    lines = WRAPPER.read_text(encoding="utf-8").splitlines()
    first_cmd = next(i for i, line in enumerate(lines) if line and not line.startswith("#"))
    block = lines[first_cmd:]
    assert all(line.strip() for line in block), "blank line in the command block"
    commands = [line for line in block if not line.lstrip().startswith("#")]
    assert commands and all(line.endswith(" #") for line in commands), commands


def test_wrapper_crlf_copy_still_exits_zero(tmp_path):
    crlf = tmp_path / "raven-inbox-hook.sh"
    crlf.write_bytes(WRAPPER.read_bytes().replace(b"\n", b"\r\n"))
    env = _env()  # no RAVEN_CONSUMER: the silent path

    result = _run_wrapper(tmp_path, env, crlf)

    assert result.returncode == 0
    assert result.stdout == ""
