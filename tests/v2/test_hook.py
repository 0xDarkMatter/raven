"""Tests for the Claude Code PreToolUse inbox hook.  LANE: hook (raven2-p3).

The hook is a peek-only adapter (ADR-006): it reads pending, renders via
``raven_bus.policy``, prints a banner+block when anything is deliverable,
never acks, and exits 0 in every path. These tests exercise that through
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

# A deterministic, assertable render double: records the pending ids the
# REAL peek code handed to plan() by stashing them on the plan, then
# renders a fixed marker + the id list. The hook's job is only to wrap
# policy.render's output verbatim (ADR-003/006), so this is all the test
# needs to assert the banner + rendered block reach stdout.


class _Plan:
    def __init__(self, pending):
        self.pending = list(pending)


def _plan(pending, *, now, **kw):
    return _Plan(pending)


def _render(plan, **kw):
    ids = ",".join(str(m.id) for m in plan.pending)
    return f"RAVEN-RENDERED-BLOCK ids=[{ids}]"


policy.plan = _plan
policy.render = _render

from raven_bus.adapters.hooks.peek import main
main()
"""


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
    # A genuinely absent DB in a creatable dir: init_db creates it, the
    # inbox is empty, and the hook stays silent. Graceful, exit 0.
    missing = tmp_path / "nested" / "does-not-exist.db"

    result = run_peek(
        env=_env(RAVEN_CONSUMER=CONSUMER, RAVEN_CHANNELS=CHANNEL, RAVEN_DB=str(missing))
    )

    assert result.returncode == 0
    assert result.stdout == ""


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
        "policy.plan = _boom\n"
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
def test_pending_messages_print_banner_and_rendered_block(tmp_path):
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
    out = result.stdout
    # README banner shape: count is the number of messages peeked.
    assert f"=== RAVEN: {len(ids)} message(s) for {CONSUMER} ===" in out
    # The policy.render output appears verbatim (sender-attributed framing
    # is policy's job; the hook only wraps it — ADR-003/006).
    assert RENDER_MARKER in out
    assert f"ids=[{','.join(str(i) for i in ids)}]" in out
    # One action-hint line (README shape).
    assert "raven read/ack" in out


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
    # 4 messages peeked, handed to render in ascending id order.
    assert "=== RAVEN: 4 message(s) for " in result.stdout
    assert "ids=[1,2,3,4]" in result.stdout


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


def test_emit_banner_shape_matches_readme(capsys):
    from raven_bus.adapters.hooks.peek import _emit

    _emit(CONSUMER, 2, "RENDERED")

    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert lines[0] == f"=== RAVEN: 2 message(s) for {CONSUMER} ==="
    assert "RENDERED" in lines[1]
    assert lines[-1] == "Use your raven tooling (or the CLI: raven read/ack) to act."


def test_plan_receives_merged_pending_ascending(monkeypatch, tmp_path):
    """In-process: policy.plan is handed exactly the merged id-ascending
    pending list (the raw store-reading path, asserted directly)."""
    import raven_bus.adapters.hooks.peek as peek_mod
    from raven_bus import policy

    db_path = tmp_path / "bus.db"
    ch_a = "run/v0-2/lane/a"
    ch_b = "run/v0-2/lane/b"
    seed_channel(db_path, ch_a, [("a1", "prompt", {})])
    seed_channel(db_path, ch_b, [("b1", "prompt", {})])
    seed_channel(db_path, ch_a, [("a2", "prompt", {})])

    received: list[list[int]] = []

    def fake_plan(pending, *, now, **kw):
        received.append([m.id for m in pending])
        return object()

    monkeypatch.setattr(policy, "plan", fake_plan)
    monkeypatch.setattr(policy, "render", lambda plan, **kw: "X")
    # peek reads os.environ directly; mirror the subprocess env in-process.
    monkeypatch.setenv("RAVEN_CONSUMER", CONSUMER)
    monkeypatch.setenv("RAVEN_CHANNELS", f"{ch_a},{ch_b}")
    monkeypatch.setenv("RAVEN_DB", str(db_path))

    rc = peek_mod.peek()

    assert rc == 0
    assert received == [[1, 2, 3]]  # merged + strictly id-ascending


def test_peek_silent_when_render_returns_empty(monkeypatch, tmp_path, capsys):
    """plan non-empty but render yields '' -> print nothing (e.g. an
    all-deferred plan this boundary)."""
    import raven_bus.adapters.hooks.peek as peek_mod
    from raven_bus import policy

    db_path = tmp_path / "bus.db"
    seed_channel(db_path, CHANNEL, [("a", "fyi", {})])

    monkeypatch.setattr(policy, "plan", lambda pending, *, now, **kw: object())
    monkeypatch.setattr(policy, "render", lambda plan, **kw: "")
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

    # No RAVEN_CONSUMER -> silent no-op; wrapper's ``|| true`` keeps it 0
    # even if the module path were unreachable.
    assert result.returncode == 0
    assert result.stdout == ""


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

    monkeypatch.setattr(policy_mod, "plan", _boom)
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
