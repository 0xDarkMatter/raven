"""Tests for raven_bus.adapters.acp.harness (the bus<->ACP loop) and
`raven acp`.  LANE: acp-harness (raven2-p3).

``policy`` (plan/render) and ``protocol.AcpClient`` are parallel-lane
frozen contracts, possibly still ``NotImplementedError`` stubs in this
worktree. So this file:

- monkeypatches ``harness.policy.plan``/``harness.policy.render`` to a
  deterministic stand-in per test (never edits policy.py — it's a
  sibling lane's file), so harness's OWN delivery/ack/telemetry logic
  is exercised regardless of policy's implementation progress;
- uses a plain-Python ``FakeAcpClient`` double implementing
  ``protocol.AcpClient``'s surface (initialize/new_session/prompt/
  cancel), never importing the real (still-stub) class.

Ack correctness is verified against the REAL ``cursors``/``log``/``db``
stack (via the shared ``db`` fixture) — only policy and the ACP
transport are faked.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from raven_bus import cursors, log
from raven_bus import db as db_mod
from raven_bus.adapters.acp import harness
from raven_bus.adapters.acp.harness import HarnessConfig, parse_channels, run_harness
from raven_bus.adapters.acp.protocol import AcpError, PromptResult
from raven_bus.cli.main import app
from raven_bus.exceptions import InvalidAddressError
from raven_bus.models import Message
from raven_bus.policy import InjectionPlan

CONSUMER = "lane3@run1"
CHANNEL = "run/run1/team"

runner = CliRunner()


# --------------------------------------------------------------------------- #
# Test doubles
# --------------------------------------------------------------------------- #
class FakeAcpClient:
    """Plain double for protocol.AcpClient's surface — no dependency on
    the (parallel-lane, possibly still-stub) real implementation."""

    def __init__(
        self,
        *,
        init_error: AcpError | None = None,
        set_mode_error: AcpError | None = None,
    ) -> None:
        self.init_error = init_error
        self.set_mode_error = set_mode_error
        self.initialized = False
        self.session_cwd: str | None = None
        self.modes: list[tuple[str, str]] = []  # (session_id, mode_id)
        self.prompts: list[tuple[str, str]] = []  # (session_id, text)
        self.cancelled: list[str] = []
        self.next_results: list[PromptResult | AcpError] = []
        self.calls: list[str] = []  # method-call order

    def initialize(self) -> dict:
        self.calls.append("initialize")
        if self.init_error is not None:
            raise self.init_error
        self.initialized = True
        return {}

    def new_session(self, *, cwd: str) -> str:
        self.calls.append("new_session")
        self.session_cwd = cwd
        return "sess-1"

    def set_mode(self, session_id: str, mode_id: str) -> None:
        self.calls.append("set_mode")
        if self.set_mode_error is not None:
            raise self.set_mode_error
        self.modes.append((session_id, mode_id))

    def prompt(self, session_id: str, text: str) -> PromptResult:
        self.calls.append("prompt")
        self.prompts.append((session_id, text))
        if self.next_results:
            outcome = self.next_results.pop(0)
            if isinstance(outcome, AcpError):
                raise outcome
            return outcome
        return PromptResult(stop_reason="end_turn", text="ok", raw_updates=[])

    def cancel(self, session_id: str) -> None:
        self.cancelled.append(session_id)


class FakeChild:
    """Popen double: a queue of poll() results, None meaning 'still
    running'. Exhausting the queue keeps returning the last value."""

    def __init__(self, poll_sequence: list[int | None]) -> None:
        self._queue = list(poll_sequence)
        self._last: int | None = None
        self.terminated = False

    def poll(self) -> int | None:
        if self._queue:
            self._last = self._queue.pop(0)
        return self._last

    def terminate(self) -> None:
        self.terminated = True


def _never_dies() -> FakeChild:
    return FakeChild([None])


def _dies_after(polls: int) -> FakeChild:
    """Alive for ``polls`` poll() calls, then exited-0."""
    return FakeChild([None] * polls + [0])


def _config(**overrides) -> HarnessConfig:
    fields = {
        "consumer": CONSUMER,
        "channels": (CHANNEL,),
        "reply_channel": None,
        "db_path": None,
        "poll_interval_s": 0.0,
        "token_budget": 2000,
        "cwd": ".",
    }
    fields.update(overrides)
    return HarnessConfig(**fields)


def _append(conn: sqlite3.Connection, *, urgency: str = "prompt", channel: str = CHANNEL) -> Message:
    return log.append(
        conn,
        channel=channel,
        sender="peer@run1",
        type="note",
        urgency=urgency,  # type: ignore[arg-type]
        body={},
    )


def _stub_plan(monkeypatch: pytest.MonkeyPatch, plan_: InjectionPlan) -> None:
    """Force policy.plan to return ``plan_`` regardless of input, and
    policy.render to a deterministic, inspectable string."""
    monkeypatch.setattr(harness.policy, "plan", lambda *_a, **_k: plan_)

    def _ids(msgs: list[Message]) -> str:
        return ",".join(str(m.id) for m in msgs)

    def _fake_render(p: InjectionPlan, *, source: str = "raven bus") -> str:
        return f"interrupt=[{_ids(p.interrupt)}] batch=[{_ids(p.batch)}] digest=[{_ids(p.digest_source)}]"

    monkeypatch.setattr(harness.policy, "render", _fake_render)


# --------------------------------------------------------------------------- #
# run_harness — delivery ordering, ack timing, telemetry
# --------------------------------------------------------------------------- #
def test_blocking_delivered_alone_and_first(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        interrupt_msg = _append(conn, urgency="blocking")
        batch_msg = _append(conn, urgency="prompt")

    plan_ = InjectionPlan(
        interrupt=[interrupt_msg],
        batch=[batch_msg],
        ack_up_to=batch_msg.id,
    )
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=1)

    assert code == 0
    assert client.initialized
    assert client.session_cwd == "."
    # exactly two prompts: the interrupt alone, first; then the batch.
    assert len(client.prompts) == 2
    assert "interrupt=[1]" in client.prompts[0][1]
    assert "batch=[]" in client.prompts[0][1]
    assert "interrupt=[]" in client.prompts[1][1]
    assert f"batch=[{batch_msg.id}]" in client.prompts[1][1]


def test_batch_and_digest_deliver_as_one_prompt(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        b1 = _append(conn, urgency="prompt")
        b2 = _append(conn, urgency="prompt")
        d1 = _append(conn, urgency="fyi")
        d2 = _append(conn, urgency="fyi")

    plan_ = InjectionPlan(
        batch=[b1, b2],
        digest_source=[d1, d2],
        ack_up_to=d2.id,
    )
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=1)

    assert code == 0
    assert len(client.prompts) == 1
    text = client.prompts[0][1]
    assert f"batch=[{b1.id},{b2.id}]" in text
    assert f"digest=[{d1.id},{d2.id}]" in text

    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
    assert cur is not None
    assert cur.last_ack_id == d2.id


def test_ack_only_after_full_boundary_succeeds(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two blocking messages; the double raises on the second prompt.
    NOTHING may be acked — the boundary did not complete, so both stay
    pending and the next process redelivers (at-least-once).

    History: this test originally pinned per-prompt acking (first
    message acked before the second was attempted). The opus verify
    round proved that shape LOSES messages — an interrupt's ack at
    ack_up_to could cover lower-id batch messages a failed batch prompt
    never delivered — so the contract moved to one ack per completed
    boundary."""
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg_a = _append(conn, urgency="blocking")
        msg_b = _append(conn, urgency="blocking")

    plan_ = InjectionPlan(interrupt=[msg_a, msg_b], ack_up_to=msg_b.id)
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    client.next_results = [
        PromptResult(stop_reason="end_turn", text="ok-a", raw_updates=[]),
        AcpError("boom"),
    ]
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=5)

    assert code == 10
    assert len(client.prompts) == 2  # both attempted; second raised

    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
        still_pending = cursors.pending(conn, CONSUMER, CHANNEL)

    assert cur is None  # no ack landed — the boundary never completed
    assert [m.id for m in still_pending] == [msg_a.id, msg_b.id]


def test_deferred_fyi_never_acked_and_blocks_later_ack(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A deferred fyi with a LOWER id than a later batch message on the
    same channel: ack_up_to stops before it (policy's contract), so the
    harness must not advance the cursor past — or even up to — the
    later batch message either (single per-channel cursor)."""
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        deferred_msg = _append(conn, urgency="fyi")
        batch_msg = _append(conn, urgency="prompt")

    plan_ = InjectionPlan(
        batch=[batch_msg],
        deferred=[deferred_msg],
        ack_up_to=0,  # stops BEFORE deferred_msg.id, per policy's contract
    )
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=1)

    assert code == 0
    assert len(client.prompts) == 1  # batch still delivered — delivery isn't ack

    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
    assert cur is None  # never acked at all


def test_reply_posted_with_stop_reason(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reply_channel = "run/run1/telemetry"
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg = _append(conn, urgency="blocking")

    plan_ = InjectionPlan(interrupt=[msg], ack_up_to=msg.id)
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    client.next_results = [
        PromptResult(stop_reason="max_tokens", text="hello there", raw_updates=[{"a": 1}])
    ]
    config = _config(db_path=db, reply_channel=reply_channel)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=1)

    assert code == 0
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        posted = log.read_after(conn, reply_channel, 0)

    replies = [m for m in posted if m.type == "acp-reply"]
    assert len(replies) == 1
    assert replies[0].body == {"text": "hello there", "stop_reason": "max_tokens", "boundary": 1}
    assert replies[0].sender == CONSUMER


def test_no_reply_channel_posts_nothing(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg = _append(conn, urgency="blocking")

    plan_ = InjectionPlan(interrupt=[msg], ack_up_to=msg.id)
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    config = _config(db_path=db, reply_channel=None)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=1)

    assert code == 0  # no reply_channel configured -> nothing to assert beyond a clean run


def test_child_death_exits_zero_without_delivering(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_ = InjectionPlan()  # unused: loop must exit before ever calling plan()
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    dead_child = FakeChild([0])  # already exited when first polled
    config = _config(db_path=db)
    code = run_harness(config, dead_child, client=client, max_boundaries=None)

    assert code == 0
    assert client.prompts == []


def test_acp_error_on_initialize_returns_10(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan_ = InjectionPlan()
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient(init_error=AcpError("child EOF"))
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=None)

    assert code == 10


def test_max_boundaries_stops_the_loop(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg = _append(conn, urgency="prompt")

    plan_ = InjectionPlan(batch=[msg], ack_up_to=msg.id)
    _stub_plan(monkeypatch, plan_)

    client = FakeAcpClient()
    config = _config(db_path=db)
    code = run_harness(config, _never_dies(), client=client, max_boundaries=3)

    assert code == 0
    assert len(client.prompts) == 3  # same fixed plan delivered once per boundary


def test_nothing_to_deliver_sleeps_then_rechecks_liveness(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from raven_bus import channels

    with db_mod.connection(db) as conn:
        channels.ensure_channel(conn, CHANNEL)

    _stub_plan(monkeypatch, InjectionPlan())  # always empty: nothing deliverable
    monkeypatch.setattr(harness.time, "sleep", lambda _s: None)

    client = FakeAcpClient()
    # still running on the first two checks, dead on the third.
    child = FakeChild([None, None, 0])
    config = _config(db_path=db)
    code = run_harness(config, child, client=client, max_boundaries=None)

    assert code == 0
    assert client.prompts == []


# --------------------------------------------------------------------------- #
# parse_channels
# --------------------------------------------------------------------------- #
def test_parse_channels_validates_and_returns_tuple() -> None:
    assert parse_channels(["run/r1/a", "run/r1/b"]) == ("run/r1/a", "run/r1/b")


def test_parse_channels_rejects_bad_grammar() -> None:
    with pytest.raises(InvalidAddressError):
        parse_channels(["Not-Valid/UPPER"])


def test_parse_channels_rejects_empty() -> None:
    with pytest.raises(InvalidAddressError):
        parse_channels([])


# --------------------------------------------------------------------------- #
# CLI — `raven acp`
# --------------------------------------------------------------------------- #
def test_cli_registration_exists() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "acp" in result.stdout


def test_cli_missing_double_dash_command_is_usage_error(db: Path) -> None:
    result = runner.invoke(
        app,
        ["acp", "--as", CONSUMER, "--channel", CHANNEL, "--db", str(db)],
    )
    assert result.exit_code == 2


def test_cli_double_dash_parsing_and_exit_code(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict = {}

    class _FakeProc:
        def __init__(self, argv, **kwargs) -> None:
            captured["argv"] = argv
            captured["kwargs"] = kwargs

        def poll(self):
            return None

        def terminate(self) -> None:
            captured["terminated"] = True

    def _fake_run_harness(config, child, **kwargs) -> int:
        captured["config"] = config
        return 7

    monkeypatch.setattr("raven_bus.cli.acp.subprocess.Popen", _FakeProc)
    monkeypatch.setattr("raven_bus.cli.acp.run_harness", _fake_run_harness)

    result = runner.invoke(
        app,
        [
            "acp",
            "--as",
            CONSUMER,
            "--channel",
            CHANNEL,
            "--reply-to",
            "run/run1/telemetry",
            "--db",
            str(db),
            "--poll-interval",
            "0.5",
            "--budget",
            "111",
            "--cwd",
            "/tmp/agent",
            "--mode",
            "bypassPermissions",
            "--",
            "some-agent",
            "--flag",
            "value",
        ],
    )

    assert result.exit_code == 7
    assert captured["argv"] == ["some-agent", "--flag", "value"]
    config = captured["config"]
    assert config.consumer == CONSUMER
    assert config.channels == (CHANNEL,)
    assert config.reply_channel == "run/run1/telemetry"
    assert config.poll_interval_s == 0.5
    assert config.token_budget == 111
    assert config.cwd == "/tmp/agent"
    assert config.mode == "bypassPermissions"


def test_cli_initial_prompt_file_read_and_passed_verbatim(
    db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packet = tmp_path / "packet.md"
    packet.write_text("do the thing\n", encoding="utf-8")
    captured: dict = {}

    class _FakeProc:
        def __init__(self, argv, **kwargs) -> None:
            pass

        def poll(self):
            return 0

        def terminate(self) -> None:
            pass  # pragma: no cover -- child already exited

    def _fake_run_harness(config, child, **kwargs) -> int:
        captured["config"] = config
        return 0

    monkeypatch.setattr("raven_bus.cli.acp.subprocess.Popen", _FakeProc)
    monkeypatch.setattr("raven_bus.cli.acp.run_harness", _fake_run_harness)

    result = runner.invoke(
        app,
        [
            "acp", "--as", CONSUMER, "--channel", CHANNEL, "--db", str(db),
            "--initial-prompt-file", str(packet), "--", "agent",
        ],
    )
    assert result.exit_code == 0
    assert captured["config"].initial_prompt == "do the thing\n"


def test_cli_ensures_watched_and_reply_channels_before_loop(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lane must be startable before its orchestrator has sent anything:
    the CLI pre-creates its channels (broadcast) so the first pending()
    poll cannot raise UnknownChannelError (P4b live finding)."""

    class _FakeProc:
        def __init__(self, argv, **kwargs) -> None:
            pass

        def poll(self):
            return 0

        def terminate(self) -> None:
            pass  # pragma: no cover -- child already exited

    monkeypatch.setattr("raven_bus.cli.acp.subprocess.Popen", _FakeProc)
    monkeypatch.setattr("raven_bus.cli.acp.run_harness", lambda *a, **k: 0)

    result = runner.invoke(
        app,
        [
            "acp", "--as", CONSUMER, "--channel", "run/run1/fresh",
            "--reply-to", "run/run1/freshtele", "--db", str(db), "--", "agent",
        ],
    )
    assert result.exit_code == 0
    from raven_bus import channels as channels_mod

    with db_mod.connection(db) as conn:
        assert channels_mod.get_channel(conn, "run/run1/fresh").kind == "broadcast"
        assert channels_mod.get_channel(conn, "run/run1/freshtele").kind == "broadcast"


def test_cli_initial_prompt_file_missing_is_usage_error(
    db: Path, tmp_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "acp", "--as", CONSUMER, "--channel", CHANNEL, "--db", str(db),
            "--initial-prompt-file", str(tmp_path / "nope.md"), "--", "agent",
        ],
    )
    assert result.exit_code == 2


def test_cli_initial_prompt_file_empty_is_usage_error(
    db: Path, tmp_path: Path
) -> None:
    packet = tmp_path / "blank.md"
    packet.write_text("   \n", encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "acp", "--as", CONSUMER, "--channel", CHANNEL, "--db", str(db),
            "--initial-prompt-file", str(packet), "--", "agent",
        ],
    )
    assert result.exit_code == 2


def test_cli_blank_mode_is_usage_error(db: Path) -> None:
    result = runner.invoke(
        app,
        [
            "acp", "--as", CONSUMER, "--channel", CHANNEL,
            "--db", str(db), "--mode", "  ", "--", "echo",
        ],
    )
    assert result.exit_code == 2


def test_mode_sent_once_after_session_new_before_any_prompt(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db_mod.connection(db) as conn:
        msg = _append(conn)
    _stub_plan(monkeypatch, InjectionPlan(batch=[msg], ack_up_to=msg.id))

    client = FakeAcpClient()
    rc = run_harness(
        _config(db_path=db, mode="bypassPermissions"),
        _never_dies(),
        client=client,
        max_boundaries=1,
    )

    assert rc == 0
    assert client.modes == [("sess-1", "bypassPermissions")]
    assert client.calls[:3] == ["initialize", "new_session", "set_mode"]
    assert client.calls.count("set_mode") == 1


def test_mode_none_never_sends_set_mode(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db_mod.connection(db) as conn:
        msg = _append(conn)
    _stub_plan(monkeypatch, InjectionPlan(batch=[msg], ack_up_to=msg.id))

    client = FakeAcpClient()
    rc = run_harness(_config(db_path=db), _never_dies(), client=client, max_boundaries=1)

    assert rc == 0
    assert client.modes == []
    assert "set_mode" not in client.calls


def test_initial_prompt_sent_verbatim_first_with_boundary_zero_telemetry(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db_mod.connection(db) as conn:
        msg = _append(conn)
    _stub_plan(monkeypatch, InjectionPlan(batch=[msg], ack_up_to=msg.id))

    packet = "GUARD PREAMBLE\n\nDo the task.\n"
    client = FakeAcpClient()
    rc = run_harness(
        _config(
            db_path=db,
            reply_channel="run/run1/telemetry",
            mode="bypassPermissions",
            initial_prompt=packet,
        ),
        _never_dies(),
        client=client,
        max_boundaries=1,
    )

    assert rc == 0
    # verbatim — never through policy.render (trusted spawner input),
    # sent after set_mode and before the first bus boundary
    assert client.prompts[0] == ("sess-1", packet)
    assert client.calls[:4] == ["initialize", "new_session", "set_mode", "prompt"]
    with db_mod.connection(db) as conn:
        replies = [
            (m.body["boundary"], m.type)
            for m in log.read_after(conn, "run/run1/telemetry", after_id=0)
            if m.type == "acp-reply"
        ]
    assert replies == [(0, "acp-reply"), (1, "acp-reply")]


def test_set_mode_error_returns_10_and_delivers_nothing(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db_mod.connection(db) as conn:
        msg = _append(conn)
    _stub_plan(monkeypatch, InjectionPlan(batch=[msg], ack_up_to=msg.id))

    client = FakeAcpClient(set_mode_error=AcpError("mode refused"))
    rc = run_harness(
        _config(db_path=db, mode="no-such-mode"),
        _never_dies(),
        client=client,
    )

    assert rc == 10
    assert client.prompts == []
    # the message was never delivered, so it must still be pending
    with db_mod.connection(db) as conn:
        assert [m.id for m in cursors.pending(conn, CONSUMER, CHANNEL)] == [msg.id]


def test_cli_rejects_bad_consumer_id(db: Path) -> None:
    result = runner.invoke(
        app,
        ["acp", "--as", "not valid", "--channel", CHANNEL, "--db", str(db), "--", "echo"],
    )
    assert result.exit_code == 2


def test_reply_channel_must_not_be_watched() -> None:
    """Feedback-loop guard (opus verify finding): telemetry posted to a
    watched channel would be digested back into the agent."""
    with pytest.raises(ValueError, match="telemetry back"):
        HarnessConfig(
            consumer=CONSUMER,
            channels=(CHANNEL,),
            reply_channel=CHANNEL,
        )


def test_delivered_watermark_prevents_redelivery_storm(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A deferred fyi pins ack_up_to below a delivered blocking message;
    the session must NOT redeliver the blocking message every tick
    (opus verify finding: full-CPU byte-identical prompt storm)."""
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg_fyi = _append(conn, urgency="fyi")
        msg_block = _append(conn, urgency="blocking")

    # Real policy: fyi deferred (below thresholds) pins ack to fyi.id-1;
    # blocking delivers. Second boundary must find nothing deliverable.
    client = FakeAcpClient()
    client.next_results = [
        PromptResult(stop_reason="end_turn", text="ok", raw_updates=[]),
    ]
    config = _config(db_path=db, poll_interval_s=0.01)
    code = run_harness(config, _dies_after(3), client=client, max_boundaries=5)

    assert code == 0
    delivered_ids = [p for p in client.prompts]
    # exactly ONE delivery of the blocking message, not one per tick
    assert len(delivered_ids) == 1
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        still = cursors.pending(conn, CONSUMER, CHANNEL)
    assert [m.id for m in still] == [msg_fyi.id, msg_block.id]


# --------------------------------------------------------------------------- #
# Deferred-below-delivered: never ack past an undelivered id (issue #3).
#
# The redelivery-storm guard above used to be a per-channel MAX delivered
# id. That hid every pending id below the max — including ones policy had
# DEFERRED, not delivered — from plan(), which then computed ack_up_to
# without them and the cursor jumped over messages injected zero times.
# These run the REAL policy against the real store; a sender publishes
# mid-turn via _PublishingClient.
# --------------------------------------------------------------------------- #
class _PublishingClient(FakeAcpClient):
    """FakeAcpClient that calls ``on_prompt(n)`` after its n-th prompt —
    a sender publishing onto the bus while the agent is mid-turn."""

    def __init__(self, on_prompt: Callable[[int], None]) -> None:
        super().__init__()
        self._on_prompt = on_prompt

    def prompt(self, session_id: str, text: str) -> PromptResult:
        result = super().prompt(session_id, text)
        self._on_prompt(len(self.prompts))
        return result


def _injected_ids(text: str) -> list[int]:
    """Ids ``policy.render`` put in front of the agent: full message
    blocks (``id: N`` lines) plus digest lines (``[N] sender ...``)."""
    full = re.findall(r"^id: (\d+)$", text, re.M)
    digest = re.findall(r"^\[(\d+)\] ", text, re.M)
    return [int(i) for i in full + digest]


def test_deferred_fyi_below_delivered_prompt_is_not_acked_past(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #3 scenario A: fyi #1 (below digest thresholds) + prompt #2;
    a second prompt arrives mid-turn. Boundary 2 must still see #1 as
    deferred — the old max-watermark hid it and acked straight past."""
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        msg_fyi = _append(conn, urgency="fyi")
        msg_a = _append(conn, urgency="prompt")

    late: list[Message] = []

    def _publish(n: int) -> None:
        if n == 1:
            with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
                late.append(_append(conn, urgency="prompt"))

    client = _PublishingClient(_publish)
    code = run_harness(_config(db_path=db), _dies_after(6), client=client, max_boundaries=2)

    assert code == 0
    assert [_injected_ids(t) for _, t in client.prompts] == [[msg_a.id], [late[0].id]]
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
        still = [m.id for m in cursors.pending(conn, CONSUMER, CHANNEL)]
    assert cur is None  # the never-injected fyi still pins the cursor
    assert msg_fyi.id in still


def test_budget_shed_prompts_below_delivered_blocking_are_not_lost(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #3 scenario B: six ~3 KB prompts + a blocking #7. Boundary 1
    delivers #7 and the two prompts the budget fits; #3-#6 are shed. A
    late steer must not let the cursor jump over the shed four — every
    message is injected exactly once, and the cursor lands on the last."""
    pad = "x" * 3000
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        shed = [
            log.append(conn, channel=CHANNEL, sender="peer@run1", type="steer",
                       body={"n": i, "pad": pad})
            for i in range(6)
        ]
        stop = _append(conn, urgency="blocking")

    late: list[Message] = []

    def _publish(n: int) -> None:
        if n == 1:
            with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
                late.append(_append(conn, urgency="prompt"))

    client = _PublishingClient(_publish)
    code = run_harness(_config(db_path=db), _dies_after(8), client=client, max_boundaries=3)

    assert code == 0
    injected = [i for _, t in client.prompts for i in _injected_ids(t)]
    expected = [m.id for m in shed] + [stop.id, late[0].id]
    assert sorted(injected) == sorted(expected)  # each exactly once, none lost
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
        still = cursors.pending(conn, CONSUMER, CHANNEL)
    assert cur is not None and cur.last_ack_id == late[0].id
    assert still == []


def test_deferred_fyi_still_reaches_digest_after_later_delivery(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #3 scenario C: a deferred fyi must stay visible to plan() so
    its digest trigger can fire. digest_min_count=2 here; the second fyi
    arrives mid-turn, so boundary 2 digests BOTH and the cursor clears."""
    real_plan = harness.policy.plan
    monkeypatch.setattr(
        harness.policy, "plan",
        lambda pending, **kw: real_plan(pending, digest_min_count=2, **kw),
    )
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        fyi_1 = _append(conn, urgency="fyi")
        msg_a = _append(conn, urgency="prompt")

    late: list[Message] = []

    def _publish(n: int) -> None:
        if n == 1:
            with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
                late.append(_append(conn, urgency="fyi"))

    client = _PublishingClient(_publish)
    code = run_harness(_config(db_path=db), _dies_after(6), client=client, max_boundaries=2)

    assert code == 0
    assert [_injected_ids(t) for _, t in client.prompts] == [
        [msg_a.id], [fyi_1.id, late[0].id],
    ]
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, CHANNEL)
        still = cursors.pending(conn, CONSUMER, CHANNEL)
    assert cur is not None and cur.last_ack_id == late[0].id
    assert still == []


def test_store_error_exits_ten_not_traceback(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GLM verify finding: an sqlite busy-timeout in the store path
    escaped run_harness as a crash; it must exit 10 like AcpError."""
    import sqlite3 as sqlite3_mod

    def _busy(*_args):
        raise sqlite3_mod.OperationalError("database is locked")

    monkeypatch.setattr(harness, "_gather_pending", _busy)
    client = FakeAcpClient()
    code = run_harness(_config(db_path=db), _never_dies(), client=client)
    assert code == 10


def test_cli_unlaunchable_agent_is_one_line_error() -> None:
    """Found by the live P3 smoke: a missing agent binary tracebacked."""
    result = runner.invoke(
        app,
        ["acp", "--as", CONSUMER, "--channel", CHANNEL, "--",
         "definitely-not-a-real-binary-xyz"],
    )
    assert result.exit_code == 10
    assert "error:" in result.output
    assert "cannot launch agent" in result.output
    assert "Traceback" not in result.output


# --------------------------------------------------------------------------- #
# QA findings A1/A6 — per-channel prefix acking and gathering past a pin.
#
# The old ack acked only THIS boundary's ids, capped at the plan's GLOBAL
# ack_up_to. Ids delivered under a pin (an undelivered lower id) were never
# acked afterwards; once cursors.pending's 100-row window was all such ids
# the channel stalled for the session (later BLOCKING messages included), a
# clean restart re-injected them, and one channel's deferred fyi pinned
# every other channel. These run the REAL policy against the real store.
# --------------------------------------------------------------------------- #
A_CH = "run/run1/a"
B_CH = "run/run1/b"


class _StagedChild(FakeChild):
    """Alive for ``alive`` polls; runs ``hooks[n]`` on the n-th poll (a
    sender publishing, a clock jump) before answering."""

    def __init__(self, alive: int, hooks: dict[int, Callable[[], None]]) -> None:
        super().__init__([None])
        self.polls = 0
        self._alive = alive
        self._hooks = hooks

    def poll(self) -> int | None:
        self.polls += 1
        if self.polls in self._hooks:
            self._hooks[self.polls]()
        return None if self.polls < self._alive else 0


def _clock(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Fake harness clock; bump ``state['offset']`` to age fyi mid-run."""
    from datetime import datetime as real_datetime
    from datetime import timedelta

    state = {"offset": timedelta(0)}

    class _FakeDatetime:
        @staticmethod
        def now(tz=None):
            return real_datetime.now(tz) + state["offset"]

    monkeypatch.setattr(harness, "datetime", _FakeDatetime)
    return state


def _all_injected(client: FakeAcpClient) -> list[int]:
    return [i for _, t in client.prompts for i in _injected_ids(t)]


def _cursor(db: Path, channel: str) -> int | None:
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        cur = cursors.get_cursor(conn, CONSUMER, channel)
    return None if cur is None else cur.last_ack_id


def test_pin_on_one_channel_neither_stalls_nor_pins_another(db: Path) -> None:
    """probe_stall: a held fyi on A + a 110-message burst on B + a later
    BLOCKING on B, one session. Old code: B's window filled with
    delivered-but-unackable ids and the blocking message was never seen."""
    from raven_bus import channels

    late: list[Message] = []
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        channels.ensure_channel(conn, A_CH)
        fyi = _append(conn, urgency="fyi", channel=A_CH)
        burst = [_append(conn, channel=B_CH) for _ in range(110)]

    def _publish() -> None:
        with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
            late.append(_append(conn, urgency="blocking", channel=B_CH))

    client = FakeAcpClient()
    code = run_harness(
        _config(db_path=db, channels=(A_CH, B_CH)),
        _StagedChild(40, {20: _publish}),
        client=client,
    )

    assert code == 0
    injected = _all_injected(client)
    assert sorted(injected) == [m.id for m in burst] + [late[0].id]  # each once
    assert _cursor(db, B_CH) == late[0].id  # B fully acked despite A's pin
    assert _cursor(db, A_CH) is None  # the held fyi was never injected
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        assert [m.id for m in cursors.pending(conn, CONSUMER, A_CH)] == [fyi.id]


def test_blocking_beyond_a_delivered_window_is_injected_promptly(db: Path) -> None:
    """probe_starve1 (A6): ONE channel, a held fyi pinning 110 delivered
    prompts. A blocking message landing past the 100-row window must be
    injected at once, not after the fyi ages out."""
    late: list[Message] = []
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        fyi = _append(conn, urgency="fyi")
        prompts = [_append(conn) for _ in range(110)]

    def _publish() -> None:
        with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
            late.append(_append(conn, urgency="blocking"))

    client = FakeAcpClient()
    child = _StagedChild(40, {20: _publish})
    code = run_harness(_config(db_path=db), child, client=client)

    assert code == 0
    injected = _all_injected(client)
    assert sorted(injected) == [m.id for m in prompts] + [late[0].id]
    assert fyi.id not in injected
    assert _cursor(db, CHANNEL) is None  # pinned by the held fyi, correctly


def test_cleared_pin_acks_the_whole_prefix_so_restart_reinjects_nothing(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """probe_followup: once the fyi pin is digested, every id delivered
    under it is acked; a clean restart then injects nothing again."""
    from datetime import timedelta

    from raven_bus import channels

    clock = _clock(monkeypatch)
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        channels.ensure_channel(conn, B_CH)
        f = _append(conn, urgency="fyi", channel=A_CH)
        pa = _append(conn, channel=A_CH)
        pb = _append(conn, channel=B_CH)

    def _age() -> None:
        clock["offset"] = timedelta(minutes=10)

    cfg = _config(db_path=db, channels=(A_CH, B_CH))
    first = FakeAcpClient()
    assert run_harness(cfg, _StagedChild(20, {10: _age}), client=first) == 0
    assert [_injected_ids(t) for _, t in first.prompts] == [[pa.id, pb.id], [f.id]]
    assert _cursor(db, A_CH) == pa.id
    assert _cursor(db, B_CH) == pb.id

    second = FakeAcpClient()
    assert run_harness(cfg, _StagedChild(5, {}), client=second) == 0
    assert second.prompts == []


def test_idle_tick_acks_a_prefix_whose_pin_cleared_without_a_delivery(
    db: Path,
) -> None:
    """The pin can leave pending with no delivery (another process acks
    past it, or it expires): the next idle tick acks the delivered
    prefix instead of waiting for some later message on the channel."""
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        fyi = _append(conn, urgency="fyi")
        msg = _append(conn)

    def _external_ack() -> None:
        with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
            cursors.ack(conn, CONSUMER, CHANNEL, fyi.id)

    client = FakeAcpClient()
    code = run_harness(_config(db_path=db), _StagedChild(12, {6: _external_ack}), client=client)

    assert code == 0
    assert [_injected_ids(t) for _, t in client.prompts] == [[msg.id]]
    assert _cursor(db, CHANNEL) == msg.id


def test_gather_pages_past_delivered_ids_and_is_bounded(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(harness, "_GATHER_PAGE", 2)
    monkeypatch.setattr(harness, "_GATHER_MAX_PAGES", 3)
    with db_mod.connection(db) as conn:  # type: ignore[attr-defined]
        ids = [_append(conn).id for _ in range(10)]
    cfg = _config(db_path=db)

    # nothing delivered: one page, as before
    assert [m.id for m in harness._gather_pending(cfg, {})[CHANNEL]] == ids[:2]
    # first 3 delivered: pages until a page-worth (2) is undelivered
    got = harness._gather_pending(cfg, {CHANNEL: set(ids[:3])})[CHANNEL]
    assert [m.id for m in got] == ids[:6][:len(got)] and len(got) == 6
    # everything delivered: stops at the page bound, contiguous
    got = harness._gather_pending(cfg, {CHANNEL: set(ids)})[CHANNEL]
    assert [m.id for m in got] == ids[:6]


def test_prune_forgets_only_ids_that_can_never_be_pending_again() -> None:
    def _m(i: int) -> Message:
        from datetime import UTC, datetime

        return Message(id=i, channel=CHANNEL, sender="p@run1", type="t", body={},
                       created_at=datetime.now(UTC))

    delivered = {CHANNEL: {3, 10, 500}, A_CH: {7}}
    harness._prune_delivered(delivered, {CHANNEL: [_m(10), _m(11)], A_CH: []})
    # 3 is below the lowest pending id (acked/expired); 500 is past the
    # gathered window and may still be pending — kept.
    assert delivered == {CHANNEL: {10, 500}, A_CH: set()}
