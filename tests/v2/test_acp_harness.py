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

import sqlite3
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

    def __init__(self, *, init_error: AcpError | None = None) -> None:
        self.init_error = init_error
        self.initialized = False
        self.session_cwd: str | None = None
        self.prompts: list[tuple[str, str]] = []  # (session_id, text)
        self.cancelled: list[str] = []
        self.next_results: list[PromptResult | AcpError] = []

    def initialize(self) -> dict:
        if self.init_error is not None:
            raise self.init_error
        self.initialized = True
        return {}

    def new_session(self, *, cwd: str) -> str:
        self.session_cwd = cwd
        return "sess-1"

    def prompt(self, session_id: str, text: str) -> PromptResult:
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
