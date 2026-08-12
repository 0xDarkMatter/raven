"""Minimal ACP stdio protocol tests against the deterministic fake agent."""

from __future__ import annotations

import io
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from raven_bus.adapters.acp.protocol import AcpClient, AcpError, build_agent_command

FAKE_AGENT = Path(__file__).with_name("fake_acp_agent.py")


def spawn_agent(scenario: str = "echo", *, text: bool = False) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(FAKE_AGENT), "--scenario", scenario],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=text,
        encoding="utf-8" if text else None,
    )


@pytest.fixture()
def children() -> Iterator[list[subprocess.Popen]]:
    spawned: list[subprocess.Popen] = []
    yield spawned
    for child in spawned:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=5)
        for pipe in (child.stdin, child.stdout, child.stderr):
            if pipe is not None:
                pipe.close()


def client_for(
    children: list[subprocess.Popen],
    scenario: str = "echo",
    **kwargs: Any,
) -> tuple[subprocess.Popen, AcpClient]:
    child = spawn_agent(scenario)
    children.append(child)
    return child, AcpClient(child, **kwargs)


def ready_client(
    children: list[subprocess.Popen],
    scenario: str = "echo",
    **kwargs: Any,
) -> tuple[subprocess.Popen, AcpClient, str]:
    child, client = client_for(children, scenario, **kwargs)
    assert client.initialize() == {"fakeAgent": True}
    session_id = client.new_session(cwd=".")
    assert session_id == "fake-session"
    return child, client, session_id


def test_echo_round_trip_collects_chunks_and_update_order(children) -> None:
    seen: list[dict[str, Any]] = []
    _, client, session_id = ready_client(children, on_update=seen.append)

    result = client.prompt(session_id, "hello")

    assert result.stop_reason == "end_turn"
    assert result.text == "echo: hello"
    assert [params["ordinal"] for params in result.raw_updates] == [1, 2]
    assert seen == result.raw_updates


def test_multiple_prompts_use_monotonic_ids_and_correlate_responses(children) -> None:
    _, client, session_id = ready_client(children)

    assert client.prompt(session_id, "first").text == "echo: first"
    assert client.prompt(session_id, "second").text == "echo: second"


def test_slow_scenario_completes_within_timeout(children) -> None:
    _, client, session_id = ready_client(children, "slow", timeout_s=2.0)

    assert client.prompt(session_id, "patient").text == "echo: patient"


def test_slow_scenario_checks_wall_clock_after_blocking_read(children) -> None:
    # Generous timeout for the handshake (a 50ms budget expires during
    # python child startup on Windows), shrunk only for the prompt so
    # the slow scenario's inter-chunk sleep is what trips the deadline.
    _, client, session_id = ready_client(children, "slow")
    client._timeout_s = 0.05

    with pytest.raises(AcpError, match="timed out"):
        client.prompt(session_id, "late")


def test_permission_request_gets_method_not_found_and_prompt_completes(
    children,
) -> None:
    _, client, session_id = ready_client(children, "request-perms")

    result = client.prompt(session_id, "safe")

    assert result.text == "echo: safe"
    assert [params["ordinal"] for params in result.raw_updates] == [0, 1, 2]
    reply = result.raw_updates[0]["observedPermissionReply"]
    assert reply == {
        "jsonrpc": "2.0",
        "id": 9001,
        "error": {"code": -32601, "message": "Method not found"},
    }


def test_die_mid_prompt_reports_agent_exited(children) -> None:
    child, client, session_id = ready_client(children, "die-mid-prompt")

    with pytest.raises(AcpError, match="agent exited"):
        client.prompt(session_id, "goodbye")

    assert child.wait(timeout=5) == 1


def test_garbage_line_is_a_protocol_error(children) -> None:
    _, client = client_for(children, "garbage")

    with pytest.raises(AcpError, match="malformed JSON"):
        client.initialize()


def test_client_accepts_explicit_utf8_text_mode_pipes(children) -> None:
    child = spawn_agent(text=True)
    children.append(child)
    client = AcpClient(child)

    assert client.initialize() == {"fakeAgent": True}


def test_cancel_is_fire_and_forget(children) -> None:
    _, client, session_id = ready_client(children)

    client.cancel(session_id)

    assert client.prompt(session_id, "still alive").text == "echo: still alive"


class ScriptedChild:
    def __init__(self, messages: list[dict[str, Any] | str]) -> None:
        self.stdin = io.StringIO()
        self.stdout = io.StringIO(
            "".join(
                (message if isinstance(message, str) else json.dumps(message)) + "\n"
                for message in messages
            )
        )


class BrokenReader(io.StringIO):
    def readline(self, *args: Any, **kwargs: Any) -> str:
        raise OSError("read failed")


class BrokenWriter(io.StringIO):
    def write(self, value: str) -> int:
        raise BrokenPipeError("write failed")


def test_interleaved_notifications_do_not_break_response_correlation() -> None:
    update = {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "s",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": [
                    {"type": "text", "text": "a"},
                    {"type": "image", "data": "ignored"},
                    {"type": "text", "text": "b"},
                ],
            },
        },
    }
    child = ScriptedChild(
        [update, {"jsonrpc": "2.0", "id": 1, "result": {"stopReason": "end_turn"}}]
    )

    result = AcpClient(child).prompt("s", "question")  # type: ignore[arg-type]

    assert result.text == "ab"
    assert result.raw_updates == [update["params"]]


def test_response_id_must_match_outstanding_request() -> None:
    child = ScriptedChild(
        [{"jsonrpc": "2.0", "id": 99, "result": {"stopReason": "end_turn"}}]
    )

    with pytest.raises(AcpError, match="unexpected response id 99"):
        AcpClient(child).prompt("s", "question")  # type: ignore[arg-type]


def test_json_rpc_error_response_becomes_acp_error() -> None:
    child = ScriptedChild(
        [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32000, "message": "agent refused"},
            }
        ]
    )

    with pytest.raises(AcpError, match="-32000: agent refused"):
        AcpClient(child).initialize()  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ({"jsonrpc": "2.0", "result": {}}, "invalid JSON-RPC response"),
        ({"jsonrpc": "2.0", "id": 1, "error": "bad"}, "JSON-RPC error"),
        ({"jsonrpc": "2.0", "id": 1, "result": []}, "result must be an object"),
    ],
)
def test_invalid_response_shapes_raise_acp_error(response, message) -> None:
    with pytest.raises(AcpError, match=message):
        AcpClient(ScriptedChild([response])).initialize()  # type: ignore[arg-type]


def test_invalid_json_rpc_frame_raises_acp_error() -> None:
    child = ScriptedChild([{"jsonrpc": "1.0", "id": 1, "result": {}}])

    with pytest.raises(AcpError, match="malformed JSON-RPC"):
        AcpClient(child).initialize()  # type: ignore[arg-type]


def test_read_failure_raises_acp_error() -> None:
    child = SimpleNamespace(stdin=io.StringIO(), stdout=BrokenReader())

    with pytest.raises(AcpError, match="failed to read agent output"):
        AcpClient(child).initialize()  # type: ignore[arg-type]


def test_write_failure_reports_agent_exited() -> None:
    child = SimpleNamespace(stdin=BrokenWriter(), stdout=io.StringIO())

    with pytest.raises(AcpError, match="agent exited"):
        AcpClient(child).initialize()  # type: ignore[arg-type]


def test_expired_deadline_raises_timeout_before_read() -> None:
    with pytest.raises(AcpError, match="timed out"):
        AcpClient(ScriptedChild([]), timeout_s=-1).initialize()  # type: ignore[arg-type]


def test_blank_lines_are_ignored() -> None:
    child = ScriptedChild(
        ["", "   ", {"jsonrpc": "2.0", "id": 1, "result": {"sessionId": "s"}}]
    )

    assert AcpClient(child).new_session(cwd=".") == "s"  # type: ignore[arg-type]


def test_unknown_notifications_are_ignored() -> None:
    child = ScriptedChild(
        [
            {"jsonrpc": "2.0", "method": "agent/status", "params": {}},
            {"jsonrpc": "2.0", "id": 1, "result": {"agentCapabilities": {}}},
        ]
    )

    assert AcpClient(child).initialize() == {}  # type: ignore[arg-type]


def test_invalid_session_update_params_raise_acp_error() -> None:
    child = ScriptedChild(
        [{"jsonrpc": "2.0", "method": "session/update", "params": []}]
    )

    with pytest.raises(AcpError, match="params must be an object"):
        AcpClient(child).initialize()  # type: ignore[arg-type]


def test_non_object_update_is_collected_but_not_rendered() -> None:
    params = {"sessionId": "s", "update": "status"}
    child = ScriptedChild(
        [
            {"jsonrpc": "2.0", "method": "session/update", "params": params},
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"stopReason": "end_turn"},
            },
        ]
    )

    result = AcpClient(child).prompt("s", "question")  # type: ignore[arg-type]

    assert result.text == ""
    assert result.raw_updates == [params]


def test_initialize_advertises_no_client_capabilities() -> None:
    child = ScriptedChild(
        [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"agentCapabilities": {}},
            }
        ]
    )
    client = AcpClient(child)  # type: ignore[arg-type]

    assert client.initialize() == {}
    frame = json.loads(child.stdin.getvalue())
    assert frame["method"] == "initialize"
    assert frame["params"]["clientCapabilities"] == {}


def test_request_ids_are_monotonically_increasing_integers() -> None:
    child = ScriptedChild(
        [
            {"jsonrpc": "2.0", "id": 1, "result": {"agentCapabilities": {}}},
            {"jsonrpc": "2.0", "id": 2, "result": {"sessionId": "s"}},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "result": {"stopReason": "end_turn"},
            },
        ]
    )
    client = AcpClient(child)  # type: ignore[arg-type]

    client.initialize()
    client.new_session(cwd=".")
    client.prompt("s", "question")

    frames = [json.loads(line) for line in child.stdin.getvalue().splitlines()]
    assert [frame["id"] for frame in frames] == [1, 2, 3]
    assert all(type(frame["id"]) is int for frame in frames)


@pytest.mark.parametrize(
    ("method", "result", "message"),
    [
        ("initialize", {"agentCapabilities": []}, "invalid agentCapabilities"),
        ("new_session", {"sessionId": 42}, "invalid sessionId"),
        ("prompt", {"stopReason": None}, "invalid stopReason"),
    ],
)
def test_invalid_contract_results_raise_acp_error(method, result, message) -> None:
    child = ScriptedChild([{"jsonrpc": "2.0", "id": 1, "result": result}])
    client = AcpClient(child)  # type: ignore[arg-type]

    with pytest.raises(AcpError, match=message):
        if method == "initialize":
            client.initialize()
        elif method == "new_session":
            client.new_session(cwd=".")
        else:
            client.prompt("s", "question")


def test_constructor_requires_both_pipes() -> None:
    with pytest.raises(ValueError, match="stdin and stdout"):
        AcpClient(SimpleNamespace(stdin=None, stdout=io.StringIO()))  # type: ignore[arg-type]


def test_build_agent_command_returns_an_independent_list() -> None:
    argv = ["agent", "--flag"]

    command = build_agent_command(argv)

    assert command == argv
    assert command is not argv


def test_build_agent_command_accepts_non_list_sequences() -> None:
    assert build_agent_command(("agent", "run")) == ["agent", "run"]


def test_build_agent_command_rejects_empty_argv() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        build_agent_command([])
