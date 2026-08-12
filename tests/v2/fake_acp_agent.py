"""Deterministic stdlib-only ACP agent used by protocol and harness tests.

Run as ``python fake_acp_agent.py [--scenario NAME]``. The scenarios are:

``echo`` (default)
    Complete initialize and session/new, then stream every prompt as exactly
    two ``agent_message_chunk`` updates before an ``end_turn`` response.
``slow``
    Behave like echo, sleeping 0.2 seconds between the two chunks.
``request-perms``
    Send a permission request during each prompt, wait for the client's reply,
    expose that reply in a non-text session/update, then complete like echo.
``die-mid-prompt``
    Emit the first text chunk and exit with status 1.
``garbage``
    Emit one non-JSON line at startup, then otherwise behave like echo.

All messages use JSON-RPC 2.0, one UTF-8 JSON object per LF-terminated line.
The module is import-safe because the harness lane imports its helpers too.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, TextIO

SCENARIOS = ("echo", "slow", "request-perms", "die-mid-prompt", "garbage")
SESSION_ID = "fake-session"
PERMISSION_REQUEST_ID = 9001


def send(stream: TextIO, message: dict[str, Any]) -> None:
    stream.write(json.dumps(message) + "\n")
    stream.flush()


def receive(stream: TextIO) -> dict[str, Any] | None:
    for line in stream:
        if line.strip():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("expected a JSON object")
            return value
    return None


def response(request: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request["id"], "result": result}


def update(content: str, *, ordinal: int) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": SESSION_ID,
            "ordinal": ordinal,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": content},
            },
        },
    }


def prompt_text(request: dict[str, Any]) -> str:
    params = request.get("params", {})
    blocks = params.get("prompt", []) if isinstance(params, dict) else []
    return "".join(
        block["text"]
        for block in blocks
        if isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
    )


def serve(scenario: str, stdin: TextIO, stdout: TextIO) -> int:
    # Set via session/set_mode; when present, echoes are prefixed
    # "[mode=<id>] " so tests can assert the mode round-tripped.
    mode: str | None = None
    if scenario == "garbage":
        stdout.write("this is not json\n")
        stdout.flush()

    while (request := receive(stdin)) is not None:
        method = request.get("method")
        if method == "initialize":
            send(
                stdout,
                response(
                    request,
                    {
                        "protocolVersion": 1,
                        "agentCapabilities": {"fakeAgent": True},
                    },
                ),
            )
            continue
        if method == "session/new":
            send(stdout, response(request, {"sessionId": SESSION_ID}))
            continue
        if method == "session/set_mode":
            # Result is deliberately null: zed's claude-code-acp answers
            # set_mode with a null result, and the client must tolerate it.
            params = request.get("params", {})
            mode = params.get("modeId") if isinstance(params, dict) else None
            send(stdout, {"jsonrpc": "2.0", "id": request["id"], "result": None})
            continue
        if method == "session/cancel":
            continue
        if method != "session/prompt":
            send(
                stdout,
                {
                    "jsonrpc": "2.0",
                    "id": request.get("id"),
                    "error": {"code": -32601, "message": "Method not found"},
                },
            )
            continue

        if scenario == "request-perms":
            send(
                stdout,
                {
                    "jsonrpc": "2.0",
                    "id": PERMISSION_REQUEST_ID,
                    "method": "session/request_permission",
                    "params": {"sessionId": SESSION_ID, "options": []},
                },
            )
            permission_reply = receive(stdin)
            if permission_reply is None:
                return 1
            send(
                stdout,
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": SESSION_ID,
                        "ordinal": 0,
                        "observedPermissionReply": permission_reply,
                        "update": {"sessionUpdate": "tool_call_update"},
                    },
                },
            )

        prefix = f"[mode={mode}] " if mode is not None else ""
        rendered = f"{prefix}echo: {prompt_text(request)}"
        midpoint = len(rendered) // 2
        send(stdout, update(rendered[:midpoint], ordinal=1))
        if scenario == "die-mid-prompt":
            return 1
        if scenario == "slow":
            time.sleep(0.2)
        send(stdout, update(rendered[midpoint:], ordinal=2))
        send(stdout, response(request, {"stopReason": "end_turn"}))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=SCENARIOS, default="echo")
    args = parser.parse_args(argv)
    sys.stdin.reconfigure(encoding="utf-8", newline="\n")
    sys.stdout.reconfigure(
        encoding="utf-8", newline="\n", line_buffering=True, write_through=True
    )
    return serve(args.scenario, sys.stdin, sys.stdout)


if __name__ == "__main__":
    raise SystemExit(main())
