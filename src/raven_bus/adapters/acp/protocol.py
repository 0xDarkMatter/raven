"""Minimal ACP client over a child's stdio.  LANE: acp-protocol (raven2-p3).

FROZEN signatures (wave-0). Speaks the Agent Client Protocol's client
side, minimal subset only (ADR-006): JSON-RPC 2.0, newline-delimited
JSON messages over the child process's stdin/stdout (one JSON object
per line — the ACP stdio transport). Supported calls:

- ``initialize``      (client → agent; advertise no fs/terminal caps)
- ``session/new``     (client → agent)
- ``session/set_mode`` (client → agent; optional, select a session mode
  the agent advertised, e.g. a permission mode — a headless harness
  refuses ``session/request_permission``, so lanes that need tools must
  be switched into a non-prompting mode up front)
- ``session/prompt``  (client → agent; text content blocks)
- ``session/update``  (agent → client notification; streamed chunks)
- ``session/cancel``  (client → agent notification)
- agent → client REQUESTS (permission/fs/terminal) are answered with a
  JSON-RPC error (method not supported) — we granted no capabilities.

Synchronous API — the harness drives one call at a time (dumb pipe) —
over ONE daemon reader thread that drains the child's stdout into a
queue. The thread is what makes the waits enforceable (QA finding A7): a
blocking ``readline`` could neither time out a silent agent nor notice
that the agent had exited while a grandchild (a tool subprocess) still
held its stdout open. It also keeps the child from blocking on a full
stdout pipe between turns. No respawn logic here (ADR-006).
"""

from __future__ import annotations

import io
import json
import queue
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, TextIO

from pydantic import BaseModel, ConfigDict, Field


class AcpError(Exception):
    """Protocol-level failure: malformed frame, JSON-RPC error response,
    the agent exiting (EOF, or the child process gone while a grandchild
    keeps its stdout open), or — only when ``timeout_s`` is set — no
    frame at all from the agent for ``timeout_s`` seconds."""


_EOF = object()
"""Queue sentinel: the agent's stdout reached end-of-file."""

_WAIT_SLICE_S = 0.1
"""How often a wait re-checks the inactivity deadline and child liveness."""

_EXIT_GRACE_S = 0.5
"""After the child process has exited, how long to keep draining frames it
wrote before dying. The reader thread moves already-buffered lines into the
queue within milliseconds; a pipe that stays silent this long after exit is
held open only by a grandchild, so the agent is treated as exited."""


class PromptResult(BaseModel):
    """Outcome of one session/prompt round trip."""

    model_config = ConfigDict(frozen=True)

    stop_reason: str
    """ACP stopReason (end_turn, max_tokens, refusal, cancelled, ...)."""

    text: str
    """Concatenated agent text chunks from session/update notifications."""

    raw_updates: list[dict[str, Any]] = Field(default_factory=list)
    """Every session/update params dict, in arrival order (telemetry)."""


class AcpClient:
    """Client for ONE agent child process. Not thread-safe; call from
    one thread. The caller owns the child's lifecycle (spawn args,
    reaping) — this class only speaks the protocol over its pipes."""

    def __init__(
        self,
        child: subprocess.Popen,
        *,
        timeout_s: float | None = None,
        on_update: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        """``child`` must have stdin/stdout as pipes. Binary pipes are
        wrapped as UTF-8 with LF framing (the ACP stdio transport);
        text-mode pipes are used as their owner configured them.

        ``timeout_s`` is an INACTIVITY limit: a request fails when the
        agent sends no frame at all (response, notification or request)
        for that many seconds; every received frame restarts the clock,
        so a long, actively streaming turn is never cut off. It used to
        be a TOTAL per-request deadline that aborted any turn streaming
        past 10 minutes — and never fired for a silent agent, because
        ``readline`` blocked past it (QA finding A7).

        Default None = no inactivity limit, deliberately: ACP streams
        nothing while a tool call runs, so a healthy agent can be silent
        for as long as its longest build or test run, and wall-clock
        lifecycle belongs to the spawner (ADR-006 — ff-spawn's journal).
        A DEAD agent needs no timeout: waits notice child exit even
        while a grandchild holds the pipe open.

        ``on_update`` fires for every session/update as it arrives
        (live telemetry hook for the harness)."""
        if child.stdin is None or child.stdout is None:
            raise ValueError("child must have stdin and stdout pipes")

        self._child = child
        self._stdin = self._text_stream(child.stdin, writing=True)
        self._stdout = self._text_stream(child.stdout, writing=False)
        self._timeout_s = timeout_s
        self._on_update = on_update
        self._next_id = 1
        # Lines (str), _EOF, or the exception that stopped the reader.
        self._frames: queue.Queue[object] = queue.Queue()
        threading.Thread(
            target=self._pump_stdout, name="raven-acp-reader", daemon=True
        ).start()

    def initialize(self) -> dict[str, Any]:
        """initialize handshake; returns the agent's capabilities dict.
        Advertise a client with NO fs/terminal capabilities."""
        result = self._request(
            "initialize",
            {"protocolVersion": 1, "clientCapabilities": {}},
        )
        capabilities = result.get("agentCapabilities", {})
        if not isinstance(capabilities, dict):
            raise AcpError("initialize returned invalid agentCapabilities")
        return capabilities

    def new_session(self, *, cwd: str) -> str:
        """session/new → session id."""
        result = self._request("session/new", {"cwd": cwd, "mcpServers": []})
        session_id = result.get("sessionId")
        if not isinstance(session_id, str):
            raise AcpError("session/new returned invalid sessionId")
        return session_id

    def set_mode(self, session_id: str, mode_id: str) -> None:
        """session/set_mode → select an agent-advertised session mode.

        The spec allows a null result (zed's adapter returns one), so
        this is the one request that tolerates a non-object result."""
        self._request(
            "session/set_mode",
            {"sessionId": session_id, "modeId": mode_id},
            allow_null_result=True,
        )

    def prompt(self, session_id: str, text: str) -> PromptResult:
        """session/prompt with one text content block; pumps
        notifications (and rejects agent-initiated requests) until the
        prompt's response arrives; returns the collected result.
        Raises AcpError when the agent exits, on a JSON-RPC error, or on
        ``timeout_s`` of agent silence (only if configured)."""
        updates: list[dict[str, Any]] = []
        text_chunks: list[str] = []
        result = self._request(
            "session/prompt",
            {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": text}],
            },
            updates=updates,
            text_chunks=text_chunks,
        )
        stop_reason = result.get("stopReason")
        if not isinstance(stop_reason, str):
            raise AcpError("session/prompt returned invalid stopReason")
        return PromptResult(
            stop_reason=stop_reason,
            text="".join(text_chunks),
            raw_updates=updates,
        )

    def cancel(self, session_id: str) -> None:
        """session/cancel notification (fire and forget)."""
        self._write(
            {
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": session_id},
            }
        )

    @staticmethod
    def _text_stream(stream: Any, *, writing: bool) -> TextIO:
        """Use explicit UTF-8/LF framing when Popen supplied binary pipes.

        Text-mode ``Popen`` streams are also accepted and used as configured by
        their owner; binary streams are wrapped here with the transport's
        required encoding and newline convention.
        """
        if isinstance(stream, io.TextIOBase):
            return stream
        return io.TextIOWrapper(
            stream,
            encoding="utf-8",
            newline="\n",
            write_through=writing,
        )

    def _request(
        self,
        method: str,
        params: dict[str, Any],
        *,
        updates: list[dict[str, Any]] | None = None,
        text_chunks: list[str] | None = None,
        allow_null_result: bool = False,
    ) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        self._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )

        while True:
            message = self._read()
            if "method" in message:
                self._handle_agent_message(message, updates, text_chunks)
                continue
            if "id" not in message:
                raise AcpError("invalid JSON-RPC response")
            if message["id"] != request_id:
                raise AcpError(f"unexpected response id {message['id']!r}")
            if "error" in message:
                error = message["error"]
                if isinstance(error, dict):
                    code = error.get("code")
                    detail = error.get("message", "unknown JSON-RPC error")
                    raise AcpError(f"JSON-RPC error {code}: {detail}")
                raise AcpError(f"JSON-RPC error: {error!r}")
            result = message.get("result")
            if not isinstance(result, dict):
                if allow_null_result and result is None:
                    return {}
                raise AcpError("JSON-RPC response result must be an object")
            return result

    def _pump_stdout(self) -> None:
        """Reader thread: move every stdout line into the queue, then the
        EOF sentinel — or the exception that stopped reading (a decode
        error, a pipe closed under us), re-raised by ``_next_line``."""
        try:
            while True:
                line = self._stdout.readline()
                if line == "":
                    self._frames.put(_EOF)
                    return
                self._frames.put(line)
        except Exception as exc:  # noqa: BLE001 -- surfaced to the caller
            self._frames.put(exc)

    def _child_exited(self) -> bool:
        poll = getattr(self._child, "poll", None)
        return poll is not None and poll() is not None

    def _next_line(self) -> str:
        """The next raw stdout line, waiting at most ``timeout_s`` (when
        set) — the deadline is per call, i.e. per frame."""
        deadline = None if self._timeout_s is None else time.monotonic() + self._timeout_s
        exited_at: float | None = None
        while True:
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                raise AcpError("agent response timed out")
            wait = _WAIT_SLICE_S if deadline is None else min(_WAIT_SLICE_S, deadline - now)
            try:
                item = self._frames.get(timeout=wait)
            except queue.Empty:
                if self._child_exited():
                    exited_at = exited_at if exited_at is not None else time.monotonic()
                    if time.monotonic() - exited_at >= _EXIT_GRACE_S:
                        raise AcpError("agent exited") from None
                continue
            if isinstance(item, str):
                return item
            # Terminal states stay terminal: re-queue so any later read
            # fails the same way instead of blocking forever.
            self._frames.put(item)
            if isinstance(item, BaseException):
                raise AcpError(f"failed to read agent output: {item}") from item
            raise AcpError("agent exited")

    def _read(self) -> dict[str, Any]:
        while True:
            line = self._next_line()
            if line.strip():
                break
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            raise AcpError("malformed JSON from agent") from exc
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            raise AcpError("malformed JSON-RPC frame from agent")
        return message

    def _handle_agent_message(
        self,
        message: dict[str, Any],
        updates: list[dict[str, Any]] | None,
        text_chunks: list[str] | None,
    ) -> None:
        if "id" in message:
            self._write(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {"code": -32601, "message": "Method not found"},
                }
            )
            return

        if message.get("method") != "session/update":
            return
        params = message.get("params")
        if not isinstance(params, dict):
            raise AcpError("session/update params must be an object")
        if updates is not None:
            updates.append(params)
        if text_chunks is not None:
            text_chunks.extend(self._update_text(params))
        if self._on_update is not None:
            self._on_update(params)

    @staticmethod
    def _update_text(params: dict[str, Any]) -> list[str]:
        update = params.get("update")
        if not isinstance(update, dict):
            return []
        update_kind = update.get("sessionUpdate", update.get("type"))
        if update_kind != "agent_message_chunk":
            return []
        content = update.get("content")
        blocks = content if isinstance(content, list) else [content]
        return [
            block["text"]
            for block in blocks
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        ]

    def _write(self, message: dict[str, Any]) -> None:
        try:
            self._stdin.write(json.dumps(message) + "\n")
            self._stdin.flush()
        except (BrokenPipeError, OSError, UnicodeError) as exc:
            raise AcpError("agent exited") from exc


def build_agent_command(argv: Sequence[str]) -> list[str]:
    """Validate/normalise the agent command line passed after ``--`` on
    `raven acp` (non-empty, returned as list)."""
    command = list(argv)
    if not command:
        raise ValueError("agent command must not be empty")
    return command


__all__ = ["AcpClient", "AcpError", "PromptResult", "build_agent_command"]
