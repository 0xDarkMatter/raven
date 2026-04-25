"""Five-role editorial pipeline.

Each agent is an ``async def`` that subscribes to its own inbox,
processes messages, and forwards downstream. They share a single
SQLite-backed bus.

Message flow::

    scout ──lead──► writer ──draft──► editor       ──approval────┐
                            └─draft──► fact_checker ──verification┤
                                                                   ▼
                                                              publisher

`correlation_id` ties an article's lifecycle together: every message
descended from a given `draft` carries the draft's id, so the
publisher can pair an editor approval with the matching fact-check
verification.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

from pydantic import BaseModel, Field

from claude_bus import BusClient, Message, SchemaRegistry

# ----- session + addresses -------------------------------------------

SESSION = "newsdesk"

# Helper so each agent can be addressed by name without typing the
# "<role>:<session>" string everywhere.
def _addr(role: str) -> str:
    return f"{role}:{SESSION}"


SCOUT = _addr("scout")
WRITER = _addr("writer")
EDITOR = _addr("editor")
FACT_CHECKER = _addr("fact_checker")
PUBLISHER = _addr("publisher")


# ----- typed message bodies ------------------------------------------


class LeadBody(BaseModel):
    topic: str = Field(min_length=1)
    angle: str


class DraftBody(BaseModel):
    headline: str
    body: str


class ApprovalBody(BaseModel):
    headline: str
    note: str | None = None


class VerificationBody(BaseModel):
    headline: str
    sources_ok: bool
    note: str | None = None


class WrapBody(BaseModel):
    """Sent by run.py when N articles have published — every agent exits."""
    reason: str = "done"


def register_schemas() -> None:
    """Idempotent schema registration. Run once before any agent starts."""
    SchemaRegistry.register("lead", LeadBody)
    SchemaRegistry.register("draft", DraftBody)
    SchemaRegistry.register("approval", ApprovalBody)
    SchemaRegistry.register("verification", VerificationBody)
    SchemaRegistry.register("wrap", WrapBody)


# ----- transcript helper ---------------------------------------------


@dataclass
class Transcript:
    """Tiny stdout printer with consistent role-prefix alignment."""

    started_at: float

    def line(self, role: str, msg: str) -> None:
        ts = (time.monotonic() - self.started_at) * 1000
        print(f"  +{ts:5.0f}ms  [{role:<10}] {msg}", flush=True)


# ----- agent coroutines ----------------------------------------------

# Agents share a stop signal that wrap-up sends to everyone.
async def _agent_loop(
    client: BusClient,
    handle: Callable[[Message], None | Callable[[], None]],
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Drive a subscribe() loop until stop_event is set or a wrap arrives."""
    async for msg in client.subscribe(poll_interval_s=0.05):
        if msg.type == "wrap":
            transcript.line(client.role, "wrap received -> exit")
            return
        try:
            handle(msg)
        except Exception as exc:  # pragma: no cover -- demo-only safety net
            transcript.line(client.role, f"error handling #{msg.id}: {exc}")
        if stop_event.is_set():
            return


async def run_scout(
    db_path: str,
    n_articles: int,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Generate ``n_articles`` leads with 50ms spacing, then exit."""
    scout = BusClient(session_id=SESSION, role="scout", db_path=db_path)
    topics = [
        ("batteries", "next-gen sodium-ion economics"),
        ("agriculture", "vertical-farming yields plateau"),
        ("space", "private orbital servicing race"),
        ("transport", "high-speed rail re-emerges in Asia"),
        ("energy", "grid-scale storage cost crossover"),
    ]
    for i in range(n_articles):
        topic, angle = topics[i % len(topics)]
        sent = scout.send(
            to=WRITER, type="lead",
            body={"topic": topic, "angle": angle},
        )
        transcript.line("scout", f"sent lead       #{sent.id}  topic={topic}")
        await asyncio.sleep(0.05)
    transcript.line("scout", "done")


async def run_writer(
    db_path: str,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Drafts an article from each lead and fans it out for review."""
    writer = BusClient(session_id=SESSION, role="writer", db_path=db_path)

    def handle(msg: Message) -> None:
        topic = msg.body["topic"]
        angle = msg.body["angle"]
        headline = f"{topic.title()}: {angle}"
        body = f"In a sweeping look at {topic}, we find that {angle.lower()}..."
        # Fan out — same body, same correlation_id, two recipients.
        draft_to_editor = writer.send(
            to=EDITOR, type="draft",
            body={"headline": headline, "body": body},
            correlation_id=msg.id,
            reply_to=msg.id,
        )
        writer.send(
            to=FACT_CHECKER, type="draft",
            body={"headline": headline, "body": body},
            correlation_id=msg.id,
            reply_to=msg.id,
        )
        transcript.line(
            "writer",
            f"drafted article #{draft_to_editor.id}  "
            f"(in reply to lead #{msg.id})  -> editor + fact_checker",
        )

    await _agent_loop(writer, handle, transcript, stop_event)


async def run_editor(
    db_path: str,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Reads each draft, approves, forwards to publisher with the
    same correlation_id."""
    editor = BusClient(session_id=SESSION, role="editor", db_path=db_path)

    def handle(msg: Message) -> None:
        headline = msg.body["headline"]
        approval = editor.send(
            to=PUBLISHER, type="approval",
            body={"headline": headline, "note": "lede works; tighten para 3"},
            correlation_id=msg.correlation_id or msg.id,
            reply_to=msg.id,
        )
        transcript.line(
            "editor",
            f"approved        #{approval.id}  "
            f"(correlation #{msg.correlation_id})",
        )

    await _agent_loop(editor, handle, transcript, stop_event)


async def run_fact_checker(
    db_path: str,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Reads each draft, verifies sources, forwards to publisher."""
    fc = BusClient(session_id=SESSION, role="fact_checker", db_path=db_path)

    def handle(msg: Message) -> None:
        headline = msg.body["headline"]
        verification = fc.send(
            to=PUBLISHER, type="verification",
            body={
                "headline": headline,
                "sources_ok": True,
                "note": "two corroborating sources",
            },
            correlation_id=msg.correlation_id or msg.id,
            reply_to=msg.id,
        )
        transcript.line(
            "fact_chk",
            f"verified        #{verification.id}  "
            f"(correlation #{msg.correlation_id})",
        )

    await _agent_loop(fc, handle, transcript, stop_event)


async def run_publisher(
    db_path: str,
    n_articles: int,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    """Waits for editor approval AND fact-checker verification for the
    same correlation_id, then publishes. Exits after n_articles."""
    publisher = BusClient(
        session_id=SESSION, role="publisher", db_path=db_path
    )

    pending: dict[int, dict[str, Message]] = defaultdict(dict)
    published_count = 0

    def handle(msg: Message) -> None:
        nonlocal published_count
        cid = msg.correlation_id
        if cid is None:  # pragma: no cover -- demo invariant
            return
        pending[cid][msg.type] = msg
        if "approval" in pending[cid] and "verification" in pending[cid]:
            approval = pending[cid]["approval"]
            verification = pending[cid]["verification"]
            headline = approval.body["headline"]
            published_count += 1
            transcript.line(
                "publisher",
                f"PUBLISHED       (correlation #{cid})  \"{headline}\""
                f"  [editor #{approval.id}, fact_chk #{verification.id}]",
            )
            del pending[cid]
            if published_count >= n_articles:
                transcript.line(
                    "publisher",
                    f"published {published_count}/{n_articles} articles, exiting",
                )
                stop_event.set()

    await _agent_loop(publisher, handle, transcript, stop_event)


async def run_wrap_broadcaster(
    db_path: str,
    stop_event: asyncio.Event,
    transcript: Transcript,
) -> None:
    """Sentinel sender: when stop_event fires, broadcast a 'wrap' to every
    role so each subscribe() loop returns cleanly."""
    await stop_event.wait()
    bell = BusClient(session_id=SESSION, role="bell", db_path=db_path)
    for target in (WRITER, EDITOR, FACT_CHECKER, PUBLISHER):
        bell.send(to=target, type="wrap", body={"reason": "done"})
    transcript.line("bell", "wrap broadcast")
