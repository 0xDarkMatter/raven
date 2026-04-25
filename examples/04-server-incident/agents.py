"""Five SRE agents responding to incidents on a flaky in-memory server.

Pipeline::

    monitor ──incident──► triager ──investigate──► diagnoser
                                                       │
                                                       │ prescription
                                                       ▼
                                                     fixer
                                                       │
                                                       │ fix_applied
                                                       ▼
                                                    verifier
                                                       │
                                                       │ resolved (or escalate)
                                                       ▼
                                                  (transcript only —
                                                   pipeline ends per
                                                   incident)

Every message carries a `correlation_id` equal to the original
incident's id, so the audit trail of any single fault is one
``read --json`` away.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

from pydantic import BaseModel, Field

from claude_bus import BusClient, Message, SchemaRegistry
from server import PRESCRIPTION, FlakyServer

# ----- session + addresses -------------------------------------------

SESSION = "incident"


def _addr(role: str) -> str:
    return f"{role}:{SESSION}"


MONITOR = _addr("monitor")
TRIAGER = _addr("triager")
DIAGNOSER = _addr("diagnoser")
FIXER = _addr("fixer")
VERIFIER = _addr("verifier")


# ----- typed message bodies ------------------------------------------


class IncidentBody(BaseModel):
    symptom: str = Field(min_length=1)
    health: dict


class InvestigateBody(BaseModel):
    symptom: str
    severity: str = Field(pattern=r"^(low|medium|high)$")


class PrescriptionBody(BaseModel):
    symptom: str
    fix: str
    evidence: str


class FixAppliedBody(BaseModel):
    fix: str
    success: bool


class ResolvedBody(BaseModel):
    health: dict
    duration_ms: int


class WrapBody(BaseModel):
    reason: str = "done"


def register_schemas() -> None:
    SchemaRegistry.register("incident", IncidentBody)
    SchemaRegistry.register("investigate", InvestigateBody)
    SchemaRegistry.register("prescription", PrescriptionBody)
    SchemaRegistry.register("fix_applied", FixAppliedBody)
    SchemaRegistry.register("resolved", ResolvedBody)
    SchemaRegistry.register("wrap", WrapBody)


# ----- transcript ----------------------------------------------------


@dataclass
class Transcript:
    started_at: float

    def line(self, role: str, msg: str) -> None:
        ts = (time.monotonic() - self.started_at) * 1000
        print(f"  +{ts:5.0f}ms  [{role:<10}] {msg}", flush=True)


# ----- agent loop ----------------------------------------------------


async def _agent_loop(
    client: BusClient,
    handle: Callable[[Message], None],
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    async for msg in client.subscribe(poll_interval_s=0.05):
        if msg.type == "wrap":
            transcript.line(client.role, "wrap received -> exit")
            return
        try:
            handle(msg)
        except Exception as exc:  # pragma: no cover -- demo safety net
            transcript.line(client.role, f"error on #{msg.id}: {exc}")
        if stop_event.is_set():
            return


# ----- agents --------------------------------------------------------


async def run_monitor(
    db_path: str,
    server: FlakyServer,
    fault_schedule: list[str],
    transcript: Transcript,
    stop_event: asyncio.Event,
    seen_incidents: dict[int, float],
    resolved_count: dict[str, int],
) -> None:
    """Polls the server every 50ms. When health is bad, fires an incident.

    Also drives the demo by injecting the next scheduled fault — but only
    once the previous incident has been fully resolved by the verifier,
    so the pipeline is one-incident-at-a-time. (Removing this gate would
    let multiple incidents stack up; the bus would still handle it, but
    the transcript becomes harder to read.)
    """
    monitor = BusClient(session_id=SESSION, role="monitor", db_path=db_path)
    last_problems: set[str] = set()
    fault_idx = 0

    while not stop_event.is_set():
        # Gate: inject the next scheduled fault only when the server is
        # currently healthy AND every previously-injected fault has
        # already been counted as resolved by the verifier.
        if (
            fault_idx < len(fault_schedule)
            and resolved_count["n"] >= fault_idx
            and server.health()["ok"]
        ):
            kind = fault_schedule[fault_idx]
            server.inject_fault(kind)  # type: ignore[arg-type]
            fault_idx += 1
            transcript.line("monitor", f"(fault injected externally: {kind})")
            await asyncio.sleep(0.02)  # let the next poll see it

        h = server.health()
        new_problems = set(h["problems"]) - last_problems
        for sym in new_problems:
            sent = monitor.send(
                to=TRIAGER, type="incident",
                body={"symptom": sym, "health": h},
            )
            seen_incidents[sent.id] = time.monotonic()
            transcript.line(
                "monitor",
                f"INCIDENT #{sent.id}  symptom={sym}  health={h['problems']}",
            )
        last_problems = set(h["problems"])

        await asyncio.sleep(0.05)


async def run_triager(
    db_path: str,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    triager = BusClient(session_id=SESSION, role="triager", db_path=db_path)

    def severity_of(sym: str) -> str:
        # Demo policy: db = high, errors = medium, cpu = low
        return {
            "db_disconnected": "high",
            "errors_spiking": "medium",
            "cpu_saturated": "low",
        }.get(sym, "medium")

    def handle(msg: Message) -> None:
        sym = msg.body["symptom"]
        sev = severity_of(sym)
        out = triager.send(
            to=DIAGNOSER, type="investigate",
            body={"symptom": sym, "severity": sev},
            correlation_id=msg.id,
            reply_to=msg.id,
        )
        transcript.line(
            "triager",
            f"investigate #{out.id}  symptom={sym}  severity={sev}",
        )

    await _agent_loop(triager, handle, transcript, stop_event)


async def run_diagnoser(
    db_path: str,
    server: FlakyServer,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    diag = BusClient(session_id=SESSION, role="diagnoser", db_path=db_path)

    def handle(msg: Message) -> None:
        sym = msg.body["symptom"]
        report = server.diagnose(sym)  # type: ignore[arg-type]
        out = diag.send(
            to=FIXER, type="prescription",
            body={
                "symptom": sym,
                "fix": report["prescription"],
                "evidence": report["evidence"],
            },
            correlation_id=msg.correlation_id,
            reply_to=msg.id,
        )
        transcript.line(
            "diagnoser",
            f"prescription #{out.id}  symptom={sym}  "
            f"fix={report['prescription']}  evidence=\"{report['evidence']}\"",
        )

    await _agent_loop(diag, handle, transcript, stop_event)


async def run_fixer(
    db_path: str,
    server: FlakyServer,
    transcript: Transcript,
    stop_event: asyncio.Event,
) -> None:
    fixer = BusClient(session_id=SESSION, role="fixer", db_path=db_path)

    def handle(msg: Message) -> None:
        fix = msg.body["fix"]
        ok = server.apply(fix)
        out = fixer.send(
            to=VERIFIER, type="fix_applied",
            body={"fix": fix, "success": ok},
            correlation_id=msg.correlation_id,
            reply_to=msg.id,
        )
        transcript.line(
            "fixer",
            f"applied        #{out.id}  fix={fix}  success={ok}",
        )

    await _agent_loop(fixer, handle, transcript, stop_event)


async def run_verifier(
    db_path: str,
    server: FlakyServer,
    transcript: Transcript,
    stop_event: asyncio.Event,
    seen_incidents: dict[int, float],
    expected_count: int,
    resolved_count: dict[str, int],
) -> None:
    verifier = BusClient(
        session_id=SESSION, role="verifier", db_path=db_path
    )

    def handle(msg: Message) -> None:
        h = server.health()
        cid = msg.correlation_id
        opened_at = seen_incidents.get(cid, time.monotonic())
        duration_ms = int((time.monotonic() - opened_at) * 1000)
        verifier.send(
            to=MONITOR, type="resolved",
            body={"health": h, "duration_ms": duration_ms},
            correlation_id=cid,
            reply_to=msg.id,
        )
        if h["ok"]:
            transcript.line(
                "verifier",
                f"RESOLVED       (correlation #{cid}, "
                f"duration={duration_ms}ms, health={h['problems'] or 'all clear'})",
            )
            resolved_count["n"] += 1
            if resolved_count["n"] >= expected_count:
                transcript.line(
                    "verifier",
                    f"resolved {resolved_count['n']}/{expected_count} incidents, exiting",
                )
                stop_event.set()
        else:  # pragma: no cover -- defensive: in this demo every fix works
            transcript.line(
                "verifier",
                f"ESCALATE       (correlation #{cid})  still broken: {h['problems']}",
            )

    await _agent_loop(verifier, handle, transcript, stop_event)


async def run_wrap_broadcaster(
    db_path: str,
    stop_event: asyncio.Event,
    transcript: Transcript,
) -> None:
    await stop_event.wait()
    bell = BusClient(session_id=SESSION, role="bell", db_path=db_path)
    for target in (TRIAGER, DIAGNOSER, FIXER, VERIFIER):
        bell.send(to=target, type="wrap", body={"reason": "done"})
    transcript.line("bell", "wrap broadcast")
