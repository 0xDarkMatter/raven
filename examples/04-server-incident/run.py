"""Run the 5-agent server-incident demo."""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

from agents import (
    SESSION,
    Transcript,
    register_schemas,
    run_diagnoser,
    run_fixer,
    run_monitor,
    run_triager,
    run_verifier,
    run_wrap_broadcaster,
)
from server import FlakyServer

DEFAULT_FAULTS = ["db_disconnected", "cpu_saturated", "errors_spiking"]


async def amain(db_path: Path, faults: list[str]) -> None:
    register_schemas()
    transcript = Transcript(started_at=time.monotonic())
    stop = asyncio.Event()
    server = FlakyServer()
    seen_incidents: dict[int, float] = {}
    resolved = {"n": 0}

    db = str(db_path)
    transcript.line(
        "setup",
        f"db={db_path}, faults={faults}, session={SESSION}",
    )
    transcript.line("setup", f"server initial health = {server.health()}")

    consumers = [
        asyncio.create_task(run_triager(db, transcript, stop)),
        asyncio.create_task(run_diagnoser(db, server, transcript, stop)),
        asyncio.create_task(run_fixer(db, server, transcript, stop)),
        asyncio.create_task(
            run_verifier(
                db, server, transcript, stop,
                seen_incidents, len(faults), resolved,
            )
        ),
        asyncio.create_task(run_wrap_broadcaster(db, stop, transcript)),
    ]
    # Let consumers subscribe before monitor starts firing.
    await asyncio.sleep(0.05)

    # Monitor injects + detects faults from a fixed schedule, then exits when
    # the verifier signals all done.
    monitor_task = asyncio.create_task(
        run_monitor(
            db, server, faults, transcript, stop, seen_incidents, resolved
        )
    )

    try:
        await asyncio.wait_for(stop.wait(), timeout=15.0)
    except asyncio.TimeoutError:  # pragma: no cover -- only if pipeline stalls
        transcript.line("setup", "TIMEOUT — pipeline stalled")
        stop.set()

    monitor_task.cancel()
    await asyncio.gather(monitor_task, *consumers, return_exceptions=True)
    transcript.line("setup", f"final server health = {server.health()}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the server-incident 5-agent demo."
    )
    parser.add_argument(
        "--faults",
        type=str,
        nargs="*",
        default=DEFAULT_FAULTS,
        help="Sequence of faults to inject (default: db_disconnected, cpu_saturated, errors_spiking)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).with_name("incident.db"),
        help="SQLite path (default ./incident.db, deleted before run)",
    )
    parser.add_argument(
        "--keep-db", action="store_true",
        help="Keep the DB after the run (default deletes it)",
    )
    args = parser.parse_args()

    db = args.db
    if not args.keep_db:
        for sib in (db, db.with_suffix(".db-wal"), db.with_suffix(".db-shm")):
            if sib.exists():
                sib.unlink()

    asyncio.run(amain(db, args.faults))


if __name__ == "__main__":
    main()
