"""Run the 5-agent news-desk demo against a fresh SQLite bus."""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

from agents import (
    SESSION,
    Transcript,
    register_schemas,
    run_editor,
    run_fact_checker,
    run_publisher,
    run_scout,
    run_wrap_broadcaster,
    run_writer,
)


async def amain(db_path: Path, n_articles: int) -> None:
    register_schemas()
    transcript = Transcript(started_at=time.monotonic())
    stop = asyncio.Event()

    db = str(db_path)
    transcript.line("setup", f"db={db_path}, articles={n_articles}, session={SESSION}")

    # Start the four consumer-roles + the publisher first so they're
    # already subscribed when the scout sends.
    consumers = [
        asyncio.create_task(run_writer(db, transcript, stop)),
        asyncio.create_task(run_editor(db, transcript, stop)),
        asyncio.create_task(run_fact_checker(db, transcript, stop)),
        asyncio.create_task(run_publisher(db, n_articles, transcript, stop)),
        asyncio.create_task(run_wrap_broadcaster(db, stop, transcript)),
    ]
    # Give them ~50ms to subscribe before scout fires.
    await asyncio.sleep(0.05)

    # Run scout to completion (it just sends, doesn't subscribe).
    await run_scout(db, n_articles, transcript, stop)

    # Wait until publisher signals done (or a 10s safety fuse).
    try:
        await asyncio.wait_for(stop.wait(), timeout=10.0)
    except asyncio.TimeoutError:  # pragma: no cover -- only fires if pipeline stalls
        transcript.line("setup", "TIMEOUT — pipeline stalled")
        stop.set()

    # Wait for everyone to exit (wrap_broadcaster sends 'wrap' to each).
    await asyncio.gather(*consumers, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the news-desk 5-agent demo."
    )
    parser.add_argument(
        "--articles", type=int, default=3, help="How many articles to push (default 3)"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).with_name("newsdesk.db"),
        help="SQLite path (default ./newsdesk.db, deleted before run)",
    )
    parser.add_argument(
        "--keep-db", action="store_true",
        help="Keep the DB after the run (default deletes for a clean slate)",
    )
    args = parser.parse_args()

    db = args.db
    if not args.keep_db and db.exists():
        db.unlink()
    for sib in (db.with_suffix(".db-wal"), db.with_suffix(".db-shm")):
        if sib.exists():
            sib.unlink()

    asyncio.run(amain(db, args.articles))


if __name__ == "__main__":
    main()
