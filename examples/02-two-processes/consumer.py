"""Claim tasks from the ``run/demo/work`` queue channel until stopped.

Run one or more instances (each with a distinct ``--id``) before or
after `producer.py`, in separate terminals, to see the 5 tasks split
between them — every id is delivered to exactly one consumer, via
``claims.claim_next``'s atomic ``INSERT ... ON CONFLICT DO NOTHING``.
Press Ctrl-C to stop.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from raven_bus import claims, db
from raven_bus.exceptions import UnknownChannelError

CHANNEL = "run/demo/work"


def _default_db() -> Path:
    """``$RAVEN_DB`` if set, else ``bus.db`` next to this script (not the cwd)."""
    env = os.environ.get("RAVEN_DB")
    return Path(env) if env else Path(__file__).with_name("bus.db")


def main(db_path: Path, consumer_id: str, poll_interval_s: float) -> None:
    db.init_db(db_path)
    print(f"[{consumer_id}] claiming from {CHANNEL}, db={db_path}", flush=True)

    while True:
        try:
            with db.connection(db_path) as conn:
                msg = claims.claim_next(conn, consumer_id, CHANNEL)
        except UnknownChannelError:
            # producer.py hasn't created the queue channel yet.
            msg = None
        if msg is None:
            time.sleep(poll_interval_s)
            continue
        latency_ms = (time.time() - msg.body.get("ts", time.time())) * 1000
        print(
            f"[{consumer_id}] got #{msg.id} from {msg.sender}  "
            f"body={msg.body}  latency={latency_ms:.0f}ms",
            flush=True,
        )
        with db.connection(db_path) as conn:
            claims.complete(conn, msg.id, consumer_id)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compete for tasks on the demo queue channel."
    )
    parser.add_argument(
        "--id",
        default=f"consumer-{os.getpid()}@demo",
        help="Consumer identity, '<role>@<run>' form (default: unique per process)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite path (default: $RAVEN_DB, else bus.db next to this script)",
    )
    parser.add_argument("--poll-interval", type=float, default=0.1)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    db_path = args.db if args.db is not None else _default_db()
    try:
        main(db_path, args.id, args.poll_interval)
    except KeyboardInterrupt:
        print("stopped", flush=True)
