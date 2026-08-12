"""Send 5 tasks, 200ms apart, to the ``run/demo/work`` queue channel.

Run before or after ``consumer.py`` — queued tasks wait for the next
claim regardless of arrival order.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from raven_bus import channels, db, log

CHANNEL = "run/demo/work"
PRODUCER = "producer@demo"


def _default_db() -> Path:
    """``$RAVEN_DB`` if set, else ``./bus.db`` next to this script."""
    env = os.environ.get("RAVEN_DB")
    return Path(env) if env else Path(__file__).with_name("bus.db")


def main(db_path: Path) -> None:
    db.init_db(db_path)
    with db.connection(db_path) as conn:
        channels.ensure_channel(conn, CHANNEL, kind="queue")

    for i in range(5):
        with db.connection(db_path) as conn:
            msg = log.append(
                conn,
                channel=CHANNEL,
                sender=PRODUCER,
                type="task",
                body={"i": i, "ts": time.time()},
                ensure=False,
            )
        print(f"[producer] sent #{msg.id}  body={msg.body}", flush=True)
        time.sleep(0.2)

    print("[producer] done", flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Feed the demo queue channel.")
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite path (default: $RAVEN_DB, else ./bus.db)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(args.db if args.db is not None else _default_db())
