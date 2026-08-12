"""Smallest possible raven_bus round-trip — see ./README.md.

Native v2 primitives only (no CLI, no compat shim): one broadcast
channel, one sender, one receiver-with-cursor.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from raven_bus import cursors, db, log

CHANNEL = "run/hello/lobby"
ALICE = "alice@hello"
BOB = "bob@hello"


def main(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    db.init_db(db_path, force=True)

    with db.connection(db_path) as conn:
        sent = log.append(
            conn,
            channel=CHANNEL,
            sender=ALICE,
            type="greeting",
            body={"text": "hello, bob"},
        )
    print(f"sent #{sent.id} {sent.sender} -> {CHANNEL} type={sent.type}")

    with db.connection(db_path) as conn:
        inbox = cursors.pending(conn, BOB, CHANNEL)
    print(f"bob inbox: {len(inbox)} message{'s' if len(inbox) != 1 else ''}")
    for msg in inbox:
        print(f"  body: {msg.body}")
        with db.connection(db_path) as conn:
            cursors.ack(conn, BOB, CHANNEL, up_to_id=msg.id)

    with db.connection(db_path) as conn:
        drained = cursors.pending(conn, BOB, CHANNEL)
    print(f"acked. inbox now empty: {drained == []}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smallest raven_bus round-trip.")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).with_name("hello.db"),
        help="SQLite path (default ./hello.db, deleted before run)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(args.db)
