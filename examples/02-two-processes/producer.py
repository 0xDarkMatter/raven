"""Send 5 messages, 200ms apart, to address ``consumer:demo``.

Run after starting `consumer.py` in another terminal.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from claude_bus import BusClient

DB = Path(os.environ.get("BUS_DB", Path(__file__).with_name("bus.db")))


def main() -> None:
    producer = BusClient(session_id="demo", role="producer", db_path=DB)
    target = "consumer:demo"

    for i in range(5):
        msg = producer.send(
            to=target,
            type="ping",
            body={"i": i, "ts": time.time()},
        )
        print(f"[producer] sent #{msg.id}  body={msg.body}")
        time.sleep(0.2)

    print("[producer] done")


if __name__ == "__main__":
    main()
