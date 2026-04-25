"""Subscribe to the ``consumer:demo`` inbox and print each message live.

Run *before* `producer.py` (in a separate terminal) to see messages
arrive in real time. Press Ctrl-C to stop.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from claude_bus import BusClient

DB = Path(os.environ.get("BUS_DB", Path(__file__).with_name("bus.db")))


async def main() -> None:
    consumer = BusClient(session_id="demo", role="consumer", db_path=DB)
    print(f"[consumer] subscribing as {consumer.address}, db={DB}", flush=True)

    async for msg in consumer.subscribe(poll_interval_s=0.1):
        latency_ms = (time.time() - msg.body.get("ts", time.time())) * 1000
        print(
            f"[consumer] got #{msg.id} from {msg.sender}  "
            f"body={msg.body}  latency={latency_ms:.0f}ms",
            flush=True,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[consumer] stopped")
