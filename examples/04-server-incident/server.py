"""A small flaky in-memory server that the SRE swarm fixes.

State is mutable and shared across the agents. Faults are injected
externally by the orchestrator (``run.py``) to drive the pipeline.

Three fault classes, each with a known fix:

============================  ==============================  =========================
symptom                       prescription                    apply() effect
============================  ==============================  =========================
db_disconnected               reconnect_db                    db_connected = True
cpu_saturated                 kill_runaway_process            cpu_pct = 25
errors_spiking                restart_service                 error_rate = 0.01
============================  ==============================  =========================
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Literal

Symptom = Literal["db_disconnected", "cpu_saturated", "errors_spiking"]
Fix = Literal["reconnect_db", "kill_runaway_process", "restart_service"]

# What each symptom prescribes — the diagnoser uses this map.
PRESCRIPTION: dict[Symptom, Fix] = {
    "db_disconnected": "reconnect_db",
    "cpu_saturated": "kill_runaway_process",
    "errors_spiking": "restart_service",
}


@dataclass
class FlakyServer:
    """A toy server with three failure modes that can each be repaired."""

    db_connected: bool = True
    cpu_pct: int = 25
    error_rate: float = 0.01
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # ----- inspection ------------------------------------------------

    def health(self) -> dict:
        """Snapshot of the current state plus any active alerts."""
        with self._lock:
            problems: list[Symptom] = []
            if not self.db_connected:
                problems.append("db_disconnected")
            if self.cpu_pct > 90:
                problems.append("cpu_saturated")
            if self.error_rate > 0.1:
                problems.append("errors_spiking")
            return {
                "db_connected": self.db_connected,
                "cpu_pct": self.cpu_pct,
                "error_rate": round(self.error_rate, 3),
                "problems": problems,
                "ok": not problems,
            }

    def diagnose(self, symptom: Symptom) -> dict:
        """Return a structured diagnostic report for ``symptom``."""
        report = {
            "symptom": symptom,
            "evidence": {
                "db_disconnected": "TCP connect to db:5432 timed out",
                "cpu_saturated": "process 'worker-7' consuming 84% sustained",
                "errors_spiking": "5xx rate climbed from 0.8% to 14% in 90s",
            }[symptom],
            "prescription": PRESCRIPTION[symptom],
        }
        return report

    # ----- mutation --------------------------------------------------

    def apply(self, fix: Fix) -> bool:
        """Apply a fix; return True if it actually changed state."""
        with self._lock:
            if fix == "reconnect_db" and not self.db_connected:
                self.db_connected = True
                return True
            if fix == "kill_runaway_process" and self.cpu_pct > 90:
                self.cpu_pct = 25
                return True
            if fix == "restart_service" and self.error_rate > 0.1:
                self.error_rate = 0.01
                return True
            return False

    # ----- fault injection (used by run.py for the demo) -------------

    def inject_fault(self, kind: Symptom) -> None:
        """Externally simulate a fault. Demo-only."""
        with self._lock:
            if kind == "db_disconnected":
                self.db_connected = False
            elif kind == "cpu_saturated":
                self.cpu_pct = 96
            elif kind == "errors_spiking":
                self.error_rate = 0.18
