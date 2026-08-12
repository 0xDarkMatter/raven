"""Adapters — deliver bus messages into running agent sessions.

Thin by decree (ADR-006): adapters own WHEN (turn boundaries, byte
shuttling); :mod:`raven_bus.policy` owns WHAT and HOW (tiers, budgets,
data framing). The ACP harness acks after delivery; the hook only
peeks.
"""
