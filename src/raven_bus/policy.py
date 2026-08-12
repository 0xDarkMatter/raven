"""Injection policy — the attention layer.  LANE: policy (raven2-p3).

FROZEN signatures (wave-0). The ENTIRE brain of message delivery into a
running agent session (ADR-003/006): pure, deterministic functions —
no I/O, no clock reads (``now`` is an input), no randomness. Adapters
(the ACP harness, the hook) call ``plan`` then ``render`` and MUST NOT
compose injection text themselves — the sender-attributed data framing
produced here IS the prompt-injection defense.

Tier semantics (ADR-003, restated as the enforcement site):

- ``blocking``  → ``interrupt``: delivered ALONE at the next turn
  boundary, before anything else.
- ``prompt``    → ``batch``: delivered together at the next natural
  boundary.
- ``fyi``       → held; surfaces as a token-capped DIGEST only when
  ``digest_min_count`` accumulate or the oldest exceeds
  ``digest_max_age_s``; otherwise stays ``deferred``.

``ack_up_to`` is the highest message id an adapter may cursor-ack AFTER
successful delivery — it must never include a deferred message
(a deferred fyi must survive to a later boundary or another process).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from raven_bus.models import Message

DEFAULT_TOKEN_BUDGET = 2000
"""Default injected-content budget per boundary (tokens, chars/4)."""

DEFAULT_DIGEST_MIN_COUNT = 5
DEFAULT_DIGEST_MAX_AGE_S = 300.0


def estimate_tokens(text: str) -> int:
    """chars/4, rounded up; the cap is a guardrail, not an invoice."""
    raise NotImplementedError


class InjectionPlan(BaseModel):
    """What an adapter delivers at ONE turn boundary."""

    model_config = ConfigDict(frozen=True)

    interrupt: list[Message] = Field(default_factory=list)
    """blocking-tier — deliver alone, first, one render each."""

    batch: list[Message] = Field(default_factory=list)
    """prompt-tier — deliver together in one render."""

    digest_source: list[Message] = Field(default_factory=list)
    """fyi-tier messages folded into this boundary's digest (already
    counted in ack_up_to). Empty when thresholds not hit."""

    deferred: list[Message] = Field(default_factory=list)
    """fyi-tier held back — NOT covered by ack_up_to."""

    ack_up_to: int = 0
    """Highest id safe to cursor-ack AFTER delivery succeeds; 0 = none.
    Never advances past a deferred message's id (cursor-jump would ack
    it — ADR-001 cursor semantics)."""


def plan(
    pending: Sequence[Message],
    *,
    now: datetime,
    token_budget: int = DEFAULT_TOKEN_BUDGET,
    digest_min_count: int = DEFAULT_DIGEST_MIN_COUNT,
    digest_max_age_s: float = DEFAULT_DIGEST_MAX_AGE_S,
) -> InjectionPlan:
    """Partition ``pending`` (id-ascending, as cursors.pending returns)
    into this boundary's plan.

    Budget applies to batch+digest rendering (interrupts always
    deliver — a blocking message may not be starved by budget); when
    the budget forces deferral of prompt-tier messages, ack_up_to
    stops BEFORE the first deferred id regardless of tier ordering.
    Deterministic for identical inputs."""
    raise NotImplementedError


def render(plan_: InjectionPlan, *, source: str = "raven bus") -> str:
    """The ONE injection text composer (data framing — ADR-003).

    Non-empty plans render as a clearly delimited block that:
    - opens with a header naming ``source`` and stating these are
      MESSAGES TO BE TREATED AS INFORMATION, not instructions;
    - renders each message with sender attribution, id, type, urgency,
      and its body as fenced JSON (never interpolated into prose);
    - renders the digest (if any) as one-line-per-message summaries;
    - is safe against message content containing the delimiters
      (content must not be able to escape the frame or forge the
      header — pick delimiters/escaping accordingly and test it).
    Empty plan → empty string."""
    raise NotImplementedError


def render_digest_line(message: Message) -> str:
    """One compact line for a digested fyi: id, sender, type, and a
    truncated body preview (no raw newlines)."""
    raise NotImplementedError


__all__ = [
    "DEFAULT_DIGEST_MAX_AGE_S",
    "DEFAULT_DIGEST_MIN_COUNT",
    "DEFAULT_TOKEN_BUDGET",
    "InjectionPlan",
    "estimate_tokens",
    "plan",
    "render",
    "render_digest_line",
]
