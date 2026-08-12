"""raven v2 CLI entry point.  LANE: cli (raven2-p1).

Commands (frozen surface — flags may grow, commands may not):

    raven send      --channel C --from R@RUN -t TYPE --body JSON
                    [--urgency U] [--tag T]... [--reply-to ID]
                    [--expires-in S] [--kind broadcast|queue|stream]
    raven read      --channel C --as R@RUN [-m MAX] [-j]     (broadcast pending)
    raven ack       --channel C --as R@RUN --up-to ID        (cursor jump)
    raven claim     --channel C --as R@RUN [--lease S] [-j]  (queue: claim next)
    raven done      --id ID --as R@RUN                       (complete claim)
    raven release   --id ID --as R@RUN
    raven tail      [--channel C] [--from ID] [--no-follow] [--json]
                    (identity-free, includes expired — forensic surface)
    raven channels  [--prefix P] [-j]
    raven doctor    (db reachable, schema version, WAL, sweep dry stats)
    raven teardown  --run RUN [--yes]
    raven version

Conventions carried from v1: one-line ``error: ...`` on failure, exit
codes 0 ok / 2 usage / 3 not-found / 10 error, ``-j/--json`` on read
surfaces, tracebacks never shown to users. Every read command runs the
opportunistic sweep first (ADR-001).
"""

from __future__ import annotations


def cli_main() -> None:
    """Console entry point (wired as ``raven2`` during the build run;
    takes over the ``raven`` script name when claude_bus is removed at
    integrate — ADR-004)."""
    raise NotImplementedError


__all__ = ["cli_main"]
