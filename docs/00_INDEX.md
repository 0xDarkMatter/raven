# docs/ — index

One line per doc: what it is, and why you'd open it. Start at
[../AGENTS.md](../AGENTS.md) (commands, structure, landmines) if you're about
to change code.

> Maintenance: add a line here in the same commit that adds a doc. ADRs are
> not listed individually — `ls docs/adr/` is the list.

| Doc | What / when to read |
|---|---|
| [QUICKSTART.md](QUICKSTART.md) | Hands-on walkthrough of every surface (CLI, Python, ravend, `raven acp`, the hook). Read to *use* raven. |
| [CLI.md](CLI.md) | The frozen `raven` command surface — every command and flag. Read before adding a flag (commands may not be added). |
| [TESTING.md](TESTING.md) | Test fixtures, sibling-stub and raw-SQL setup patterns. Read before writing tests. |
| [adr/](adr/) | Decisions of record, one per file (`ls docs/adr/`). Read before changing anything an ADR names — the ADR owns the *why*. |
| [design/raven2-architecture.md](design/raven2-architecture.md) | The v2 design and its phasing (§8). Written pre-build; inline notes mark where the shipped code differs. Read for the big picture. |
| [plans/HANDOFF-2026-08-12.md](plans/HANDOFF-2026-08-12.md) | **Historical.** State at the end of the v2 build day. Superseded; kept for provenance. |
| [plans/raven2-p1-run.md](plans/raven2-p1-run.md), [p2](plans/raven2-p2-run.md), [p3](plans/raven2-p3-run.md) | **Historical.** The fleet run plans that built P1-P3; their wave tables show how each phase was decomposed. Do not re-run. |
