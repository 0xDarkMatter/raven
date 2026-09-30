# raven — task runner. `just check` is THE gate (AGENTS.md "Run & test"):
# everything a change must pass before it lands. Uses the repo venv's
# python so it never picks up a system interpreter without raven_bus.

python := if os_family() == "windows" { ".venv/Scripts/python.exe" } else { ".venv/bin/python" }

# List recipes
default:
    @just --list

# The landing gate: lint + full suite at 100% coverage + landmine gates
check: lint cov

# Ruff over src, tests and examples
lint:
    {{python}} -m ruff check src tests examples

# Full suite, no coverage (fast inner loop)
test *args:
    {{python}} -m pytest tests/ -p no:cacheprovider --tb=short -q {{args}}

# Full suite with the 100% coverage lock
cov:
    {{python}} -m pytest tests/ -p no:cacheprovider --cov=raven_bus --cov-fail-under=100 -q
