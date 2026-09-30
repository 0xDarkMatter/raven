# Testing patterns

Moved out of AGENTS.md (which keeps only the landmines) — read this before
writing tests. The gate is `just check`; see AGENTS.md "Run & test".

## Fixtures (tests/v2/conftest.py)

Every test gets an isolated DB under `tmp_path` with the init cache reset:

```python
@pytest.fixture()
def db(tmp_path):
    from raven_bus.db import _reset_init_cache, init_db
    _reset_init_cache()
    path = tmp_path / "bus.db"
    init_db(path, force=True)
    return path
```

`init_db` is process-cached (`force=True` bypasses; `_reset_init_cache()` clears
it for multi-DB-in-one-process tests). Add module-specific fixtures in your own
test file, not in `conftest.py` (a frozen wave-0 artifact).

## Landmine: scope sibling stubs via `monkeypatch`, never bare assignment

Lanes call only sibling modules' public contracts. During the parallel build a
lane's siblings may be stubs, so a test stands them in with faithful contract
implementations. **Always do this through `pytest.MonkeyPatch`:**

```python
def _install_sibling_stubs(monkeypatch):
    monkeypatch.setattr(cursors_mod.db, "sweep", lambda _conn: None)
    monkeypatch.setattr(cursors_mod.channels, "get_channel", get_channel)
    monkeypatch.setattr(cursors_mod.log, "read_after", read_after)
```

A bare `cursors_mod.db.sweep = ...` replaces the **real** module attribute for
every later test file in the process — it caused **8 cross-file failures at the
wave-1 landing**. `monkeypatch` auto-restores on teardown. (`tests/v2/test_cursors.py`
documents this in `_install_sibling_stubs`.)

## Raw-SQL data setup

Sibling-lane stand-ins set up rows with direct SQL (e.g. `_insert_message`
appends a `messages` row matching the `log.append` contract) so the code under
test exercises real v2 semantics against a live schema.
