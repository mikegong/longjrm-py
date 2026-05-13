# CLAUDE.md

Guidance for Claude Code working in this repository. The README is the
authoritative user-facing reference (quickstart, config, full CRUD/async
examples, testing setup). This file captures only the design intent and
invariants that are *not* obvious from the source itself.

## What longjrm is

A lightweight JSON Relational Mapping (JRM) library: JSON in, SQL out, no
data models inside applications. The point is to be the **opposite** of an
ORM — code talks to database models directly via dicts and lists.

When choosing between alternative implementations, prefer the one that
keeps the user's data shape as plain JSON-compatible Python (`dict`,
`list`, scalars, `datetime`). Don't introduce row classes, schemas, or
mapping layers.

## Supported databases & extension point

Postgres, MySQL/MariaDB, Oracle, DB2, SQL Server, SQLite, Spark, plus any
DB-API 2.0 driver via `GenericConnector`. The authoritative driver
registry is [longjrm/connection/driver_map.json](longjrm/connection/driver_map.json) —
add a new entry there, then implement a `Db` subclass (see "ABC trap"
below) and register it in [longjrm/database/__init__.py](longjrm/database/__init__.py).

## Async API design (Strategy A: threadpool-backed)

This is the load-bearing design decision in 0.2.0 and the one most likely
to be misunderstood.

- `AsyncDb` ([longjrm/database/async_db.py](longjrm/database/async_db.py)) wraps the synchronous `Db`
  returned by `get_db()`. Each public method dispatches through
  `asyncio.to_thread`, so the event loop is free while the (still-sync)
  DB-API driver does its I/O.
- `Pool.aclient()` / `Pool.atransaction()` ([longjrm/connection/pool.py](longjrm/connection/pool.py))
  **deliberately reuse** the synchronous `client()` / `transaction()`
  context managers via `asyncio.to_thread(cm.__enter__ / __exit__)`.
  Session setup/teardown, autocommit toggling, and isolation levels have
  a single source of truth in the sync path; the async path inherits any
  change there automatically. Do not reimplement these on the async side.
- Each `AsyncDb` instance holds an `asyncio.Lock` to serialize concurrent
  calls on a shared connection. `gather()` on one `AsyncDb` is safe
  (serialized), not corrupt; for real concurrency, check out one client
  per branch (`async with pool.aclient()` inside each task).

### Async invariants — do not break these

1. **Never call sync `Db` methods directly inside `async def` code in
   this repo.** Wrap with `await asyncio.to_thread(...)` or move the call
   into the sync path. Direct calls block the event loop.
2. **Mirror sync method signatures 1:1 in `AsyncDb`.** When a new method
   is added to `Db` and is appropriate for async, add a matching
   `async def` that delegates via `asyncio.to_thread`, keeping parameter
   names, defaults, and return shape identical. This is what lets users
   mechanically migrate sync → async by adding `await`.
3. **Don't duplicate session / transaction logic.** If async needs to
   diverge, surface it on the sync side first.

### Scope (Phase 1 vs Phase 2)

- **Phase 1 (shipped)**: `select`, `query`, `execute`, `insert`, `update`,
  `delete`, `merge`, `commit` / `rollback` / `set_autocommit` /
  `get_autocommit`.
- **Phase 2 (shipped 0.2.0)**: `bulk_update`, `merge_select`, the
  `stream_*` family (with `AsyncIterator` adapters), `run_*_from_file`,
  `execute_script`.

### Async — out of scope

- **Spark**: owns its own scheduling; async tests skip it explicitly.
- **SQLite + SQLAlchemy `SingletonThreadPool`**: incompatible with
  threadpool dispatch (connections are thread-bound, `to_thread` hops
  threads). Use the DBUtils backend for SQLite under async. (SQLite +
  DBUtils works because `SqliteConnector.connect()` defaults to
  `check_same_thread=False`; the pool guarantees exclusive ownership.)

## ABC trap when adding a database

`Db` is an `abc.ABC`. A new subclass **must** implement:

- `get_cursor()`
- `get_stream_cursor()`
- `_build_upsert_clause(key_columns, update_columns, for_values=True)`

Missing any of these gives a `TypeError` at instantiation, not at
import — easy to miss in a quick smoke test. SQL dialect tweaks live in
overrides like `_construct_select_sql()` if the default isn't portable.

## Test layout pointer

- Sync tests: `longjrm/tests/*_test.py` — run via `python -m unittest discover longjrm/tests *_test.py`.
- Async smoke: `longjrm/tests/async_select_test.py` — gated on TCP
  reachability (1s probe) so an environment without DB2/Oracle/SQL
  Server doesn't pay the driver timeout cost.
- Filter to a single DB with `TEST_DB=postgres` or `--db=postgres`.

## Things that are *not* in this file on purpose

- CRUD / where-condition / placeholder syntax → README + `db.py`.
- Module tree → `ls`.
- Dependency list → [pyproject.toml](pyproject.toml) / [requirements.txt](requirements.txt).
- Step-by-step "add a database" tutorial → contribute to README if needed.
