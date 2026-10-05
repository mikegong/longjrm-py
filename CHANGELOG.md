# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-10-05

### Fixed

- **The DBUtils pool handed out connections that had died while idle, on every database whose driver has no `ping()`.** DBUtils checks a pooled connection at checkout by calling `ping()` on the connection object; `ping()` is a MySQLdb interface, and psycopg, pyodbc, sqlite3 and `ibm_db_dbi` do not have one. DBUtils catches the resulting `AttributeError`, reads it as "this driver cannot be pinged", sets its own flag to `0` and never checks that connection again — with no log line and no error, so `"ping": 1` in the pool options was silently inert on Postgres, SQL Server, SQLite and DB2. A connection dropped by an idle firewall or NAT timeout was then handed out as healthy, and the failure surfaced on the caller's next statement. `BaseConnector.attach_liveness()` now supplies the method DBUtils looks for, for every connection the DBUtils backend creates; the probe behind it is the new overridable `BaseConnector.ping_dbapi()`. Postgres uses an empty query (one protocol round trip, no cursor, and — unlike `SELECT 1` — no risk of opening a transaction when autocommit is off); DB2 uses `SELECT 1 FROM SYSIBM.SYSDUMMY1`; MySQL and Oracle keep their driver's own `ping()`; SQLite opts out, since a local file connection cannot go stale. **SQL Server remains uncovered**: `pyodbc.Connection` is a C type that accepts no new attributes, so creating a DBUtils pool for it now logs a warning naming the gap and pointing at the SQLAlchemy backend, which has always had working `pool_pre_ping`. Probe implementations must never leak `AttributeError`/`IndexError`/`TypeError`/`ValueError` — those are the four DBUtils reads as "unsupported" — so `attach_liveness` funnels every failure into the driver's `OperationalError`; `longjrm/tests/liveness_test.py` covers that, the native-`ping()` passthrough, the SQL Server warning, and (against a live database) recovery from a killed session. The missing check had been masked: until this release the DBUtils pool also reconnected and re-ran a failed statement after checkout, and that retry is gone now (see Changed), so the check at checkout is what keeps a dead pooled connection from reaching the caller.

- **Time-zone-aware datetimes lost their offset on the write path, silently shifting the stored instant.** `Db._process_value` and the bulk converter in `datalist_to_dataseq` serialized every datetime with `strftime('%Y-%m-%d %H:%M:%S.%f')` — a format with no offset field — so an aware value bound to a `TIMESTAMPTZ` / `timestamp with time zone` column arrived as bare wall-clock digits and the server re-read them in its **session** time zone. Nothing raised: a UTC value written to a session running `Asia/Shanghai` was stored 8 hours off, and only a comparison against a server-side `NOW()` revealed it. Aware values are now emitted in ISO 8601 with their offset (`2026-08-12 11:26:53.525447+00:00`), which an engine with a zoned type parses back to the same instant regardless of session settings (an engine without one is given the instant in UTC -- see Changed); the shared helper is `longjrm.utils.data.serialize_datetime`. The same fix lands on the Spark literal builder (`_prepare_sql`), which additionally dropped sub-second precision for aware and naive values alike — it now keeps both. **Naive datetimes are untouched** (they carry no offset to preserve, so the historical format stays), and `datetime.date` is unaffected. Oracle already passed datetimes to the driver natively and was never affected; the same is true of WHERE-clause values on all backends, which are bound rather than serialized — that asymmetry is what made the bug hard to see, since filtering on an aware datetime always worked.

- **Inside a manual transaction, a failed write could report success and commit outside the transaction.** On the DBUtils backend the pool told DBUtils that every database error meant a broken connection, and DBUtils answers a broken connection by opening a new one and running the failed statement again. With autocommit off, an INSERT that conflicted with an uncommitted row of the same transaction was enough on PostgreSQL: the uncommitted rows were dropped along with the old connection, the INSERT was re-run on a fresh one where the conflict no longer existed, and that connection carried the pool's default autocommit, so the re-run committed. The caller saw `status: 0`, and the row landed outside the transaction they believed they were in. The same case stalled on a lock wait on MySQL and hung on DB2, and a session really lost in mid-transaction left only the later statements committed. The pool no longer hands DBUtils any failure class a driver can raise, so after checkout a connection is never replaced and a statement is never re-run (see Changed). The stream handlers' `commit_count` contract is now explicit: N > 0 commits every N rows, and 0 does no commit management, so a bare stream runs with per-row autocommit and a stream inside `pool.transaction()` is one all-or-nothing transaction. Rejected rows are isolated by a savepoint whenever the stream runs inside a transaction, not only when it batches, so on PostgreSQL a tolerated reject no longer aborts the enclosing transaction. `longjrm/tests/pool_contract_test.py` holds all of this against live databases on both backends, and `longjrm/tests/test_pool_no_failover.py` holds the core of it without a server.
- **A DB2 `bulk_load` whose input never opened reported an empty, successful load.** `ADMIN_CMD` answers a bad input path with a counts row of zeros, and zeros with no rejects read as success. The messages that mean the input could not be opened (`SQL2036N`, `SQL2037N`, `SQL3025N`) now set the result's `status` to `-1`.
- **`select()` with a `limit` rendered `limit N` on Db2**, which is not standard Db2 SQL. Db2 now renders `FETCH FIRST N ROWS ONLY`.
- **`AsyncDb` stayed locked after a `break` out of a streaming read, and the next call on it hung forever.** `stream_query`, `stream_query_batch` and `stream_select` hold the `AsyncDb` lock while their cursor is open, and were documented to release it on `break` or on an exception in the loop body. They did not: the iterator was a hand-written class, and `async for` never calls `aclose()` on such an object when the loop exits early, so only running to the last row released the lock. The three methods now return a native async generator, which Python finalizes when it is dropped, so an early exit releases the lock on the next turn of the event loop; `contextlib.aclosing(...)` releases it immediately, and replaces the `async with db.stream_query(...)` form the old class accepted, which nothing documented. The smoke test for exactly this case (`async_select_test.py`, "early-abort releases the lock") had been timing out since it was written in 0.2.0 and reporting the timeout as "database not reachable", because `TimeoutError` is a subclass of `OSError` and the test skipped on `OSError`; a timeout now fails the test, and `longjrm/tests/test_async_stream_lock.py` checks the release on break, exception, exhaustion and explicit close with no database at all.
- **`stream_insert`, `stream_update` and `stream_merge` rejected the plain dict rows the documentation shows.** The README and docs/database.md feed `stream_insert` a generator of row dicts, but the handler behind all three only understood the `(row_number, row, status)` tuples `stream_query` yields, and unpacked a dict into its own keys. Depending on how many keys the row had, the call returned `status: -1` with "not enough values to unpack", with "'str' object has no attribute 'keys'", or with "Upstream error at row" followed by the row's first key. A stream item may now be either shape: a dict is a row, numbered from 1 by the handler, and a tuple keeps its upstream row number and status, so a `stream_query` can still be piped straight into a write. `longjrm/tests/test_stream_row_shapes.py` covers both without a database.
- **On DB2, `stream_insert`, `stream_update` and `stream_merge` returned `status: -1` before the first row**, with `'Connection' object has no attribute 'autocommit'`, whenever `commit_count` was not 0, which includes the default. `Db2Connector` could set autocommit and could not read it back: `ibm_db_dbi` connections have `set_autocommit()` and no attribute to read, so the inherited getter raised. A batched stream reads the state before it starts, so that it can put it back afterwards, and `execute_script(transaction=True)` does the same. The connector now asks the driver for the state on the connection's handle. This went unseen because the stream test suite ran on PostgreSQL and MySQL whichever database was selected; it now runs on every configured engine, and a table-setup failure in it fails the run instead of printing an error and reporting success.
- **Tolerant streams (`max_error_count` or `reject_sink`) failed at the first row on DB2.** A tolerated reject is confined by a savepoint, and the handler sent the same bare `SAVEPOINT name` to every engine; DB2 requires `ON ROLLBACK RETAIN CURSORS`. The savepoint statements are now the engine's own: DB2 adds its clause, SQL Server uses `SAVE TRANSACTION` and `ROLLBACK TRANSACTION`, and Oracle and SQL Server, which have no `RELEASE SAVEPOINT`, are not sent one.
- **`GenericConnector` could not read autocommit back.** It sets autocommit in whichever of three ways the driver offers, and read it only as a plain attribute. For a driver whose `autocommit` is a method that reads as always on, so a batched stream or a transactional script could put the wrong state back afterwards. It now reads the state the way it writes it, and raises `ValueError` for a connection that offers no way to read it. A stream with `commit_count=0` and no reject handling does not read autocommit at all.

### Changed

- **The DBUtils pool no longer reconnects and re-runs a failed statement** (behavior change). After checkout a connection is never replaced and a statement is never re-run; a database error reaches the caller as that error. The SQLAlchemy backend has always behaved this way, and on the DBUtils backend it is what makes transactions safe (see Fixed). It cannot be switched back on: a `failures` entry in `dbutils_opts` is ignored. What changes for callers: if a connection dies while it is checked out, the next statement raises the driver's error, where DBUtils used to retry it silently on a new connection. Return the connection and check out again, since a dead pooled connection is still replaced at checkout, and retry at a level that knows whether a retry is safe. SQL Server on the DBUtils backend has no check at checkout, so a stale pooled connection there now fails on its first statement; use the SQLAlchemy backend for SQL Server. See README, "After checkout: no reconnect, no re-run".
- **An aware datetime written to MySQL, DB2 or SQLite is converted to UTC and written with no offset.** These engines have no type that keeps a time zone, and each mishandled the offset its own way: DB2 refused it (`SQL0180N`, on insert and on the single-row merge, which builds literals), MySQL 8 converted it to the session time zone before storing it in a `DATETIME`, and SQLite stored the text with the offset. A zoned instant on these engines now lives in the plain timestamp as UTC, and such a column is read as UTC. Which engines this is is read off the type map (`sql_types.stores_zones_as_utc`: the engines that write `TIMESTAMPTZ` as their plain timestamp), so the map and the write path cannot disagree. Engines with a zoned type keep receiving the offset, and naive datetimes are untouched.
- **`stream_insert`, `stream_update` and `stream_merge` refuse a negative `commit_count`** with `ValueError`. It used to mean one transaction, silently.

### Added

- **`execute_script` and `run_script_from_file` take a `delimiter`** (default `';'`, and `AsyncDb` mirrors both). A script is split on every occurrence of its delimiter, as SQL scripts are conventionally terminated, so a script whose statements or comments contain a semicolon -- a procedure body, a comment -- is written with another terminator and run with that delimiter, as DB2's command line takes `-td`. An empty delimiter raises `ValueError`.
- **Column types across databases: `longjrm.utils.sql_types`.** `canonical_type(database_type, raw_type)` maps a column type as an engine names it -- what its catalog reports -- to a canonical token; `render_type(canonical, database_type, length, scale)` renders a token as the type an engine writes in DDL; `fallback_type(database_type)` gives the widest text type, which a column of unknown type is created as. Postgres, MySQL/MariaDB, DB2, Oracle, SQL Server, SQLite and Spark are described on both sides; a timestamp with a time zone is its own token (`TIMESTAMPTZ`), written as the plain timestamp on an engine with no such type; and a rendering never narrows silently: a size past what the engine's type holds takes its widest type of the same kind, and a negative length (SQL Server's `MAX`) is no limit. The aliases `get_db` accepts (`postgresql`, `mariadb`, `mssql`) name the same engines, and `engine_name(database_type)` resolves them for a caller keying its own per-engine tables; no driver is imported.
- **`bulk_load` works on every engine.** The base `Db.bulk_load` raised `NotImplementedError`, so only the engines with a native load channel (DB2, Postgres, MySQL, Spark) could bulk-load at all. The base now implements a fallback that needs nothing but the driver: a query source becomes one in-engine `INSERT INTO ... SELECT`, and a file source is parsed client-side and written in `executemany` batches. SQLite uses it as is. Oracle and SQL Server add fast paths that a driver can always reach, since their own loaders (`sqlldr`, `bcp`, `BULK INSERT`) need an external program or a file on the server. Oracle writes direct-path, through `INSERT /*+ APPEND */ ... SELECT` for a query source and `INSERT /*+ APPEND_VALUES */` with array binds for a file; a direct-path load locks the table exclusively until commit. SQL Server sends each file batch in one round trip through pyodbc's `fast_executemany`. MySQL falls back to the generic path, with a warning, when `LOAD DATA LOCAL INFILE` is refused. DB2's `bulk_load` also accepts the neutral `source_type` (`'file'` or `'cursor'`) the other engines take, and an explicit `filetype` still wins. The route a load takes is logged at INFO.
- **`str()` of a `Raw` is its SQL text.** Interpolating a `Raw` into hand-built SQL, as in `f"... <= {cutoff}"`, produced its repr, `Raw('CURRENT_DATE')`, which is invalid SQL; it now produces `CURRENT_DATE`. Passing a `Raw` to `insert`, `update` or `select` was never affected.

## [0.3.0] - 2026-07-21

### Fixed

- **`merge_select` broken on Oracle / SQL Server / Spark**: those backends inherited the base `INSERT ... ON CONFLICT` implementation, which they don't support, so every `merge_select` call returned a `NotImplementedError` failure (only PostgreSQL/MySQL/SQLite and Db2 worked). `merge_select` is now generic across **all** backends — Db2/Oracle/SQL Server/Spark use a shared `MERGE INTO ... USING (SELECT ...)` builder (with per-dialect handling for the `AS` alias keyword, Db2 `ELSE IGNORE`, the SQL Server trailing `;`, and Spark's qualified `SET` targets), while PostgreSQL/MySQL/SQLite keep the `INSERT ... ON CONFLICT / ON DUPLICATE KEY` path. The Db2-specific `merge_select` override is removed in favor of the shared builder.
- **`merge_select` conditions: operators silently dropped on Db2**: the old Db2 override only understood a flat-equality dict (`{col: val}`) or a raw string; any other shape (e.g. operator/range conditions) fell through and the WHERE clause was **silently omitted**, merging the entire source table. Conditions now route through the shared `where_parser` on every backend.
- **`merge_select` on SQLite**: `INSERT ... SELECT ... ON CONFLICT` with no `WHERE` raised `near "DO": syntax error` (SQLite parses `ON` as a join clause); a `WHERE 1=1` disambiguator is now added on SQLite when the source SELECT has no filter.
- **A `None` in a WHERE condition matched no rows.** `{"col": None}` rendered `col = NULL`, which is never true. It now renders `col IS NULL`, `{"col": {"!=": None}}` renders `col IS NOT NULL`, and an operator with no meaning against NULL, such as `>`, raises `ValueError`. `IN` and `NOT IN` now handle `None` members and empty lists: a `None` member becomes a separate `IS [NOT] NULL` branch, which avoids the trap where one NULL in a `NOT IN` list makes the predicate unknown for every row, and an empty list is always false for `IN` and always true for `NOT IN`. `$in` is added alongside `$nin`.
- **A literal `%` in SQL was doubled to `%%` on drivers that take it literally**, so a value written as `'50%'` was stored as `'50%%'`. The escape is needed only when values are bound through a pyformat (`%s`) driver such as psycopg or PyMySQL, and is now applied only then; qmark drivers (sqlite3, pyodbc, `ibm_db_dbi`) and statements run without parameters receive the SQL as written.
- **Oracle rejected SQL written with `?` placeholders** with `DPY-4009`, reporting zero binds for the values provided, because the Oracle backend never converted `?` to its `:1, :2, ...` binds. It now converts `?` like the other backends.

### Changed

- **`merge_select` `order_by` is ignored for the MERGE INTO backends** (Db2/Oracle/SQL Server/Spark): it cannot affect merge semantics and is illegal inside the `USING` subquery on SQL Server. It is still applied for the `INSERT ... SELECT` (PostgreSQL/MySQL/SQLite) family.
- **Consistent error contract: data methods now raise on failure instead of returning `{"status": -1}`** (BREAKING). Methods that previously caught operational errors and returned a `{"status": -1, "message": ...}` dict now log and re-raise. The single contract is now: **success → result dict with `status: 0`; operational failure → raise the driver exception; misuse (bad arguments) → raise `ValueError`/`TypeError`.** This spans:
  - **Base methods**: `insert`, `bulk_update`, `merge_select`, `execute_script`, `run_script_from_file` (joining `query`/`execute`/`select`/`update`/`delete`/`merge`, which already raised).
  - **Backend overrides** (these were missed initially and swallowed independently of the base): `sqlite.query`; `spark.query`/`execute`/`_single_insert`/`_bulk_insert`/`update`/`delete`/`merge`/`bulk_load`; `oracle._bulk_insert`/`merge`; `db2.merge` and the `except` paths of `load_admin_cmd`/`export_admin_cmd`/`admin_cmd`; `mysql.bulk_load`; `postgres.bulk_load`; `sqlserver.merge`.

  This fixes a latent atomicity bug — inside `pool.transaction()`, a swallowed error let the context manager commit partial work instead of rolling back (rollback only fires when an exception propagates). **Preserved as result statuses (not raises):** DB2 `ADMIN_CMD` LOAD/EXPORT outcomes (`ROWS_LOADED`/`ROWS_REJECTED`/`ROWS_DELETED` reporting), Spark's `_check_delta_support()` capability guards, and the **streaming** methods (`stream_*`, `stream_to_csv`) with their per-row / aggregate `status` + `max_error_count` / `abort_on_error` semantics. Applications that need to continue past an error should wrap the call (see docs/database.md → "The Error Contract"). Callers that checked `result['status'] == -1` must switch to `try/except`.
- **`bulk_update` misuse now raises `ValueError`** (BREAKING, minor): passing rows whose data is missing the declared `key_columns` previously returned `{"status": -1}`; it now raises `ValueError`, consistent with how `merge`/`select` already report invalid arguments. (Operational DB errors raise the driver exception; argument/misuse errors raise `ValueError`/`TypeError`.)

### Added

- **`merge_select`: SQL-injection-safe conditions by default**: condition values are now **bound as parameters** by default (`dynamic_param='Y'`, consistent with `select()`), instead of being inlined into the SQL. Pass `dynamic_param='N'` to inline (quoted/escaped); Spark always inlines because its connector can't bind here. New shared helper `longjrm.utils.sql.build_where` returns `(clause, values)`; `build_inline_where` remains as a thin inline wrapper.
- **`merge_select` conditions: operators and list-of-conditions support**: `conditions` accepts, on every backend, (1) a raw clause string (verbatim), (2) a dict with operator/`IN`/`$and`/`$or` support (e.g. `{"col": {">": x, "<=": y}}`), or (3) a list of condition dicts AND-ed together (e.g. `[{"col": {">": x}}, {"col": {"<=": y}}]`). Backtick-escaped CURRENT keywords are emitted as SQL keywords.
- **`merge_select`: `isolation_clause` available on all backends** (previously Db2-only), appended to the source SELECT; defaults to empty.
- **`Raw`, the type-safe way to pass a SQL expression as a value.** `Raw("...")` is exported from the package root together with the constants `CURRENT_TIMESTAMP` and `CURRENT_DATE`. Every WHERE parser and value serializer renders it verbatim and never binds it. A `Raw` can only be constructed in Python code, so JSON-deserialized data can never become SQL, which no content-based marker can guarantee; the backtick CURRENT keywords are kept for backward compatibility only.
- **`stream_select`**, the streaming counterpart of `select()`, on `Db` and `AsyncDb`. It builds the same SQL as `select()`, so the `data_fetch_limit` default still applies and `options={"limit": 0}` streams the whole table, and it yields `(row_number, row, status)` like `stream_query`.
- **`reject_sink` on the streaming handlers.** `stream_insert`, `stream_update`, `stream_merge` and `stream_query` take an optional keyword-only `reject_sink(row_number, row, reason)`, called for every rejected row so the caller can keep it with its reason, and the result carries `reject_count` alongside `record_count`. When a sink is given or `max_error_count > 0`, each row runs under a savepoint, so a failing row is rolled back and recorded as a reject instead of aborting the batch transaction. The sink must not raise and must not write through the same connection, since a rollback of the target would erase its writes. Without a sink, behavior is unchanged.
- **Data-engineering `rows_*` count keys (additive, non-breaking)**: result dicts now expose the standard `rows_*` names alongside the historical count keys, so counts read uniformly across a pipeline — `rows_read` (`query`/`select`, stream pull = `record_count`), `rows_inserted`/`rows_updated`/`rows_deleted`/`rows_merged` (the matching write methods, from `count`), and `rows_rejected` (streams, from `reject_count`). `SparkDb` gets the same on its overrides; async delegates and inherits for free. Each alias is added only when its source key is present.

### Deprecated

- **Old count keys now nudge toward `rows_*`**: reading `count` (`query`/`insert`/`update`/`delete`/`merge`) or `record_count`/`reject_count` (streams) emits a `DeprecationWarning` pointing to its `rows_*` replacement. The old keys still work and reads of the new keys are silent — this is only the migration hint. Planned unification path:
  - **this release**: `rows_*` available + old keys deprecated (warn-on-access);
  - **next**: add `rows_affected` / `rows_returned` (for `execute` and the file ops, whose `count`/`row_count` are not yet aliased and therefore not yet deprecated), and deprecate the remaining old keys;
  - **then**: remove the old keys — unification complete.

---

## [0.2.0] - 2026-05-11

### Fixed

- **MySQL `port` ignored**: `MySQLConnector` was not forwarding `port` to `pymysql.connect()`, so non-default ports were silently dropped. Now passed correctly.

### Added

- **Driver options passthrough**: `DatabaseConfig.options` (or DSN query params) are now forwarded to each driver's `connect()` via a per-connector `_PASSTHROUGH` allowlist. Previously most options were silently dropped. Notable additions:
  - **Postgres**: `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`, `application_name`, `tcp_user_timeout`, `target_session_attrs`, `client_encoding`, `gssencmode`, `channel_binding`, `sslcert`, `sslkey`, `sslrootcert`, `sslpassword`, `service`, `passfile`, `options`, etc.
  - **MySQL**: `charset`, `ssl`, `ssl_ca`, `ssl_cert`, `ssl_key`, `ssl_verify_cert`, `ssl_verify_identity`, `read_timeout`, `write_timeout`, `init_command`, `unix_socket`, `client_flag`, `bind_address`, `program_name`, `max_allowed_packet`, `compress`, etc.
  - **Oracle**: `mode`, `wallet_location`, `wallet_password`, `events`, `edition`, `purity`, `cclass`, `tag`, `expire_time`, `retry_count`, `retry_delay`, `ssl_server_dn_match`, `https_proxy`, etc.
  - **DB2**: `SECURITY`, `AUTHENTICATION`, `CURRENTSCHEMA`, `SSLClientKeystoreDB`, `SSLClientKeystash`, `SSLServerCertificate`, `QueryTimeoutInterval`, `PROGRAMNAME`, etc.
  - **SQLite**: `timeout` (busy-lock), `detect_types`, `check_same_thread`, `cached_statements`, `uri`.
  - **SQL Server**: All non-pyodbc-kwarg options continue to be inlined into the ODBC connection string (`Encrypt`, `TrustServerCertificate`, etc.); pyodbc-only kwargs (`readonly`, `ansi`, `encoding`, `attrs_before`) are now correctly routed to `pyodbc.connect()` instead of being inlined.
  - See each connector's `_PASSTHROUGH` set in [`longjrm/connection/connectors.py`](longjrm/connection/connectors.py) for the authoritative list per driver.
- **Per-database `connect_timeout` override**: `options.connect_timeout` (per-database) now overrides `JrmConfig.connect_timeout` (global). Previously the global value was the only knob. Mapped per-driver to the parameter the driver actually understands:
  - Postgres / MySQL: `connect_timeout` (libpq / PyMySQL)
  - Oracle: `tcp_connect_timeout`
  - SQL Server: `pyodbc.connect(timeout=...)` (login timeout)
  - DB2: `ConnectTimeout` (in ibm_db connection string)
  - SQLite: **not mapped** — sqlite3's `timeout` is a busy-lock timeout with different semantics; set it explicitly via `options.timeout` if needed.

### Added — Async API

- **Async API (Phase 1 + Phase 2)**: First-class support for using longjrm inside event-loop frameworks (FastAPI, aiohttp, Sanic, Starlette) without manually wrapping every call in `run_in_threadpool`.
  - New `AsyncDb` class in `longjrm.database.async_db`. Methods mirror `Db` 1:1 in name, parameters, and return shape — only the return type changes from `T` to `Awaitable[T]`.
  - **Phase 1** (CRUD core): `select`, `query`, `execute`, `insert`, `update`, `delete`, `merge`, plus `commit` / `rollback` / `set_autocommit` / `get_autocommit`.
  - **Phase 2** (bulk / streaming / scripting):
    - Bulk: `bulk_update`, `merge_select`, `bulk_load`.
    - Streaming reads: `stream_query` and `stream_query_batch` return an `_AsyncGenAdapter` async-iterator. Use `async for row_num, row, status in db.stream_query(sql):`. The adapter holds the `AsyncDb` lock for the lifetime of iteration (since the underlying DB-API cursor cannot be shared) and releases it on exhaustion or `aclose()` (auto-called on early break / exception).
    - Streaming writes: `stream_insert`, `stream_update`, `stream_merge`. Accept a **sync** iterable / generator as the `stream` argument; the whole consumption runs in a single worker thread. Async iterables are not supported; materialize first if needed.
    - Files / scripts: `run_query_from_file`, `execute_script`, `run_script_from_file`, `stream_to_csv`.
  - New factory `get_async_db(client)` alongside the existing `get_db(client)`.
  - New async context managers `Pool.aclient()` and `Pool.atransaction()`. They reuse the synchronous `client()` / `transaction()` internally (single source of truth for session_setup, autocommit, isolation, teardown), with `__enter__` / `__exit__` dispatched via `asyncio.to_thread` so the event loop stays unblocked during connection acquisition.
  - `AsyncDb` instances guard their wrapped DB-API connection with an internal `asyncio.Lock`. Accidentally `gather()`-ing multiple calls on the same `AsyncDb` is serialized rather than corrupt; for real concurrency, each branch should `async with pool.aclient()` to check out its own connection.

### Architecture notes

- This is **threadpool-backed async**, not native async I/O. Underlying drivers (psycopg, pymysql, oracledb, pyodbc, ibm_db, sqlite3) remain synchronous; `AsyncDb` runs each call in `asyncio.to_thread` so it does not block the event loop. For C10K-class throughput requirements, evaluate a native async driver (asyncpg, psycopg.AsyncConnection, aiomysql) directly.
- The synchronous `Db`, `Pool.client()`, `Pool.transaction()`, and `get_db()` APIs are unchanged. Existing sync users see zero behavioral change.
- **Known limitation**: SQLite + SQLAlchemy `SingletonThreadPool` is not compatible with threadpool dispatch (SQLite connections are thread-bound by default and the singleton pool keeps them on a single thread). For SQLite under async, use the DBUtils backend.

### Compatibility

- No new runtime dependencies. `asyncio.to_thread` is stdlib (Python 3.9+) and the project already requires 3.10+.
- No changes to existing public APIs, configuration formats, or method signatures.

---

## [0.1.2] - 2026-02-10

### Added

- **Session Setup Support**: Added support for `session_setup` and `session_teardown` in database configuration, enabling PostgreSQL Row Level Security (RLS) and custom session initialization.
- **No-Update Merge**: Added `no_update` parameter to `merge` operation, enabling the ability to skip updates if a record already exists.

## [0.1.1] - 2026-01-31

### Changed

- **License Change**: Changed project license from MIT to Apache License 2.0.
- **ABC Interface Pattern**: The `Db` class now inherits from Python's `ABC` (Abstract Base Class), providing:
  - Compile-time enforcement of required methods via `@abstractmethod`
  - Better IDE support with autocomplete for abstract methods
  - Clear interface contracts for database adapter developers
  - Required abstract methods: `get_cursor()`, `get_stream_cursor()`, `_build_upsert_clause()`

---

## [0.1.0] - 2026-01-11

### Breaking Changes

- **Python 3.10+ Required**: Dropped support for Python 3.8/3.9. The library now requires Python 3.10 or later.
- **PostgreSQL Driver Migration**: Replaced `psycopg2` with `psycopg` (v3). Update your dependencies from `psycopg2-binary` to `psycopg[binary]>=3.1.0`.
- **Removed `DatabaseConnection` Class**: The monolithic `DatabaseConnection` class has been replaced by the `Pool` and `connectors` factory pattern.
- **Removed MongoDB Support**: MongoDB support has been removed to focus on SQL/Relational databases and Spark SQL.

### Added

#### New Architecture
- **Connector Factory Pattern**: New `get_connector_class()` factory for dynamic database connector selection.
- **Database-Specific Subclasses**: Added `PostgresDb`, `MySQLDb`, `SqliteDb`, `OracleDb`, `Db2Db`, `SqlServerDb`, `SparkDb`, and `GenericDb` classes.
- **`get_db()` Factory**: New factory function in `longjrm.database` to automatically select the correct Db subclass.

#### New Database Support
- **Oracle Database**: Full support via `oracledb` driver.
- **IBM DB2**: Full support including `ADMIN_CMD` operations, partition management, and `RUNSTATS`.
- **SQL Server**: Full support via `pyodbc` driver.
- **Apache Spark SQL**: Comprehensive support including Delta Lake integration.

#### Spark SQL Features
- Delta Lake MERGE, UPDATE, DELETE operations
- Memory-efficient streaming via `toLocalIterator()`
- Bulk loading from CSV, Parquet, JSON, ORC files
- Auto-detection of parameterized query support (Spark 3.4+)
- SparkSession singleton management

#### Advanced Operations
- **Streaming Operations**: `stream_insert()`, `stream_update()`, `stream_merge()` for processing large datasets with periodic commits.
- **Bulk Loading**: Database-specific high-performance loading via `bulk_load()` method.
- **CSV Export**: Universal `stream_to_csv()` for efficient file export.
- **Partition Management**: DB2-specific `add_partition()`, `attach_partition()`, `detach_partition()`, `drop_detached_partition()`.

### Documentation

- Added Quickstart guide to README.md
- Added [Spark SQL Integration Guide](docs/spark.md)
- Added [API Reference](docs/api-reference.md)
- Added [Migration Guide](docs/migration.md) for upgrading from v0.0.x
- Updated all documentation to reflect psycopg v3 migration
- Expanded driver support tables to include all databases

### Internal

- Refactored `insert` method to use `_single_insert` and `_bulk_insert` pattern.
- Consolidated value formatting logic into `_prepare_sql`.
- Improved logging throughout the library.

---

## [0.0.2] - 2024-xx-xx

### Added

- Initial MySQL and PostgreSQL support
- Basic CRUD operations (select, insert, update, delete)
- Connection pooling via DBUtils
- JSON-based query building
- Placeholder support

---

## [0.0.1] - 2024-xx-xx

### Added

- Initial release
- Core `Db` class with query execution
- `DatabaseConnection` class for connection management
- Basic configuration loading
