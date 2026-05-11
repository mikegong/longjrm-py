# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

---

## [0.2.0] - 2026-05-07

### Added

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
