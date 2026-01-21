# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
