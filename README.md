# longjrm-py

**longjrm-py** is a JSON Relational Mapping (JRM) library for Python 3.10+ that provides a hybrid adapter for connecting object-oriented programming with various database types. The library bridges the gap between OOP and databases that don't naturally support object-oriented paradigms, offering a more adaptable alternative to traditional ORMs.

## Quickstart

```python
import os
from longjrm.database import get_db
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.config.config import DatabaseConfig

# 1. Configure and create pool (load password from environment variable)
config = DatabaseConfig(
    type="postgres",
    host="localhost",
    user="app",
    password=os.environ["DB_PASSWORD"],
    database="mydb"
)
pool = Pool.from_config(config, PoolBackend.DBUTILS)

# 2. Use database with context manager
with pool.client() as client:
    db = get_db(client)
    
    # Insert
    db.insert("users", {"name": "Alice", "email": "alice@example.com"})
    
    # Select
    result = db.select("users", ["id", "name"], {"name": "Alice"})
    print(result["data"])  # [{"id": 1, "name": "Alice"}]

# 3. Cleanup
pool.dispose()
```

## Overview

**JRM (JSON Relational Mapping)** is designed to build a direct adapter between applications and databases via JSON data instead of OOP objects. The core philosophy:

- **Eliminate application data models** - Work directly with database models as the only data source
- **JSON in, JSON out** - Input and output of database operations are JSON data instead of OOP objects
- **Leverage the database engine** - Generate required data structures through the database engine, which is optimized for data computation and outperforms most customized applications
- **Reduce development cost** - This programming paradigm can reduce development cost tremendously

This innovative approach circumvents the limitations often encountered with traditional Object-Relational Mapping (ORM) in non-object-oriented databases.

## Key Features

- **Multi-Database Support**: Unified interface for MySQL, PostgreSQL, and more databases that support DB-API 2.0
- **JSON-Based Operations**: Direct JSON input/output for database operations
- **Connection Pooling**: Efficient connection management with DBUtils, SQLAlchemy, and Spark pooling backends
- **Configuration Management**: Flexible configuration via JSON files or environment variables
- **Lightweight Design**: Minimal overhead compared to traditional ORMs
- **Database Agnostic**: Consistent API across different database types
- **Automatic Connection Management**: Context manager pattern for safe resource handling
- **Session Context Support**: Pass variables like `user_id` to connection sessions for RLS or auditing

## Advanced Features

- **Placeholder Support**: Supports both positional (`%s`, `?`) and named placeholders (`:name`, `%(name)s`, `$name`) with automatic detection and conversion

## Architecture

`longjrm` has been refactored to use a **Connector Factory** pattern, replacing the legacy `DatabaseConnection` class. This allows for:

1.  **Strict Configuration**: `DatabaseConfig` explicitly defines connection parameters.
2.  **Dynamic Connector Selection**: The factory selects the appropriate `BaseConnector` subclass (e.g., `PostgresConnector`, `OracleConnector`) or falls back to a **Generic Connector** for any DB-API 2.0 compliant driver.
3.  **Connection Pooling**: Seamless integration with `SQLAlchemy` and `DBUtils` pooling backends via the `Pool` class.

## Supported Databases

`longjrm` supports a wide range of databases "out of the box":

-   **PostgreSQL** (`psycopg` v3)
-   **MySQL / MariaDB** (`pymysql`)
-   **Oracle** (`oracledb`)
-   **IBM Db2** (`ibm_db_dbi`)
-   **SQL Server** (`pyodbc`)
-   **SQLite** (`sqlite3`)
-   **Apache Spark** (`pyspark` SQL) — Uses singleton `SparkSession` which natively manages concurrency; exposed as a pooling backend for unified API
-   **Generic DB-API**: Any other database with a standard Python DB-API 2.0 driver.

## Features

### Core Operations
-   **CRUD**: Simple `insert`, `select`, `update`, `delete`, `merge` (upsert) methods.
-   **Querying**: `query()` for standard fetches, with automatic dictionary row conversion.
-   **Transactions**: `commit`, `rollback`, and context managers (`with pool.transaction() as tx:`).

### Advanced Operations

#### Streaming
Memory-efficient operations for large datasets using Python generators:
-   **`stream_query()`**: Yields rows one-by-one to avoid loading everything into RAM.
-   **`stream_query_batch()`**: Yields data in configurable batches (buckets).
-   **Tx Stream Operations**: `stream_insert()`, `stream_update()`, `stream_merge()` handle massive data movement with periodic auto-commits (e.g., every 10k rows).
-   **CSV Export**: `stream_to_csv()` streams query results directly to a file.

#### Bulk Loading
High-performance bulk operations:
-   **Bulk Load**: High-performance data loading using native methods (`COPY` for Postgres, `LOAD` for DB2).
-   **Data Export**: universal `stream_to_csv` for efficient file export and DB2-specific `EXPORT` support.
-   **Partition Management**: DB2-specific tools for partition attach/detach/add.
-   **Streaming**: Memory-efficient row-by-row processing for massive datasets.

#### Partition Management (DB2)
Specialized support for DB2 partition maintenance:
-   `add_partition()`, `attach_partition()`, `detach_partition()`, `drop_detached_partition()`.

## Installation

### Basic Installation

```bash
# Install core package with DBUtils pooling
pip install longjrm

# Or install from source
pip install -e .
```

### Installation with Optional Dependencies

```bash
# Install with SQLAlchemy support for advanced connection pooling
pip install longjrm[sqlalchemy]

# Install with all optional dependencies
pip install longjrm[all]

# Development installation with optional dependencies
pip install -e .[sqlalchemy]
```

### Development Setup

```bash
# Clone the repository
git clone https://github.com/mikegong/longjrm-py.git
cd longjrm-py

# Install in development mode with all dependencies
pip install -e .[all]

# Or install dependencies manually
pip install -r requirements.txt
pip install -e .

# Build package
python setup.py sdist bdist_wheel
```

### Dependency Overview

- **Core dependencies**: DBUtils, cryptography (always installed)
- **Optional dependencies**: 
  - `mysql`: PyMySQL ~= 1.1.0 for MySQL support
  - `postgres`: psycopg[binary] >= 3.1.0 for PostgreSQL support
  - `sqlalchemy`: SQLAlchemy ~= 2.0.0 for advanced connection pooling features

## Configuration

### Runtime Configuration (12-Factor App Compliant)

The library follows **12-Factor App** principles for configuration management, particularly **Factor III: Config**. Configuration can be provided through:

1. **Environment Variables** (recommended for production)
2. **Configuration Files** (suitable for development or multiple databases environment)
3. **Programmatic Configuration** (for testing and embedded applications)

#### Environment-Based Configuration

For production deployments following 12-Factor principles, configure via environment variables:

```bash
# Single database configuration
export JRM_DB_TYPE=mysql
export JRM_DB_HOST=localhost
export JRM_DB_USER=username
export JRM_DB_PASSWORD=password
export JRM_DB_PORT=3306
export JRM_DB_NAME=production_db
export JRM_DB_KEY=mysql-prod

# Or use a complete JSON configuration
export JRM_DATABASES_JSON='{"mysql-prod": {"type": "mysql", "host": "db.example.com", "user": "app", "password": "secret", "port": 3306, "database": "prod"}}'

# Control configuration source precedence
export JRM_SOURCE=env  # Force environment-only (ignore files)
export JRM_SOURCE=files  # Force file-only (ignore environment)
export JRM_SOURCE=auto  # Default: smart precedence (env > files > defaults)
```

#### Configuration Precedence (JRM_SOURCE=auto)

1. **Environment paths**: If `JRM_CONFIG_PATH` or `JRM_DBINFOS_PATH` is set → load from files
2. **Environment variables**: If any `JRM_*` database variables exist → load from environment
3. **Default files**: If `./config/{jrm.config.json, dbinfos.json}` exists → load those
4. **Fallback**: Attempt environment loading (raises error if nothing configured)

### Database Configuration Files

Create database configuration files in JSON format:

```json
{
    "mysql-test": {
        "type": "mysql",
        "host": "localhost",
        "user": "username",
        "password": "password",
        "port": 3306,
        "database": "test"
    },
    "postgres-test": {
        "type": "postgres",
        "host": "localhost",
        "user": "username",
        "password": "password",
        "port": 5432,
        "database": "test"
    }
}
```

## Usage

### Runtime Configuration Usage

#### Automatic Configuration (Recommended)

```python
from longjrm.config.runtime import get_config, require_db, using_db
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

# Automatically loads config based on environment or files
cfg = get_config()
db_config = require_db("mysql-prod")  # or require_db() for default

# Create connection pool
pool = Pool.from_config(db_config, PoolBackend.DBUTILS)

# Use with context manager for automatic DB selection
with using_db("mysql-prod"):
    with pool.client() as client:
        db = Db(client)
        result = db.select("users", ["*"])
```

#### Manual Configuration

```python
from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure, using_config
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

# Load configuration from specific files
cfg = JrmConfig.from_files("config/jrm.config.json", "config/dbinfos.json")

# Set as current configuration
with using_config(cfg):
    db_config = require_db("mysql-test")
    pool = Pool.from_config(db_config, PoolBackend.DBUTILS)
    # ... database operations

# Or configure globally for the current context
configure(cfg)

# Create connection pool
pool = Pool.from_config(cfg.require("mysql-test"), PoolBackend.DBUTILS)

# Use database with automatic connection management (recommended for single operations)
with pool.client() as client:
    db = Db(client)
    
    # Insert operation with JSON data
    data = {"name": "John Doe", "email": "john@example.com"}
    result = db.insert("users", data)

    # Select operation
    conditions = {"email": "john@example.com"}
    users = db.select("users", ["*"], where=conditions)

    # Update operation
    updates = {"name": "Jane Doe"}
    db.update("users", updates, conditions)

    # Delete operation
    db.delete("users", conditions)
```

## Usage Examples

### Basic Connection & Query

```python
from longjrm import Pool, get_config

# 1. Initialize Pool (loads config automatically from env/files)
#    or explicitly: Pool.from_config(DatabaseConfig(type='postgres', ...))
cfg = get_config()
pool = Pool.from_config(cfg.require('mydb'), pool_backend='dbutils')

# 2. Use client context
with pool.client() as client:
    db = get_db(client)
    
    # Select
    result = db.select("users", ["id", "name"], {"active": True})
    print(result['data'])
    
    # Insert
    db.insert("users", {"name": "Alice", "active": True})
```

### Streaming Export

```python
# Stream query results to CSV with low memory usage
db.stream_to_csv(
    sql="SELECT * FROM large_audit_logs", 
    csv_file="audit_export.csv",
    options={"header": "Y", "batch_size": 5000}
)
```

## Dependencies

-   **Core**: `DBUtils`, `cryptography`
-   **Drivers (Optional)**:
    -   `psycopg` (Postgres)
    -   `pymysql` (MySQL)
    -   `oracledb` (Oracle)
    -   `pyodbc` (SQL Server)
    -   `ibm_db` (DB2)
    -   `pyspark` (Spark)

### Database Client Architecture

Each database operation requires a "client" object containing:
- `conn`: Database connection object
- `database_type`: Type identifier (mysql, postgres, etc.)
- `database_name`: Logical database name
- `db_lib`: Python database driver such as psycopg, pymysql

## Testing

### Setup for Testing

First, install the package in development mode:

```bash
# Install longjrm in development mode
pip install -e .
```

### Configure Test Databases

Test scripts expect database configurations in `test_config/dbinfos.json`. Copy from `test_config/dbinfos_example.json` and update with actual credentials:

```json
{
    "postgres-test": {
        "type": "postgres",
        "host": "localhost",
        "user": "username",
        "password": "password",
        "port": 5432,
        "database": "test"
    },
    "mysql-test": {
        "type": "mysql",
        "host": "localhost",
        "user": "username", 
        "password": "password",
        "port": 3306,
        "database": "test"
    }
}
```

### Run Tests

```bash
# Recommended: Run as module from project root
python -m longjrm.tests.select_test
python -m longjrm.tests.connection_test

# Filter for specific database (e.g. Oracle, DB2, SQLServer)
# This uses dynamic discovery from test_config/dbinfos.json
python -m longjrm.tests.connection_test --db=oracle
python -m longjrm.tests.select_test --db=db2
python -m longjrm.tests.transaction_test --db=sqlserver

# Alternatively, if installed in editable mode (pip install -e .):
python longjrm/tests/select_test.py --db=postgres
```

### Test Coverage

The test suite provides comprehensive coverage of all database operations:

- **`connection_test.py`**: Basic database connectivity testing
- **`pool_test.py`**: Connection pooling and select operations testing  
- **`insert_test.py`**: Comprehensive insert functionality testing
  - Single record insertion
  - Bulk record insertion
  - Error handling and edge cases
  - CURRENT keyword support
  - PostgreSQL RETURNING clause support
- **`select_test.py`**: SELECT query operations testing
  - Basic SELECT with WHERE conditions
  - Column selection and filtering
  - Complex query patterns
- **`update_test.py`**: UPDATE operations testing
  - Single and batch record updates
  - Conditional updates with WHERE clauses
  - Cross-database compatibility
- **`delete_test.py`**: DELETE operations testing
  - Conditional deletions
  - Bulk deletion operations
  - Safety checks and constraints
- **`merge_test.py`**: MERGE/UPSERT operations testing
  - Insert-or-update logic
  - Conflict resolution strategies
  - Database-specific merge implementations
- **`placeholder_test.py`**: SQL placeholder handling testing
  - Positional placeholder support (`%s`, `?`)
  - Named placeholder support (`:name`, `%(name)s`, `$name`)
  - Automatic placeholder detection and conversion
  - Cross-database placeholder compatibility

## Design Patterns

### JRM Query Pattern
The `Db` class provides JSON-based CRUD operations with:
- JSON data format for column-value pairs
- Dynamic SQL generation with proper escaping
- Support for SQL CURRENT keywords with backtick escaping
- Placeholder handling varies by database library (`%s` for dbutils, `?` for others)

### ABC Interface Pattern
The `Db` class uses Python's Abstract Base Class (ABC) to enforce a formal interface contract:

```python
from abc import ABC, abstractmethod

class Db(ABC):
    @abstractmethod
    def get_cursor(self):
        """Required: Get execution cursor."""
        ...
    
    @abstractmethod
    def get_stream_cursor(self):
        """Required: Get streaming cursor."""
        ...
    
    @abstractmethod
    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """Required: Database-specific upsert syntax."""
        ...
```

**Benefits:**
- **Compile-time enforcement**: Missing implementations fail at class instantiation, not runtime
- **IDE support**: Better autocomplete and type hints for abstract methods
- **Clear contracts**: Explicit interface documentation for database adapter developers

### Factory Pattern
The `get_db()` factory function creates the appropriate database-specific instance:

```python
from longjrm.database import get_db

db = get_db(client)  # Returns PostgresDb, MySQLDb, etc. based on database_type
```

### Logging Strategy
The library follows Python logging best practices:
- Uses `logging.getLogger(__name__)` in modules
- Root package includes `NullHandler()` for library silence
- Applications control logging configuration and output

## Dependencies

### Core Dependencies
- DBUtils >= 3.0.3 (Connection pooling)
- cryptography >= 41.0.0 (Security)

### Database Drivers (installed based on your database needs)
- psycopg[binary] >= 3.1.0 (PostgreSQL support)
- PyMySQL >= 1.1.0 (MySQL support)
- oracledb >= 2.0.0 (Oracle support)
- pyodbc >= 4.0.39 (SQL Server support)
- ibm_db >= 3.2.0 (DB2 support)
- pyspark >= 3.3.0 (Spark support)

### Connection Pooling
- SQLAlchemy >= 2.0.0 (Advanced connection pooling backend)

## License

This project is licensed under the [Apache License 2.0](LICENSE).

Developed by Mike Gong at LONGINFO.

## Documentation

All the documentation was compiled with assistance from Gemini 3.5 Pro, Claude Opus 4.5, and Claude Sonnet 4.
