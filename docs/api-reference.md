# LongJRM API Reference

This document provides a comprehensive reference for all public classes and methods in the LongJRM library.

## Table of Contents

- [Database Operations](#database-operations)
  - [get_db()](#get_db)
  - [Db Class](#db-class)
  - [SparkDb Class](#sparkdb-class)
- [Connection Management](#connection-management)
  - [Pool Class](#pool-class)
  - [TransactionContext](#transactioncontext)
  - [PoolBackend](#poolbackend)
- [Configuration](#configuration)
  - [DatabaseConfig](#databaseconfig)
  - [JrmConfig](#jrmconfig)
  - [Runtime Functions](#runtime-functions)
- [Exceptions](#exceptions)

---

## Database Operations

### get_db()

Factory function to create database-specific Db instance.

```python
from longjrm.database import get_db

db = get_db(client)
```

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | `dict` | Client dictionary from `pool.client()` |

**Returns:** `Db` subclass instance (PostgresDb, MySQLDb, SparkDb, etc.)

**Client Structure:**
```python
{
    "conn": <database_connection_object>,
    "database_type": "postgres",  # postgres, mysql, sqlite, oracle, db2, sqlserver, spark
    "database_name": "mydb",
    "db_lib": "psycopg"
}
```

---

### Db Class

Base class for all database operations. Subclasses: `PostgresDb`, `MySQLDb`, `SqliteDb`, `OracleDb`, `Db2Db`, `SqlServerDb`, `SparkDb`, `GenericDb`.

#### Constructor

```python
from longjrm.database.db import Db

db = Db(client)
```

#### Core Methods

##### query()

Execute raw SQL query and return results.

```python
result = db.query(sql, arr_values=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sql` | `str` | required | SQL statement to execute |
| `arr_values` | `list\|dict` | `None` | Values to bind (positional or named) |

**Returns:**
```python
{
    "status": 0,           # 0 = success, -1 = error
    "message": "...",      # Status message
    "data": [...],         # List of row dictionaries
    "columns": [...],      # List of column names
    "count": 10            # Number of rows returned
}
```

##### select()

Execute SELECT query with JSON-based conditions.

```python
result = db.select(table, columns=None, where=None, options=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name |
| `columns` | `list[str]` | `["*"]` | Columns to select |
| `where` | `dict` | `None` | WHERE conditions |
| `options` | `dict` | `None` | Query options (`limit`, `order_by`) |

**Where Condition Formats:**
```python
# Simple equality
{"status": "active"}

# Operators
{"age": {">": 18, "<=": 65}}
{"email": {"LIKE": "%@company.com"}}
{"status": {"IN": ["active", "pending"]}}

# Comprehensive (explicit control)
{"price": {"operator": ">", "value": 100, "placeholder": "Y"}}
```

##### insert()

Insert single record or bulk records.

```python
result = db.insert(table, data, return_columns=None, bulk_size=1000)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name |
| `data` | `dict\|list[dict]` | required | Single record or list of records |
| `return_columns` | `list[str]` | `None` | Columns to return (PostgreSQL only) |
| `bulk_size` | `int` | `1000` | Batch size for bulk inserts |

**Returns:**
```python
{"status": 0, "message": "...", "data": [], "count": 5}
```

##### update()

Update records with WHERE conditions.

```python
result = db.update(table, data, where=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name |
| `data` | `dict` | required | Column-value pairs to update |
| `where` | `dict` | `None` | WHERE conditions |

##### delete()

Delete records with WHERE conditions.

```python
result = db.delete(table, where=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name |
| `where` | `dict` | `None` | WHERE conditions |

##### merge()

UPSERT operation with key-based conflict resolution.

```python
result = db.merge(table, data, key_columns, column_meta=None, 
                  update_columns=None, no_update='N', bulk_size=0)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table` | `str` | required | Table name |
| `data` | `dict\|list[dict]` | required | Data to merge |
| `key_columns` | `list[str]` | required | Columns to match on |
| `update_columns` | `list[str]` | `None` | Columns to update (default: all non-key) |
| `no_update` | `str` | `'N'` | `'Y'` to skip update on match |

##### execute()

Execute DDL or DML statement without returning rows.

```python
result = db.execute(sql, arr_values=None)
```

#### Streaming Methods

##### stream_query()

Stream results row by row (memory-efficient).

```python
for row_num, row_dict, status in db.stream_query(sql, arr_values=None, max_error_count=0):
    if status == 0:
        process(row_dict)
```

**Yields:** `tuple(row_number: int, row_data: dict, status: int)`

##### stream_query_batch()

Stream results in batches.

```python
for total_rows, batch, status in db.stream_query_batch(sql, arr_values=None, 
                                                        batch_size=1000, max_error_count=0):
    process_batch(batch)  # batch is list of dicts
```

##### stream_insert()

Insert from iterator with periodic commits.

```python
result = db.stream_insert(stream, table, commit_count=10000, max_error_count=0)
```

##### stream_update()

Update from iterator with periodic commits.

```python
result = db.stream_update(stream, table, commit_count=10000, max_error_count=0)
```

##### stream_merge()

Merge from iterator with periodic commits.

```python
result = db.stream_merge(stream, table, key_columns, commit_count=10000, max_error_count=0)
```

##### stream_to_csv()

Export query results to CSV file.

```python
result = db.stream_to_csv(sql, csv_file, values=None, options=None)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `str` | SELECT query |
| `csv_file` | `str` | Output file path |
| `values` | `list` | Query parameters |
| `options` | `dict` | `{"header": "Y", "null_value": "", "batch_size": 5000}` |

#### Bulk Operations

##### bulk_load()

High-performance data loading (database-specific).

```python
result = db.bulk_load(table, load_info=None, command=None)
```

**PostgreSQL:** Uses `COPY` command  
**DB2:** Uses `ADMIN_CMD('LOAD ...')`  
**Spark:** Uses `spark.read` → `saveAsTable`

##### bulk_update()

Update multiple rows efficiently.

```python
result = db.bulk_update(table, data_list, key_columns, bulk_size=1000)
```

#### Transaction Control

```python
db.commit()      # Commit current transaction
db.rollback()    # Rollback current transaction
db.set_autocommit(True)   # Set autocommit mode
db.get_autocommit()       # Get current autocommit state
```

---

### SparkDb Class

Spark SQL implementation. Inherits from `Db` with Spark-specific behavior.

```python
from longjrm.database.spark import SparkDb

db = SparkDb(client, use_parameterized=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `dict` | required | Client with SparkSession |
| `use_parameterized` | `bool` | `None` | Force parameterized mode (auto-detects for Spark 3.4+) |

**Key Differences:**
- `commit()`, `rollback()` are no-ops
- `update()`, `delete()`, `merge()` require Delta Lake tables
- `bulk_load()` supports file and query sources

---

## Connection Management

### Pool Class

Unified connection pool over multiple backends.

```python
from longjrm.connection.pool import Pool, PoolBackend
```

#### Factory Method

```python
pool = Pool.from_config(
    db_cfg,                           # DatabaseConfig
    pool_backend=PoolBackend.DBUTILS, # or PoolBackend.SQLALCHEMY
    sa_opts=None,                     # SQLAlchemy-specific options
    dbutils_opts=None                 # DBUtils-specific options
)
```

#### Methods

##### client()

Get connection via context manager.

```python
with pool.client() as client:
    db = get_db(client)
    # ... use db
# Connection automatically returned to pool
```

##### transaction()

Execute operations in a transaction.

```python
with pool.transaction(isolation_level=None) as tx:
    db = get_db(tx.client)
    db.insert("users", data)
    db.update("profiles", updates, where)
    # Auto-commit on success, auto-rollback on exception
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `isolation_level` | `str` | `"READ COMMITTED"`, `"SERIALIZABLE"`, etc. |

##### execute_transaction()

Execute function in transaction.

```python
def my_operations(db):
    db.insert("users", data)
    return db.select("users", ["id"])

result = pool.execute_transaction(my_operations)
```

##### execute_batch()

Execute batch of operations in transaction.

```python
operations = [
    {"method": "insert", "params": {"table": "users", "data": user_data}},
    {"method": "update", "params": {"table": "profiles", "data": profile, "where": where}}
]
results = pool.execute_batch(operations, isolation_level="REPEATABLE READ")
```

##### dispose()

Cleanup pool resources.

```python
pool.dispose()
```

---

### TransactionContext

Context for transaction operations.

```python
with pool.transaction() as tx:
    tx.client     # Access client dict
    tx.commit()   # Manual commit
    tx.rollback() # Manual rollback
```

---

### PoolBackend

Enum for pool backend selection.

```python
from longjrm.connection.pool import PoolBackend

PoolBackend.SQLALCHEMY  # Enterprise-grade, advanced features
PoolBackend.DBUTILS     # Lightweight, high-performance
```

---

## Configuration

### DatabaseConfig

Database connection configuration.

```python
from longjrm.config.config import DatabaseConfig

config = DatabaseConfig(
    type="postgres",          # Database type
    host="localhost",         # Host address
    port=5432,                # Port number
    user="app_user",          # Username
    password="secret",        # Password
    database="mydb",          # Database name
    dsn=None,                 # Alternative: full connection string
    options={}                # Driver-specific options
)
```

**Supported Types:** `postgres`, `mysql`, `mariadb`, `sqlite`, `oracle`, `db2`, `sqlserver`, `spark`

### JrmConfig

Application-wide JRM configuration.

```python
from longjrm.config.config import JrmConfig

config = JrmConfig(
    _databases={"primary": db_config, "replica": replica_config},
    default_db="primary",
    connect_timeout=40,
    data_fetch_limit=1000,
    min_pool_size=1,
    max_pool_size=10,
    max_cached_conn=5,
    pool_timeout=60
)
```

### Runtime Functions

```python
from longjrm.config.runtime import (
    get_config,      # Get active configuration
    configure,       # Set global configuration
    require_db,      # Get database config by name
    use_db,          # Set default database
    reload_default,  # Clear configuration cache
    using_config,    # Temporary config context manager
    using_db         # Temporary database context manager
)
```

#### get_config()

Get active configuration (lazy-loaded from env/files).

```python
cfg = get_config()
```

#### require_db()

Get database configuration by name.

```python
db_cfg = require_db("primary")  # Specific database
db_cfg = require_db()           # Default database
```

#### using_db()

Temporary database context.

```python
with using_db("analytics"):
    db_cfg = require_db()  # Returns "analytics" config
```

---

## Exceptions

### JrmConnectionError

Raised when database connection fails.

```python
from longjrm.connection.connectors import JrmConnectionError

try:
    with pool.client() as client:
        ...
except JrmConnectionError as e:
    print(f"Connection failed: {e}")
    print(f"Extra info: {e.extra_info}")
```

### JrmConfigurationError

Raised for configuration errors.

```python
from longjrm.config import JrmConfigurationError

try:
    cfg = require_db("nonexistent")
except JrmConfigurationError as e:
    print(f"Config error: {e}")
```

---

## Response Format

All database operations return a standardized response:

```python
{
    "status": 0,           # 0 = success, -1 = error
    "message": "...",      # Human-readable status message
    "data": [...],         # Result data (list of dicts for queries)
    "columns": [...],      # Column names (queries only)
    "count": 10            # Row count (returned or affected)
}
```
