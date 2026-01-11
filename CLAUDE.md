# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

##- **Project Name**: longjrm-py
- **Version**: 0.1.0
- **Description**: A generic JSON Relational Mapping (JRM) library for Python 3.10+ that wraps SQL CRUD operations via DB-API 2.0.
- **Key Features**:
    - Database-oriented CRUD (insert, select, update, delete, merge)
    - **Connector Factory** architecture for dynamic DB support
    - JSON-based data and query structures
    - Connection pooling (via DBUtils 2/3 or SQLAlchemy)
    - **Streaming** support for queries and transactional inserts
    - **Bulk Load** and **Partition** management (DB2/Postgres specific)
    - 12-Factor App configuration compliance
- **Supported Databases**: Postgres, MySQL/MariaDB, Oracle, DB2, SQL Server, SQLite, Spark, and generic DB-API 2.0 drivers.

### Project Philosophy
- Eliminate data models inside applications
- Work directly with database models using JSON data format
- Reduce backend development cost and increase performance
- Provide lightweight alternative to traditional ORMs

## Architecture

The library supports multiple database types through a unified interface:
- **MySQL** (via PyMySQL) 
- **PostgreSQL** (via psycopg v3)
- **Oracle** (via oracledb)
- **DB2** (via ibm_db_dbi)
- **SQL Server** (via pyodbc)
- **SQLite** (via sqlite3)
- **Spark SQL** (via pyspark)

**Extensible Architecture**: Easily supports additional databases implementing Python DB-API 2.0 (PEP 249). Database driver mappings are defined in `longjrm/connection/driver_map.json`.

## CRUD Operations API

### Complete CRUD Support
The `Db` class provides comprehensive database operations with consistent JSON-based interface across all supported database types.

#### Database Client Setup
```python
from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

# Load configuration
cfg = JrmConfig.from_files("config/jrm.config.json", "config/dbinfos.json")
configure(cfg)

# Create database client with automatic connection management
pool = Pool.from_config(cfg.require("database-key"), PoolBackend.DBUTILS)
with pool.client() as client:
    db = Db(client)
    # Perform database operations here...
```

#### Connection Management

The Pool class provides two connection management patterns:

```python
# 1. Automatic connection management (Recommended for single operations)
with pool.client() as client:
    db = Db(client)
    result = db.select("users", ["*"])
    # Connection automatically returned to pool on exit

# 2. Manual connection management (Required for transaction control)
client = pool.get_client()
try:
    db = Db(client)
    
    # Begin transaction (database-specific)
    db.execute("BEGIN")
    
    # Multiple operations in same transaction
    db.insert("users", {"name": "John", "email": "john@test.com"})
    db.update("users", {"status": "active"}, {"name": "John"})
    
    # Commit transaction
    db.execute("COMMIT")
    
finally:
    pool.close_client(client)  # Always return to pool
```

**Connection Patterns:**
- **Context Manager (`with pool.client()`)**: Automatic transactional scope - each `with` block is isolated
- **Manual Management (`pool.get_client()`)**: Full transaction control - multiple operations can share same connection/transaction

#### 1. SELECT Operations
```python
with pool.client() as client:
    db = Db(client)
    
    # Basic select with columns
    result = db.select("users", ["id", "name", "email"])

    # Select with where conditions
    result = db.select("users", ["*"], where={"status": "active"})

    # Select with complex where and options
    result = db.select("users", 
        columns=["name", "email"],
        where={"age": {">": 25}, "status": "active"},
        options={"limit": 10, "order_by": ["name ASC"]}
    )

    # Returns: {"data": [...], "columns": [...], "count": N}
```

#### 2. INSERT Operations
```python
with pool.client() as client:
    db = Db(client)
    
    # Single record insert
    record = {"name": "John Doe", "email": "john@example.com", "age": 30}
    result = db.insert("users", record)

    # Bulk insert
    records = [
        {"name": "Jane Smith", "email": "jane@example.com", "age": 28},
        {"name": "Bob Wilson", "email": "bob@example.com", "age": 35}
    ]
    result = db.insert("users", records)

    # Insert with RETURNING (PostgreSQL)
    result = db.insert("users", record, return_columns=["id", "created_at"])

    # Insert with CURRENT keywords
    record = {"name": "Alice", "created_at": "`CURRENT_TIMESTAMP`"}
    result = db.insert("users", record)

    # Returns: {"status": 0, "message": "...", "data": [], "total": N}
```

#### 3. UPDATE Operations
```python
with pool.client() as client:
    db = Db(client)
    
    # Simple update
    data = {"status": "inactive", "updated_at": "`CURRENT_TIMESTAMP`"}
    where = {"id": 123}
    result = db.update("users", data, where)

    # Update with complex where conditions
    data = {"last_login": "`CURRENT_TIMESTAMP`"}
    where = {"status": "active", "age": {">": 18}}
    result = db.update("users", data, where)

    # Update with comprehensive where condition
    data = {"verified": True}
    where = {"email": {"operator": "LIKE", "value": "%@company.com", "placeholder": "Y"}}
    result = db.update("users", data, where)

    # Returns: {"status": 0, "message": "...", "data": [], "total": N}
```

#### 4. DELETE Operations
```python
with pool.client() as client:
    db = Db(client)
    
    # Delete with simple condition
    where = {"status": "inactive"}
    result = db.delete("users", where)

    # Delete with regular where condition
    where = {"age": {">": 65}, "last_login": {"<": "2023-01-01"}}
    result = db.delete("users", where)

    # Delete with comprehensive where condition
    where = {"email": {"operator": "LIKE", "value": "%@spam.com", "placeholder": "Y"}}
    result = db.delete("users", where)

    # Delete with LIKE operator (regular format)
    where = {"email": {"LIKE": "%@oldcompany.com"}}
    result = db.delete("users", where)

    # Returns: {"status": 0, "message": "...", "data": [], "total": N}
```

### Where Condition Formats

The library supports three where condition formats:

#### 1. Simple Condition
```python
{"column": "value"}
# Equivalent to: column = 'value'
```

#### 2. Regular Condition
```python
{"column": {"operator": "value"}}
# Examples:
{"age": {">": 25, "<=": 65}}  # age > 25 AND age <= 65
{"email": {"LIKE": "%@company.com"}}  # email LIKE '%@company.com'
{"status": {"IN": ["active", "pending"]}}  # status IN ('active', 'pending')
```

#### 3. Comprehensive Condition
```python
{"column": {"operator": "=", "value": "data", "placeholder": "Y"}}
# Provides explicit control over placeholder usage
# placeholder: "Y" = use parameterized query, "N" = inline value
```

### Special Features

#### Placeholder Support

The library supports **both positional and named placeholders** automatically:

- **Positional placeholders**: `%s` (PostgreSQL/MySQL), `?` (other databases)
- **Named placeholders**: `:name` (colon style), `%(name)s` (percent style), `$name` (dollar style)
- **Automatic detection**: The library automatically detects and converts named placeholders to positional format

```python
with pool.client() as client:
    db = Db(client)
    
    # ✅ Positional placeholders (traditional)
    result = db.query("SELECT * FROM users WHERE name = %s AND age = %s", ["John", 25])
    
    # ✅ Named placeholders (colon style)
    result = db.query(
        "SELECT * FROM users WHERE name = :name AND age = :age", 
        {"name": "John", "age": 25}
    )
    
    # ✅ Named placeholders (percent style)
    result = db.query(
        "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s",
        {"name": "John", "age": 25}
    )
    
    # ✅ Named placeholders (dollar style)
    result = db.query(
        "SELECT * FROM users WHERE name = $name AND age = $age",
        {"name": "John", "age": 25}
    )
    
    # ✅ CRUD operations with JSON conditions (automatic positional placeholders)
    result = db.select("users", ["*"], where={"name": "John", "age": 25})
```

#### CURRENT Keywords Support
```python
with pool.client() as client:
    db = Db(client)
    
    # SQL CURRENT keywords with backtick escaping
    data = {
        "created_at": "`CURRENT_TIMESTAMP`",
        "updated_date": "`CURRENT_DATE`"
    }
    result = db.insert("users", data)
    # Automatically unescaped to: CURRENT_TIMESTAMP, CURRENT_DATE
```

#### Data Type Handling
- **JSON Objects**: Automatically serialized to JSON strings
- **Lists**: Joined with `|` separator or JSON serialized for complex objects
- **Datetime Objects**: Formatted to ISO strings
- **Null Values**: Handled as SQL NULL

## Development Commands

### Install Dependencies
```bash
pip install -r requirements.txt
pip install -e .[all]
```

### Run Tests
```bash
# Run all tests
python -m unittest discover longjrm/tests *_test.py

# Run specific tests
python longjrm/tests/connection_test.py
python longjrm/tests/pool_test.py
python longjrm/tests/select_test.py
python longjrm/tests/insert_test.py
python longjrm/tests/spark_test.py
```

## Configuration

### Database Configuration Format
```json
{
    "mysql-dev": {
        "type": "mysql",
        "host": "localhost",
        "user": "developer",
        "password": "password",
        "port": 3306,
        "database": "app_dev"
    },
    "postgres-prod": {
        "type": "postgres",
        "host": "db.company.com",
        "user": "app_user",
        "password": "secure_password",
        "port": 5432,
        "database": "app_production"
    }
}
```

### Environment Configuration
```bash
# Option 1: Environment variables

# Option 2: JSON configuration files (recommended)
python -c "
from longjrm.config.config import JrmConfig
cfg = JrmConfig.from_files('jrm.config.json', 'dbinfos.json')
"
```

## Module Structure

```
longjrm/
├── __init__.py                 # Package initialization with NullHandler logging
├── config/                     # Configuration management
│   ├── config.py              # Main configuration loader  
│   └── runtime.py             # Runtime configuration management
├── connection/                 # Database connection management
│   ├── connectors.py          # Base and database-specific connectors
│   ├── driver_map.json        # Database driver mappings
│   ├── driver_registry.py     # Driver registration system
│   ├── dsn_parts_helper.py    # DSN parsing utilities
│   └── pool.py                # Connection pooling implementation
├── database/                   # Database operations
│   ├── __init__.py            # get_db() factory function
│   ├── db.py                  # Base Db class with CRUD operations
│   ├── postgres.py            # PostgreSQL-specific operations
│   ├── mysql.py               # MySQL/MariaDB-specific operations
│   ├── sqlite.py              # SQLite-specific operations
│   ├── oracle.py              # Oracle-specific operations
│   ├── db2.py                 # DB2-specific operations
│   ├── sqlserver.py           # SQL Server-specific operations
│   ├── spark.py               # Spark SQL-specific operations
│   ├── generic.py             # Generic DB-API 2.0 fallback
│   └── placeholder_handler.py # SQL placeholder utilities
├── tests/                      # Comprehensive test suite
│   ├── insert_test.py         # Insert operation tests
│   ├── update_test.py         # Update operation tests
│   ├── delete_test.py         # Delete operation tests
│   ├── merge_test.py          # Merge/upsert operation tests
│   ├── connection_test.py     # Connection testing
│   ├── pool_test.py           # Pool testing
│   └── spark_test.py          # Spark SQL testing
└── utils/                      # Utility modules
    ├── sql.py                 # SQL utilities
    ├── data.py                # Data processing utilities
    └── file_loader.py         # File loading utilities
```

## Database Client Architecture

Each database operation requires a "client" object containing:
- `conn`: Database connection/pool object
- `database_type`: Type identifier (mysql, postgres, etc.)  
- `database_name`: Logical database name
- `db_lib`: Python database driver such as psycopg2, pymysql

## Package Information

- **Version**: 0.1.0
- **Author**: Mike Gong at LONGINFO
- **Package**: longjrm
- **Python**: >= 3.10

### Dependencies

**Core Dependencies:**
- DBUtils >= 3.0.3 (Connection pooling)
- cryptography >= 41.0.0 (Security)

**Database Drivers (installed based on database needs):**
- psycopg[binary] >= 3.1.0 (PostgreSQL support)
- PyMySQL >= 1.1.0 (MySQL support)
- oracledb >= 2.0.0 (Oracle support)
- pyodbc >= 4.0.39 (SQL Server support)
- ibm_db >= 3.2.0 (DB2 support)
- pyspark >= 3.3.0 (Spark support)

**Connection Pooling:**
- SQLAlchemy >= 2.0.0 (Advanced connection pooling backend)

## Development Guidelines

### Code Standards
- Follow Python PEP 8 conventions
- Use descriptive variable names and comprehensive docstrings
- Maintain consistent import ordering and error handling

### Testing Strategy
- **Abort-on-Failure**: All tests stop immediately on first failure
- **Individual Test Isolation**: Each test wrapped in try-catch blocks
- **Comprehensive Validation**: Status codes, row counts, and verification queries
- **Multi-Backend Testing**: Tests run against DBUTILS and SQLAlchemy backends

### Error Handling
- Use appropriate exception types with meaningful error messages
- Log errors at appropriate levels using `logging.getLogger(__name__)`
- Handle connection failures gracefully with proper cleanup

### Logging Standards
- Root package provides NullHandler for library silence
- Applications control logging configuration and output
- Follow Python logging best practices

## Common Development Tasks

### Adding New Database Support
1. **Update Driver Mapping**: Add to `longjrm/connection/driver_map.json`
2. **Implement Connection Logic**: Update `dbconn.py` with driver-specific logic
3. **Handle SQL Variations**: Add database-specific patterns in `db.py`
4. **Add Dependencies**: Include DB-API 2.0 compliant driver in `requirements.txt`
5. **Create Tests**: Add test configuration and comprehensive test cases
6. **Update Documentation**: Update README.md and CLAUDE.md

**Supported DB-API 2.0 Databases** (easily added):
SQLite, Oracle, SQL Server, IBM DB2, Firebird, and others following DB-API 2.0 standard.

## Performance Optimization

- **Connection Pooling**: DBUtils-based pooling for SQL databases
- **Prepared Statements**: Available internally via `_execute_prepared()` and `_query_prepared()` methods
- **Bulk Operations**: Optimized bulk insert support for large datasets
- **Memory Management**: Efficient handling in long-running applications

## Security Best Practices

- **Parameterized Queries**: Default placeholder usage prevents SQL injection
- **Credential Protection**: Never log or expose database credentials
- **Connection Security**: Proper connection cleanup and thread safety
- **Input Validation**: Comprehensive validation of JSON data and where conditions

## Important Development Notes

- **No ORM Dependencies**: Maintain lightweight architecture
- **JSON-First Approach**: All data exchange uses JSON format
- **Multi-Database Compatibility**: Ensure changes work across all supported databases
- **Backward Compatibility**: Maintain API compatibility when making changes
- **Thread Safety**: Ensure connection pooling remains thread-safe