# longjrm-py

**longjrm-py** is a JSON Relational Mapping (JRM) library for Python that provides a hybrid adapter for connecting object-oriented programming with various database types. The library bridges the gap between OOP and databases that don't naturally support object-oriented paradigms, offering a more adaptable alternative to traditional ORMs.

## Overview

Introducing JRM (JSON Relational Mapping) - a dynamic and high-efficiency library designed to revolutionize the way databases are populated. Recognizing that the majority of databases don't naturally support object-oriented programming, JRM steps in as a game-changing solution, bridging the gap with finesse and efficiency.

The core philosophy is to eliminate data models inside applications and work directly with database models. The input and output of database operations are JSON data instead of OOP objects. This innovative approach circumvents the limitations often encountered with traditional Object-Relational Mapping (ORM) in non-object-oriented databases, reduces backend development cost, and increases performance.

## Key Features

- **Multi-Database Support**: Unified interface for MySQL, PostgreSQL, and more databases that support DB-API 2.0
- **JSON-Based Operations**: Direct JSON input/output for database operations
- **Connection Pooling**: Efficient connection management with DBUtils-based pooling and SQLAlchemy pooling
- **Configuration Management**: Flexible configuration via JSON files or environment variables
- **Lightweight Design**: Minimal overhead compared to traditional ORMs
- **Database Agnostic**: Consistent API across different database types
- **Automatic Connection Management**: Context manager pattern for safe resource handling

## Advanced Features

- **Placeholder Support**: Supports both positional (`%s`, `?`) and named placeholders (`:name`, `%(name)s`, `$name`) with automatic detection and conversion

## Architecture

### Core Components

- **`longjrm.database.db.Db`**: Core JRM class that wraps CRUD SQL statements via database APIs
- **`longjrm.connection.pool.Pool`**: Multi-database connection pool management
- **`longjrm.connection.dbconn.DatabaseConnection`**: Database connection abstraction layer
- **`longjrm.config.config`**: Environment and database configuration loader
- **`longjrm.utils`**: Library-standard utilities including logging

### Supported Databases

- **MySQL** (via PyMySQL)
- **PostgreSQL** (via psycopg2)

**Extensible Database Support**: The library is designed to easily support additional databases that implement the Python DB-API 2.0 specification. New database types can be added by updating the driver mappings and implementing the appropriate connection logic.

Database type mappings are defined in `longjrm/connection/driver_map.json`.

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
git clone <repository-url>
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

- **Core dependencies**: PyMySQL, psycopg2-binary, DBUtils (always installed)
- **Optional dependencies**: 
  - `sqlalchemy`: SQLAlchemy ~= 2.0.0 for advanced connection pooling features

## Configuration

### Runtime Configuration (12-Factor App Compliant)

The library follows **12-Factor App** principles for configuration management, particularly **Factor III: Config**. Configuration can be provided through:

1. **Environment Variables** (recommended for production)
2. **Configuration Files** (suitable for development)
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

# Manual connection management (for transaction control)
client = pool.get_client()
try:
    db = Db(client)
    
    # Begin transaction
    db.execute("BEGIN")
    
    # Multiple operations in same transaction
    db.insert("orders", {"user_id": 1, "amount": 100.00})
    db.update("users", {"last_order": "`CURRENT_TIMESTAMP`"}, {"id": 1})
    
    # Commit transaction
    db.execute("COMMIT")
    
finally:
    pool.close_client(client)
```

### Database Client Architecture

Each database operation requires a "client" object containing:
- `conn`: Database connection object
- `database_type`: Type identifier (mysql, postgres, etc.)
- `database_name`: Logical database name
- `db_lib`: Python database driver such as psycopg2, pymysql

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
# Run individual test files
python longjrm/tests/pool_test.py
python longjrm/tests/connection_test.py
python longjrm/tests/insert_test.py

# Or run from project root without installation (for quick testing)
cd longjrm-py
python longjrm/tests/insert_test.py
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

### Logging Strategy
The library follows Python logging best practices:
- Uses `logging.getLogger(__name__)` in modules
- Root package includes `NullHandler()` for library silence
- Applications control logging configuration and output

## Dependencies

### Database Drivers (installed based on your database needs)
- PyMySQL ~= 1.1.0 (MySQL support)
- psycopg2-binary ~= 2.9.0 (PostgreSQL support)

### Connection Pooling
- SQLAlchemy ~= 2.0.0 (Advanced connection pooling backend)
- DBUtils ~= 3.0.3 (Lightweight connection pooling backend)

## License

This project is developed by Mike Gong at LONGINFO.

## Documentation

This documentation has been compiled by Claude Sonnet 4 model via Claude Code, providing comprehensive coverage of the longjrm-py library's features, architecture, and usage patterns.

