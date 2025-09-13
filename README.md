# longjrm-py

**longjrm-py** is a JSON Relational Mapping (JRM) library for Python that provides a hybrid adapter for connecting object-oriented programming with various database types. The library bridges the gap between OOP and databases that don't naturally support object-oriented paradigms, offering a more adaptable alternative to traditional ORMs.

## Overview

Introducing JRM (JSON Relational Mapping) - a dynamic and high-efficiency library designed to revolutionize the way databases are populated. Recognizing that the majority of databases don't naturally support object-oriented programming, JRM steps in as a game-changing solution, bridging the gap with finesse and efficiency.

The core philosophy is to eliminate data models inside applications and work directly with database models. The input and output of database operations are JSON data instead of OOP objects. This innovative approach circumvents the limitations often encountered with traditional Object-Relational Mapping (ORM) in non-object-oriented databases, reduces backend development cost, and increases performance.

## Key Features

- **Multi-Database Support**: Unified interface for MySQL, PostgreSQL, and MongoDB
- **JSON-Based Operations**: Direct JSON input/output for database operations
- **Connection Pooling**: Efficient connection management with DBUtils-based pooling
- **Configuration Management**: Flexible configuration via JSON files or environment variables
- **Lightweight Design**: Minimal overhead compared to traditional ORMs
- **Database Agnostic**: Consistent API across different database types

## Architecture

### Core Components

- **`longjrm.database.db.Db`**: Core JRM class that wraps CRUD SQL statements via database APIs
- **`longjrm.connection.pool.Pools`**: Multi-database connection pool management
- **`longjrm.connection.dbconn.DatabaseConnection`**: Database connection abstraction layer
- **`longjrm.config.config`**: Environment and database configuration loader
- **`longjrm.utils`**: Library-standard utilities including logging

### Supported Databases

- **MySQL** (via PyMySQL)
- **PostgreSQL** (via psycopg2)
- **MongoDB** (via pymongo)

**Extensible Database Support**: The library is designed to easily support additional databases that implement the Python DB-API 2.0 specification. New database types can be added by updating the driver mappings and implementing the appropriate connection logic.

Database type mappings are defined in `longjrm/connection/driver_map.json`.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .

# Build package
python setup.py sdist bdist_wheel
```

## Configuration

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
    },
    "mongodb-test": {
        "type": "mongodb",
        "host": "localhost",
        "port": 27017,
        "database": "test"
    }
}
```

### Environment Configuration

The library supports environment-based configuration when `USE_DOTENV=true` and `DOTENV_PATH` is set.

## Usage

### Basic Database Operations

```python
from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

# Load configuration
cfg = JrmConfig.from_files("config/jrm.config.json", "config/dbinfos.json")
configure(cfg)

# Create connection pool
pool = Pool.from_config(cfg.require("mysql-test"), PoolBackend.DBUTILS)

# Use database with automatic connection management
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

### Database Client Architecture

Each database operation requires a "client" object containing:
- `conn`: Database connection/pool object
- `database_type`: Type identifier (mysql, postgres, mongodb, etc.)
- `database_name`: Logical database name
- `db_lib`: Library identifier (typically "dbutils")

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
    },
    "mongodb-test": {
        "type": "mongodb",
        "host": "localhost",
        "port": 27017,
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

- **`connection_test.py`**: Basic database connectivity testing
- **`pool_test.py`**: Connection pooling and select operations testing  
- **`insert_test.py`**: Comprehensive insert functionality testing
  - Single record insertion
  - Bulk record insertion
  - MongoDB document operations
  - Error handling and edge cases
  - CURRENT keyword support
  - PostgreSQL RETURNING clause support

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

- PyMySQL ~= 1.1.0
- DBUtils ~= 3.0.3
- python-dotenv ~= 1.0.0
- setuptools ~= 65.5.1
- cryptography = 42.0.2

## License

This project is developed by Mike Gong at LONGINFO.

## Documentation

This documentation has been compiled by Claude Sonnet 4 model via Claude Code, providing comprehensive coverage of the longjrm-py library's features, architecture, and usage patterns.

