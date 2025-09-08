# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**longjrm-py** is a JSON Relational Mapping (JRM) library for Python that provides a hybrid adapter for connecting object-oriented programming with various database types. The library bridges the gap between OOP and databases that don't naturally support object-oriented paradigms, offering a more adaptable alternative to traditional ORMs.

## Architecture

### Core Components

- **`longjrm.database.db.Db`**: Core JRM class that wraps CRUD SQL statements via database APIs
- **`longjrm.connection.pool.Pools`**: Multi-database connection pool management 
- **`longjrm.connection.dbconn.DatabaseConnection`**: Database connection abstraction layer
- **`longjrm.env.load_env`**: Environment and database configuration loader
- **`longjrm.utils.logger`**: Library-standard logging utilities

### Database Support

The library supports multiple database types through a unified interface:
- **MySQL** (via PyMySQL) 
- **PostgreSQL** (via psycopg2)
- **MongoDB** (via pymongo)

Database type mappings are defined in `longjrm/database/db_lib_map.json`.

### Configuration Architecture

- **Database configurations**: JSON files in `longjrm/env/` (e.g., `dbinfos_dev.json`)
- **Environment loading**: Uses python-dotenv when `USE_DOTENV=true` and `DOTENV_PATH` is set
- **Connection pooling**: DBUtils-based pooling for SQL databases, native MongoDB connections

## Development Commands

### Package Management
```bash
# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .

# Build package
python setup.py sdist bdist_wheel
```

### Testing
```bash
# Run individual test files (no test runner configured)
python longjrm/tests/pool_test.py
python longjrm/tests/connection_test.py
```

## Key Design Patterns

### Database Client Architecture
Each database operation requires a "client" object containing:
- `conn`: Database connection/pool object
- `database_type`: Type identifier (mysql, postgres, mongodb, etc.)  
- `database_name`: Logical database name
- `db_lib`: Library identifier (typically "dbutils")

### JRM Query Pattern
The `Db` class provides JSON-based CRUD operations:
- JSON data format for column-value pairs
- Dynamic SQL generation with proper escaping
- Support for SQL CURRENT keywords with backtick escaping
- Placeholder handling varies by database library (`%s` for dbutils, `?` for others)

### Logging Strategy
The library follows Python logging best practices:
- Uses `logging.getLogger(__name__)` in modules
- Root package (`longjrm/__init__.py`) includes `NullHandler()` for library silence
- Applications control logging configuration and output

### Environment Configuration
Database connections are configured via:
1. JSON configuration files (recommended for development)
2. Environment variables via dotenv (when `USE_DOTENV=true`)
3. Direct programmatic configuration

## Testing Database Connections

Test scripts expect database configurations in `longjrm/env/dbinfos_dev.json`. Copy from `dbinfos_dev_example.json` and update with actual credentials:

```json
{
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