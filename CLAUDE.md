# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**longjrm-py** is a JSON Relational Mapping (JRM) library for Python that provides a hybrid adapter for connecting object-oriented programming with various database types. The library bridges the gap between OOP and databases that don't naturally support object-oriented paradigms, offering a more adaptable alternative to traditional ORMs.

### Project Philosophy
- Eliminate data models inside applications
- Work directly with database models using JSON data format
- Reduce backend development cost and increase performance
- Provide lightweight alternative to traditional ORMs

## Architecture

### Core Components

- **`longjrm.database.db.Db`**: Core JRM class that wraps CRUD SQL statements via database APIs
- **`longjrm.connection.pool.Pools`**: Multi-database connection pool management with thread-safe operations
- **`longjrm.connection.dbconn.DatabaseConnection`**: Database connection abstraction layer
- **`longjrm.config.config`**: Environment and database configuration loader (replaces old env.load_env)
- **`longjrm.config.runtime`**: Runtime configuration management
- **`longjrm.connection.driver_registry`**: Database driver registration and management
- **`longjrm.connection.dsn_parts_helper`**: DSN parsing and connection string utilities
- **`longjrm.utils.file_loader`**: File loading utilities for configuration files

### Database Support

The library supports multiple database types through a unified interface:
- **MySQL** (via PyMySQL) 
- **PostgreSQL** (via psycopg2)
- **MongoDB** (via pymongo)

**Extensible Architecture**: The library is architected to easily support additional databases that implement the Python DB-API 2.0 specification (PEP 249). This includes databases like SQLite, Oracle, SQL Server, and others. New database support requires minimal code changes - primarily updating driver mappings and connection logic.

Database driver mappings are defined in `longjrm/connection/driver_map.json`.

### Configuration Architecture

- **Database configurations**: JSON configuration files for database connection settings
- **Environment loading**: Uses python-dotenv when `USE_DOTENV=true` and `DOTENV_PATH` is set
- **Connection pooling**: DBUtils-based pooling for SQL databases, native MongoDB connections
- **Runtime configuration**: Dynamic configuration management via `longjrm.config.runtime`

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

Test scripts expect database configurations in JSON format. Example configuration:

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

## Package Information

### Current Version
- **Version**: 0.0.2
- **Author**: Mike Gong at LONGINFO
- **Package Name**: longjrm

### Dependencies
- PyMySQL ~= 1.1.0 (MySQL database support)
- DBUtils ~= 3.0.3 (Connection pooling)
- python-dotenv ~= 1.0.0 (Environment variable loading)
- setuptools ~= 65.5.1 (Package building)
- cryptography = 42.0.2 (Security and encryption support)

## Module Structure

```
longjrm/
├── __init__.py                 # Package initialization with NullHandler logging
├── config/                     # Configuration management
│   ├── __init__.py
│   ├── config.py              # Main configuration loader
│   └── runtime.py             # Runtime configuration management
├── connection/                 # Database connection management
│   ├── __init__.py
│   ├── dbconn.py              # Database connection abstraction
│   ├── driver_map.json        # Database driver mappings
│   ├── driver_registry.py     # Driver registration system
│   ├── dsn_parts_helper.py    # DSN parsing utilities
│   └── pool.py                # Connection pooling implementation
├── database/                   # Database operations
│   ├── __init__.py
│   └── db.py                  # Core JRM database class
├── tests/                      # Test suite
│   ├── connection_test.py     # Connection testing
│   └── pool_test.py           # Pool testing
└── utils/                      # Utility modules
    ├── __init__.py
    └── file_loader.py         # File loading utilities
```

## Development Guidelines

### Code Style
- Follow Python PEP 8 conventions
- Use descriptive variable names
- Maintain consistent import ordering
- Add docstrings for public methods

### Error Handling
- Use appropriate exception types
- Provide meaningful error messages
- Log errors at appropriate levels
- Handle connection failures gracefully

### Testing Strategy
- Test individual modules with their respective test files
- No unified test runner currently configured
- Manual testing required for database connections
- Use realistic database configurations for testing

### Logging Standards
- Use `logging.getLogger(__name__)` in each module
- Root package provides NullHandler for library silence
- Applications should configure logging as needed
- Follow Python logging best practices

## Common Development Tasks

### Adding New Database Support
The library's architecture makes it straightforward to add support for any database that implements Python DB-API 2.0 (PEP 249):

1. **Update Driver Mapping**: Add new database type to `longjrm/connection/driver_map.json`
2. **Implement Connection Logic**: Add driver-specific connection logic in `dbconn.py`
3. **Handle SQL Variations**: Add any database-specific SQL generation patterns in `db.py`
4. **Add Dependencies**: Include the appropriate DB-API 2.0 compliant driver in `requirements.txt`
5. **Create Tests**: Create test configuration and test cases
6. **Update Documentation**: Update README.md and CLAUDE.md with the new database support

**Supported DB-API 2.0 Databases** (can be easily added):
- SQLite (via sqlite3)
- Oracle (via cx_Oracle)
- SQL Server (via pyodbc)
- IBM DB2 (via ibm_db)
- Firebird (via fdb)
- And others that follow DB-API 2.0 standard

### Extending Configuration Options
1. Modify `longjrm/config/config.py` for new configuration parameters
2. Update JSON configuration schema
3. Add environment variable support if needed
4. Test with various configuration scenarios

### Performance Optimization
1. Focus on connection pooling efficiency
2. Optimize SQL generation and execution
3. Monitor memory usage in long-running applications
4. Profile database operation performance

## Important Notes for Development

- **No ORM Dependencies**: Avoid adding traditional ORM dependencies
- **JSON-First Approach**: All data exchange should use JSON format
- **Multi-Database Compatibility**: Ensure changes work across all supported databases
- **Backward Compatibility**: Maintain API compatibility when making changes
- **Security First**: Never log or expose sensitive database credentials
- **Thread Safety**: Ensure connection pooling remains thread-safe