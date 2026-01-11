# Migration Guide (v0.0.x to v0.1.0)

Version 0.1.0 introduces significant architectural improvements to LongJRM, moving from a monolithic `DatabaseConnection` class to a modular **Connector Factory** and **Pool** architecture. This guide helps you upgrade your existing application code.

## Key Changes

- **Removed `DatabaseConnection`**: Consolidated into connector classes (e.g. `PostgresConnector`). Use `Pool` for connection management.
- **Dynamic Connectors**: Replaced `Db` class constructor with `get_db()` factory to support dynamic subclassing (e.g., `PostgresDb`, `OracleDb`).
- **Removed MongoDB**: Focused support on SQL/Relational databases (SQL + Spark).

## Migration Steps

### 1. Client Management & Database Operations

**Old (v0.0.x):**
```python
from longjrm.database.db import Db

# Context manager was already available
with pool.client() as client:
    # Always used generic Db class directly
    db = Db(client)
    db.select("users")
```

**New (v0.1.0):**
```python
from longjrm.database import get_db

# Context manager handles checkout/checkin automatically
with pool.client() as client:
    # Use factory function instead of Db class
    # Factory returns specific subclass (e.g. PostgresDb)
    db = get_db(client)
    db.select("users")
# Connection returned to pool here
```



### 2. Removed Features

- **MongoDB Support**: Removed to focus on SQL/Relational databases (SQL + Spark).
- **Legacy Helpers**: Removed `longjrm.utils.helpers` in favor of specialized utils in `longjrm.utils.*`.

## Summary Checklist

1. [ ] Replace manual `get_client`/`close_client` with `with pool.client():`.\n2. [ ] Replace `Db(client)` with `get_db(client)`.
3. [ ] Ensure `requirements.txt` includes any specific drivers (e.g., `psycopg` instead of `psycopg2` if migrating driver too).
