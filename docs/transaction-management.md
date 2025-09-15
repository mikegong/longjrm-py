# Transaction Management Architecture

## Overview

This document describes the transaction management system implemented in the longjrm connection pool, including design decisions, implementation details, and usage patterns.

## Key Design Principles

### 1. **Unified API Across Backends**
- Consistent transaction interface for both SQLAlchemy and DBUtils backends
- Same method signatures and behavior regardless of underlying pool implementation
- Transparent handling of backend-specific connection types

### 2. **Automatic State Management**
- **Default State**: All connections have `autocommit=True` for immediate commits
- **Transaction State**: Automatic `autocommit=False` during transaction contexts
- **Cleanup**: Guaranteed restoration of `autocommit=True` on context exit

### 3. **Proper Resource Lifecycle**
- Connection acquisition and release managed by context managers
- Automatic rollback on exceptions
- No connection leaks even during error conditions

## Architecture Components

### Pool Class Enhancements

```python
class Pool:
    def client() -> Iterator[dict[str, Any]]
        # Basic connection context manager

    def transaction(isolation_level: Optional[str] = None) -> Iterator[TransactionContext]
        # Transaction context manager with autocommit control

    def execute_transaction(operations_func: Callable, isolation_level: Optional[str] = None) -> Any
        # Function-based transaction execution

    def execute_batch(operations: List[Dict[str, Any]], isolation_level: Optional[str] = None) -> List[Any]
        # Batch operation execution in single transaction
```

### TransactionContext

Simple, focused class for transaction control:

```python
class TransactionContext:
    def __init__(self, client: Dict[str, Any])
        # Takes only client - no redundant parameters

    def commit() -> None
        # Manual transaction commit

    def rollback() -> None
        # Manual transaction rollback

    @property
    def client -> Dict[str, Any]
        # Access to connection client for Db instance creation
```

### DatabaseConnection Enhancements

Enhanced with transaction-specific methods:

```python
class DatabaseConnection:
    def set_autocommit(self, autocommit: bool) -> None
        # Cross-database autocommit control

    def set_isolation_level(self, isolation_level: str) -> None
        # Database-specific isolation level management
```

## Backend Implementation Details

### DBUtils Backend

**Connection Lifecycle:**
1. **Creation**: Fresh `DatabaseConnection` instances via creator function
2. **Checkout**: Direct return of `DatabaseConnection` instance
3. **Check-in**: Custom reset function calls `conn.set_autocommit(True)` + rollback
4. **Pool Management**: DBUtils handles connection pooling and lifecycle

**Transaction Flow:**
```python
# DBUtils connections are DatabaseConnection instances
raw_conn = client['conn']  # DatabaseConnection instance
raw_conn.set_autocommit(False)  # Start transaction
# ... transaction operations ...
raw_conn.commit()  # Commit transaction
raw_conn.set_autocommit(True)  # Restore default state
```

### SQLAlchemy Backend

**Connection Lifecycle:**
1. **Creation**: Raw DB-API connections via engine
2. **Checkout**: Checkout event enforces `autocommit=True`
3. **Return**: `pool_reset_on_return="rollback"` handles cleanup
4. **Pool Management**: SQLAlchemy engine manages connection pool

**Transaction Flow:**
```python
# SQLAlchemy connections are raw DB-API connections
raw_conn = client['conn']  # Raw connection (psycopg2, PyMySQL, etc.)
raw_conn.set_autocommit(False)  # Start transaction
# ... transaction operations ...
raw_conn.commit()  # Commit transaction
raw_conn.set_autocommit(True)  # Restore default state
```

## Usage Patterns

### 1. Simple Transactions

```python
with pool.transaction() as tx:
    db = Db(tx.client)
    db.insert("users", user_data)
    db.insert("profiles", profile_data)
    # Auto-commit on success
```

### 2. Manual Transaction Control

```python
with pool.transaction() as tx:
    db = Db(tx.client)
    try:
        result = db.insert("users", user_data)
        validate_business_rules(result)
        tx.commit()  # Explicit commit
    except BusinessRuleError:
        tx.rollback()  # Explicit rollback
        raise
```

### 3. Isolation Level Control

```python
with pool.transaction(isolation_level="SERIALIZABLE") as tx:
    db = Db(tx.client)
    # Critical operations with highest isolation
    transfer_funds(from_account, to_account, amount)
```

### 4. Function-Based Transactions

```python
def create_user_with_profile(db):
    user_id = db.insert("users", user_data)
    db.insert("profiles", {"user_id": user_id, **profile_data})
    return user_id

# Execute function in transaction context
result = pool.execute_transaction(create_user_with_profile)
```

### 5. Batch Operations

```python
operations = [
    {"method": "insert", "params": {"table": "users", "data": user_data}},
    {"method": "insert", "params": {"table": "profiles", "data": profile_data}},
    {"method": "update", "params": {"table": "settings", "data": {"active": True}, "where": {"user_id": user_id}}}
]

results = pool.execute_batch(operations)
```

## Error Handling and Recovery

### Exception Safety

- **Automatic rollback** on any exception during transaction
- **Connection cleanup** guaranteed via `finally` blocks
- **State restoration** ensures connections returned to pool in known state

### Isolation Level Handling

- **Database-specific** SQL generation for isolation levels
- **Graceful degradation** for unsupported databases (warnings, not errors)
- **Validation** of isolation level values per database type

### Connection State Recovery

```python
# Pattern used throughout the system:
try:
    # Transaction operations
    raw_conn.set_autocommit(False)
    # ... work ...
    raw_conn.commit()
except Exception:
    raw_conn.rollback()
    raise
finally:
    raw_conn.set_autocommit(True)  # Always restore default state
    self.close_client(client)      # Always return to pool
```

## Performance Considerations

### Connection Pool Efficiency

- **No wrapper creation** during normal operation
- **Minimal overhead** for autocommit state management
- **Backend-optimized** connection handling

### Transaction Overhead

- **Single autocommit toggle** at transaction start/end
- **No state detection** - force known states instead
- **Direct method calls** instead of complex conditional logic

## Migration Guide

### From Manual Connection Management

```python
# Old pattern
client = pool.get_client()  # Now pool._get_client() - internal
try:
    db = Db(client)
    db.insert("users", data)
finally:
    pool.close_client(client)

# New pattern
with pool.client() as client:
    db = Db(client)
    db.insert("users", data)
```

### From Manual Transaction Management

```python
# Old pattern
client = pool.get_client()
try:
    raw_conn = client['conn']
    raw_conn.autocommit = False
    db = Db(client)
    db.insert("users", data)
    raw_conn.commit()
    raw_conn.autocommit = True
finally:
    pool.close_client(client)

# New pattern
with pool.transaction() as tx:
    db = Db(tx.client)
    db.insert("users", data)
```

## Future Enhancements

### Planned Features

1. **Nested Transaction Support** - Savepoint management for complex operations
2. **Transaction Decorators** - Function decoration for automatic transaction wrapping
3. **Async Transaction Support** - Integration with async/await patterns
4. **Transaction Metrics** - Performance monitoring and statistics
5. **Custom Isolation Policies** - Application-specific isolation level selection

### Extension Points

The architecture supports extension through:

- **Custom reset functions** for DBUtils pools
- **Additional isolation levels** via `DatabaseConnection.set_isolation_level()`
- **Backend-specific optimizations** in pool implementations
- **Transaction middleware** via operation function wrappers