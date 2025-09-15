# LongJRM Database Connection & Pooling Guide

## Overview

LongJRM provides a sophisticated connection management system that abstracts database connectivity across multiple database types while maintaining high performance through configurable connection pooling. The system supports both direct connections and pooled connections with multiple backend implementations.

## Architecture

### Core Components

- **`DatabaseConnection`**: Low-level database connection abstraction
- **`Pool`**: High-level connection pooling with multiple backends
- **`DriverRegistry`**: Dynamic driver loading and SQLAlchemy dialect mapping
- **Connection Backends**: SQLAlchemy, DBUtils, and MongoDB-specific implementations

### Driver Support

The connection system supports multiple database types through a driver registry system:

| Database | DB-API Module | SQLAlchemy Dialect | SQLAlchemy Driver |
|----------|---------------|-------------------|-------------------|
| PostgreSQL | `psycopg2` | `postgresql` | `psycopg2` |
| MySQL | `pymysql` | `mysql` | `pymysql` |
| MariaDB | `pymysql` | `mysql` | `pymysql` |
| SQLite | `sqlite3` | `sqlite` | (built-in) |
| MongoDB | `pymongo` | (NoSQL - no dialect) | (NoSQL - no driver) |

## Direct Database Connections

### DatabaseConnection Class

The `DatabaseConnection` class provides direct, non-pooled database connections with automatic driver loading and connection management.

#### Basic Usage

```python
from longjrm.config.config import DatabaseConfig
from longjrm.connection.dbconn import DatabaseConnection

# Create database configuration
db_config = DatabaseConfig(
    type="postgres",
    host="localhost",
    port=5432,
    user="app_user",
    password="secret",
    database="production"
)

# Create and connect
db_conn = DatabaseConnection(db_config)
conn = db_conn.connect()

# Use the connection
cursor = conn.cursor()
cursor.execute("SELECT VERSION()")
result = cursor.fetchone()
print(f"Database version: {result}")

# Clean up
db_conn.close()
```

#### DSN-Based Connection

```python
# Using DSN (Data Source Name) - preferred method
db_config = DatabaseConfig(
    type="postgres",
    dsn="postgres://user:password@localhost:5432/mydb?sslmode=require"
)

db_conn = DatabaseConnection(db_config)
conn = db_conn.connect()
```

#### Database-Specific Connection Examples

**PostgreSQL:**
```python
db_config = DatabaseConfig(
    type="postgres",
    host="db.example.com",
    port=5432,
    user="app_user",
    password="secure_password",
    database="production",
    options={
        "sslmode": "require",
        "connect_timeout": "10"
    }
)
```

**MySQL:**
```python
db_config = DatabaseConfig(
    type="mysql",
    host="mysql.example.com",
    port=3306,
    user="mysql_user",
    password="mysql_password",
    database="app_data",
    options={
        "charset": "utf8mb4",
        "autocommit": True
    }
)
```

**MongoDB:**
```python
# MongoDB Atlas (recommended)
db_config = DatabaseConfig(
    type="mongodb+srv",
    dsn="mongodb+srv://user:pass@cluster.mongodb.net/database"
)

# Traditional MongoDB
db_config = DatabaseConfig(
    type="mongodb",
    host="mongo.example.com",
    port=27017,
    user="mongo_user",
    password="mongo_password",
    database="app_database"
)
```

### Connection Features

#### Automatic Driver Loading

The system automatically loads the appropriate database driver based on the database type:

```python
# Driver automatically selected based on type
db_conn = DatabaseConnection(db_config)  # Loads psycopg2 for postgres, pymysql for mysql, etc.
```

#### Autocommit Management

Autocommit behavior is automatically configured based on database type and can be overridden:

```python
# Set autocommit explicitly
db_conn.set_autocommit(True)

# Or via configuration
db_config = DatabaseConfig(
    type="postgres",
    # ... other config
    options={"autocommit": False}
)
```

#### Connection Timeout

Configure connection timeouts globally or per-connection:

```python
# Global timeout via JRM configuration
config = JrmConfig(
    # ... databases
    connect_timeout=30  # 30 seconds
)

# The timeout is automatically applied to all connections
```

## Connection Pooling

### Pool Architecture

LongJRM provides a unified `Pool` interface with multiple backend implementations for optimal performance across different deployment scenarios.

#### Available Pool Backends

1. **SQLAlchemy Pool** (`PoolBackend.SQLALCHEMY`)
   - Best for complex applications with ORM integration
   - Advanced connection lifecycle management
   - Built-in connection validation and recovery

2. **DBUtils Pool** (`PoolBackend.DBUTILS`)
   - Lightweight, high-performance pooling
   - Lower memory overhead
   - Simple connection management

3. **MongoDB Pool** (`PoolBackend.MONGODB`)
   - Native MongoDB connection pooling
   - Automatic replica set handling
   - Built-in connection recovery

### Basic Pool Usage

```python
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.config.config import DatabaseConfig

# Create database configuration
db_config = DatabaseConfig(
    type="postgres",
    dsn="postgres://user:pass@localhost:5432/mydb"
)

# Create pool with SQLAlchemy backend
pool = Pool.from_config(db_config, PoolBackend.SQLALCHEMY)

# Get connection client and use the connection
with pool.client() as client:
    # Use the connection
    conn = client["conn"]
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    # Connection automatically returned to pool

# Dispose pool when done
pool.dispose()
```

### Pool Configuration

Pool behavior is controlled through JRM configuration settings:

```python
from longjrm.config.config import JrmConfig

config = JrmConfig(
    # ... database configurations
    min_pool_size=2,           # Minimum connections to maintain
    max_pool_size=20,          # Maximum connections allowed
    max_cached_conn=10,        # Maximum idle connections to cache
    pool_timeout=60,           # Timeout when acquiring connection
    connect_timeout=30         # Individual connection timeout
)
```

### Advanced Pool Usage

#### SQLAlchemy Pool with Custom Options

```python
# Custom SQLAlchemy pool options
sa_opts = {
    "pool_size": 15,
    "max_overflow": 25,
    "pool_timeout": 30,
    "pool_recycle": 3600,      # Recycle connections after 1 hour
    "pool_pre_ping": True,     # Validate connections before use
    "pool_reset_on_return": "rollback"
}

pool = Pool.from_config(
    db_config, 
    PoolBackend.SQLALCHEMY,
    sa_opts=sa_opts
)
```

#### DBUtils Pool with Custom Options

```python
# Custom DBUtils pool options
dbutils_opts = {
    "maxconnections": 50,      # Maximum total connections
    "mincached": 5,           # Minimum cached connections
    "maxcached": 20,          # Maximum cached connections
    "blocking": True,         # Block when pool exhausted
    "ping": 1,               # Test connections on checkout
    "reset": True            # Reset connections on return
}

pool = Pool.from_config(
    db_config, 
    PoolBackend.DBUTILS,
    dbutils_opts=dbutils_opts
)
```

### Pool Client Structure

Pool clients provide a standardized interface across all database types:

```python
with pool.client() as client:
    print(client)
    # Output:
    # {
    #     "conn": <database_connection_object>,
    #     "database_type": "postgres",
    #     "database_name": "mydb", 
    #     "db_lib": "psycopg2"
    # }
    
    # Access the raw connection
    raw_connection = client["conn"]
    
    # Database type for conditional logic
    if client["database_type"] == "mongodb":
        # MongoDB-specific operations
        collection = client["conn"][client["database_name"]]["users"]
        result = collection.find_one({"_id": "user123"})
    else:
        # SQL database operations
        cursor = client["conn"].cursor()
        cursor.execute("SELECT * FROM users WHERE id = %s", ("user123",))
        result = cursor.fetchone()
```

## Production Patterns

### Connection Pool Management

#### Application Lifecycle

```python
from longjrm.config.runtime import get_config, require_db
from longjrm.connection.pool import Pool, PoolBackend

class DatabaseManager:
    def __init__(self):
        self.pools = {}
        
    def initialize(self):
        """Initialize pools for all configured databases"""
        config = get_config()
        
        for db_name in config.databases().keys():
            db_config = require_db(db_name)
            
            # Choose backend based on database type
            if db_config.type in ['mongodb', 'mongodb+srv']:
                backend = PoolBackend.MONGODB
            else:
                backend = PoolBackend.SQLALCHEMY  # or DBUTILS for lighter weight
                
            self.pools[db_name] = Pool.from_config(db_config, backend)
            
    def get_connection(self, db_name: str = None):
        """Get connection client context manager for database"""
        db_name = db_name or get_config().default_db
        return self.pools[db_name].client()
        
    def shutdown(self):
        """Clean shutdown of all pools"""
        for pool in self.pools.values():
            pool.dispose()

# Application startup
db_manager = DatabaseManager()
db_manager.initialize()

# Usage in request handlers
with db_manager.get_connection("primary") as client:
    # Perform database operations
    conn = client["conn"]
    # ... database work

# Application shutdown
db_manager.shutdown()
```

#### Context Manager Pattern

```python
from contextlib import contextmanager

# Usage with pool's built-in context manager
with pool.client() as client:
    conn = client["conn"]
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
    # Connection automatically returned to pool
```

### High-Availability Patterns

#### Connection Recovery

```python
from longjrm.connection.dbconn import JrmConnectionError
import time

@contextmanager
def robust_connection(pool, max_retries=3, delay=1):
    """Get connection with retry logic"""
    for attempt in range(max_retries):
        try:
            with pool.client() as client:
                yield client
                return
        except JrmConnectionError as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay * (2 ** attempt))  # Exponential backoff
    
# Usage
try:
    with robust_connection(pool) as client:
        # ... use connection
        pass
except JrmConnectionError:
    # Handle final failure
    pass
```

#### Multi-Database Failover

```python
class FailoverDatabaseManager:
    def __init__(self, primary_pool, replica_pools):
        self.primary = primary_pool
        self.replicas = replica_pools
        
    def get_read_connection(self):
        """Get connection context manager preferring replicas for read operations"""
        # Try replicas first for read load balancing
        for replica in self.replicas:
            try:
                return replica.client()
            except JrmConnectionError:
                continue
        
        # Fall back to primary
        return self.primary.client()
    
    def get_write_connection(self):
        """Get connection context manager to primary for write operations"""
        return self.primary.client()
```

## Database-Specific Considerations

### PostgreSQL

#### Connection Configuration

```python
# Optimal PostgreSQL configuration
db_config = DatabaseConfig(
    type="postgres",
    dsn="postgres://user:pass@host:5432/db",
    options={
        "sslmode": "require",
        "connect_timeout": "10",
        "statement_timeout": "30000",  # 30 seconds
        "application_name": "longjrm-app"
    }
)

# Pool configuration for PostgreSQL
pg_pool_opts = {
    "pool_size": 20,
    "max_overflow": 30,
    "pool_timeout": 30,
    "pool_recycle": 3600,  # Recycle connections every hour
    "pool_pre_ping": True
}
```

### MySQL

#### Connection Configuration

```python
# Optimal MySQL configuration
db_config = DatabaseConfig(
    type="mysql",
    host="mysql.example.com",
    port=3306,
    user="app_user",
    password="password",
    database="production",
    options={
        "charset": "utf8mb4",
        "autocommit": True,
        "sql_mode": "STRICT_TRANS_TABLES",
        "connect_timeout": 10,
        "read_timeout": 30,
        "write_timeout": 30
    }
)
```

### MongoDB

#### Connection Configuration

```python
# MongoDB connection with replica set
db_config = DatabaseConfig(
    type="mongodb",
    dsn="mongodb://user:pass@host1:27017,host2:27017,host3:27017/database?replicaSet=rs0&readPreference=secondaryPreferred"
)

# MongoDB Atlas
db_config = DatabaseConfig(
    type="mongodb+srv",
    dsn="mongodb+srv://user:pass@cluster.mongodb.net/database?retryWrites=true&w=majority"
)
```

#### MongoDB-Specific Usage

```python
# MongoDB connection usage
pool = Pool.from_config(db_config, PoolBackend.MONGODB)

with pool.client() as client:
    mongo_client = client["conn"]
    database = mongo_client[client["database_name"]]
    
    # Collection operations
    users = database["users"]
    
    # Insert document
    result = users.insert_one({"name": "John", "email": "john@example.com"})
    
    # Find documents
    user = users.find_one({"_id": result.inserted_id})
    
    # Aggregation pipeline
    pipeline = [
        {"$match": {"status": "active"}},
        {"$group": {"_id": "$department", "count": {"$sum": 1}}}
    ]
    results = list(users.aggregate(pipeline))
```

## Error Handling

### Connection Errors

```python
from longjrm.connection.dbconn import JrmConnectionError

try:
    with pool.client() as client:
        # ... database operations
        pass
except JrmConnectionError as e:
    logger.error(f"Database connection failed: {e}")
    # Handle connection failure
    # - Retry with backoff
    # - Use fallback data source
    # - Return cached data
    # - Fail gracefully
```

## Testing

### Unit Testing with Pools

```python
import unittest
from longjrm.config.config import DatabaseConfig
from longjrm.connection.pool import Pool, PoolBackend

class TestDatabaseConnection(unittest.TestCase):
    def setUp(self):
        self.db_config = DatabaseConfig(
            type="sqlite",
            database=":memory:"  # In-memory SQLite for testing
        )
        self.pool = Pool.from_config(self.db_config, PoolBackend.SQLALCHEMY)
        
    def tearDown(self):
        self.pool.dispose()
        
    def test_connection_pool(self):
        """Test basic pool functionality"""
        with self.pool.client() as client:
            self.assertIsNotNone(client["conn"])
            self.assertEqual(client["database_type"], "sqlite")
            
            # Test connection works
            cursor = client["conn"].cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)
```

### Integration Testing

```python
def test_real_database_connection():
    """Integration test with real database"""
    # Load test configuration
    config = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    # Configure runtime
    from longjrm.config.runtime import configure
    configure(config)
    
    # Test each configured database
    for db_name in config.databases().keys():
        db_config = require_db(db_name)
        
        if db_config.type in ['mongodb', 'mongodb+srv']:
            backend = PoolBackend.MONGODB
        else:
            backend = PoolBackend.SQLALCHEMY
            
        pool = Pool.from_config(db_config, backend)
        
        try:
            with pool.client() as client:
                if db_config.type in ['mongodb', 'mongodb+srv']:
                    # Test MongoDB connection
                    mongo_client = client["conn"]
                    result = mongo_client.admin.command('ping')
                    assert result['ok'] == 1
                else:
                    # Test SQL connection
                    cursor = client["conn"].cursor()
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    assert result[0] == 1
                
        finally:
            pool.dispose()
```

## Troubleshooting

### Common Issues

1. **"No module named" errors**:
   - Install required database drivers: `pip install psycopg2-binary pymysql pymongo`
   - Check driver mapping in `driver_map.json`

2. **Connection timeout errors**:
   - Increase `connect_timeout` in configuration
   - Check network connectivity and firewall rules
   - Verify database server is accepting connections

3. **Pool exhaustion**:
   - Increase `max_pool_size` configuration
   - Check for connection leaks (not returning connections to pool)
   - Monitor application connection usage patterns

4. **SSL/TLS errors**:
   - Configure proper SSL options in database configuration
   - Verify SSL certificates and CA bundles
   - Check database server SSL configuration

### Debugging Tools

```python
import logging

# Enable detailed connection logging
logging.getLogger('longjrm.connection').setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

# Monitor pool statistics (SQLAlchemy)
def print_pool_status(pool):
    if hasattr(pool._b, '_engine'):
        engine = pool._b._engine
        pool_obj = engine.pool
        print(f"Pool size: {pool_obj.size()}")
        print(f"Checked out: {pool_obj.checkedout()}")
        print(f"Overflow: {pool_obj.overflow()}")
        print(f"Invalid: {pool_obj.invalid()}")
```

## API Reference

### Core Classes

#### DatabaseConnection

```python
class DatabaseConnection:
    def __init__(self, db_cfg: DatabaseConfig)
    def connect() -> Any  # Returns db-specific connection object
    def set_autocommit(self, autocommit: bool) -> None
    def set_isolation_level(self, isolation_level: str) -> None
    def close() -> None
```

#### Pool

```python
class Pool:
    @classmethod
    def from_config(
        cls,
        db_cfg: DatabaseConfig,
        pool_backend: PoolBackend,
        sa_opts: Optional[Mapping[str, Any]] = None,
        dbutils_opts: Optional[Mapping[str, Any]] = None,
    ) -> "Pool"

    def client() -> Iterator[dict[str, Any]]  # Context manager
    def transaction(isolation_level: Optional[str] = None) -> Iterator[TransactionContext]  # Transaction context manager
    def execute_transaction(operations_func: Callable, isolation_level: Optional[str] = None) -> Any
    def execute_batch(operations: List[Dict[str, Any]], isolation_level: Optional[str] = None) -> List[Any]
    def dispose() -> None

class TransactionContext:
    def __init__(self, client: Dict[str, Any])
    def commit() -> None
    def rollback() -> None
    
    @property
    def config() -> DatabaseConfig
```

## Transaction Management

### Basic Transaction Usage

```python
# Simple transaction with auto-commit
with pool.transaction() as tx:
    db = Db(tx.client)
    db.insert("users", {"name": "John", "email": "john@example.com"})
    db.update("profiles", {"active": True}, {"user_id": 123})
    # Auto-commit on success, auto-rollback on exception

# Transaction with isolation level
with pool.transaction(isolation_level="READ COMMITTED") as tx:
    db = Db(tx.client)
    db.insert("orders", order_data)
    db.update("inventory", {"quantity": new_qty}, {"product_id": pid})

# Manual transaction control
with pool.transaction() as tx:
    db = Db(tx.client)
    try:
        db.insert("users", user_data)
        validate_user_data(user_data)
        tx.commit()  # Manual commit
    except ValidationError:
        tx.rollback()  # Manual rollback
        raise
```

### Function-Based Transactions

```python
# Execute function in transaction
def create_user_with_profile(db):
    user_id = db.insert("users", {"name": "John"})
    db.insert("profiles", {"user_id": user_id, "bio": "Developer"})
    return user_id

result = pool.execute_transaction(create_user_with_profile)

# Batch operations
operations = [
    {"method": "insert", "params": {"table": "users", "data": {"name": "John"}}},
    {"method": "update", "params": {"table": "profiles", "data": {"active": True}, "where": {"user_id": 123}}}
]
results = pool.execute_batch(operations)
```

### Isolation Levels

Supported isolation levels by database:

**PostgreSQL:**
- `READ UNCOMMITTED`
- `READ COMMITTED` (default)
- `REPEATABLE READ`
- `SERIALIZABLE`

**MySQL:**
- `READ UNCOMMITTED`
- `READ COMMITTED`
- `REPEATABLE READ` (default)
- `SERIALIZABLE`

**SQLite:**
- Limited isolation level support (warning logged)

```python
# Set isolation level for specific transaction
with pool.transaction(isolation_level="SERIALIZABLE") as tx:
    db = Db(tx.client)
    # Critical operations requiring highest isolation
    db.transfer_funds(from_account, to_account, amount)
```

## Connection Lifecycle Management

### Autocommit Management

The pool automatically manages autocommit settings:

- **Default State**: All connections have `autocommit=True` by default
- **Transaction Mode**: `pool.transaction()` sets `autocommit=False` for grouped operations
- **Cleanup**: Connections returned to pool always have `autocommit=True` restored

### Backend-Specific Behavior

**DBUtils Backend:**
- Uses custom reset function on connection check-in
- Calls `conn.set_autocommit(True)` when returning to pool
- Leverages DBUtils' built-in rollback functionality

**SQLAlchemy Backend:**
- Uses checkout event to normalize connection state
- Enforces `autocommit=True` when connections leave pool
- Integrates with SQLAlchemy's `pool_reset_on_return="rollback"`

### Connection State Consistency

```python
# All these patterns ensure consistent connection state:

# Simple usage - autocommit=True by default
with pool.client() as client:
    db = Db(client)
    db.insert("logs", {"message": "test"})  # Auto-committed

# Transaction usage - autocommit=False during transaction
with pool.transaction() as tx:
    db = Db(tx.client)
    db.insert("users", data1)     # Part of transaction
    db.insert("profiles", data2)  # Part of transaction
    # Auto-commit at end, autocommit=True restored

# Connection returned to pool always has autocommit=True
```

#### PoolBackend

```python
class PoolBackend(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    DBUTILS = "dbutils"
    MONGODB = "mongodb"
```

### Exceptions

- `JrmConnectionError`: Database connection failures and errors

### Driver Registry Functions

- `load_driver_map()`: Load database driver mappings
- `sa_minimal_url()`: Generate minimal SQLAlchemy URLs for connection creation