# LongJRM Configuration Guide

## Overview

LongJRM follows **12-Factor App** principles for configuration management, providing a flexible and production-ready system for managing database connections and application settings. The configuration system supports multiple databases, environment-based configuration, and follows cloud-native deployment patterns.

## 12-Factor Configuration Principles

This library strictly adheres to [12-Factor App methodology](https://12factor.net), specifically:

- **Config in Environment (Factor III)**: Configuration is read from OS environment variables, never hardcoded
- **No I/O at Import Time**: All configuration loading is lazy via `get_config()`
- **Deterministic Precedence**: Clear, documented configuration source precedence
- **Stateless & Concurrency Safe**: Uses `ContextVar` for per-request defaults
- **Disposability**: Fast startup/shutdown with `reload_default()` for cache clearing
- **Dev/Prod Parity**: Same code paths across all environments
- **Backing Services as Attached Resources**: Databases selected/parameterized at runtime

## Configuration Sources & Precedence

### Default Precedence (`JRM_SOURCE=auto`)

1. **Explicit File Paths**: If `JRM_CONFIG_PATH` or `JRM_DBINFOS_PATH` environment variables are set
2. **Environment Variables**: If any `JRM_*` database variables exist
3. **Conventional Files**: If `./config/{jrm.config.json, dbinfos.json}` exist in current directory
4. **Fallback**: Environment-only mode (raises error if nothing configured)

### Forced Precedence

Set `JRM_SOURCE` to override default behavior:

- `JRM_SOURCE=files`: Always use file-based configuration (paths must exist)
- `JRM_SOURCE=env`: Always use environment variables, ignore files
- `JRM_SOURCE=auto`: Use default precedence (recommended)

## Configuration Methods

### 1. File-Based Configuration

#### Main Configuration File (`jrm.config.json`)

```json
{
  "default_db": "postgres-prod",
  "jrm_connect_timeout": 40,
  "jrm_data_fetch_limit": 1000,
  "jrm_min_conn_pool_size": 1,
  "jrm_max_conn_pool_size": 10,
  "jrm_max_cached_conn": 5,
  "jrm_pool_timeout": 60,
  "databases": {
    "postgres-prod": {
      "type": "postgres",
      "host": "db.example.com",
      "port": 5432,
      "user": "app_user",
      "password": "${DB_PASSWORD}",
      "database": "production",
      "options": {
        "sslmode": "require",
        "connect_timeout": "10"
      }
    }
  }
}
```

#### Database Info File (`dbinfos.json`)

```json
{
  "mysql-primary": {
    "type": "mysql",
    "host": "mysql.example.com",
    "port": 3306,
    "user": "${MYSQL_USER}",
    "password": "${MYSQL_PASSWORD}",
    "database": "app_data"
  },
  "postgres-analytics": {
    "type": "postgres", 
    "dsn": "postgres://${PG_USER}:${PG_PASS}@analytics.db:5432/warehouse"
  },
  "mongodb-cache": {
    "type": "mongodb+srv",
    "host": "cluster0.mongodb.net",
    "user": "${MONGO_USER}",
    "password": "${MONGO_PASS}",
    "database": "cache_db"
  }
}
```

**Environment Variable Expansion**: Use `${VAR_NAME}` syntax in JSON files to expand environment variables. Missing variables become empty strings.

### 2. Environment-Based Configuration

#### Multiple Databases via JSON

```bash
export JRM_DATABASES_JSON='{
  "primary": {
    "type": "postgres",
    "dsn": "postgres://user:pass@localhost:5432/main"
  },
  "cache": {
    "type": "redis", 
    "host": "redis.example.com",
    "port": 6379
  }
}'
```

#### Single Database via Flat Variables

```bash
# Database identification
export JRM_DB_KEY="production-db"
export JRM_DB_DEFAULT="production-db"

# Connection via DSN (preferred)
export JRM_DB_DSN="postgres://user:${DB_PASS}@db.prod.com:5432/app"

# OR connection via individual parts
export JRM_DB_TYPE="postgres"
export JRM_DB_HOST="db.prod.com"
export JRM_DB_PORT="5432"
export JRM_DB_USER="app_user"
export JRM_DB_PASSWORD="${DB_PASS}"
export JRM_DB_NAME="app_database"
export JRM_DB_OPTIONS='{"sslmode": "require"}'

# Connection tuning
export JRM_CONNECT_TIMEOUT="30"
export JRM_DATA_FETCH_LIMIT="2000"
export JRM_MIN_CONN_POOL_SIZE="2"
export JRM_MAX_CONN_POOL_SIZE="20"
export JRM_MAX_CACHED_CONN="10"
export JRM_POOL_TIMEOUT="120"
```

#### File Path Environment Variables

```bash
export JRM_CONFIG_PATH="/app/config/production.json"
export JRM_DBINFOS_PATH="/app/config/databases.json"
```

### 3. Programmatic Configuration

```python
from longjrm.config import JrmConfig, DatabaseConfig
from longjrm.config.runtime import configure, using_config

# Direct configuration
config = JrmConfig(
    _databases={
        "main": DatabaseConfig(
            type="postgres",
            dsn="postgres://user:pass@localhost:5432/app"
        )
    },
    default_db="main",
    max_pool_size=15
)

# Set globally for current context
configure(config)

# Or use temporarily with context manager
with using_config(config):
    # Configuration active only within this block
    db_config = require_db("main")
```

## Database Configuration

### DSN (Data Source Name) Format

**Preferred Method**: DSN provides a standardized connection string format.

#### PostgreSQL
```
postgres://username:password@hostname:5432/database_name?sslmode=require
postgresql+psycopg://user:pass@host:5432/db?connect_timeout=10
```

#### MySQL
```
mysql://user:password@hostname:3306/database?charset=utf8mb4
mysql+pymysql://user:pass@host/db?autocommit=true
```

#### MongoDB
```
mongodb://user:pass@host:27017/database
mongodb+srv://user:pass@cluster.mongodb.net/database
mongodb://host1:27017,host2:27017,host3:27017/database?replicaSet=rs0
```

#### SQLite
```
sqlite:///absolute/path/to/database.db
sqlite://relative/path/database.db
file:./local.db
```

### Individual Parts Configuration

When DSN is not used, specify connection details individually:

```json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5432,
  "user": "app_user", 
  "password": "secret",
  "database": "app_db",
  "options": {
    "sslmode": "require",
    "application_name": "longjrm-app"
  }
}
```

### Supported Database Types

- **MySQL**: `mysql`, `mysql+pymysql`
- **PostgreSQL**: `postgres`, `postgresql`, `postgresql+psycopg2`
- **MongoDB**: `mongodb`, `mongodb+srv`
- **SQLite**: `sqlite`

Additional drivers can be specified with `+driver` syntax (e.g., `mysql+pymysql`).

## Connection Pool Configuration

Configure connection pooling for optimal performance:

```json
{
  "jrm_connect_timeout": 40,          // Connection timeout (seconds)
  "jrm_data_fetch_limit": 1000,      // Max rows per query
  "jrm_min_conn_pool_size": 1,       // Minimum pool connections
  "jrm_max_conn_pool_size": 10,      // Maximum pool connections  
  "jrm_max_cached_conn": 5,          // Max cached idle connections
  "jrm_pool_timeout": 60             // Pool acquisition timeout (seconds)
}
```

## Runtime Usage

### Basic Usage

```python
from longjrm.config.runtime import get_config, require_db

# Get active configuration
config = get_config()

# Get specific database configuration
db_config = require_db("postgres-primary")
db_config = require_db()  # Uses default database

# Access configuration properties
print(f"Database type: {db_config.type}")
print(f"Host: {db_config.host}")
print(f"Pool size: {config.max_pool_size}")
```

### Context Management

```python
from longjrm.config.runtime import using_db, using_config

# Temporarily use specific database
with using_db("analytics-db"):
    db_config = require_db()  # Returns "analytics-db" config

# Temporarily use different configuration
test_config = JrmConfig(_databases={"test": test_db_config})
with using_config(test_config):
    # All database operations use test_config
    pass
```

### Dynamic Configuration

```python
from longjrm.config.runtime import use_db, reload_default

# Set default database for current context/request
use_db("user-specific-db")

# Reload configuration after environment changes
import os
os.environ["JRM_DB_HOST"] = "new-host.com"
reload_default()  # Clears cache, re-reads environment
```

## Production Deployment Examples

### Docker Compose

```yaml
version: '3.8'
services:
  app:
    image: myapp:latest
    environment:
      JRM_SOURCE: env
      JRM_DB_KEY: production
      JRM_DB_DSN: postgres://user:${DB_PASSWORD}@postgres:5432/app
      JRM_MAX_CONN_POOL_SIZE: 20
      JRM_POOL_TIMEOUT: 120
    depends_on:
      - postgres
      
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: app
      POSTGRES_USER: user
      POSTGRES_PASSWORD: ${DB_PASSWORD}
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jrm-config
data:
  jrm.config.json: |
    {
      "default_db": "postgres-primary",
      "max_conn_pool_size": 15,
      "databases": {
        "postgres-primary": {
          "type": "postgres",
          "host": "postgres-service.default.svc.cluster.local",
          "port": 5432,
          "user": "${DB_USER}",
          "password": "${DB_PASSWORD}",
          "database": "app_production"
        }
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
spec:
  template:
    spec:
      containers:
      - name: app
        env:
        - name: JRM_CONFIG_PATH
          value: "/config/jrm.config.json"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: password
        volumeMounts:
        - name: config
          mountPath: /config
      volumes:
      - name: config
        configMap:
          name: jrm-config
```

### AWS ECS with Parameter Store

```python
# Application startup code
import boto3
import os
from longjrm.config.runtime import configure
from longjrm.config import JrmConfig, DatabaseConfig

def load_aws_config():
    ssm = boto3.client('ssm')
    
    # Get database credentials from Parameter Store
    db_password = ssm.get_parameter(
        Name='/app/db/password', 
        WithDecryption=True
    )['Parameter']['Value']
    
    config = JrmConfig(
        _databases={
            "primary": DatabaseConfig(
                type="postgres",
                dsn=f"postgres://app_user:{db_password}@rds.amazonaws.com:5432/prod"
            )
        },
        default_db="primary"
    )
    
    configure(config)

if __name__ == "__main__":
    load_aws_config()
    # Start application
```

## Error Handling

```python
from longjrm.config import JrmConfigurationError
from longjrm.config.runtime import get_config, require_db

try:
    config = get_config()
    db_config = require_db("nonexistent-db")
except JrmConfigurationError as e:
    print(f"Configuration error: {e}")
    # Handle missing configuration appropriately
```

## Security Best Practices

1. **Never commit secrets**: Use environment variables or secure secret management
2. **Use DSN format**: Encapsulates credentials in a single variable
3. **Least privilege**: Database users should have minimal required permissions
4. **Connection encryption**: Always use SSL/TLS in production (`sslmode=require`)
5. **Secret rotation**: Design for credential rotation without application restart
6. **Environment isolation**: Separate configurations for dev/staging/production

## Migration from Legacy Systems

### From environment files (.env)

**Before** (using python-dotenv):
```python
from dotenv import load_dotenv
load_dotenv()
```

**After** (12-factor compliant):
```python
# No dotenv loading in application code
# Set environment variables via deployment system
from longjrm.config.runtime import get_config
config = get_config()  # Reads from environment automatically
```

### From hardcoded configurations

**Before**:
```python
DATABASE_CONFIG = {
    "host": "localhost",
    "user": "hardcoded_user",
    "password": "hardcoded_password"
}
```

**After**:
```python
from longjrm.config.runtime import require_db
db_config = require_db("primary")  # Configured via environment
```

## Troubleshooting

### Common Issues

1. **"No configuration found" error**:
   - Ensure `JRM_*` environment variables are set, OR
   - Provide `JRM_CONFIG_PATH`/`JRM_DBINFOS_PATH`, OR  
   - Create `./config/jrm.config.json` or `./config/dbinfos.json`

2. **"Unknown database key" error**:
   - Check database name matches configuration
   - Verify `default_db` is set if not specifying database name

3. **DSN parsing errors**:
   - Validate DSN format with database documentation
   - Use URL encoding for special characters in passwords

4. **Environment variable expansion not working**:
   - Ensure variables are set before application starts
   - Use `${VAR_NAME}` syntax, not `$VAR_NAME`

### Debug Configuration

```python
from longjrm.config.runtime import get_config

config = get_config()
print(f"Default database: {config.default_db}")
print(f"Available databases: {list(config.databases().keys())}")

for name, db_config in config.databases().items():
    print(f"{name}: {db_config.type} @ {db_config.host}")
```

## API Reference

### Core Classes

- `JrmConfig`: Main configuration container
- `DatabaseConfig`: Individual database configuration
- `JrmConfigurationError`: Configuration-related exceptions

### Runtime Functions

- `get_config()`: Get active configuration
- `require_db(name=None)`: Get database configuration by name
- `configure(config)`: Set configuration override
- `use_db(key)`: Set default database for context
- `reload_default()`: Clear configuration cache
- `using_config(config)`: Temporary configuration context manager
- `using_db(key)`: Temporary database context manager

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `JRM_SOURCE` | Force configuration source | `env`, `files`, `auto` |
| `JRM_CONFIG_PATH` | Main configuration file path | `/app/config.json` |
| `JRM_DBINFOS_PATH` | Database info file path | `/app/databases.json` |
| `JRM_DATABASES_JSON` | Inline JSON database config | `{"db1": {...}}` |
| `JRM_DB_KEY` | Single database name | `primary` |
| `JRM_DB_DSN` | Single database DSN | `postgres://...` |
| `JRM_DB_TYPE` | Database type | `postgres`, `mysql` |
| `JRM_DB_HOST` | Database hostname | `localhost` |
| `JRM_DB_PORT` | Database port | `5432` |
| `JRM_DB_USER` | Database username | `app_user` |
| `JRM_DB_PASSWORD` | Database password | `secret123` |
| `JRM_DB_NAME` | Database name | `production` |
| `JRM_DB_OPTIONS` | Database options (JSON) | `{"sslmode": "require"}` |
| `JRM_DB_DEFAULT` | Default database key | `primary` |

### Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_db` | `str` | `None` | Default database key |
| `jrm_connect_timeout` | `int` | `40` | Connection timeout (seconds) |
| `jrm_data_fetch_limit` | `int` | `1000` | Maximum rows per query |
| `jrm_min_pool_size` | `int` | `1` | Minimum connection pool size |
| `jrm_max_pool_size` | `int` | `10` | Maximum connection pool size |
| `jrm_max_cached_conn` | `int` | `5` | Maximum cached connections |
| `jrm_pool_timeout` | `int` | `60` | Pool acquisition timeout (seconds) |