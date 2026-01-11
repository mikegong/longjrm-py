# Spark SQL Integration Guide

LongJRM provides comprehensive support for Apache Spark SQL via the `SparkDb` class, enabling you to use the same familiar API for distributed data processing.

## Overview

The Spark integration supports:
- **Standard SQL queries** via Spark SQL
- **Delta Lake** for MERGE, UPDATE, DELETE operations (ACID transactions)
- **Memory-efficient streaming** via DataFrame.toLocalIterator()
- **Bulk loading** from files (CSV, Parquet, JSON, ORC) and queries

## Key Differences from SQL Databases

| Feature | SQL Databases | Spark SQL |
|---------|--------------|-----------|
| Transactions | Full ACID (commit/rollback) | No-op (use Delta Lake for ACID) |
| Autocommit | Configurable | Always "on" (each statement executes immediately) |
| UPSERT | `INSERT ON CONFLICT` | Requires Delta Lake `MERGE INTO` |
| UPDATE/DELETE | Native SQL support | Requires Delta Lake tables |
| Connection Pooling | DBUtils/SQLAlchemy | Singleton SparkSession |
| Parameterized Queries | Native support | Spark 3.4+ only |

## Configuration

### Basic Configuration

```python
from longjrm.config.config import DatabaseConfig

# Local Spark (development)
config = DatabaseConfig(
    type="spark",
    database="default",
    options={
        "master": "local[*]",
        "app_name": "my-jrm-app"
    }
)

# Remote Spark Cluster
config = DatabaseConfig(
    type="spark",
    database="default",
    options={
        "master": "spark://spark-master:7077",
        "app_name": "production-app",
        "enable_delta": True,
        "spark_config": {
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2"
        }
    }
)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `master` | str | `local[*]` | Spark master URL |
| `app_name` | str | `longjrm-spark` | Spark application name |
| `enable_hive` | bool | `False` | Enable Hive metastore support |
| `enable_delta` | bool | `True` | Enable Delta Lake extensions |
| `spark_config` | dict | `{}` | Additional Spark configurations |
| `spark_session` | SparkSession | `None` | Use existing SparkSession |

### Using Existing SparkSession

```python
from pyspark.sql import SparkSession

# Your existing Spark session
spark = SparkSession.builder.appName("existing").getOrCreate()

# Use with LongJRM
config = DatabaseConfig(
    type="spark",
    options={"spark_session": spark}
)
```

## Usage

### Basic Operations

```python
from longjrm.database import get_db
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.config.config import DatabaseConfig

# Create pool
config = DatabaseConfig(type="spark", options={"master": "local[*]"})
pool = Pool.from_config(config, PoolBackend.DBUTILS)

with pool.client() as client:
    db = get_db(client)  # Returns SparkDb instance
    
    # Create Delta table
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT,
            name STRING,
            email STRING
        ) USING DELTA
    """)
    
    # Insert
    db.insert("users", {"id": 1, "name": "Alice", "email": "alice@example.com"})
    
    # Select
    result = db.select("users", ["id", "name"], {"name": "Alice"})
    print(result["data"])
    
    # Update (requires Delta Lake)
    db.update("users", {"email": "alice.new@example.com"}, {"id": 1})
    
    # Delete (requires Delta Lake)
    db.delete("users", {"id": 1})

pool.dispose()
```

### MERGE (Upsert) Operations

MERGE requires Delta Lake tables:

```python
# Single row merge
db.merge(
    table="users",
    data={"id": 1, "name": "Alice", "email": "alice@example.com"},
    key_columns=["id"]
)

# Bulk merge
data = [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
db.merge(table="users", data=data, key_columns=["id"])

# Merge with specific update columns
db.merge(
    table="users",
    data={"id": 1, "name": "Alice Updated", "email": "unchanged"},
    key_columns=["id"],
    update_columns=["name"]  # Only update 'name', not 'email'
)

# Insert-only merge (no update on match)
db.merge(
    table="users",
    data={"id": 1, "name": "Alice"},
    key_columns=["id"],
    no_update="Y"
)
```

### Streaming Queries

Memory-efficient processing for large datasets:

```python
# Stream query results row by row
for row_num, row_data, status in db.stream_query("SELECT * FROM large_table"):
    if status == 0:
        process_row(row_data)
    else:
        handle_error(row_num)

# Stream in batches
for total, batch, status in db.stream_query_batch(
    "SELECT * FROM large_table",
    batch_size=10000
):
    if status == 0:
        process_batch(batch)
```

### Bulk Loading

```python
# Load from CSV file
db.bulk_load("target_table", {
    "source": "/path/to/data.csv",
    "source_type": "file",
    "format": "csv",
    "header": True,
    "delimiter": ",",
    "mode": "append"
})

# Load from Parquet file
db.bulk_load("target_table", {
    "source": "/path/to/data.parquet",
    "format": "parquet",
    "mode": "overwrite"
})

# Load from query (INSERT INTO ... SELECT)
db.bulk_load("target_table", {
    "source": "SELECT * FROM source_table WHERE status = 'active'",
    "source_type": "cursor"
})

# Load with column mapping
db.bulk_load("target_table(name, email)", {
    "source": "/path/to/data.csv",
    "format": "csv",
    "columns": ["name", "email"]
})
```

### Export to CSV

```python
# Stream results to CSV file
result = db.stream_to_csv(
    sql="SELECT * FROM users WHERE status = 'active'",
    csv_file="/output/users.csv",
    options={
        "header": "Y",
        "null_value": "",
        "batch_size": 5000
    }
)

if result["status"] == 0:
    print(f"Exported {result['row_count']} rows")
```

## Delta Lake Integration

### Creating Delta Tables

```python
# Create Delta table
db.execute("""
    CREATE TABLE sales (
        id BIGINT,
        product STRING,
        amount DECIMAL(10,2),
        sale_date DATE
    ) USING DELTA
    PARTITIONED BY (sale_date)
""")

# Create from existing data
db.execute("""
    CREATE TABLE sales_copy
    USING DELTA
    AS SELECT * FROM sales
""")
```

### Time Travel

```python
# Query historical version
result = db.query("SELECT * FROM sales VERSION AS OF 5")

# Query by timestamp
result = db.query(
    "SELECT * FROM sales TIMESTAMP AS OF '2024-01-15 10:00:00'"
)

# Restore to previous version
db.execute("RESTORE TABLE sales TO VERSION AS OF 10")
```

### Table Maintenance

```python
# Optimize table (compaction)
db.execute("OPTIMIZE sales")

# Vacuum old files (>7 days)
db.execute("VACUUM sales RETAIN 168 HOURS")

# Describe history
history = db.query("DESCRIBE HISTORY sales")
```

## Parameterized Queries

SparkDb auto-detects Spark version and uses native parameterized queries for Spark 3.4+:

```python
# Spark 3.4+: Uses native :name placeholders
db = SparkDb(client)  # Automatically detects version

# Spark < 3.4: Falls back to inline value formatting
# (safe escaping is applied)

# You can also control this explicitly
db = SparkDb(client, use_parameterized=True)   # Force native
db = SparkDb(client, use_parameterized=False)  # Force inline
```

## Best Practices

### 1. Use Delta Lake for DML Operations

```python
# Always create tables with USING DELTA for UPDATE/DELETE/MERGE
db.execute("CREATE TABLE users (...) USING DELTA")
```

### 2. Partition Large Tables

```python
db.execute("""
    CREATE TABLE events (...)
    USING DELTA
    PARTITIONED BY (event_date)
""")
```

### 3. Use Streaming for Large Results

```python
# DON'T: Load entire result into memory
result = db.query("SELECT * FROM billion_row_table")  # OOM risk!

# DO: Stream results
for row_num, row, status in db.stream_query("SELECT * FROM billion_row_table"):
    process(row)
```

### 4. Leverage Bulk Load for ETL

```python
# DON'T: Insert rows one by one
for row in million_rows:
    db.insert("table", row)  # Very slow!

# DO: Use bulk insert or bulk load
db.insert("table", million_rows)  # Batched automatically

# Or load from file (even faster)
db.bulk_load("table", {"source": "/path/to/data.parquet", "format": "parquet"})
```

## Troubleshooting

### Delta Lake Not Available

```
MERGE requires Delta Lake. Install delta-spark package.
```

**Solution**: Install delta-spark:
```bash
pip install delta-spark>=2.3.0
```

### Parameterized Query Errors

If you see binding errors on older Spark versions:
```python
# Force inline formatting
db = SparkDb(client, use_parameterized=False)
```

### Python Version Mismatch

Ensure your Python version matches the Spark cluster's Python version (both should be 3.10+).

## API Reference

See [API Reference](api-reference.md#sparkdb) for complete SparkDb method signatures.
