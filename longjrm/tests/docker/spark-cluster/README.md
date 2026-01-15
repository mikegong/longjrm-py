# Spark Cluster Test Environment

This directory contains a Docker Compose configuration for testing longjrm's Spark SQL and Delta Lake operations.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     spark-network                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │spark-master │  │spark-worker │  │    spark-client     │  │
│  │  :7077      │◄─│      -1     │  │  (Test Runner)      │  │
│  │  :8080 (UI) │  │  :8081 (UI) │  │  Python 3.10        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

| Container | Image | Python | Purpose |
|-----------|-------|--------|---------|
| `spark-master` | `apache/spark:3.5.1` | 3.8 | Spark Master node |
| `spark-worker-1` | `apache/spark:3.5.1` | 3.8 | Spark Worker node |
| `spark-client` | Custom Dockerfile | 3.10 | Test execution client with longjrm |

> **Note:** The client uses Python 3.10 for modern language features. All Spark operations use pure SQL to avoid serialization issues between Python versions.

---

## Quick Start

### 1. Start the Cluster

```powershell
cd longjrm/tests/docker/spark-cluster

# Start all containers (builds client on first run)
docker-compose up -d --build

# Verify all 3 containers are running
docker ps --filter "name=spark"
```

### 2. Run Tests

```powershell
# Execute the Spark test suite
docker exec spark-client python /workspace/longjrm-py/longjrm/tests/spark_test.py
```

### 3. Clean Up

```powershell
# Stop the cluster
docker-compose down

# Full cleanup (including volumes)
docker-compose down -v
```

---

## Test Suite Overview

The `spark_test.py` executes comprehensive tests for Spark SQL and Delta Lake operations:

### Test Categories

| Test Function | Description | Operations Tested |
|---------------|-------------|-------------------|
| `test_insert` | Basic Insert Operations | Single insert, bulk insert (batched) |
| `test_select` | Basic Select Operations | WHERE conditions, ORDER BY, LIMIT |
| `test_advanced_select` | Analytics Queries | JOIN, LEFT JOIN, GROUP BY, CTE, Window Functions, SQL Functions |
| `test_merge` | Delta Lake MERGE (Upsert) | Insert new rows, update existing, `no_update` mode |
| `test_bulk_load` | Bulk Data Loading | Cursor/query-based load, auto-detect source type |
| `test_update` | Update Operations | Single update, conditional bulk update |
| `test_delete` | Delete Operations | Conditional delete, delete all |

### Sample Test Output

```
--- Setup: Creating Delta tables ---
Delta Enabled: True
✓ Tables created

=== Test: Insert ===
✓ Single insert users successful
✓ Bulk insert users successful
✓ Bulk insert departments successful
✓ Verification: 4 users, 3 departments

=== Test: Advanced Select (Analytics) ===
✓ INNER JOIN working
✓ LEFT JOIN working
✓ Aggregation (GROUP BY) working
✓ SQL Functions working
✓ CTE (WITH clause) working
✓ Window Functions working

=== Test: Merge (Delta Lake UPSERT) ===
✓ Merge (as INSERT) succeeded
✓ Merge (UPDATE existing) verified
✓ Merge with no_update='Y' verified

=== Test: Bulk Load ===
✓ Cursor bulk load succeeded: 50 rows loaded
✓ Auto-detect cursor bulk load: 30 rows loaded

========================================
ALL SPARK TESTS PASSED ✓
========================================
```

---

## Configuration

### Spark Configuration in `dbinfos.json`

The test uses configurations defined in `test_config/dbinfos.json`:

```json
{
  "spark-test": {
    "type": "spark",
    "spark_config": {
      "spark.master": "local[*]",
      "spark.app.name": "longjrm-spark-test"
    },
    "options": {
      "enable_delta": true
    }
  }
}
```

### Key Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `spark.master` | Spark master URL (`local[*]` or `spark://host:7077`) | `local[*]` |
| `spark.app.name` | Application name shown in Spark UI | Required |
| `enable_delta` | Enable Delta Lake support for MERGE/UPDATE/DELETE | `false` |

---

## Cluster Management

### Starting/Stopping

```powershell
# Start cluster in background
docker-compose up -d

# Stop cluster (preserves data)
docker-compose down

# Stop and remove all data
docker-compose down -v
```

### Viewing Logs

```powershell
# View all logs
docker-compose logs -f

# View specific container logs
docker logs spark-master
docker logs spark-worker-1
docker logs spark-client
```

### Rebuilding Client

After changing longjrm code or `client.Dockerfile`:

```powershell
docker-compose up -d --build spark-client
```

---

## Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| Spark Master | http://localhost:8080 | Cluster overview, worker status |
| Spark Worker | http://localhost:8081 | Worker details, executor logs |

---

## Interactive Testing

### Open a Shell in the Client Container

```powershell
docker exec -it spark-client bash
```

### Run Python Interactively

```python
# Inside the container
python3

>>> from longjrm.config.config import JrmConfig
>>> from longjrm.connection.pool import Pool, PoolBackend
>>> from longjrm.database import get_db

>>> cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
>>> pool = Pool.from_config(cfg.require("spark-test"), PoolBackend.DBUTILS)

>>> with pool.client() as client:
...     db = get_db(client)
...     result = db.query("SELECT 1 + 1 as answer")
...     print(result['data'])
[{'answer': 2}]
```

---

## Troubleshooting

### "Table already exists" Error

Clean the Spark warehouse before running tests:

```powershell
docker exec spark-client rm -rf /workspace/longjrm-py/spark-warehouse
```

### Network Issues

Ensure all containers are on the same network:

```powershell
docker network inspect spark-cluster_spark-network
```

### Python Version Check

```powershell
docker exec spark-client python3 --version    # Should be 3.10+
docker exec spark-worker-1 python3 --version  # Should be 3.8
```

### Delta Lake Not Working

Ensure `enable_delta: true` is set in your configuration and the Delta Spark package is installed:

```powershell
docker exec spark-client pip list | grep delta
# Should show: delta-spark 3.x.x
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection refused spark-master:7077` | Cluster not started | Run `docker-compose up -d` |
| `DeltaTable not found` | Delta not enabled | Add `enable_delta: true` to config |
| `Table already exists` | Stale warehouse data | Remove `spark-warehouse/` directory |
| `MERGE requires Delta Lake` | Using non-Delta table | Create table with `USING DELTA` |

---

## Files

| File | Description |
|------|-------------|
| `docker-compose.yml` | Container orchestration config |
| `client.Dockerfile` | Custom client image with Python 3.10 + dependencies |
| `README.md` | This documentation |
