# JRM Test Suite

The `longjrm` test suite supports validting functionality across multiple database types including MySQL, PostgreSQL, DB2, Oracle, and SQL Server.

## Dynamic Database Discovery

The test suite relies on `test_config/dbinfos.json` to discover available database configurations.
It uses a shared utility `longjrm.tests.test_utils` to:
1.  Read `dbinfos.json`.
2.  Dynamically generate dialect-specific DDL (Create Table) statements.
3.  Filter execution based on arguments.

## Running Tests

Tests should be run from the repository root.

### 1. Run as Module (Recommended)
This ensures imports resolve correctly without needing to install the package.

```bash
# Run all configured database tests for Select functionality
python -m longjrm.tests.select_test

# Run Connection tests
python -m longjrm.tests.connection_test
```

### 2. Filtering by Database
You can run tests against a specific database type using the `--db` argument or `TEST_DB` environment variable.

```bash
# Run only Oracle tests
python -m longjrm.tests.connection_test --db=oracle

# Run only DB2 tests
python -m longjrm.tests.select_test --db=db2

# Run only SQL Server tests
python -m longjrm.tests.insert_test --db=sqlserver
```

**Note:** The value for `--db` matches the keys in your `dbinfos.json` (partial match supported).

### 3. Running with `pip install -e .`
If you have installed the package in editable mode, you can run detailed scripts directly:

```bash
python longjrm/tests/transaction_test.py --db=mysql
```

## Supported Databases
The suite includes dialect-specific support (DDL, Placeholders, etc.) for:
- PostgreSQL
- MySQL
- DB2
- Oracle
- SQL Server

Ensure you have the necessary drivers installed (`oracledb`, `ibm_db`, `pyodbc`) if testing those databases.

## Troubleshooting

### DB2: DLL load failed (Windows)
If you see `ImportError: DLL load failed while importing ibm_db`, the driver cannot find its DLLs.
**Fix:** Add the `.../site-packages/ibm_db-X.X.X/clidriver/bin` folder to your system `PATH`.
