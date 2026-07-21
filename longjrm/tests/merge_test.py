"""
JRM Merge Function Test Suite

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/merge_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/merge_test.py

Make sure your test_config/dbinfos.json file contains valid database configurations.
"""

import logging
import json
import datetime
import sys
import os

# Add the project root to Python path for development testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db
from longjrm.tests import test_utils

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def test_sql_database(db_key, backend=PoolBackend.DBUTILS):
    """Test merge functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} Merge Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    # Use specified backend
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client:
        db = get_db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Create test table if it doesn't exist
        try:
             # Drop table first for clean state
            test_utils.drop_table_silently(db, "test_merge_users")
            
            create_table_sql = test_utils.get_create_table_sql(db.database_type, "test_merge_users")
            if create_table_sql:
                db.execute(create_table_sql)
                print("SUCCESS: Test merge table created/verified")
        except Exception as e:
            print(f"WARNING: Could not create test table (may already exist or other error): {e}")
        
        # Clean up any existing test data
        try:
            result = db.execute("DELETE FROM test_merge_users WHERE email LIKE '%@mergetest.com'")
            print(f"SUCCESS: Cleaned up {result['count']} existing test records")
        except Exception as e:
            print(f"WARNING: Could not clean up test data: {e}")
        
        # Test 1: Single record merge (INSERT scenario)
        print("\n--- Test 1: Single Record Merge (INSERT) ---")
        new_user = {
            "email": "john@mergetest.com",
            "name": "John Doe",
            "age": 30,
            "department": "Engineering",
            "status": "active",
            "metadata": {"level": "Senior", "skills": ["Python", "SQL"]},
            "last_updated": "`CURRENT_TIMESTAMP`"
        }
        
        result = db.merge("test_merge_users", new_user, ["email"])
        print(f"Single merge (INSERT) result: {result}")
        assert result["status"] == 0, "Single merge insert should succeed"
        assert result["count"] >= 1, "Single merge insert should affect at least 1 row"
        
        # Verify the record was inserted
        verify_result = db.query("SELECT * FROM test_merge_users WHERE email = 'john@mergetest.com'")
        assert len(verify_result["data"]) == 1, "Should find exactly 1 inserted record"
        assert verify_result["data"][0]["name"] == "John Doe", "Name should match inserted value"
        print("SUCCESS: Verified record was inserted correctly")
        
        # Test 2: Single record merge (UPDATE scenario)
        print("\n--- Test 2: Single Record Merge (UPDATE) ---")
        updated_user = {
            "email": "john@mergetest.com",  # Same email (key)
            "name": "John Smith",  # Updated name
            "age": 31,  # Updated age
            "department": "DevOps",  # Updated department
            "status": "active",
            "metadata": {"level": "Lead", "skills": ["Python", "Docker", "Kubernetes"]},
            "last_updated": "`CURRENT_TIMESTAMP`"
        }
        
        result = db.merge("test_merge_users", updated_user, ["email"])
        print(f"Single merge (UPDATE) result: {result}")
        assert result["status"] == 0, "Single merge update should succeed"
        assert result["count"] >= 1, "Single merge update should affect at least 1 row"
        
        # Verify the record was updated
        verify_result = db.query("SELECT * FROM test_merge_users WHERE email = 'john@mergetest.com'")
        assert len(verify_result["data"]) == 1, "Should still find exactly 1 record"
        assert verify_result["data"][0]["name"] == "John Smith", "Name should be updated"
        assert verify_result["data"][0]["age"] == 31, "Age should be updated"
        assert verify_result["data"][0]["department"] == "DevOps", "Department should be updated"
        print("SUCCESS: Verified record was updated correctly")
        
        # Test 3: Bulk merge (mixed INSERT/UPDATE)
        print("\n--- Test 3: Bulk Merge (Mixed INSERT/UPDATE) ---")
        bulk_users = [
            {
                "email": "john@mergetest.com",  # Existing - should UPDATE
                "name": "John Doe Updated",
                "age": 32,
                "department": "Engineering",
                "status": "active",
                "metadata": {"level": "Principal", "skills": ["Python", "Architecture"]}
            },
            {
                "email": "jane@mergetest.com",  # New - should INSERT
                "name": "Jane Smith",
                "age": 28,
                "department": "Marketing",
                "status": "active",
                "metadata": {"level": "Manager", "skills": ["Analytics", "Strategy"]}
            },
            {
                "email": "bob@mergetest.com",  # New - should INSERT
                "name": "Bob Wilson",
                "age": 35,
                "department": "Sales",
                "status": "active",
                "metadata": {"level": "Director", "skills": ["B2B", "Enterprise"]}
            }
        ]
        
        result = db.merge("test_merge_users", bulk_users, ["email"])
        print(f"Bulk merge result: {result}")
        assert result["status"] == 0, "Bulk merge should succeed"
        assert result["count"] >= 3, "Bulk merge should affect at least 3 rows"
        
        # Verify all records are present
        verify_result = db.query("SELECT * FROM test_merge_users WHERE email LIKE '%@mergetest.com' ORDER BY email")
        assert len(verify_result["data"]) == 3, "Should find exactly 3 records after bulk merge"
        
        # Verify John's record was updated
        john_record = next(r for r in verify_result["data"] if r["email"] == "john@mergetest.com")
        assert john_record["name"] == "John Doe Updated", "John's name should be updated"
        assert john_record["age"] == 32, "John's age should be updated"
        
        # Verify Jane and Bob were inserted
        jane_record = next(r for r in verify_result["data"] if r["email"] == "jane@mergetest.com")
        bob_record = next(r for r in verify_result["data"] if r["email"] == "bob@mergetest.com")
        assert jane_record["name"] == "Jane Smith", "Jane should be inserted correctly"
        assert bob_record["name"] == "Bob Wilson", "Bob should be inserted correctly"
        print("SUCCESS: Verified bulk merge worked correctly")
        
        # Test 4: Multi-column key merge
        print("\n--- Test 4: Multi-column Key Merge ---")
        # First create a table with composite key
        try:
            # Drop table first
            test_utils.drop_table_silently(db, "test_user_roles")
            
            create_composite_sql = test_utils.get_create_table_sql(db.database_type, "test_user_roles")
            if create_composite_sql:
                db.execute(create_composite_sql)
                print("SUCCESS: Test user_roles table created/verified")
        except Exception as e:
            print(f"WARNING: Could not create user_roles table: {e}")
        
        # Clean up existing data
        try:
            db.execute("DELETE FROM test_user_roles WHERE granted_by = 'merge_test'")
        except:
            pass
        
        # Merge with composite key
        user_role = {
            "user_id": 1,
            "role_id": 2,
            "permissions": "read,write",
            "granted_by": "merge_test",
            "granted_at": "`CURRENT_TIMESTAMP`"
        }
        
        result = db.merge("test_user_roles", user_role, ["user_id", "role_id"])
        print(f"Multi-column key merge result: {result}")
        assert result["status"] == 0, "Multi-column key merge should succeed"
        assert result["count"] >= 1, "Multi-column key merge should affect at least 1 row"
        
        # Update the same record
        updated_role = {
            "user_id": 1,
            "role_id": 2,
            "permissions": "read,write,admin",
            "granted_by": "merge_test",
            "granted_at": "`CURRENT_TIMESTAMP`"
        }
        
        result = db.merge("test_user_roles", updated_role, ["user_id", "role_id"])
        print(f"Multi-column key merge update result: {result}")
        assert result["status"] == 0, "Multi-column key merge update should succeed"
        
        # Verify the update
        verify_result = db.query("SELECT * FROM test_user_roles WHERE user_id = 1 AND role_id = 2")
        assert len(verify_result["data"]) == 1, "Should find exactly 1 user_role record"
        assert "admin" in verify_result["data"][0]["permissions"], "Permissions should be updated"
        print("SUCCESS: Verified multi-column key merge worked")
        
        # Test 5: Error handling - empty data
        print("\n--- Test 5: Error Handling - Empty Data ---")
        result = db.merge("test_merge_users", {}, ["email"])
        print(f"Empty data merge result: {result}")
        assert result["status"] == 0, "Empty data merge should succeed"
        assert result["count"] == 0, "Empty data should result in 0 affected rows"
        
        # Test 6: Error handling - empty key columns
        print("\n--- Test 6: Error Handling - Empty Key Columns ---")
        try:
            result = db.merge("test_merge_users", {"name": "Test"}, [])
            assert False, "Should have raised ValueError for empty key_columns"
        except ValueError as e:
            print(f"SUCCESS: Correctly caught empty key_columns error: {e}")
        except Exception as e:
            print(f"FAILED: Unexpected error type: {e}")
        
        # Test 7: Error handling - missing key column in data
        print("\n--- Test 7: Error Handling - Missing Key Column ---")
        try:
            result = db.merge("test_merge_users", {"name": "Test"}, ["nonexistent_key"])
            assert False, "Should have raised ValueError for missing key column"
        except ValueError as e:
            print(f"SUCCESS: Correctly caught missing key column error: {e}")
        except Exception as e:
            print(f"FAILED: Unexpected error type: {e}")
        
        # Clean up test data
        try:
            result1 = db.execute("DELETE FROM test_merge_users WHERE email LIKE '%@mergetest.com'")
            result2 = db.execute("DELETE FROM test_user_roles WHERE granted_by = 'merge_test'")
            print(f"SUCCESS: Cleaned up {result1['count']} merge_users and {result2['count']} user_roles test records")
        except Exception as e:
            print(f"WARNING: Could not clean up test data: {e}")
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")


def test_error_handling():
    """Test error handling for merge operations"""
    print(f"\n=== Testing Error Handling ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    
    # Test with first available database
    # Test with first available database
    available_dbs = test_utils.get_active_test_configs(cfg)
    db_key = None
    
    if available_dbs:
        db_key = available_dbs[0][0] # Just take the first one
    
    if not db_key:
        print("No SQL database available for error handling tests")
        return
    
    pools = {}
    pools[db_key] = Pool.from_config(cfg.require(db_key), PoolBackend.DBUTILS)
    
    with pools[db_key].client() as client:
        db = get_db(client)
        # Test empty data
        print("\n--- Test: Empty Data Handling ---")
        result = db.merge("test_merge_users", [], ["email"])
        assert result["status"] == 0, "Empty list should succeed"
        assert result["count"] == 0, "Empty list should affect 0 rows"
        print("SUCCESS: Empty data handled correctly")
        
        # Test empty key columns
        print("\n--- Test: Empty Key Columns Error ---")
        try:
            result = db.merge("test_merge_users", {"name": "Test"}, [])
            assert False, "Should have raised ValueError for empty key_columns"
        except ValueError as e:
            print(f"SUCCESS: Correctly caught empty key_columns error: {e}")
        
        # Test missing key column
        print("\n--- Test: Missing Key Column Error ---")
        try:
            result = db.merge("test_merge_users", {"name": "Test"}, ["missing_key"])
            assert False, "Should have raised ValueError for missing key column"
        except ValueError as e:
            print(f"SUCCESS: Correctly caught missing key column error: {e}")
        
    
    pools[db_key].dispose()


def _merge_select_create_sql(database_type, table, is_target):
    """DDL for the merge_select test tables, per dialect.

    The merge key is ``email``. Target tables get it as NOT NULL PRIMARY KEY so
    the INSERT ... ON CONFLICT backends (PostgreSQL/MySQL/SQLite) have the unique
    index they require (and Db2 won't allow a nullable PK); the MERGE INTO
    backends don't need it but it's harmless. Spark/Delta has no PK constraints,
    so it's omitted there and the table is created USING DELTA. Avoiding an
    identity/auto-increment ``id`` column keeps the DDL portable everywhere.
    """
    vc = "VARCHAR2" if database_type == "oracle" else "VARCHAR"
    email = f"email {vc}(100)"
    if is_target and database_type != "spark":
        email += " NOT NULL PRIMARY KEY"
    cols = f"{email}, name {vc}(100), status {vc}(20), age INTEGER"
    sql = f"CREATE TABLE {table} ({cols})"
    if database_type == "spark":
        sql += " USING DELTA"
    return sql


def _merge_select_drop(db, table):
    """Drop a table, tolerating 'does not exist' (Db2/Oracle lack IF EXISTS)."""
    try:
        if db.database_type in ("oracle", "db2"):
            db.execute(f"DROP TABLE {table}")
        else:
            db.execute(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass


def test_merge_select_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test merge_select across all SQL backends: PostgreSQL/MySQL/SQLite via
    INSERT ... ON CONFLICT, and Db2/Oracle/SQL Server via MERGE INTO.

    (Spark merge_select is covered in spark_test.py, like the other Spark tests.)
    """
    print(f"\n=== Testing {db_key} merge_select Operations with {backend.value} backend ===")

    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)

    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)

    with pools[db_key].client() as client:
        db = get_db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")

        # Set up test tables
        print("\n--- Setting up test data for merge_select ---")
        try:
            _merge_select_drop(db, "merge_select_source")
            _merge_select_drop(db, "merge_select_target")
            db.execute(_merge_select_create_sql(db.database_type, "merge_select_source", is_target=False))
            db.execute(_merge_select_create_sql(db.database_type, "merge_select_target", is_target=True))

            source_data = [
                {"email": "alice@test.com", "name": "Alice Source", "status": "active", "age": 25},
                {"email": "bob@test.com", "name": "Bob Source", "status": "active", "age": 30},
                {"email": "charlie@test.com", "name": "Charlie Source", "status": "inactive", "age": 35},
            ]
            db.insert("merge_select_source", source_data)
            db.insert("merge_select_target",
                      [{"email": "alice@test.com", "name": "Alice Old", "status": "pending", "age": 20}])
            print("SUCCESS: Test tables created with initial data")
        except Exception as e:
            print(f"ERROR: Could not set up merge_select test data: {e}")
            import traceback
            traceback.print_exc()
            return

        # Test 1: Basic merge (update existing + insert new)
        print("\n--- Test 1: Basic merge_select ---")
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"])
        assert result["status"] == 0, f"merge_select should succeed: {result.get('message')}"
        rows = db.query("SELECT email, name, status, age FROM merge_select_target")["data"]
        assert len(rows) == 3, "should have 3 rows"
        alice = next(r for r in rows if r["email"] == "alice@test.com")
        assert alice["name"] == "Alice Source" and alice["age"] == 25, "alice should be updated from source"
        print("SUCCESS: Basic merge_select test passed")

        # Test 2: equality dict condition
        print("\n--- Test 2: merge_select with Conditions ---")
        db.execute("DELETE FROM merge_select_target")
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            conditions={"status": "active"})
        assert result["status"] == 0, f"conditional merge_select should succeed: {result.get('message')}"
        assert len(db.query("SELECT email FROM merge_select_target")["data"]) == 2, "2 active users"
        print("SUCCESS: merge_select with conditions test passed")

        # Test 3: custom source SELECT (no ORDER BY: meaningless for a merge
        # source and SQL Server rejects it inside the USING subquery)
        print("\n--- Test 3: merge_select with Custom Source SELECT ---")
        db.execute("DELETE FROM merge_select_target")
        result = db.merge_select(
            source_table=None, target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            source_select="SELECT email, name, status, age FROM merge_select_source WHERE age >= 30")
        assert result["status"] == 0, f"custom SELECT merge_select should succeed: {result.get('message')}"
        assert len(db.query("SELECT email FROM merge_select_target")["data"]) == 2, "2 users age>=30"
        print("SUCCESS: merge_select with custom SELECT test passed")

        # Test 4: order_by param (ignored on MERGE backends, applied on the rest)
        print("\n--- Test 4: merge_select with ORDER BY ---")
        db.execute("DELETE FROM merge_select_target")
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            order_by="age DESC")
        assert result["status"] == 0, f"ORDER BY merge_select should succeed: {result.get('message')}"
        print("SUCCESS: merge_select with ORDER BY test passed")

        # Test 5: custom update_columns (name must NOT be updated)
        print("\n--- Test 5: merge_select with Custom Update Columns ---")
        db.execute("DELETE FROM merge_select_target")
        db.insert("merge_select_target",
                  [{"email": "alice@test.com", "name": "Keep This Name", "status": "old_status", "age": 99}])
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            update_columns=["status", "age"])
        assert result["status"] == 0, f"custom update_columns should succeed: {result.get('message')}"
        rows = db.query("SELECT email, name, status, age FROM merge_select_target")["data"]
        alice = next(r for r in rows if r["email"] == "alice@test.com")
        assert alice["name"] == "Keep This Name", "alice name should NOT be updated"
        assert alice["status"] == "active" and alice["age"] == 25, "alice status/age SHOULD be updated"
        print("SUCCESS: merge_select with custom update_columns test passed")

        # Test 6: operator + list conditions, parameterized by default
        print("\n--- Test 6: Operator/List Conditions (parameterized) ---")
        db.execute("DELETE FROM merge_select_target")
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            conditions=[{"age": {">=": 30}}])  # bob(30), charlie(35)
        assert result["status"] == 0, f"operator/list conditions should succeed: {result.get('message')}"
        rows = db.query("SELECT email, age FROM merge_select_target")["data"]
        assert len(rows) == 2 and all(r["age"] >= 30 for r in rows), "only age>=30 rows merged"
        print("SUCCESS: operator/list conditions (parameterized) test passed")

        # Test 7: same filter via inline mode (dynamic_param='N')
        print("\n--- Test 7: Inline Conditions (dynamic_param='N') ---")
        db.execute("DELETE FROM merge_select_target")
        result = db.merge_select(
            source_table="merge_select_source", target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"], key_columns=["email"],
            conditions=[{"age": {">=": 30}}], dynamic_param="N")
        assert result["status"] == 0, f"inline conditions should succeed: {result.get('message')}"
        assert len(db.query("SELECT email FROM merge_select_target")["data"]) == 2, "same 2 rows"
        print("SUCCESS: inline conditions (dynamic_param='N') test passed")

        # Test 8: a single-quote value must not break either mode
        # (bind path binds it; inline path single-quote-escapes it)
        print("\n--- Test 8: Quote Safety (bind + inline) ---")
        for mode in ("Y", "N"):
            db.execute("DELETE FROM merge_select_target")
            result = db.merge_select(
                source_table="merge_select_source", target_table="merge_select_target",
                insert_columns=["email", "name", "status", "age"], key_columns=["email"],
                conditions=[{"name": {"=": "O'Brien"}}], dynamic_param=mode)  # matches nothing
            assert result["status"] == 0, f"quote-safe conditions (mode {mode}) should succeed: {result.get('message')}"
            assert len(db.query("SELECT email FROM merge_select_target")["data"]) == 0, f"no O'Brien match ({mode})"
        print("SUCCESS: quote-safety (bind + inline) test passed")

        # Cleanup
        _merge_select_drop(db, "merge_select_source")
        _merge_select_drop(db, "merge_select_target")
        print("SUCCESS: Cleaned up merge_select test tables")

    pools[db_key].dispose()


if __name__ == "__main__":
    print("=== JRM Merge Function Test Suite ===")
    
    # Test database and backend combinations
    # Test database and backend combinations
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    test_combinations = []
    active_configs = test_utils.get_active_test_configs(cfg)
    
    for db_key, backend in active_configs:
         test_combinations.append((db_key, backend, test_sql_database))
         test_combinations.append((db_key, backend, test_merge_select_sql))
    
    # Test all available database/backend combinations, abort on first failure
    combinations_tested = 0
    for db_key, backend, test_function in test_combinations:
        try:
            # Check if database configuration exists
            db_cfg = cfg.require(db_key)
            
            # Skip Spark databases (they require separate spark_test.py)
            if db_cfg.type == 'spark':
                print(f"Skipping {db_key} (Spark databases are run in spark_test.py)")
                continue
            
            # Run all tests for this database/backend combination
            print(f"\n>>> Running tests with {db_key} using {backend.value} backend")
            test_function(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) tests completed successfully")
            combinations_tested += 1
            
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
            continue
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) tests failed: {e}")
            print("ABORTING: Test failed, stopping execution")
            sys.exit(1)  # Abort on first failure
    

    
    if combinations_tested == 0:
        print("ERROR: No database configurations found")
        sys.exit(1)
    
    # Test error handling
    try:
        test_error_handling()
        print("SUCCESS: Error handling tests completed successfully")
    except Exception as e:
        print(f"FAILED: Error handling tests failed: {e}")
        print("ABORTING: Error handling test failed")
        sys.exit(1)
    
    print("\n=== Merge Test Suite Complete ===")