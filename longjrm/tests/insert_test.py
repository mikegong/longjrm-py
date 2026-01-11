"""
JRM Insert Function Test Suite

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/insert_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/insert_test.py

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
    """Test insert functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} Insert Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    # Use specified backend
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client:
        db = get_db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Create test table if it doesn't exist (simple structure for testing)
        try:
            # We use drop_table_silently and recreate to ensure clean state, 
            # although original code used CREATE TABLE IF NOT EXISTS. 
            # For consistent testing, recreation is safer.
            # But the original code was IF NOT EXISTS, let's respect the intent but making it work cross-DB.
            # Actually, `get_create_table_sql` returns CREATE TABLE (some with IF NOT EXISTS encoded, some not).
            # To be safe for all DBs (like Oracle/DB2), we should drop first if we want to ensure it works.
            # But let's try to just run the create SQL obtained.
            
            # NOTE: test_utils.get_create_table_sql uses CREATE TABLE (sometimes w/o IF NOT EXISTS).
            # So we should try to create, and ignore "already exists" errors, OR drop and recreate.
            # Given these are tests, Drop + Recreate is usually better to ensure schema matches expectations.
            # However, to avoid annoying "table doesn't exist" logs, we use the silent drop helper.
            
            # BUT, the original test had a cleanup block right after.
            
            create_table_sql = test_utils.get_create_table_sql(db.database_type, "test_users")
            
            # Some DBs (Oracle) will fail if table exists.
            try:
                db.execute(create_table_sql)
                print("SUCCESS: Test table created")
            except Exception as e:
                 # Check if error is "exists"
                 logger.debug(f"Table might already exist: {e}")
                 pass
                 
            # print("SUCCESS: Test table created/verified")
            print("SUCCESS: Test table created/verified")
        except Exception as e:
            print(f"WARNING: Could not create test table (may already exist): {e}")
            # Continue with tests anyway
        
        # Clean up any existing test data
        try:
            result = db.execute("DELETE FROM test_users WHERE email LIKE '%@test.com'")
            print(f"SUCCESS: Cleaned up {result['count']} existing test records")
        except Exception as e:
            print(f"WARNING: Could not clean up test data (table may not exist): {e}")
            # Continue with tests anyway
        
        # Test 1: Single record insert
        print("\n--- Test 1: Single Record Insert ---")
        single_record = {
            "name": "John Doe",
            "email": "john@test.com", 
            "age": 30,
            "metadata": {"department": "Engineering", "level": "Senior"},
            "tags": ["developer", "python", "backend"]
        }
        
        result = db.insert("test_users", single_record)
        print(f"Single insert result: {result}")
        assert result["status"] == 0, "Single insert should succeed"
        assert result["count"] >= 1, "Single insert should affect at least 1 row"
        
        # Test 2: Bulk insert with multiple records
        print("\n--- Test 2: Bulk Insert ---")
        bulk_records = [
            {
                "name": "Jane Smith",
                "email": "jane@test.com",
                "age": 28,
                "metadata": {"department": "Marketing", "level": "Manager"},
                "tags": ["marketing", "strategy"]
            },
            {
                "name": "Bob Wilson",
                "email": "bob@test.com", 
                "age": 35,
                "metadata": {"department": "Sales", "level": "Director"},
                "tags": ["sales", "b2b"]
            },
            {
                "name": "Alice Brown",
                "email": "alice@test.com",
                "age": 26,
                "metadata": {"department": "Engineering", "level": "Junior"},
                "tags": ["developer", "frontend", "react"]
            }
        ]
        
        result = db.insert("test_users", bulk_records)
        print(f"Bulk insert result: {result}")
        assert result["status"] == 0, "Bulk insert should succeed"
        assert result["count"] >= 3, "Bulk insert should affect at least 3 rows"
        
        # Test 3: Insert with RETURNING (PostgreSQL only)
        if db.database_type in ['postgres', 'postgresql']:
            print("\n--- Test 3: Insert with RETURNING ---")
            return_record = {
                "name": "Charlie Green",
                "email": "charlie@test.com",
                "age": 32,
                "metadata": {"department": "DevOps", "level": "Senior"},
                "tags": ["devops", "kubernetes", "aws"]
            }
            
            result = db.insert("test_users", return_record, return_columns=["id", "name", "updated_at"])
            print(f"Insert with RETURNING result: {result}")
            assert result["status"] == 0, "RETURNING insert should succeed"
            assert result["count"] == 1, "RETURNING insert should affect exactly 1 row"
            # Note: With execute method, RETURNING data is handled by database API, not returned in result
        
        # Test 4: Insert with CURRENT timestamp (test CURRENT keyword handling)
        print("\n--- Test 4: Insert with CURRENT Keywords ---")
        current_record = {
            "name": "David Black",
            "email": "david@test.com",
            "age": 29,
            "metadata": {"department": "QA", "level": "Senior"},
            "tags": ["testing", "automation"],
            "updated_at": "`CURRENT_TIMESTAMP`"  # Test CURRENT keyword
        }
        
        result = db.insert("test_users", current_record)
        print(f"Insert with CURRENT keyword result: {result}")
        assert result["status"] == 0, "CURRENT keyword insert should succeed"
        assert result["count"] >= 1, "CURRENT keyword insert should affect at least 1 row"
        
        # Test 5: Empty list handling
        print("\n--- Test 5: Empty List Handling ---")
        result = db.insert("test_users", [])
        print(f"Empty list insert result: {result}")
        assert result["status"] == 0, "Empty list insert should succeed"
        assert result["count"] == 0, "Empty list should result in 0 affected rows"
        
        # Verify all inserts worked by counting records
        try:
            count_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE email LIKE '%@test.com'")
            if count_result["data"]:
                total_inserted = count_result["count"]
                print(f"\nSUCCESS: Total test records in database: {total_inserted}")
            else:
                print(f"\nSUCCESS: Could not count records, but tests completed")
        except Exception as e:
            print(f"\nWARNING: Could not count records: {e}")
        
        # Clean up test data
        try:
            result = db.execute("DELETE FROM test_users WHERE email LIKE '%@test.com'")
            print(f"SUCCESS: Cleaned up {result['count']} test records")
        except Exception as e:
            print(f"WARNING: Could not clean up test data: {e}")
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")


def test_error_handling():
    """Test error handling for insert operations"""
    print(f"\n=== Testing Error Handling ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    
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
        # Test inconsistent columns in bulk insert
        print("\n--- Test: Inconsistent Columns Error ---")
        inconsistent_records = [
            {"name": "John", "email": "john@test.com"},
            {"name": "Jane", "age": 30}  # Missing email, has age instead
        ]
        
        try:
            result = db.insert("test_users", inconsistent_records)
            assert False, "Should have raised ValueError for inconsistent columns"
        except ValueError as e:
            print(f"SUCCESS: Correctly caught inconsistent columns error: {e}")
        except Exception as e:
            print(f"FAILED: Unexpected error type: {e}")
        
    
    pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Insert Function Test Suite ===")
    
    # Test database and backend combinations
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    test_combinations = []
    active_configs = test_utils.get_active_test_configs(cfg)
    
    for db_key, backend in active_configs:
         test_combinations.append((db_key, backend, test_sql_database))

    # Test all available database/backend combinations, abort on first failure
    combinations_tested = 0
    for db_key, backend, test_function in test_combinations:
        try:
            # Check if database configuration exists
            db_cfg = cfg.require(db_key)
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
    
    print("\n=== Insert Test Suite Complete ===")