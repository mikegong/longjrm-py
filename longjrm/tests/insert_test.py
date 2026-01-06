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
            if db.database_type in ['postgres', 'postgresql']:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INTEGER,
                    metadata JSONB,
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            else:  # MySQL
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS test_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INTEGER,
                    metadata JSON,
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            
            db.execute(create_table_sql)
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
    available_dbs = ["postgres-test", "mysql-test"]
    db_key = None
    
    for test_db in available_dbs:
        try:
            db_cfg = cfg.require(test_db)
            db_key = test_db
            break
        except:
            continue
    
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
    test_combinations = [
        ("postgres-test", PoolBackend.DBUTILS, test_sql_database),
        ("postgres-test", PoolBackend.SQLALCHEMY, test_sql_database),
        ("mysql-test", PoolBackend.DBUTILS, test_sql_database),
        ("mysql-test", PoolBackend.SQLALCHEMY, test_sql_database)
    ]
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    # Test all available database/backend combinations, abort on first failure
    combinations_tested = 0
    for db_key, backend, test_function in test_combinations:
        try:
            # Check if database configuration exists
            db_cfg = cfg.require(db_key)
            
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