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
from longjrm.database import get_db

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
            if db.database_type in ['postgres', 'postgresql']:
                # PostgreSQL with unique constraint for merge testing
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS test_merge_users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata JSONB,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            else:  # MySQL
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS test_merge_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata JSON,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_email (email)
                )
                """
            
            db.execute(create_table_sql)
            print("SUCCESS: Test merge table created/verified")
        except Exception as e:
            print(f"WARNING: Could not create test table (may already exist): {e}")
        
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
            if db.database_type in ['postgres', 'postgresql']:
                create_composite_sql = """
                CREATE TABLE IF NOT EXISTS test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions TEXT,
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
                """
            else:  # MySQL
                create_composite_sql = """
                CREATE TABLE IF NOT EXISTS test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions TEXT,
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
                """
            
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

if __name__ == "__main__":
    print("=== JRM Merge Function Test Suite ===")
    
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
    
    print("\n=== Merge Test Suite Complete ===")