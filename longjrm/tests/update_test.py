"""
JRM Update Function Test Suite

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/update_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/update_test.py

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
from longjrm.database.db import Db

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def setup_test_data(db, db_key):
    """Setup test data for update operations"""
    print(f"Setting up test data for {db_key}...")
    
    try:
        if db.database_type in ['postgres', 'postgresql']:
            # Create test table for PostgreSQL
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                metadata JSONB,
                tags TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        elif db.database_type == 'mysql':
            # Create test table for MySQL
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                metadata JSON,
                tags TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        
        if db.database_type in ['mysql', 'postgres', 'postgresql']:
            # Drop existing table to ensure correct structure
            db.execute("DROP TABLE IF EXISTS test_users")
            db.execute(create_table_sql)
            print("SUCCESS: Test table created/verified")
            
            # Clean up existing test data
            cleanup_result = db.execute("DELETE FROM test_users WHERE email LIKE '%@updatetest.com'")
            print(f"SUCCESS: Cleaned up {cleanup_result['count']} existing test records")
            
            # Insert test data for updates
            test_records = [
                {
                    "name": "John Doe",
                    "email": "john@updatetest.com", 
                    "age": 30,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Senior"},
                    "tags": "developer,python,backend"
                },
                {
                    "name": "Jane Smith",
                    "email": "jane@updatetest.com",
                    "age": 28,
                    "status": "active", 
                    "metadata": {"department": "Marketing", "level": "Manager"},
                    "tags": "marketing,strategy"
                },
                {
                    "name": "Bob Wilson",
                    "email": "bob@updatetest.com", 
                    "age": 35,
                    "status": "inactive",
                    "metadata": {"department": "Sales", "level": "Director"},
                    "tags": "sales,b2b"
                }
            ]
            
            for record in test_records:
                result = db.insert("test_users", record)
                assert result["status"] == 0, f"Failed to insert test record: {record['email']}"
            
            print(f"SUCCESS: Inserted {len(test_records)} test records")
            
        else:
            # MongoDB setup
            # Clean up existing test data
            try:
                collection = db.conn[db.database_name]["test_users"]
                collection.delete_many({"email": {"$regex": "@updatetest.com$"}})
                print("SUCCESS: Cleaned up existing test data")
            except:
                print("SUCCESS: No existing test data to clean up")
            
            # Insert test data for MongoDB
            test_docs = [
                {
                    "name": "John Doe",
                    "email": "john@updatetest.com", 
                    "age": 30,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Senior"},
                    "tags": ["developer", "python", "backend"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Jane Smith",
                    "email": "jane@updatetest.com",
                    "age": 28,
                    "status": "active", 
                    "metadata": {"department": "Marketing", "level": "Manager"},
                    "tags": ["marketing", "strategy"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Bob Wilson",
                    "email": "bob@updatetest.com", 
                    "age": 35,
                    "status": "inactive",
                    "metadata": {"department": "Sales", "level": "Director"},
                    "tags": ["sales", "b2b"],
                    "created_at": datetime.datetime.now()
                }
            ]
            
            for doc in test_docs:
                result = db.insert("test_users", doc)
                assert result["status"] == 0, f"Failed to insert test document: {doc['email']}"
            
            print(f"SUCCESS: Inserted {len(test_docs)} test documents")
            
    except Exception as e:
        print(f"WARNING:  Error setting up test data: {e}")
        raise

def test_sql_database_update(db_key, backend=PoolBackend.DBUTILS):
    """Test update functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} Update Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    # Use specified backend
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client:
        db = Db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Setup test data
        setup_test_data(db, db_key)
        
        # Test 1: Single record update by specific condition
        print("\n--- Test 1: Single Record Update ---")
        update_data = {
            "status": "updated",
            "age": 31,
            "metadata": {"department": "Engineering", "level": "Lead"}
        }
        where_condition = {"email": "john@updatetest.com"}
        
        result = db.update("test_users", update_data, where_condition)
        print(f"Single update result: {result}")
        assert result["status"] == 0, "Single update should succeed"
        assert result["count"] >= 1, "Single update should affect at least 1 row"
        
        # Verify the update worked
        verify_result = db.query("SELECT * FROM test_users WHERE email = %s", ["john@updatetest.com"])
        if verify_result["data"]:
            updated_record = verify_result["data"][0]
            assert updated_record["status"] == "updated", "Status should be updated"
            assert updated_record["age"] == 31, "Age should be updated"
            print("SUCCESS: Single update verified successfully")
        
        # Test 2: Bulk update with WHERE condition
        print("\n--- Test 2: Bulk Update with WHERE Condition ---")
        bulk_update_data = {
            "status": "bulk_updated",
            "tags": "updated,bulk"
        }
        bulk_where_condition = {"status": "active"}
        
        result = db.update("test_users", bulk_update_data, bulk_where_condition)
        print(f"Bulk update result: {result}")
        assert result["status"] == 0, "Bulk update should succeed"
        assert result["count"] >= 1, "Bulk update should affect at least 1 row"
        
        # Test 3: Update with NULL values
        print("\n--- Test 3: Update with NULL Values ---")
        null_update_data = {
            "tags": None,  # Test NULL handling
            "status": "null_test"
        }
        null_where_condition = {"email": "jane@updatetest.com"}
        
        result = db.update("test_users", null_update_data, null_where_condition)
        print(f"NULL update result: {result}")
        assert result["status"] == 0, "NULL update should succeed"
        assert result["count"] >= 1, "NULL update should affect at least 1 row"
        
        # Test 4: Update with CURRENT timestamp
        print("\n--- Test 4: Update with CURRENT Keywords ---")
        current_update_data = {
            "status": "current_test",
            "updated_at": "`CURRENT_TIMESTAMP`"  # Test CURRENT keyword
        }
        current_where_condition = {"email": "bob@updatetest.com"}
        
        result = db.update("test_users", current_update_data, current_where_condition)
        print(f"CURRENT keyword update result: {result}")
        assert result["status"] == 0, "CURRENT keyword update should succeed"
        assert result["count"] >= 1, "CURRENT keyword update should affect at least 1 row"
        
        # Test 5: Update with complex WHERE conditions
        print("\n--- Test 5: Complex WHERE Conditions ---")
        complex_update_data = {
            "status": "complex_updated"
        }
        complex_where_condition = {
            "age": {">": 25, "<": 35},
            "status": "bulk_updated"
        }
        
        result = db.update("test_users", complex_update_data, complex_where_condition)
        print(f"Complex WHERE update result: {result}")
        assert result["status"] == 0, "Complex WHERE update should succeed"
        
        # Test 6: Update with no WHERE condition (update all)
        print("\n--- Test 6: Update All Records (No WHERE) ---")
        global_update_data = {
            "tags": "global_update"
        }
        
        result = db.update("test_users", global_update_data)
        print(f"Global update result: {result}")
        assert result["status"] == 0, "Global update should succeed"
        assert result["count"] >= 3, "Global update should affect all test records"
        
        # Test 7: Update non-existent record
        print("\n--- Test 7: Update Non-existent Record ---")
        nonexistent_update_data = {
            "status": "should_not_exist"
        }
        nonexistent_where_condition = {"email": "nonexistent@updatetest.com"}
        
        result = db.update("test_users", nonexistent_update_data, nonexistent_where_condition)
        print(f"Non-existent update result: {result}")
        assert result["status"] == 0, "Non-existent update should succeed (but affect 0 rows)"
        assert result["count"] == 0, "Non-existent update should affect 0 rows"
        
        print("\nSUCCESS: All SQL update tests completed successfully")
        
        # Clean up test data
        try:
            cleanup_result = db.execute("DELETE FROM test_users WHERE email LIKE '%@updatetest.com'")
            print(f"SUCCESS: Cleaned up {cleanup_result['count']} test records")
        except Exception as e:
            print(f"WARNING:  Could not clean up test data: {e}")
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")

def test_mongodb_database_update(db_key):
    """Test update functionality for MongoDB"""
    print(f"\n=== Testing {db_key} Update Operations ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, PoolBackend.MONGODB)
    
    with pools[db_key].client() as client:
        db = Db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Setup test data
        setup_test_data(db, db_key)
        
        # Test 1: Single document update
        print("\n--- Test 1: Single Document Update ---")
        update_data = {
            "status": "updated",
            "age": 31,
            "metadata": {"department": "Engineering", "level": "Lead"}
        }
        where_condition = {"email": "john@updatetest.com"}
        
        result = db.update("test_users", update_data, where_condition)
        print(f"Single update result: {result}")
        assert result["status"] == 0, "Single update should succeed"
        assert result["count"] >= 1, "Single update should affect at least 1 document"
        
        # Test 2: Bulk update with filter
        print("\n--- Test 2: Bulk Update with Filter ---")
        bulk_update_data = {
            "status": "bulk_updated",
            "tags": ["updated", "bulk"]
        }
        bulk_where_condition = {"status": "active"}
        
        result = db.update("test_users", bulk_update_data, bulk_where_condition)
        print(f"Bulk update result: {result}")
        assert result["status"] == 0, "Bulk update should succeed"
        
        # Test 3: Update with complex nested data
        print("\n--- Test 3: Update with Complex Nested Data ---")
        complex_update_data = {
            "metadata": {
                "department": "Engineering", 
                "level": "Principal",
                "skills": ["python", "mongodb", "architecture"],
                "performance": {"rating": 5, "year": 2024}
            },
            "tags": ["senior", "architect", "mentor"]
        }
        complex_where_condition = {"email": "jane@updatetest.com"}
        
        result = db.update("test_users", complex_update_data, complex_where_condition)
        print(f"Complex data update result: {result}")
        assert result["status"] == 0, "Complex data update should succeed"
        
        # Test 4: Update with MongoDB-style filter
        print("\n--- Test 4: MongoDB-style Filter Update ---")
        mongodb_update_data = {
            "status": "mongodb_updated"
        }
        mongodb_where_condition = {
            "age": {"$gte": 30},
            "status": {"$ne": "inactive"}
        }
        
        result = db.update("test_users", mongodb_update_data, mongodb_where_condition)
        print(f"MongoDB-style filter update result: {result}")
        assert result["status"] == 0, "MongoDB-style filter update should succeed"
        
        # Test 5: Update all documents (no filter)
        print("\n--- Test 5: Update All Documents (No Filter) ---")
        global_update_data = {
            "updated_at": datetime.datetime.now(),
            "batch_processed": True
        }
        
        result = db.update("test_users", global_update_data)
        print(f"Global update result: {result}")
        assert result["status"] == 0, "Global update should succeed"
        assert result["count"] >= 3, "Global update should affect all test documents"
        
        # Test 6: Update non-existent document
        print("\n--- Test 6: Update Non-existent Document ---")
        nonexistent_update_data = {
            "status": "should_not_exist"
        }
        nonexistent_where_condition = {"email": "nonexistent@updatetest.com"}
        
        result = db.update("test_users", nonexistent_update_data, nonexistent_where_condition)
        print(f"Non-existent update result: {result}")
        assert result["status"] == 0, "Non-existent update should succeed (but affect 0 documents)"
        assert result["count"] == 0, "Non-existent update should affect 0 documents"
        
        print("\nSUCCESS: All MongoDB update tests completed successfully")
        
        # Clean up test data
        try:
            collection = client.conn[db.database_name]["test_users"]
            delete_result = collection.delete_many({"email": {"$regex": "@updatetest.com$"}})
            print(f"SUCCESS: Cleaned up {delete_result.deleted_count} test documents")
        except Exception as e:
            print(f"WARNING:  Could not clean up test data: {e}")
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")

def test_update_error_handling():
    """Test error handling for update operations"""
    print(f"\n=== Testing Update Error Handling ===")
    
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
        db = Db(client)
        print(f"Testing error handling with {db.database_type} database")
        
        # Test 1: Empty update data
        print("\n--- Test: Empty Update Data ---")
        try:
            result = db.update("test_users", {}, {"id": 1})
            # Empty update should succeed but do nothing
            assert result["status"] == 0, "Empty update should succeed"
            print("SUCCESS: Empty update data handled correctly")
        except Exception as e:
            print(f"FAILED: Unexpected error with empty update data: {e}")
        
        # Test 2: Invalid table name (should be handled gracefully)
        print("\n--- Test: Invalid Table Name ---")
        try:
            result = db.update("nonexistent_table", {"status": "test"}, {"id": 1})
            # This should fail at database level, but our method should handle it
            if result["status"] == -1:
                print("SUCCESS: Invalid table name handled correctly (returned error status)")
            else:
                print("WARNING:  Invalid table name didn't return error status")
        except Exception as e:
            print(f"SUCCESS: Invalid table name caused expected exception: {type(e).__name__}")
        
        print("SUCCESS: Error handling tests completed")
    
    pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Update Function Test Suite ===")
    
    # Test database and backend combinations
    test_combinations = [
        ("postgres-test", PoolBackend.DBUTILS, test_sql_database_update),
        ("postgres-test", PoolBackend.SQLALCHEMY, test_sql_database_update),
        ("mysql-test", PoolBackend.DBUTILS, test_sql_database_update),
        ("mysql-test", PoolBackend.SQLALCHEMY, test_sql_database_update)
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
        test_update_error_handling()
        print("SUCCESS: Error handling tests completed successfully")
    except Exception as e:
        print(f"FAILED: Error handling tests failed: {e}")
        print("ABORTING: Error handling test failed")
        sys.exit(1)
    
    print("\n=== Update Test Suite Complete ===")