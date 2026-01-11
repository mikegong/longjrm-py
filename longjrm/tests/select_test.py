"""
JRM Select Function Test Suite

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/select_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/select_test.py

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

def setup_test_data(db, database_type):
    """Set up test data for select operations"""
    print("\n--- Setting up test data ---")
    
    # Create test table if it doesn't exist (drop and recreate to ensure correct schema)
    # Create test table if it doesn't exist (drop and recreate to ensure correct schema)
    try:
        # Drop the table first to ensure clean schema
        test_utils.drop_table_silently(db, "test_users")
        print("SUCCESS: Dropped existing test_users table")
        
        create_table_sql = test_utils.get_create_table_sql(db.database_type, "test_users")
        if not create_table_sql:
            raise ValueError(f"No CREATE TABLE SQL for {db.database_type}")
            
        db.execute(create_table_sql)
        print("SUCCESS: Test table created with fresh schema")
    except Exception as e:
        print(f"WARNING: Could not create test table: {e}")
        return False
    
    # Clean up any existing test data
    try:
        db.execute("DELETE FROM test_users WHERE email LIKE '%@selecttest.com'")
        print("SUCCESS: Cleaned up existing test data")
    except Exception as e:
        print(f"WARNING: Could not clean up test data: {e}")
    
    # Insert comprehensive test data
    test_records = [
        {
            "name": "Alice Johnson",
            "email": "alice@selecttest.com",
            "age": 28,
            "status": "active",
            "department": "Engineering",
            "salary": 75000.50,
            "metadata": {"level": "Senior", "team": "Backend"},
            "tags": "python|django|postgresql"
        },
        {
            "name": "Bob Smith",
            "email": "bob@selecttest.com",
            "age": 35,
            "status": "active",
            "department": "Sales",
            "salary": 65000.00,
            "metadata": {"level": "Manager", "team": "Enterprise"},
            "tags": "sales|b2b|crm"
        },
        {
            "name": "Charlie Brown",
            "email": "charlie@selecttest.com",
            "age": 42,
            "status": "inactive",
            "department": "Engineering",
            "salary": 95000.75,
            "metadata": {"level": "Principal", "team": "Platform"},
            "tags": "architecture|kubernetes|aws"
        },
        {
            "name": "Diana Wilson",
            "email": "diana@selecttest.com",
            "age": 31,
            "status": "active",
            "department": "Marketing",
            "salary": 55000.25,
            "metadata": {"level": "Senior", "team": "Growth"},
            "tags": "marketing|analytics|growth"
        },
        {
            "name": "Eve Davis",
            "email": "eve@selecttest.com",
            "age": 26,
            "status": "active",
            "department": "Engineering",
            "salary": 70000.00,
            "metadata": {"level": "Mid", "team": "Frontend"},
            "tags": "javascript|react|typescript"
        }
    ]
    
    try:
        result = db.insert("test_users", test_records)
        print(f"result: {result}")
        result = db.select("test_users", ["id","name"])
        print(f"result: {result}")
        assert result["status"] == 0, "Test data insertion should succeed"
        assert result["count"] >= 5, "Should insert all 5 test records"
        print(f"SUCCESS: Inserted {result['count']} test records")
        return True
    except Exception as e:
        print(f"ERROR: Could not insert test data: {e}")
        return False

def cleanup_test_data(db):
    """Clean up test data after tests"""
    try:
        result = db.execute("DELETE FROM test_users WHERE email LIKE '%@selecttest.com'")
        print(f"SUCCESS: Cleaned up {result['count']} test records")
    except Exception as e:
        print(f"WARNING: Could not clean up test data: {e}")

def test_sql_database(db_key, backend=PoolBackend.DBUTILS):
    """Test select functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} Select Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client:
        db = get_db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Set up test data
        if not setup_test_data(db, db.database_type):
            print("ERROR: Could not set up test data, aborting tests")
            return
        
        # Test 1: Basic select all columns
        print("\n--- Test 1: Select All Columns ---")
        result = db.select("test_users", ["*"], where={"email": {"LIKE": "%@selecttest.com"}})
        print(f"Select all result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 5, "Should return 5 test records"
        assert "id" in result["data"][0], "Should include id column"
        assert "name" in result["data"][0], "Should include name column"
        print("SUCCESS: Basic select all columns test passed")
        
        # Test 2: Select specific columns
        print("\n--- Test 2: Select Specific Columns ---")
        result = db.select("test_users", ["name", "email", "age"], where={"email": {"LIKE": "%@selecttest.com"}})
        print(f"Select specific columns result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 5, "Should return 5 records"
        assert len(result["data"][0]) == 3, "Should return exactly 3 columns"
        assert "name" in result["data"][0], "Should include name column"
        assert "email" in result["data"][0], "Should include email column"
        assert "age" in result["data"][0], "Should include age column"
        assert "id" not in result["data"][0], "Should not include id column"
        print("SUCCESS: Select specific columns test passed")
        
        # Test 3: Simple where condition
        print("\n--- Test 3: Simple Where Condition ---")
        result = db.select("test_users", ["name", "department"], where={"department": "Engineering"})
        print(f"Simple where result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 3, "Should return 3 Engineering records"
        for record in result["data"]:
            assert record["department"] == "Engineering", "All records should be from Engineering"
        print("SUCCESS: Simple where condition test passed")
        
        # Test 4: Regular where conditions with operators
        print("\n--- Test 4: Regular Where Conditions ---")
        result = db.select("test_users", ["name", "age"], where={"age": {">": 30, "<": 40}})
        print(f"Regular where result: {result}")
        assert result["data"], "Should return data"
        for record in result["data"]:
            assert 30 < record["age"] < 40, f"Age should be between 30 and 40, got {record['age']}"
        print("SUCCESS: Regular where conditions test passed")
        
        # Test 5: LIKE operator
        print("\n--- Test 5: LIKE Operator ---")
        result = db.select("test_users", ["name", "email"], where={"name": {"LIKE": "A%"}})
        print(f"LIKE operator result: {result}")
        assert result["data"], "Should return data"
        for record in result["data"]:
            assert record["name"].startswith("A"), f"Name should start with 'A', got {record['name']}"
        print("SUCCESS: LIKE operator test passed")
        
        # Test 6: IN operator
        print("\n--- Test 6: IN Operator ---")
        result = db.select("test_users", ["name", "department"], 
                          where={"department": {"IN": ["Engineering", "Sales"]}})
        print(f"IN operator result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 4, "Should return 4 records (3 Engineering + 1 Sales)"
        for record in result["data"]:
            assert record["department"] in ["Engineering", "Sales"], "Department should be Engineering or Sales"
        print("SUCCESS: IN operator test passed")
        
        # Test 7: Comprehensive where condition
        print("\n--- Test 7: Comprehensive Where Condition ---")
        result = db.select("test_users", ["name", "status"], 
                          where={"status": {"operator": "=", "value": "active", "placeholder": "Y"}})
        print(f"Comprehensive where result: {result}")
        assert result["data"], "Should return data"
        for record in result["data"]:
            assert record["status"] == "active", "Status should be active"
        print("SUCCESS: Comprehensive where condition test passed")
        
        # Test 8: Select with options - LIMIT
        print("\n--- Test 8: Select with LIMIT Option ---")
        result = db.select("test_users", ["name"], 
                          where={"email": {"LIKE": "%@selecttest.com"}},
                          options={"limit": 2})
        print(f"LIMIT option result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 2, "Should return exactly 2 records"
        print("SUCCESS: LIMIT option test passed")
        
        # Test 9: Select with options - ORDER BY
        print("\n--- Test 9: Select with ORDER BY Option ---")
        result = db.select("test_users", ["name", "age"], 
                          where={"email": {"LIKE": "%@selecttest.com"}},
                          options={"order_by": ["age DESC"]})
        print(f"ORDER BY option result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 5, "Should return all 5 records"
        # Verify descending order by age
        ages = [record["age"] for record in result["data"]]
        assert ages == sorted(ages, reverse=True), "Ages should be in descending order"
        print("SUCCESS: ORDER BY option test passed")
        
        # Test 10: Select with combined options
        print("\n--- Test 10: Combined Options (ORDER BY + LIMIT) ---")
        result = db.select("test_users", ["name", "salary"], 
                          where={"email": {"LIKE": "%@selecttest.com"}},
                          options={"order_by": ["salary DESC"], "limit": 3})
        print(f"Combined options result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 3, "Should return exactly 3 records"
        # Verify descending order by salary
        salaries = [float(record["salary"]) for record in result["data"]]
        assert salaries == sorted(salaries, reverse=True), "Salaries should be in descending order"
        print("SUCCESS: Combined options test passed")
        
        # Test 11: Select with no results
        print("\n--- Test 11: Select with No Results ---")
        result = db.select("test_users", ["name"], where={"email": "nonexistent@test.com"})
        print(f"No results select result: {result}")
        assert len(result["data"]) == 0, "Should return empty data array"
        assert result["count"] == 0, "Count should be 0"
        print("SUCCESS: No results test passed")
        
        # Test 12: Count functionality
        print("\n--- Test 12: Count Functionality ---")
        result = db.select("test_users", ["name"], where={"status": "active"})
        print(f"Count functionality result: {result}")
        assert result["data"], "Should return data"
        assert result["count"] > 0, "Count should be greater than 0"
        assert len(result["data"]) == result["count"], "Data length should match count"
        print("SUCCESS: Count functionality test passed")
        
        # Test 13: Select with JSON metadata (database-specific)
        print("\n--- Test 13: JSON Metadata Handling ---")
        result = db.select("test_users", ["name", "metadata"], 
                          where={"name": "Alice Johnson"})
        print(f"JSON metadata result: {result}")
        assert result["data"], "Should return data"
        assert len(result["data"]) == 1, "Should return exactly 1 record"
        metadata = result["data"][0]["metadata"]
        if isinstance(metadata, str):
            import json
            metadata = json.loads(metadata)
        assert metadata["level"] == "Senior", "Should have correct metadata level"
        print("SUCCESS: JSON metadata handling test passed")
        
        # Clean up test data
        cleanup_test_data(db)
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")


def test_error_handling():
    """Test error handling for select operations"""
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
        
        # Test invalid table name
        print("\n--- Test: Invalid Table Name ---")
        try:
            result = db.select("nonexistent_table", ["*"])
            # Some databases might return empty result instead of error
            print(f"Result for invalid table: {result}")
            print("SUCCESS: Invalid table handled gracefully")
        except Exception as e:
            print(f"SUCCESS: Correctly caught invalid table error: {e}")
        
        # Test invalid column name (if we have test data)
        print("\n--- Test: Invalid Column Name ---")
        try:
            # This might not fail in all databases, depends on implementation
            result = db.select("test_users", ["nonexistent_column"])
            print(f"Result for invalid column: {result}")
            print("SUCCESS: Invalid column handled gracefully")
        except Exception as e:
            print(f"SUCCESS: Correctly caught invalid column error: {e}")
    
    pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Select Function Test Suite ===")
    
    # Test database and backend combinations
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    # Get active test configurations dynamically
    test_combinations = []
    active_configs = test_utils.get_active_test_configs(cfg)
    
    for db_key, backend in active_configs:
         test_combinations.append((db_key, backend, test_sql_database))

    # Test all available database/backend combinations, abort on first failure
    combinations_tested = 0
    for db_key, backend, test_function in test_combinations:
        try:
            # Check if database configuration exists (redundant with get_active_test_configs but safe)
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
    
    print("\n=== Select Test Suite Complete ===")