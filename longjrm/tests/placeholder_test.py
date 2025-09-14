"""
Test named placeholder functionality in longjrm database operations.
Tests both positional and named placeholder support across different database types.
"""

import logging
import traceback
from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

logger = logging.getLogger(__name__)

def test_named_placeholders_mysql_postgres(db_key, backend):
    """Test named placeholder functionality for MySQL/PostgreSQL"""
    print(f"\n=== Testing {db_key} Named Placeholder Operations ({backend.value}) ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    # Test using context manager for automatic connection management
    with pools[db_key].client() as client:
        db = Db(client)
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Test 1: Colon-style named placeholders
        try:
            print("\n--- Test 1: Colon-style Named Placeholders (:name) ---")
            result = db.query(
                "SELECT 1 as test_id, :name as name, :age as age",
                {"name": "John Doe", "age": 30}
            )
            assert result["count"] == 1
            assert result["data"][0]["name"] == "John Doe"
            assert result["data"][0]["age"] == 30
            print("SUCCESS: Colon-style named placeholders work correctly")
        except Exception as e:
            print(f"FAILED: Colon-style named placeholders - {e}")
            raise
        
        # Test 2: Percent-style named placeholders
        try:
            print("\n--- Test 2: Percent-style Named Placeholders (%(name)s) ---")
            result = db.query(
                "SELECT 2 as test_id, %(username)s as username, %(score)s as score",
                {"username": "jane_smith", "score": 95}
            )
            assert result["count"] == 1
            assert result["data"][0]["username"] == "jane_smith"
            assert result["data"][0]["score"] == 95
            print("SUCCESS: Percent-style named placeholders work correctly")
        except Exception as e:
            print(f"FAILED: Percent-style named placeholders - {e}")
            raise
        
        # Test 3: Dollar-style named placeholders
        try:
            print("\n--- Test 3: Dollar-style Named Placeholders ($name) ---")
            result = db.query(
                "SELECT 3 as test_id, $email as email, $active as active",
                {"email": "test@example.com", "active": True}
            )
            assert result["count"] == 1
            assert result["data"][0]["email"] == "test@example.com"
            assert result["data"][0]["active"] == True
            print("SUCCESS: Dollar-style named placeholders work correctly")
        except Exception as e:
            print(f"FAILED: Dollar-style named placeholders - {e}")
            raise
        
        # Test 4: Mixed named placeholder styles (should fail)
        try:
            print("\n--- Test 4: Mixed Named Placeholder Styles (should fail) ---")
            try:
                result = db.query(
                    "SELECT :name as name, %(age)s as age",
                    {"name": "John", "age": 25}
                )
                assert False, "Should have raised ValueError for mixed placeholder styles"
            except ValueError as e:
                print(f"SUCCESS: Correctly caught mixed placeholder error: {e}")
        except Exception as e:
            print(f"FAILED: Mixed placeholder validation - {e}")
            raise
        
        # Test 5: Positional placeholders (backward compatibility)
        try:
            print("\n--- Test 5: Positional Placeholders (backward compatibility) ---")
            result = db.query(
                f"SELECT 5 as test_id, {db.placeholder} as name, {db.placeholder} as age",
                ["Alice Johnson", 28]
            )
            assert result["count"] == 1
            assert result["data"][0]["name"] == "Alice Johnson"
            assert result["data"][0]["age"] == 28
            print("SUCCESS: Positional placeholders still work correctly")
        except Exception as e:
            print(f"FAILED: Positional placeholders - {e}")
            raise
        
        # Test 6: Execute with named placeholders
        try:
            print("\n--- Test 6: Execute with Named Placeholders ---")
            
            # Create test table
            if db.database_type in ['postgres', 'postgresql']:
                create_sql = """
                CREATE TEMPORARY TABLE test_named_placeholders (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    age INTEGER,
                    email VARCHAR(100)
                )
                """
            else:  # mysql
                create_sql = """
                CREATE TEMPORARY TABLE test_named_placeholders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    age INTEGER,
                    email VARCHAR(100)
                )
                """
            
            db.execute(create_sql)
            print("SUCCESS: Test table created")
            
            # Insert with named placeholders
            result = db.execute(
                "INSERT INTO test_named_placeholders (name, age, email) VALUES (:name, :age, :email)",
                {"name": "Test User", "age": 35, "email": "testuser@example.com"}
            )
            assert result["status"] == 0
            assert result["total"] == 1
            print("SUCCESS: INSERT with named placeholders works")
            
            # Select back the data
            result = db.query(
                "SELECT * FROM test_named_placeholders WHERE name = :name",
                {"name": "Test User"}
            )
            assert result["count"] == 1
            assert result["data"][0]["name"] == "Test User"
            assert result["data"][0]["age"] == 35
            print("SUCCESS: SELECT with named placeholders works")
            
        except Exception as e:
            print(f"FAILED: Execute with named placeholders - {e}")
            raise
        
        # Test 7: Prepared statements with named placeholders
        try:
            print("\n--- Test 7: Prepared Statements with Named Placeholders ---")
            
            values_list = [
                {"name": "User1", "age": 25, "email": "user1@test.com"},
                {"name": "User2", "age": 30, "email": "user2@test.com"},
                {"name": "User3", "age": 35, "email": "user3@test.com"}
            ]
            
            results = db.execute_prepared(
                "INSERT INTO test_named_placeholders (name, age, email) VALUES (:name, :age, :email)",
                values_list
            )
            
            assert len(results) == 3
            for result in results:
                assert result["status"] == 0
                assert result["total"] == 1
            
            print("SUCCESS: Prepared execute with named placeholders works")
            
            # Query with prepared statements
            query_values = [
                {"min_age": 25},
                {"min_age": 30},
                {"min_age": 35}
            ]
            
            results = db.query_prepared(
                "SELECT COUNT(*) as count FROM test_named_placeholders WHERE age >= :min_age",
                query_values
            )
            
            assert len(results) == 3
            assert results[0]["data"][0]["count"] >= 4  # Original + 3 new records
            assert results[1]["data"][0]["count"] >= 3  # 3 new records >= 30
            assert results[2]["data"][0]["count"] >= 1  # 1 new record >= 35
            
            print("SUCCESS: Prepared query with named placeholders works")
            
        except Exception as e:
            print(f"FAILED: Prepared statements with named placeholders - {e}")
            raise
        
        print(f"SUCCESS: All named placeholder tests passed for {db_key}")
    
    # Test 8: Transaction management with manual connection
    print(f"\n--- Test 8: Transaction Management (Manual Connection) ---")
    client = pools[db_key].get_client()
    try:
        db = Db(client)
        
        # Begin transaction
        db.execute("BEGIN")
        
        # Insert data in transaction
        db.execute(
            "INSERT INTO test_named_placeholders (name, age, email) VALUES (:name, :age, :email)",
            {"name": "Transaction User", "age": 40, "email": "tx@test.com"}
        )
        
        # Verify data exists in transaction
        result = db.query(
            "SELECT COUNT(*) as count FROM test_named_placeholders WHERE name = :name",
            {"name": "Transaction User"}
        )
        assert result["data"][0]["count"] == 1
        print("SUCCESS: Data visible within transaction")
        
        # Commit transaction
        db.execute("COMMIT")
        
        # Verify data persists after commit
        result = db.query(
            "SELECT * FROM test_named_placeholders WHERE name = :name",
            {"name": "Transaction User"}
        )
        assert result["count"] == 1
        assert result["data"][0]["email"] == "tx@test.com"
        print("SUCCESS: Transaction committed successfully")
        
    finally:
        pools[db_key].close_client(client)
    
    print(f"SUCCESS: Manual connection management test passed for {db_key}")
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} connection closed")

def test_error_handling():
    """Test error handling for named placeholders"""
    print(f"\n=== Testing Named Placeholder Error Handling ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    
    # Test with first available database
    available_dbs = ["postgres-test", "mysql-test"]
    db_key = None
    for test_db in available_dbs:
        try:
            cfg.require(test_db)
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
        
        # Test 1: Missing named parameters
        try:
            print("\n--- Test: Missing Named Parameters ---")
            try:
                result = db.query(
                    "SELECT :name as name, :age as age, :email as email",
                    {"name": "John", "age": 25}  # Missing email parameter
                )
                assert False, "Should have raised ValueError for missing parameters"
            except ValueError as e:
                print(f"SUCCESS: Correctly caught missing parameters error: {e}")
        except Exception as e:
            print(f"FAILED: Missing parameters validation - {e}")
            
        # Test 2: Empty parameters with named placeholders
        try:
            print("\n--- Test: Empty Parameters with Named Placeholders ---")
            try:
                result = db.query(
                    "SELECT :name as name",
                    {}  # Empty dict
                )
                assert False, "Should have raised ValueError for missing parameters"
            except ValueError as e:
                print(f"SUCCESS: Correctly caught empty parameters error: {e}")
        except Exception as e:
            print(f"FAILED: Empty parameters validation - {e}")
        
        print("SUCCESS: Error handling tests completed")
    
    pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Named Placeholder Test Suite ===")
    
    # Test database and backend combinations
    test_combinations = [
        ("postgres-test", PoolBackend.DBUTILS),
        ("postgres-test", PoolBackend.SQLALCHEMY),
        ("mysql-test", PoolBackend.DBUTILS),
        ("mysql-test", PoolBackend.SQLALCHEMY)
    ]
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    # Test all available database/backend combinations, abort on first failure
    for db_key, backend in test_combinations:
        try:
            cfg.require(db_key)
            test_named_placeholders_mysql_postgres(db_key, backend)
        except Exception as e:
            if "Unknown database key" in str(e):
                print(f"Database {db_key} not configured, skipping...")
                continue
            else:
                logger.error(f"Test failed for {db_key} with {backend}: {e}")
                logger.error(traceback.format_exc())
                print(f"ABORT: Test suite aborted due to failure in {db_key} with {backend}")
                exit(1)
    
    # Test error handling
    try:
        test_error_handling()
    except Exception as e:
        logger.error(f"Error handling test failed: {e}")
        logger.error(traceback.format_exc())
        print("ABORT: Error handling test failed")
        exit(1)
    
    print("\n=== All Named Placeholder Tests Completed Successfully ===")