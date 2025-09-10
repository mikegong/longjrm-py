"""
JRM Delete Function Test Suite

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/delete_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/delete_test.py

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
    """Setup test data for delete operations"""
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        
        if db.database_type in ['mysql', 'postgres', 'postgresql']:
            # Drop existing table to ensure correct structure
            db.execute("DROP TABLE IF EXISTS test_users")
            db.execute(create_table_sql)
            print("SUCCESS: Test table created/verified")
            
            # Insert test data for delete operations
            test_records = [
                {
                    "name": "John Doe",
                    "email": "john@deletetest.com", 
                    "age": 30,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Senior"},
                    "tags": "developer,python,backend"
                },
                {
                    "name": "Jane Smith",
                    "email": "jane@deletetest.com",
                    "age": 28,
                    "status": "active", 
                    "metadata": {"department": "Marketing", "level": "Manager"},
                    "tags": "marketing,strategy"
                },
                {
                    "name": "Bob Wilson",
                    "email": "bob@deletetest.com", 
                    "age": 35,
                    "status": "inactive",
                    "metadata": {"department": "Sales", "level": "Director"},
                    "tags": "sales,b2b"
                },
                {
                    "name": "Alice Brown",
                    "email": "alice@deletetest.com",
                    "age": 26,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Junior"},
                    "tags": "developer,frontend,react"
                },
                {
                    "name": "Charlie Green",
                    "email": "charlie@deletetest.com",
                    "age": 32,
                    "status": "pending",
                    "metadata": {"department": "DevOps", "level": "Senior"},
                    "tags": "devops,kubernetes,aws"
                }
            ]
            
            # Insert test records
            result = db.insert("test_users", test_records)
            if result['status'] != 0:
                raise Exception(f"Failed to insert test records: {result.get('message', 'Unknown error')}")
            print(f"SUCCESS: Inserted {result['total']} test records for delete testing")
            
            # Verify records were inserted
            count_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE email LIKE '%@deletetest.com'")
            if count_result["data"]:
                total_records = count_result["data"][0]["total"]
                print(f"SUCCESS: {total_records} test records available for delete testing")
                return total_records
            else:
                print("WARNING: Could not verify record count")
                return len(test_records)
        
        elif db.database_type in ['mongodb', 'mongodb+srv']:
            # MongoDB test data setup
            test_docs = [
                {
                    "name": "John Doe",
                    "email": "john@deletetest.com", 
                    "age": 30,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Senior"},
                    "tags": ["developer", "python", "backend"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Jane Smith",
                    "email": "jane@deletetest.com",
                    "age": 28,
                    "status": "active", 
                    "metadata": {"department": "Marketing", "level": "Manager"},
                    "tags": ["marketing", "strategy"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Bob Wilson",
                    "email": "bob@deletetest.com", 
                    "age": 35,
                    "status": "inactive",
                    "metadata": {"department": "Sales", "level": "Director"},
                    "tags": ["sales", "b2b"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Alice Brown",
                    "email": "alice@deletetest.com",
                    "age": 26,
                    "status": "active",
                    "metadata": {"department": "Engineering", "level": "Junior"},
                    "tags": ["developer", "frontend", "react"],
                    "created_at": datetime.datetime.now()
                },
                {
                    "name": "Charlie Green",
                    "email": "charlie@deletetest.com",
                    "age": 32,
                    "status": "pending",
                    "metadata": {"department": "DevOps", "level": "Senior"},
                    "tags": ["devops", "kubernetes", "aws"],
                    "created_at": datetime.datetime.now()
                }
            ]
            
            # Insert test documents
            result = db.insert("test_users", test_docs)
            if result['status'] != 0:
                raise Exception(f"Failed to insert test documents: {result.get('message', 'Unknown error')}")
            print(f"SUCCESS: Inserted {result['total']} test documents for delete testing")
            return len(test_docs)
            
    except Exception as e:
        print(f"ERROR: Failed to setup test data: {e}")
        raise

def test_sql_database(db_key, backend=PoolBackend.DBUTILS):
    """Test delete functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} Delete Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    # Use specified backend
    pools[db_key] = Pool.from_config(db_cfg, backend)
    client = pools[db_key].get_client()
    
    db = Db(client)
    
    try:
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Setup test data
        try:
            initial_count = setup_test_data(db, db_key)
            print(f"SUCCESS: Test data setup completed with {initial_count} records")
        except Exception as e:
            error_msg = f"FAILED: Test data setup failed: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test setup failed for {db_key}: {e}")
        
        # Test 1: Delete with simple where condition
        try:
            print("\n--- Test 1: Delete with Simple Where Condition ---")
            where_simple = {"status": "inactive"}
            result = db.delete("test_users", where_simple)
            print(f"Simple where delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Simple where delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            if result["total"] < 1:
                raise Exception(f"Simple where delete should affect at least 1 row, but affected {result['total']}")
            
            # Verify deletion
            verify_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE status = 'inactive' AND email LIKE '%@deletetest.com'")
            remaining_inactive = verify_result["data"][0]["total"] if verify_result["data"] else 0
            print(f"Verification: {remaining_inactive} inactive records remain (should be 0)")
            
            if remaining_inactive != 0:
                raise Exception(f"Delete verification failed: {remaining_inactive} inactive records still remain")
            
            print("SUCCESS: Test 1 passed - Simple where condition delete")
        except Exception as e:
            error_msg = f"FAILED: Test 1 - Simple where condition delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 1 failed for {db_key}: {e}")
        
        # Test 2: Delete with regular where condition (age-based)
        try:
            print("\n--- Test 2: Delete with Regular Where Condition ---")
            where_regular = {"age": {">": 30}}
            result = db.delete("test_users", where_regular)
            print(f"Regular where delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Regular where delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            # Verify deletion
            verify_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE age > 30 AND email LIKE '%@deletetest.com'")
            remaining_older = verify_result["data"][0]["total"] if verify_result["data"] else 0
            print(f"Verification: {remaining_older} records with age > 30 remain (should be 0)")
            
            if remaining_older != 0:
                raise Exception(f"Delete verification failed: {remaining_older} records with age > 30 still remain")
            
            print("SUCCESS: Test 2 passed - Regular where condition delete")
        except Exception as e:
            error_msg = f"FAILED: Test 2 - Regular where condition delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 2 failed for {db_key}: {e}")
        
        # Test 3: Delete with comprehensive where condition
        try:
            print("\n--- Test 3: Delete with Comprehensive Where Condition ---")
            where_comprehensive = {"age": {"operator": "=", "value": 28, "placeholder": "Y"}}
            result = db.delete("test_users", where_comprehensive)
            print(f"Comprehensive where delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Comprehensive where delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 3 passed - Comprehensive where condition delete")
        except Exception as e:
            error_msg = f"FAILED: Test 3 - Comprehensive where condition delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 3 failed for {db_key}: {e}")
        
        # Test 4: Delete with multiple conditions (AND logic)
        try:
            print("\n--- Test 4: Delete with Multiple Conditions ---")
            # First, let's see what records remain
            remaining_result = db.query("SELECT name, age, status FROM test_users WHERE email LIKE '%@deletetest.com'")
            print(f"Remaining records count: {remaining_result['count']}")
            
            if remaining_result["count"] > 0:
                # Find a record that exists to delete
                sample_record = remaining_result["data"][0]
                where_multiple = {
                    "name": sample_record["name"],
                    "status": sample_record["status"]
                }
                result = db.delete("test_users", where_multiple)
                print(f"Multiple conditions delete result: {result}")
                
                if result["status"] != 0:
                    raise Exception(f"Multiple conditions delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 4 passed - Multiple conditions delete")
        except Exception as e:
            error_msg = f"FAILED: Test 4 - Multiple conditions delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 4 failed for {db_key}: {e}")
        
        # Test 5: Delete remaining test records (cleanup with LIKE condition)
        try:
            print("\n--- Test 5: Delete Remaining Test Records ---")
            # Count remaining test records first
            count_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE email LIKE '%@deletetest.com'")
            remaining_count = count_result["data"][0]["total"] if count_result["data"] else 0
            print(f"Records remaining before final cleanup: {remaining_count}")
            
            if remaining_count > 0:
                # Delete remaining test records with where condition for safety
                where_cleanup = {"email": {"LIKE": "%@deletetest.com"}}
                result = db.delete("test_users", where_cleanup)
                print(f"Cleanup delete result: {result}")
                
                if result["status"] != 0:
                    raise Exception(f"Cleanup delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 5 passed - Cleanup delete with LIKE condition")
        except Exception as e:
            error_msg = f"FAILED: Test 5 - Cleanup delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 5 failed for {db_key}: {e}")
        
        # Test 6: Delete from empty result set (should affect 0 rows)
        try:
            print("\n--- Test 6: Delete from Empty Result Set ---")
            where_empty = {"email": "nonexistent@deletetest.com"}
            result = db.delete("test_users", where_empty)
            print(f"Empty delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Delete with no matches failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            if result["total"] != 0:
                raise Exception(f"Delete with no matches should affect 0 rows, but affected {result['total']}")
            
            print("SUCCESS: Test 6 passed - Delete from empty result set")
        except Exception as e:
            error_msg = f"FAILED: Test 6 - Delete from empty result set: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 6 failed for {db_key}: {e}")
        
        # Test 7: Delete with CURRENT keyword test (insert with CURRENT, then delete)
        try:
            print("\n--- Test 7: Delete with CURRENT Keywords Test ---")
            # First insert a record for this test
            current_record = {
                "name": "Test Current",
                "email": "current@deletetest.com",
                "age": 99,
                "status": "temp",
                "metadata": {"test": "current"},
                "tags": "test",
                "created_at": "`CURRENT_TIMESTAMP`"
            }
            
            insert_result = db.insert("test_users", current_record)
            print(f"Insert for CURRENT test: {insert_result}")
            
            if insert_result["status"] != 0:
                raise Exception(f"Insert for CURRENT test failed: {insert_result.get('message', 'Unknown error')}")
            
            # Now delete using a different condition (not CURRENT in where, as that's complex)
            where_current = {"status": "temp"}
            result = db.delete("test_users", where_current)
            print(f"Delete with temp status result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Delete temp record failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 7 passed - CURRENT keyword handling test")
        except Exception as e:
            error_msg = f"FAILED: Test 7 - CURRENT keyword test: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 7 failed for {db_key}: {e}")
        
        # Final verification - should have no test records
        try:
            print("\n--- Final Verification ---")
            final_count_result = db.query("SELECT COUNT(*) as total FROM test_users WHERE email LIKE '%@deletetest.com'")
            final_count = final_count_result["data"][0]["total"] if final_count_result["data"] else 0
            print(f"Final test record count: {final_count} (should be 0)")
            
            if final_count != 0:
                print(f"WARNING: {final_count} test records still remain - attempting final cleanup")
                cleanup_result = db.execute("DELETE FROM test_users WHERE email LIKE '%@deletetest.com'")
                print(f"Final cleanup result: {cleanup_result}")
            
            print("SUCCESS: All SQL delete tests completed successfully")
        except Exception as e:
            error_msg = f"FAILED: Final verification: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Final verification failed for {db_key}: {e}")
        
    except Exception as e:
        logger.error(f"SQL database test failed with error: {e}")
        print(f"ABORTING: {db_key} ({backend.value}) delete tests failed")
        raise
    finally:
        # Final cleanup
        try:
            db.execute("DELETE FROM test_users WHERE email LIKE '%@deletetest.com'")
            print("Final cleanup completed")
        except:
            pass
        
        pools[db_key].close_client(client)
        pools[db_key].dispose()
        print(f"Connection closed for {db_key}")

def test_mongodb_database(db_key):
    """Test delete functionality for MongoDB"""
    print(f"\n=== Testing {db_key} Delete Operations ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, PoolBackend.MONGODB)
    client = pools[db_key].get_client()
    
    db = Db(client)
    
    try:
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Setup test data
        try:
            initial_count = setup_test_data(db, db_key)
            print(f"SUCCESS: Test data setup completed with {initial_count} documents")
        except Exception as e:
            error_msg = f"FAILED: Test data setup failed: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test setup failed for {db_key}: {e}")
        
        # Test 1: Delete with simple where condition
        try:
            print("\n--- Test 1: Delete with Simple Where Condition ---")
            where_simple = {"status": "inactive"}
            result = db.delete("test_users", where_simple)
            print(f"Simple where delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Simple where delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            if result["total"] < 1:
                raise Exception(f"Simple where delete should affect at least 1 document, but affected {result['total']}")
            
            # Verify deletion
            collection = client["conn"][db.database_name]["test_users"]
            remaining_inactive = collection.count_documents({"status": "inactive", "email": {"$regex": "@deletetest.com$"}})
            print(f"Verification: {remaining_inactive} inactive documents remain (should be 0)")
            
            if remaining_inactive != 0:
                raise Exception(f"Delete verification failed: {remaining_inactive} inactive documents still remain")
            
            print("SUCCESS: Test 1 passed - Simple where condition delete")
        except Exception as e:
            error_msg = f"FAILED: Test 1 - Simple where condition delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 1 failed for {db_key}: {e}")
        
        # Test 2: Delete with MongoDB query operators
        try:
            print("\n--- Test 2: Delete with MongoDB Query Operators ---")
            where_mongo = {"age": {"$gt": 30}}
            result = db.delete("test_users", where_mongo)
            print(f"MongoDB operators delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"MongoDB operators delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            # Verify deletion
            collection = client["conn"][db.database_name]["test_users"]
            remaining_older = collection.count_documents({"age": {"$gt": 30}, "email": {"$regex": "@deletetest.com$"}})
            print(f"Verification: {remaining_older} documents with age > 30 remain (should be 0)")
            
            if remaining_older != 0:
                raise Exception(f"Delete verification failed: {remaining_older} documents with age > 30 still remain")
            
            print("SUCCESS: Test 2 passed - MongoDB query operators delete")
        except Exception as e:
            error_msg = f"FAILED: Test 2 - MongoDB query operators delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 2 failed for {db_key}: {e}")
        
        # Test 3: Delete with multiple conditions
        try:
            print("\n--- Test 3: Delete with Multiple Conditions ---")
            # Check what documents remain
            collection = client["conn"][db.database_name]["test_users"]
            remaining_docs = list(collection.find({"email": {"$regex": "@deletetest.com$"}}))
            print(f"Remaining documents count: {len(remaining_docs)}")
            
            if remaining_docs:
                # Delete documents with specific conditions
                where_multiple = {
                    "status": "active",
                    "email": {"$regex": "@deletetest.com$"}
                }
                result = db.delete("test_users", where_multiple)
                print(f"Multiple conditions delete result: {result}")
                
                if result["status"] != 0:
                    raise Exception(f"Multiple conditions delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 3 passed - Multiple conditions delete")
        except Exception as e:
            error_msg = f"FAILED: Test 3 - Multiple conditions delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 3 failed for {db_key}: {e}")
        
        # Test 4: Delete all remaining test documents
        try:
            print("\n--- Test 4: Delete All Remaining Test Documents ---")
            collection = client["conn"][db.database_name]["test_users"]
            remaining_count = collection.count_documents({"email": {"$regex": "@deletetest.com$"}})
            print(f"Documents remaining before cleanup: {remaining_count}")
            
            if remaining_count > 0:
                where_cleanup = {"email": {"$regex": "@deletetest.com$"}}
                result = db.delete("test_users", where_cleanup)
                print(f"Cleanup delete result: {result}")
                
                if result["status"] != 0:
                    raise Exception(f"Cleanup delete failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            
            print("SUCCESS: Test 4 passed - Cleanup delete")
        except Exception as e:
            error_msg = f"FAILED: Test 4 - Cleanup delete: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 4 failed for {db_key}: {e}")
        
        # Test 5: Delete from empty result set
        try:
            print("\n--- Test 5: Delete from Empty Result Set ---")
            where_empty = {"email": "nonexistent@deletetest.com"}
            result = db.delete("test_users", where_empty)
            print(f"Empty delete result: {result}")
            
            if result["status"] != 0:
                raise Exception(f"Delete with no matches failed with status {result['status']}: {result.get('message', 'Unknown error')}")
            if result["total"] != 0:
                raise Exception(f"Delete with no matches should affect 0 documents, but affected {result['total']}")
            
            print("SUCCESS: Test 5 passed - Delete from empty result set")
        except Exception as e:
            error_msg = f"FAILED: Test 5 - Delete from empty result set: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Test 5 failed for {db_key}: {e}")
        
        # Final verification
        try:
            print("\n--- Final Verification ---")
            collection = client["conn"][db.database_name]["test_users"]
            final_count = collection.count_documents({"email": {"$regex": "@deletetest.com$"}})
            print(f"Final test document count: {final_count} (should be 0)")
            
            if final_count != 0:
                print(f"WARNING: {final_count} test documents still remain - attempting final cleanup")
                collection.delete_many({"email": {"$regex": "@deletetest.com$"}})
                print("Final cleanup completed")
            
            print("SUCCESS: All MongoDB delete tests completed successfully")
        except Exception as e:
            error_msg = f"FAILED: Final verification: {e}"
            logger.error(error_msg)
            print(error_msg)
            raise Exception(f"Final verification failed for {db_key}: {e}")
        
    except Exception as e:
        logger.error(f"MongoDB test failed with error: {e}")
        print(f"ABORTING: {db_key} delete tests failed")
        raise
    finally:
        # Final cleanup
        try:
            collection = client["conn"][db.database_name]["test_users"]
            collection.delete_many({"email": {"$regex": "@deletetest.com$"}})
            print("Final cleanup completed")
        except:
            pass
        
        pools[db_key].close_client(client)
        pools[db_key].dispose()
        print(f"Connection closed for {db_key}")

def test_error_handling():
    """Test error handling for delete operations"""
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
    client = pools[db_key].get_client()
    
    db = Db(client)
    
    try:
        print(f"Connected to {db.database_type} database: {db.database_name}")
        
        # Test 1: Delete from non-existent table
        try:
            print("\n--- Test 1: Delete from Non-existent Table ---")
            result = db.delete("non_existent_table", {"id": 1})
            if result["status"] != 0:
                print(f"SUCCESS: Correctly handled non-existent table: {result['message']}")
            else:
                print("WARNING: Delete from non-existent table did not fail as expected")
        except Exception as e:
            print(f"SUCCESS: Correctly caught non-existent table error: {e}")
        
        # Test 2: Invalid where condition syntax
        try:
            print("\n--- Test 2: Invalid Where Condition ---")
            # Create a valid test table first
            db.execute("CREATE TABLE IF NOT EXISTS test_error_table (id INT, name VARCHAR(100))")
            db.execute("INSERT INTO test_error_table (id, name) VALUES (1, 'test')")
            
            # Try invalid where condition (this might not fail in all databases)
            invalid_where = {"nonexistent_column": "value"}
            result = db.delete("test_error_table", invalid_where)
            
            if result["status"] != 0:
                print(f"SUCCESS: Correctly handled invalid where condition: {result['message']}")
            else:
                print(f"INFO: Invalid where condition was processed (affected {result['total']} rows)")
                
        except Exception as e:
            print(f"SUCCESS: Correctly caught invalid where condition error: {e}")
        finally:
            # Cleanup test table
            try:
                db.execute("DROP TABLE IF EXISTS test_error_table")
                print("Test table cleanup completed")
            except:
                pass
        
        print("SUCCESS: Error handling tests completed")
        
    except Exception as e:
        logger.error(f"Error handling test failed: {e}")
        print(f"FAILED: Error handling tests: {e}")
        raise Exception(f"Error handling tests failed: {e}")
    finally:
        pools[db_key].close_client(client)
        pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Delete Function Test Suite ===")
    print("Testing delete operations with abort-on-failure behavior")
    
    # Test database and backend combinations
    test_combinations = [
        ("postgres-test", PoolBackend.DBUTILS, test_sql_database),
        ("postgres-test", PoolBackend.SQLALCHEMY, test_sql_database),
        ("mysql-test", PoolBackend.DBUTILS, test_sql_database),
        ("mysql-test", PoolBackend.SQLALCHEMY, test_sql_database),
        ("mongodb-test", PoolBackend.MONGODB, test_mongodb_database)
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
            if backend == PoolBackend.MONGODB:
                test_function(db_key)
            else:
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
    
    print(f"\n=== Delete Test Suite Complete ===")
    print(f"Successfully tested {combinations_tested} database/backend combinations")
    print("All tests passed with abort-on-failure behavior")