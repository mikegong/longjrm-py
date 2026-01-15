"""
JRM Stream Operations Test Suite

Tests for stream_query and stream_insert functionality.

To run this test, you have two options:

Option 1: Install longjrm in development mode (recommended):
    cd longjrm-py
    pip install -e .
    python longjrm/tests/stream_test.py

Option 2: Run directly with path modification (for quick testing):
    cd longjrm-py  
    python longjrm/tests/stream_test.py

Make sure your test_config/dbinfos.json file contains valid database configurations.
"""

import logging
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

def setup_test_data(db, database_type):
    """Set up test data for stream operations"""
    print("\n--- Setting up test data for streaming ---")
    
    # Create test tables if they don't exist (drop and recreate to ensure correct schema)
    try:
        # Drop the tables first to ensure clean schema
        try:
            db.execute("DROP TABLE IF EXISTS test_stream_users")
            db.execute("DROP TABLE IF EXISTS test_stream_target")
            print("SUCCESS: Dropped existing test tables")
        except:
            pass  # Tables might not exist
        
        if database_type in ['postgres', 'postgresql']:
            create_table_sql = """
            CREATE TABLE test_stream_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(20) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata JSONB,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            create_target_sql = """
            CREATE TABLE test_stream_target (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(20),
                department VARCHAR(50)
            )
            """
        else:  # MySQL
            create_table_sql = """
            CREATE TABLE test_stream_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(20) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata JSON,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            create_target_sql = """
            CREATE TABLE test_stream_target (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(20),
                department VARCHAR(50)
            )
            """
        
        db.execute(create_table_sql)
        db.execute(create_target_sql)
        print("SUCCESS: Test tables created with fresh schema")
    except Exception as e:
        print(f"WARNING: Could not create test tables: {e}")
        return False
    
    # Insert comprehensive test data
    test_records = [
        {
            "name": "Alice Johnson",
            "email": "alice@streamtest.com",
            "age": 28,
            "status": "active",
            "department": "Engineering",
            "salary": 75000.50,
            "metadata": {"level": "Senior", "team": "Backend"},
            "tags": "python|django|postgresql"
        },
        {
            "name": "Bob Smith",
            "email": "bob@streamtest.com",
            "age": 35,
            "status": "active",
            "department": "Sales",
            "salary": 65000.00,
            "metadata": {"level": "Manager", "team": "Enterprise"},
            "tags": "sales|b2b|crm"
        },
        {
            "name": "Charlie Brown",
            "email": "charlie@streamtest.com",
            "age": 42,
            "status": "inactive",
            "department": "Engineering",
            "salary": 95000.75,
            "metadata": {"level": "Principal", "team": "Platform"},
            "tags": "architecture|kubernetes|aws"
        },
        {
            "name": "Diana Wilson",
            "email": "diana@streamtest.com",
            "age": 31,
            "status": "active",
            "department": "Marketing",
            "salary": 55000.25,
            "metadata": {"level": "Senior", "team": "Growth"},
            "tags": "marketing|analytics|growth"
        },
        {
            "name": "Eve Davis",
            "email": "eve@streamtest.com",
            "age": 26,
            "status": "active",
            "department": "Engineering",
            "salary": 70000.00,
            "metadata": {"level": "Mid", "team": "Frontend"},
            "tags": "javascript|react|typescript"
        }
    ]
    
    try:
        result = db.insert("test_stream_users", test_records)
        db.commit() # Ensure data is visible to other connections
        print(f"Insert result: {result}")
        assert result["status"] == 0, "Test data insertion should succeed"
        print(f"SUCCESS: Inserted {result['count']} test records")
        return True
    except Exception as e:
        print(f"ERROR: Could not insert test data: {e}")
        return False

def cleanup_test_data(db):
    """Clean up test data after tests"""
    try:
        db.execute("DROP TABLE IF EXISTS test_stream_users")
        db.execute("DROP TABLE IF EXISTS test_stream_target")
        print("SUCCESS: Dropped test tables")
    except Exception as e:
        print(f"WARNING: Could not clean up test data: {e}")

def test_stream_query_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test stream_query functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} stream_query Operations with {backend.value} backend ===")
    
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
        
        # Test 1: Basic streaming - verify generator behavior
        print("\n--- Test 1: Basic Streaming Query ---")
        sql = "SELECT * FROM test_stream_users WHERE email LIKE %s ORDER BY id"
        rows_collected = []
        row_numbers = []
        
        for row_num, row, status in db.stream_query(sql, ["%@streamtest.com"]):
            if status == 0:
                rows_collected.append(row)
                row_numbers.append(row_num)
            else:
                print(f"ERROR: Stream returned error status at row {row_num}")
                break
        
        print(f"Collected {len(rows_collected)} rows via stream")
        assert len(rows_collected) == 5, f"Should stream 5 rows, got {len(rows_collected)}"
        assert row_numbers == [1, 2, 3, 4, 5], f"Row numbers should be sequential 1-5, got {row_numbers}"
        assert all("id" in row for row in rows_collected), "All rows should have 'id' column"
        assert all("name" in row for row in rows_collected), "All rows should have 'name' column"
        print("SUCCESS: Basic streaming test passed")
        
        # Test 2: Verify row data integrity
        print("\n--- Test 2: Row Data Integrity ---")
        sql = "SELECT name, email, age FROM test_stream_users WHERE name = %s"
        
        for row_num, row, status in db.stream_query(sql, ["Alice Johnson"]):
            if status == 0:
                assert row["name"] == "Alice Johnson", f"Name should be 'Alice Johnson', got {row['name']}"
                assert row["email"] == "alice@streamtest.com", f"Email mismatch"
                assert row["age"] == 28, f"Age should be 28, got {row['age']}"
                print(f"Row {row_num}: {row}")
        
        print("SUCCESS: Row data integrity test passed")
        
        # Test 3: Stream with no results
        print("\n--- Test 3: Stream with No Results ---")
        sql = "SELECT * FROM test_stream_users WHERE email = %s"
        rows_collected = []
        
        for row_num, row, status in db.stream_query(sql, ["nonexistent@test.com"]):
            if status == 0:
                rows_collected.append(row)
        
        assert len(rows_collected) == 0, "Should return no rows for non-existent email"
        print("SUCCESS: No results streaming test passed")
        
        # Test 4: Stream with filtering condition
        print("\n--- Test 4: Stream with Filtering ---")
        sql = "SELECT name, department FROM test_stream_users WHERE department = %s"
        engineering_rows = []
        
        for row_num, row, status in db.stream_query(sql, ["Engineering"]):
            if status == 0:
                engineering_rows.append(row)
                assert row["department"] == "Engineering", "All rows should be from Engineering"
        
        assert len(engineering_rows) == 3, f"Should have 3 Engineering records, got {len(engineering_rows)}"
        print(f"SUCCESS: Filtered {len(engineering_rows)} Engineering records")
        
        # Test 5: Stream with ORDER BY
        print("\n--- Test 5: Stream with ORDER BY ---")
        sql = "SELECT name, age FROM test_stream_users WHERE email LIKE %s ORDER BY age DESC"
        ages = []
        
        for row_num, row, status in db.stream_query(sql, ["%@streamtest.com"]):
            if status == 0:
                ages.append(row["age"])
                print(f"Row {row_num}: {row['name']}, age={row['age']}")
        
        assert ages == sorted(ages, reverse=True), f"Ages should be in descending order: {ages}"
        print("SUCCESS: ORDER BY streaming test passed")
        
        # Test 6: Compare stream results with regular query
        print("\n--- Test 6: Compare Stream vs Regular Query ---")
        sql = "SELECT id, name, email FROM test_stream_users WHERE email LIKE %s ORDER BY id"
        params = ["%@streamtest.com"]
        
        # Get results via regular query
        regular_result = db.query(sql, params)
        regular_data = regular_result["data"]
        
        # Get results via stream
        stream_data = []
        for row_num, row, status in db.stream_query(sql, params):
            if status == 0:
                stream_data.append(row)
        
        assert len(stream_data) == len(regular_data), f"Stream and query should return same number of rows"
        
        # Compare each row
        for i, (stream_row, regular_row) in enumerate(zip(stream_data, regular_data)):
            assert stream_row["id"] == regular_row["id"], f"Row {i} id mismatch"
            assert stream_row["name"] == regular_row["name"], f"Row {i} name mismatch"
            assert stream_row["email"] == regular_row["email"], f"Row {i} email mismatch"
        
        print(f"SUCCESS: Stream and regular query results match ({len(stream_data)} rows)")
        
        # Clean up test data
        cleanup_test_data(db)
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} stream_query connection closed")

def test_stream_insert_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test stream_insert functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} stream_insert Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client_source:
        db_source = get_db(client_source)
        print(f"Connected to {db_source.database_type} source: {db_source.database_name}")
        
        # Set up test data using source connection
        if not setup_test_data(db_source, db_source.database_type):
            print("ERROR: Could not set up test data, aborting tests")
            return
        
        # Helper to get a target DB instance on a fresh connection
        from contextlib import contextmanager
        @contextmanager
        def get_target_db():
            with pools[db_key].client() as client_target:
                yield get_db(client_target)

        # Test 1: Basic stream insert - copy from source to target
        print("\n--- Test 1: Basic Stream Insert (Source to Target) ---")
        
        # Create a stream from source table
        source_sql = "SELECT name, email, age, status, department FROM test_stream_users ORDER BY id"
        source_stream = db_source.stream_query(source_sql)
        
        # Insert stream into target table using a SECOND connection
        with get_target_db() as db_target:
            result = db_target.stream_insert(source_stream, "test_stream_target", commit_count=2)
            
            print(f"Stream insert result: {result}")
            assert result["status"] == 0, f"Stream insert should succeed: {result['message']}"
            assert result["record_count"] == 5, f"Should insert 5 records, got {result['record_count']}"
            print("SUCCESS: Basic stream insert test passed")
            
            # Verify data in target table
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_stream_target")
            assert verify_result["data"][0]["cnt"] == 5, "Target table should have 5 records"
            print("SUCCESS: Target table verification passed")
            
            # Clean up target table for next test
            db_target.execute("DELETE FROM test_stream_target")
        
        # Test 2: Stream insert with autocommit (commit_count=0)
        print("\n--- Test 2: Stream Insert with Autocommit ---")
        
        source_stream = db_source.stream_query(source_sql)
        with get_target_db() as db_target:
            result = db_target.stream_insert(source_stream, "test_stream_target", commit_count=0)
            
            print(f"Autocommit stream insert result: {result}")
            assert result["status"] == 0, "Autocommit stream insert should succeed"
            assert result["record_count"] == 5, "Should insert 5 records"
            print("SUCCESS: Autocommit stream insert test passed")
            
            # Clean up target table for next test
            db_target.execute("DELETE FROM test_stream_target")
        
        # Test 3: Stream insert with empty stream
        print("\n--- Test 3: Stream Insert with Empty Stream ---")
        
        def empty_stream():
            return
            yield  # Make it a generator
        
        with get_target_db() as db_target:
            result = db_target.stream_insert(empty_stream(), "test_stream_target")
            
            print(f"Empty stream insert result: {result}")
            assert result["status"] == 0, "Empty stream insert should succeed"
            assert result["record_count"] == 0, "Should insert 0 records for empty stream"
            print("SUCCESS: Empty stream insert test passed")
        
        # Test 4: Stream insert with custom generator (simulating data transformation)
        print("\n--- Test 4: Stream Insert with Data Transformation ---")
        
        def transform_stream():
            """Custom generator that transforms data"""
            for i, record in enumerate([
                {"name": "Transformed User 1", "email": "t1@test.com", "age": 30, "status": "pending", "department": "IT"},
                {"name": "Transformed User 2", "email": "t2@test.com", "age": 25, "status": "pending", "department": "IT"},
            ], start=1):
                yield i, record, 0  # row_number, row_dict, status
        
        with get_target_db() as db_target:
            result = db_target.stream_insert(transform_stream(), "test_stream_target", commit_count=1)
            
            print(f"Transform stream insert result: {result}")
            assert result["status"] == 0, "Transform stream insert should succeed"
            assert result["record_count"] == 2, "Should insert 2 records"
            
            # Verify transformed data
            verify_result = db_target.query("SELECT * FROM test_stream_target WHERE department = %s", ["IT"])
            assert len(verify_result["data"]) == 2, "Should have 2 IT department records"
            print("SUCCESS: Transform stream insert test passed")
            
            # Clean up target table for next test
            db_target.execute("DELETE FROM test_stream_target")
        
        # Test 5: Stream insert with upstream error
        print("\n--- Test 5: Stream Insert with Upstream Error ---")
        
        def stream_with_error():
            """Generator that yields an error row"""
            yield 1, {"name": "Good User", "email": "good@test.com", "age": 30, "status": "active", "department": "Sales"}, 0
            yield 2, {}, -1  # Error row
            yield 3, {"name": "Never Inserted", "email": "never@test.com", "age": 25, "status": "active", "department": "Sales"}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_insert(stream_with_error(), "test_stream_target")
            
            print(f"Error stream insert result: {result}")
            assert result["status"] == -1, "Stream insert with upstream error should fail"
            assert result["record_count"] == 2, "Should fail at row 2"
            print("SUCCESS: Upstream error handling test passed")
            
            # Clean up target table
            db_target.execute("DELETE FROM test_stream_target")
        
        # Test 6: Full pipeline - stream_query to stream_insert
        print("\n--- Test 6: Full Pipeline (stream_query → stream_insert) ---")
        
        # Use stream_query output directly as input to stream_insert
        source_stream = db_source.stream_query(
            "SELECT name, email, age, status, department FROM test_stream_users WHERE department = %s ORDER BY id",
            ["Engineering"]
        )
        
        with get_target_db() as db_target:
            result = db_target.stream_insert(source_stream, "test_stream_target", commit_count=1000)
            
            print(f"Pipeline result: {result}")
            assert result["status"] == 0, "Full pipeline should succeed"
            assert result["record_count"] == 3, "Should insert 3 Engineering records"
            
            # Verify pipeline data
            verify_result = db_target.query("SELECT * FROM test_stream_target ORDER BY name")
            assert len(verify_result["data"]) == 3, "Target should have 3 records"
            print("SUCCESS: Full pipeline test passed")
        
        # Clean up test data
        cleanup_test_data(db_source)
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} stream_insert connection closed")

def test_stream_update_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test stream_update functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} stream_update Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client_source:
        db_source = get_db(client_source)
        print(f"Connected to {db_source.database_type} source: {db_source.database_name}")
        
        # Set up test data using source connection
        if not setup_test_data(db_source, db_source.database_type):
            print("ERROR: Could not set up test data, aborting tests")
            return
            
        # Helper to get a target DB instance on a fresh connection
        from contextlib import contextmanager
        @contextmanager
        def get_target_db():
            with pools[db_key].client() as client_target:
                yield get_db(client_target)

        # Test 1: Basic stream update
        print("\n--- Test 1: Basic Stream Update ---")
        
        def update_stream():
            """Create update operations for existing records"""
            yield 1, {'data': {'status': 'updated'}, 'condition': {'name': 'Alice Johnson'}}, 0
            yield 2, {'data': {'status': 'updated'}, 'condition': {'name': 'Bob Smith'}}, 0
            yield 3, {'data': {'status': 'updated'}, 'condition': {'name': 'Charlie Brown'}}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_update(update_stream(), "test_stream_users", commit_count=2)
            
            print(f"Stream update result: {result}")
            assert result["status"] == 0, f"Stream update should succeed: {result['message']}"
            assert result["record_count"] == 3, f"Should update 3 records, got {result['record_count']}"
            
            # Verify updates
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_stream_users WHERE status = %s", ["updated"])
            assert verify_result["data"][0]["cnt"] == 3, "Should have 3 updated records"
            print("SUCCESS: Basic stream update test passed")
        
        # Test 2: Stream update with autocommit (commit_count=0)
        print("\n--- Test 2: Stream Update with Autocommit ---")
        
        def autocommit_update_stream():
            yield 1, {'data': {'department': 'Updated Dept'}, 'condition': {'name': 'Diana Wilson'}}, 0
            yield 2, {'data': {'department': 'Updated Dept'}, 'condition': {'name': 'Eve Davis'}}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_update(autocommit_update_stream(), "test_stream_users", commit_count=0)
            
            print(f"Autocommit stream update result: {result}")
            assert result["status"] == 0, "Autocommit stream update should succeed"
            assert result["record_count"] == 2, "Should update 2 records"
            
            # Verify
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_stream_users WHERE department = %s", ["Updated Dept"])
            assert verify_result["data"][0]["cnt"] == 2, "Should have 2 records with updated department"
            print("SUCCESS: Autocommit stream update test passed")
        
        # Test 3: Stream update with empty stream
        print("\n--- Test 3: Stream Update with Empty Stream ---")
        
        def empty_stream():
            return
            yield  # Make it a generator
        
        with get_target_db() as db_target:
            result = db_target.stream_update(empty_stream(), "test_stream_users")
            
            print(f"Empty stream update result: {result}")
            assert result["status"] == 0, "Empty stream update should succeed"
            assert result["record_count"] == 0, "Should update 0 records for empty stream"
            print("SUCCESS: Empty stream update test passed")
        
        # Test 4: Stream update with upstream error
        print("\n--- Test 4: Stream Update with Upstream Error ---")
        
        # First, reset status to test update using source connection
        db_source.execute("UPDATE test_stream_users SET status = 'active' WHERE status = 'updated'")
        
        def stream_with_error():
            """Generator that yields an error row"""
            yield 1, {'data': {'status': 'error_test'}, 'condition': {'name': 'Alice Johnson'}}, 0
            yield 2, {}, -1  # Error row
            yield 3, {'data': {'status': 'error_test'}, 'condition': {'name': 'Charlie Brown'}}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_update(stream_with_error(), "test_stream_users")
            
            print(f"Error stream update result: {result}")
            assert result["status"] == -1, "Stream update with upstream error should fail"
            assert result["record_count"] == 2, "Should fail at row 2"
            print("SUCCESS: Upstream error handling test passed")
        
        # Test 5: Stream update with invalid row format
        print("\n--- Test 5: Stream Update with Invalid Row Format ---")
        
        def invalid_format_stream():
            yield 1, {'name': 'Missing data and condition keys'}, 0  # Invalid format
        
        with get_target_db() as db_target:
            result = db_target.stream_update(invalid_format_stream(), "test_stream_users")
            
            print(f"Invalid format result: {result}")
            assert result["status"] == -1, "Stream update with invalid format should fail"
            assert "Invalid row format" in result["message"], "Should mention invalid row format"
            print("SUCCESS: Invalid format handling test passed")
        
        # Test 6: Full pipeline - stream_query with transformation to stream_update
        print("\n--- Test 6: Full Pipeline (stream_query → transform → stream_update) ---")
        
        # Reset data using source connection
        db_source.execute("UPDATE test_stream_users SET status = 'active'")
        # Reset Eve's department (changed in Test 2) so she is included in the Engineering query
        db_source.execute("UPDATE test_stream_users SET department = 'Engineering' WHERE name = 'Eve Davis'")
        db_source.commit()
        
        def transform_for_update(source_stream):
            """Transform query results to update format"""
            for row_num, row, status in source_stream:
                if status == 0:
                    yield row_num, {
                        'data': {'status': 'pipeline_updated', 'age': row['age'] + 1},
                        'condition': {'id': row['id']}
                    }, 0
        
        # Query source data and transform for update
        s_stream = db_source.stream_query(
            "SELECT id, name, age FROM test_stream_users WHERE department = %s ORDER BY id",
            ["Engineering"]
        )
        
        with get_target_db() as db_target:
            result = db_target.stream_update(transform_for_update(s_stream), "test_stream_users", commit_count=1000)
            
            print(f"Pipeline result: {result}")
            assert result["status"] == 0, "Full pipeline should succeed"
            assert result["record_count"] == 3, "Should update 3 Engineering records"
            
            # Verify pipeline updates
            verify_result = db_target.query("SELECT * FROM test_stream_users WHERE status = %s", ["pipeline_updated"])
            assert len(verify_result["data"]) == 3, "Should have 3 pipeline_updated records"
            print("SUCCESS: Full pipeline test passed")
        
        # Test 7: Stream update affecting multiple rows per condition
        print("\n--- Test 7: Stream Update with Bulk Condition ---")
        
        def bulk_condition_stream():
            """Update multiple rows with a single condition"""
            yield 1, {'data': {'tags': 'bulk_updated'}, 'condition': {'department': 'Engineering'}}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_update(bulk_condition_stream(), "test_stream_users")
            
            print(f"Bulk condition update result: {result}")
            assert result["status"] == 0, "Bulk condition update should succeed"
            
            # Verify - all Engineering records should have updated tags
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_stream_users WHERE tags = %s", ["bulk_updated"])
            assert verify_result["data"][0]["cnt"] == 3, "Should have 3 records with bulk_updated tags"
            print("SUCCESS: Bulk condition update test passed")
        
        # Clean up test data
        cleanup_test_data(db_source)
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} stream_update connection closed")

def test_stream_merge_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test stream_merge functionality for SQL databases (MySQL/PostgreSQL)"""
    print(f"\n=== Testing {db_key} stream_merge Operations with {backend.value} backend ===")
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    db_cfg = cfg.require(db_key)
    
    pools = {}
    pools[db_key] = Pool.from_config(db_cfg, backend)
    
    with pools[db_key].client() as client_source:
        db_source = get_db(client_source)
        print(f"Connected to {db_source.database_type} source: {db_source.database_name}")
        
        # Set up test data - need a table with a unique constraint for merge
        print("\n--- Setting up test data for merge ---")
        try:
            db_source.execute("DROP TABLE IF EXISTS test_merge_users")
            
            if db_source.database_type in ['postgres', 'postgresql']:
                create_sql = """
                CREATE TABLE test_merge_users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(100) UNIQUE,
                    name VARCHAR(100),
                    status VARCHAR(20) DEFAULT 'active',
                    age INTEGER
                )
                """
            else:  # MySQL
                create_sql = """
                CREATE TABLE test_merge_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(100) UNIQUE,
                    name VARCHAR(100),
                    status VARCHAR(20) DEFAULT 'active',
                    age INTEGER
                )
                """
            db_source.execute(create_sql)
            
            # Insert some initial data
            initial_data = [
                {"email": "alice@test.com", "name": "Alice Initial", "status": "active", "age": 25},
                {"email": "bob@test.com", "name": "Bob Initial", "status": "active", "age": 30},
            ]
            db_source.insert("test_merge_users", initial_data)
            db_source.commit()
            print("SUCCESS: Test merge table created with initial data")
        except Exception as e:
            print(f"ERROR: Could not set up merge test data: {e}")
            return
            
        # Helper to get a target DB instance on a fresh connection
        from contextlib import contextmanager
        @contextmanager
        def get_target_db():
            with pools[db_key].client() as client_target:
                yield get_db(client_target)

        # Test 1: Basic stream merge - update existing + insert new
        print("\n--- Test 1: Basic Stream Merge (Update Existing + Insert New) ---")
        
        def merge_stream():
            """Stream with mix of updates and inserts"""
            # Update existing (by email)
            yield 1, {'email': 'alice@test.com', 'name': 'Alice Updated', 'status': 'updated', 'age': 26}, 0
            # Insert new
            yield 2, {'email': 'charlie@test.com', 'name': 'Charlie New', 'status': 'active', 'age': 35}, 0
            # Update existing
            yield 3, {'email': 'bob@test.com', 'name': 'Bob Updated', 'status': 'updated', 'age': 31}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_merge(merge_stream(), "test_merge_users", ["email"], commit_count=2)
            
            print(f"Stream merge result: {result}")
            assert result["status"] == 0, f"Stream merge should succeed: {result['message']}"
            assert result["record_count"] == 3, f"Should process 3 records, got {result['record_count']}"
            
            # Verify results
            verify_result = db_target.query("SELECT * FROM test_merge_users ORDER BY email")
            assert len(verify_result["data"]) == 3, "Should have 3 total records"
            
            # Check updates
            alice = next((r for r in verify_result["data"] if r["email"] == "alice@test.com"), None)
            assert alice["name"] == "Alice Updated", "Alice should be updated"
            assert alice["status"] == "updated", "Alice status should be updated"
            
            bob = next((r for r in verify_result["data"] if r["email"] == "bob@test.com"), None)
            assert bob["name"] == "Bob Updated", "Bob should be updated"
            
            # Check insert
            charlie = next((r for r in verify_result["data"] if r["email"] == "charlie@test.com"), None)
            assert charlie is not None, "Charlie should be inserted"
            assert charlie["name"] == "Charlie New", "Charlie name should match"
            
            print("SUCCESS: Basic stream merge test passed")
        
        # Test 2: Stream merge with autocommit (commit_count=0)
        print("\n--- Test 2: Stream Merge with Autocommit ---")
        
        def autocommit_merge_stream():
            yield 1, {'email': 'diana@test.com', 'name': 'Diana', 'status': 'active', 'age': 40}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_merge(autocommit_merge_stream(), "test_merge_users", ["email"], commit_count=0)
            
            print(f"Autocommit stream merge result: {result}")
            assert result["status"] == 0, "Autocommit stream merge should succeed"
            assert result["record_count"] == 1, "Should process 1 record"
            
            # Verify
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_merge_users WHERE email = %s", ["diana@test.com"])
            assert verify_result["data"][0]["cnt"] == 1, "Diana should exist"
            print("SUCCESS: Autocommit stream merge test passed")
        
        # Test 3: Stream merge with empty stream
        print("\n--- Test 3: Stream Merge with Empty Stream ---")
        
        def empty_stream():
            return
            yield  # Make it a generator
        
        with get_target_db() as db_target:
            result = db_target.stream_merge(empty_stream(), "test_merge_users", ["email"])
            
            print(f"Empty stream merge result: {result}")
            assert result["status"] == 0, "Empty stream merge should succeed"
            assert result["record_count"] == 0, "Should process 0 records for empty stream"
            print("SUCCESS: Empty stream merge test passed")
        
        # Test 4: Stream merge with upstream error
        print("\n--- Test 4: Stream Merge with Upstream Error ---")
        
        def stream_with_error():
            yield 1, {'email': 'error1@test.com', 'name': 'Error 1', 'status': 'active', 'age': 20}, 0
            yield 2, {}, -1  # Error row
            yield 3, {'email': 'error2@test.com', 'name': 'Error 2', 'status': 'active', 'age': 21}, 0
        
        with get_target_db() as db_target:
            result = db_target.stream_merge(stream_with_error(), "test_merge_users", ["email"])
            
            print(f"Error stream merge result: {result}")
            assert result["status"] == -1, "Stream merge with upstream error should fail"
            assert result["record_count"] == 2, "Should fail at row 2"
            print("SUCCESS: Upstream error handling test passed")
        
        # Test 5: Full pipeline - stream_query to stream_merge (data synchronization)
        print("\n--- Test 5: Full Pipeline (stream_query → stream_merge for sync) ---")
        
        # Create another table to sync from using source DB
        try:
            db_source.execute("DROP TABLE IF EXISTS test_merge_source")
            db_source.execute("""
                CREATE TABLE test_merge_source (
                    email VARCHAR(100) PRIMARY KEY,
                    name VARCHAR(100),
                    status VARCHAR(20),
                    age INTEGER
                )
            """)
            
            # Insert source data
            source_data = [
                {"email": "sync1@test.com", "name": "Sync User 1", "status": "synced", "age": 25},
                {"email": "sync2@test.com", "name": "Sync User 2", "status": "synced", "age": 30},
                {"email": "sync3@test.com", "name": "Sync User 3", "status": "synced", "age": 35},
            ]
            db_source.insert("test_merge_source", source_data)
            db_source.commit()
        except Exception as e:
            print(f"ERROR setting up source table: {e}")
            return
        
        # Stream from source and merge into target
        s_stream = db_source.stream_query("SELECT email, name, status, age FROM test_merge_source ORDER BY email")
        with get_target_db() as db_target:
            result = db_target.stream_merge(s_stream, "test_merge_users", ["email"], commit_count=1000)
            
            print(f"Pipeline result: {result}")
            assert result["status"] == 0, "Full pipeline should succeed"
            assert result["record_count"] == 3, "Should merge 3 records from source"
            
            # Verify sync
            verify_result = db_target.query("SELECT COUNT(*) as cnt FROM test_merge_users WHERE status = %s", ["synced"])
            assert verify_result["data"][0]["cnt"] == 3, "Should have 3 synced records"
            print("SUCCESS: Full pipeline sync test passed")
        
        # Cleanup
        try:
            db_source.execute("DROP TABLE IF EXISTS test_merge_users")
            db_source.execute("DROP TABLE IF EXISTS test_merge_source")
            print("SUCCESS: Cleaned up merge test tables")
        except:
            pass
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} stream_merge connection closed")

def test_merge_select_sql(db_key, backend=PoolBackend.DBUTILS):
    """Test merge_select functionality for SQL databases (MySQL/PostgreSQL)"""
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
            db.execute("DROP TABLE IF EXISTS merge_select_source")
            db.execute("DROP TABLE IF EXISTS merge_select_target")
            
            if db.database_type in ['postgres', 'postgresql']:
                db.execute("""
                    CREATE TABLE merge_select_source (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(100) UNIQUE,
                        name VARCHAR(100),
                        status VARCHAR(20),
                        age INTEGER
                    )
                """)
                db.execute("""
                    CREATE TABLE merge_select_target (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(100) UNIQUE,
                        name VARCHAR(100),
                        status VARCHAR(20) DEFAULT 'pending',
                        age INTEGER
                    )
                """)
            else:  # MySQL
                db.execute("""
                    CREATE TABLE merge_select_source (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        email VARCHAR(100) UNIQUE,
                        name VARCHAR(100),
                        status VARCHAR(20),
                        age INTEGER
                    )
                """)
                db.execute("""
                    CREATE TABLE merge_select_target (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        email VARCHAR(100) UNIQUE,
                        name VARCHAR(100),
                        status VARCHAR(20) DEFAULT 'pending',
                        age INTEGER
                    )
                """)
            
            # Insert source data
            source_data = [
                {"email": "alice@test.com", "name": "Alice Source", "status": "active", "age": 25},
                {"email": "bob@test.com", "name": "Bob Source", "status": "active", "age": 30},
                {"email": "charlie@test.com", "name": "Charlie Source", "status": "inactive", "age": 35},
            ]
            db.insert("merge_select_source", source_data)
            
            # Insert some initial target data (to test update behavior)
            initial_target = [
                {"email": "alice@test.com", "name": "Alice Old", "status": "pending", "age": 20},
            ]
            db.insert("merge_select_target", initial_target)
            
            print("SUCCESS: Test tables created with initial data")
        except Exception as e:
            print(f"ERROR: Could not set up merge_select test data: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # Test 1: Basic merge_select - merge from source to target
        print("\n--- Test 1: Basic merge_select ---")
        
        result = db.merge_select(
            source_table="merge_select_source",
            target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"],
            key_columns=["email"]
        )
        
        print(f"merge_select result: {result}")
        assert result["status"] == 0, f"merge_select should succeed: {result['message']}"
        
        # Verify results
        verify_result = db.query("SELECT * FROM merge_select_target ORDER BY email")
        assert len(verify_result["data"]) == 3, "Should have 3 total records"
        
        # Check that Alice was updated (not duplicated)
        alice = next((r for r in verify_result["data"] if r["email"] == "alice@test.com"), None)
        assert alice["name"] == "Alice Source", "Alice should be updated from source"
        assert alice["age"] == 25, "Alice age should be updated"
        
        # Check new inserts
        bob = next((r for r in verify_result["data"] if r["email"] == "bob@test.com"), None)
        assert bob is not None, "Bob should be inserted"
        
        charlie = next((r for r in verify_result["data"] if r["email"] == "charlie@test.com"), None)
        assert charlie is not None, "Charlie should be inserted"
        
        print("SUCCESS: Basic merge_select test passed")
        
        # Test 2: merge_select with conditions
        print("\n--- Test 2: merge_select with Conditions ---")
        
        # Reset target table
        db.execute("DELETE FROM merge_select_target")
        
        # Merge only active users
        result = db.merge_select(
            source_table="merge_select_source",
            target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"],
            key_columns=["email"],
            conditions={"status": "active"}
        )
        
        print(f"Conditional merge_select result: {result}")
        assert result["status"] == 0, "Conditional merge_select should succeed"
        
        # Verify only active users were merged
        verify_result = db.query("SELECT * FROM merge_select_target")
        assert len(verify_result["data"]) == 2, "Should have 2 active users"
        print("SUCCESS: merge_select with conditions test passed")
        
        # Test 3: merge_select with custom source SELECT
        print("\n--- Test 3: merge_select with Custom Source SELECT ---")
        
        # Reset target table
        db.execute("DELETE FROM merge_select_target")
        
        custom_sql = "SELECT email, name, status, age FROM merge_select_source WHERE age >= 30 ORDER BY age DESC"
        
        result = db.merge_select(
            source_table=None,
            target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"],
            key_columns=["email"],
            source_select=custom_sql
        )
        
        print(f"Custom SELECT merge_select result: {result}")
        assert result["status"] == 0, "Custom SELECT merge_select should succeed"
        
        # Verify only age >= 30 were merged
        verify_result = db.query("SELECT * FROM merge_select_target")
        assert len(verify_result["data"]) == 2, "Should have 2 users with age >= 30"
        print("SUCCESS: merge_select with custom SELECT test passed")
        
        # Test 4: merge_select with order_by
        print("\n--- Test 4: merge_select with ORDER BY ---")
        
        # This test just verifies the query runs successfully with order_by
        db.execute("DELETE FROM merge_select_target")
        
        result = db.merge_select(
            source_table="merge_select_source",
            target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"],
            key_columns=["email"],
            order_by="age DESC"
        )
        
        print(f"ORDER BY merge_select result: {result}")
        assert result["status"] == 0, "ORDER BY merge_select should succeed"
        print("SUCCESS: merge_select with ORDER BY test passed")
        
        # Test 5: merge_select with custom update_columns
        print("\n--- Test 5: merge_select with Custom Update Columns ---")
        
        # Reset and set up for update test
        db.execute("DELETE FROM merge_select_target")
        db.insert("merge_select_target", [{"email": "alice@test.com", "name": "Keep This Name", "status": "old_status", "age": 99}])
        
        result = db.merge_select(
            source_table="merge_select_source",
            target_table="merge_select_target",
            insert_columns=["email", "name", "status", "age"],
            key_columns=["email"],
            update_columns=["status", "age"]  # Only update status and age, not name
        )
        
        print(f"Custom update columns result: {result}")
        assert result["status"] == 0, "Custom update columns merge_select should succeed"
        
        # Verify Alice's name was NOT updated (only status and age)
        verify_result = db.query("SELECT * FROM merge_select_target WHERE email = %s", ["alice@test.com"])
        alice = verify_result["data"][0]
        assert alice["name"] == "Keep This Name", "Alice name should NOT be updated"
        assert alice["status"] == "active", "Alice status SHOULD be updated"
        assert alice["age"] == 25, "Alice age SHOULD be updated"
        print("SUCCESS: merge_select with custom update_columns test passed")
        
        # Cleanup
        try:
            db.execute("DROP TABLE IF EXISTS merge_select_source")
            db.execute("DROP TABLE IF EXISTS merge_select_target")
            print("SUCCESS: Cleaned up merge_select test tables")
        except:
            pass
    
    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} merge_select connection closed")

    pools[db_key].dispose()
    print(f"SUCCESS: {db_key} merge_select connection closed")


def test_error_handling():
    """Test error handling for stream operations"""
    print(f"\n=== Testing Stream Error Handling ===")
    
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
        
        # Test 1: stream_query with invalid table
        print("\n--- Test: Stream Query from Invalid Table ---")
        sql = "SELECT * FROM nonexistent_table_xyz"
        error_received = False
        
        try:
            for row_num, row, status in db.stream_query(sql):
                if status == -1:
                    error_received = True
                    print(f"Received error status as expected at row {row_num}")
                    break
        except Exception as e:
            error_received = True
            print(f"Received exception as expected: {e}")
        
        assert error_received, "Should receive error for invalid table"
        print("SUCCESS: Invalid table error handling passed")
        
        # Test 2: stream_insert to invalid table
        print("\n--- Test: Stream Insert to Invalid Table ---")
        
        def simple_stream():
            yield 1, {"name": "Test", "email": "test@test.com"}, 0
        
        result = db.stream_insert(simple_stream(), "nonexistent_table_xyz")
        assert result["status"] == -1, "Stream insert to invalid table should fail"
        print(f"Error message: {result['message']}")
        print("SUCCESS: Invalid table stream insert error handling passed")
    
    pools[db_key].dispose()

if __name__ == "__main__":
    print("=== JRM Stream Operations Test Suite ===")
    print("Tests: stream_query, stream_insert")
    
    # Test database and backend combinations for stream_query
    test_combinations = [
        ("postgres-test", PoolBackend.DBUTILS),
        ("postgres-test", PoolBackend.SQLALCHEMY),
        ("mysql-test", PoolBackend.DBUTILS),
        ("mysql-test", PoolBackend.SQLALCHEMY)
    ]
    
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    
    # Test stream_query
    print("\n" + "="*60)
    print("PART 1: Testing stream_query")
    print("="*60)
    
    combinations_tested = 0
    for db_key, backend in test_combinations:
        try:
            db_cfg = cfg.require(db_key)
            print(f"\n>>> Running stream_query tests with {db_key} using {backend.value} backend")
            test_stream_query_sql(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) stream_query tests completed")
            combinations_tested += 1
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) stream_query tests failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Test stream_insert
    print("\n" + "="*60)
    print("PART 2: Testing stream_insert")
    print("="*60)
    
    for db_key, backend in test_combinations:
        try:
            db_cfg = cfg.require(db_key)
            print(f"\n>>> Running stream_insert tests with {db_key} using {backend.value} backend")
            test_stream_insert_sql(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) stream_insert tests completed")
            combinations_tested += 1
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) stream_insert tests failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Test stream_update
    print("\n" + "="*60)
    print("PART 3: Testing stream_update")
    print("="*60)
    
    for db_key, backend in test_combinations:
        try:
            db_cfg = cfg.require(db_key)
            print(f"\n>>> Running stream_update tests with {db_key} using {backend.value} backend")
            test_stream_update_sql(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) stream_update tests completed")
            combinations_tested += 1
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) stream_update tests failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Test stream_merge
    print("\n" + "="*60)
    print("PART 4: Testing stream_merge")
    print("="*60)
    
    for db_key, backend in test_combinations:
        try:
            db_cfg = cfg.require(db_key)
            print(f"\n>>> Running stream_merge tests with {db_key} using {backend.value} backend")
            test_stream_merge_sql(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) stream_merge tests completed")
            combinations_tested += 1
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) stream_merge tests failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Test merge_select
    print("\n" + "="*60)
    print("PART 5: Testing merge_select")
    print("="*60)
    
    for db_key, backend in test_combinations:
        try:
            db_cfg = cfg.require(db_key)
            print(f"\n>>> Running merge_select tests with {db_key} using {backend.value} backend")
            test_merge_select_sql(db_key, backend)
            print(f"SUCCESS: {db_key} ({backend.value}) merge_select tests completed")
            combinations_tested += 1
        except KeyError:
            print(f"WARNING: {db_key} configuration not found, skipping...")
        except Exception as e:
            print(f"FAILED: {db_key} ({backend.value}) merge_select tests failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    if combinations_tested == 0:
        print("ERROR: No database configurations found")
        sys.exit(1)
    
    # Test error handling
    print("\n" + "="*60)
    print("PART 7: Testing Error Handling")
    print("="*60)
    
    try:
        test_error_handling()
        print("SUCCESS: Error handling tests completed")
    except Exception as e:
        print(f"FAILED: Error handling tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "="*60)
    print("=== Stream Operations Test Suite Complete ===")
    print(f"Total test sets run: {combinations_tested}")
    print("="*60)
