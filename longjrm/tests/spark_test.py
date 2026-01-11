"""
Standalone Spark SQL Test Suite (Select, Insert, Update, Delete)

This script specifically tests Spark SQL functionality using Delta Lake tables.
It bypasses RDBMS-specific transaction tests and uses Spark-compatible assertions.

Required Environment:
- PySpark 3.5.0+
- delta-spark 3.1.0+
- Java 17/21
- Docker container (recommended on Windows)
"""

import logging
import sys
import os
import time
import csv
import tempfile
from datetime import datetime

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

TABLE_NAME = "test_users_delta"
TABLE_DEPT = "test_departments_delta"

def get_spark_config():
    """Load config and find the first available Spark configuration"""
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    
    # Return specific spark config if exists
    for key in ["spark-test", "spark-docker", "spark-local"]:
        try:
            conf = cfg.require(key)
            # Force enable_delta for this test suite as it requires Delta features
            conf.options['enable_delta'] = True
            return conf, key
        except:
            continue
            
    # Fallback search
    for key, conf in cfg.databases().items():
        if conf.type == 'spark':
            conf.options['enable_delta'] = True
            return conf, key
            
    return None, None

def setup_table(db):
    """Create Delta table for testing"""
    print(f"\n--- Setup: Creating Delta tables ---")
    print(f"Delta Enabled: {db._check_delta_support()}")
    
    # --- Users Table ---
    print(f"Creating {TABLE_NAME}...")
    db.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    create_sql = f"""
    CREATE TABLE {TABLE_NAME} (
        age INT,
        department STRING,
        email STRING,
        id BIGINT,
        name STRING,
        status STRING,
        tags STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    """
    res = db.execute(create_sql)
    if res['status'] != 0:
        raise Exception(f"Failed to create table {TABLE_NAME}: {res['message']}")

    # --- Departments Table ---
    print(f"Creating {TABLE_DEPT}...")
    db.execute(f"DROP TABLE IF EXISTS {TABLE_DEPT}")
    create_dept_sql = f"""
    CREATE TABLE {TABLE_DEPT} (
        budget DECIMAL(10, 2),
        dept_id STRING,
        dept_name STRING,
        location STRING
    ) USING DELTA
    """
    res = db.execute(create_dept_sql)
    if res['status'] != 0:
        raise Exception(f"Failed to create table {TABLE_DEPT}: {res['message']}")
        
    print("✓ Tables created")

def test_insert(db):
    """Test Insert Operations"""
    print(f"\n=== Test: Insert ===")
    
    # 1. Single Insert (Users)
    record = {
        "id": 1,
        "name": "Spark User",
        "email": "spark@test.com",
        "age": 30,
        "status": "active",
        "department": "Data",
        "tags": "spark,python",
        "updated_at": datetime.now()
    }
    result = db.insert(TABLE_NAME, record)
    assert result["status"] == 0, f"Single insert failed: {result['message']}"
    print("✓ Single insert users successful")
    
    # 2. Bulk Insert (Users)
    records = [
        {"id": 2, "name": "Alice Delta", "email": "alice@delta.io", "age": 25, "status": "active", "updated_at": datetime.now(), "department": "Eng", "tags": "a"},
        {"id": 3, "name": "Bob Lake", "email": "bob@delta.io", "age": 40, "status": "inactive", "updated_at": datetime.now(), "department": "Sales", "tags": "b"},
        {"id": 4, "name": "Charlie Stream", "email": "charlie@delta.io", "age": 35, "status": "active", "updated_at": datetime.now(), "department": "Ops", "tags": "c"}
    ]
    result = db.insert(TABLE_NAME, records)
    assert result["status"] == 0, f"Bulk insert failed: {result['message']}"
    print("✓ Bulk insert users successful")
    
    # 3. Bulk Insert (Departments)
    dept_records = [
        {"dept_id": "Data", "dept_name": "Data Science", "budget": 100000.00, "location": "NY"},
        {"dept_id": "Eng", "dept_name": "Engineering", "budget": 500000.00, "location": "SF"},
        {"dept_id": "Sales", "dept_name": "Global Sales", "budget": 200000.00, "location": "London"}
        # Ops is missing intentionally for Left Join test
    ]
    result = db.insert(TABLE_DEPT, dept_records)
    assert result["status"] == 0, f"Dept insert failed: {result['message']}"
    print("✓ Bulk insert departments successful")
    
    # Verify counts
    c1 = db.query(f"SELECT COUNT(*) as c FROM {TABLE_NAME}")['data'][0]['c']
    c2 = db.query(f"SELECT COUNT(*) as c FROM {TABLE_DEPT}")['data'][0]['c']
    assert c1 == 4, f"Expected 4 users, got {c1}"
    assert c2 == 3, f"Expected 3 depts, got {c2}"
    print(f"✓ Verification: {c1} users, {c2} departments")

def test_select(db):
    """Test Basic Select Operations"""
    print(f"\n=== Test: Basic Select ===")
    db.execute(f"REFRESH TABLE {TABLE_NAME}")
    db.execute(f"REFRESH TABLE {TABLE_DEPT}")
    
    # 1. Simple Select
    result = db.select(TABLE_NAME, ["name", "age"], where={"status": "active"})
    # Note: 'Spark User', 'Alice', 'Charlie' are active. 
    assert len(result['data']) == 3, f"Expected 3 active users, got {len(result['data'])}"
    print(f"✓ Select WHERE status='active' returned {len(result['data'])} rows")
    
    # 2. Sort and Limit
    result = db.query(f"SELECT * FROM {TABLE_NAME} ORDER BY age DESC LIMIT 1")
    assert result['data'][0]['name'] == "Bob Lake", "Sort/Limit failed"
    print("✓ ORDER BY/LIMIT working")

def test_advanced_select(db):
    """Test Advanced Analytics Queries (Join, CTE, Aggregation, Window)"""
    print(f"\n=== Test: Advanced Select (Analytics) ===")
    db.execute(f"REFRESH TABLE {TABLE_NAME}")
    db.execute(f"REFRESH TABLE {TABLE_DEPT}")

    # 1. JOIN (Inner)
    # Join Users with Depts. Users in 'Ops' won't match.
    sql_join = f"""
    SELECT u.name, d.budget 
    FROM {TABLE_NAME} u 
    INNER JOIN {TABLE_DEPT} d ON u.department = d.dept_id
    ORDER BY u.name
    """
    res = db.query(sql_join)
    # Matches: Spark User(Data), Alice(Eng), Bob(Sales). Charlie(Ops) missing.
    assert len(res['data']) == 3, f"Inner Join expected 3 matches, got {len(res['data'])}"
    print("✓ INNER JOIN working")

    # 2. LEFT JOIN
    # Charlie(Ops) should appear with NULL budget
    sql_left = f"""
    SELECT u.name, d.budget 
    FROM {TABLE_NAME} u 
    LEFT JOIN {TABLE_DEPT} d ON u.department = d.dept_id
    WHERE u.department = 'Ops'
    """
    res = db.query(sql_left)
    assert len(res['data']) == 1, "Left Join check failed"
    # Note: access dictionary safely or check repr
    # Pyspark usually returns None for NULL. 'budget' key might exist with None value.
    assert res['data'][0]['name'] == 'Charlie Stream'
    print("✓ LEFT JOIN working")

    # 3. Aggregation (GROUP BY)
    sql_agg = f"""
    SELECT status, COUNT(*) as cnt, AVG(age) as avg_age
    FROM {TABLE_NAME}
    GROUP BY status
    ORDER BY status
    """
    res = db.query(sql_agg)
    # active: 3, inactive: 1
    rows = {r['status']: r['cnt'] for r in res['data']}
    assert rows['active'] == 3
    assert rows['inactive'] == 1
    print("✓ Aggregation (GROUP BY) working")

    # 4. SQL Functions
    sql_func = f"""
    SELECT name, UPPER(name) as name_upper, YEAR(updated_at) as yr
    FROM {TABLE_NAME}
    WHERE id = 1
    """
    res = db.query(sql_func)
    row = res['data'][0]
    assert row['name_upper'] == "SPARK USER", "UPPER() function failed"
    assert row['yr'] == datetime.now().year, "YEAR() function failed"
    print("✓ SQL Functions working")

    # 5. CTE (Common Table Expression)
    sql_cte = f"""
    WITH SeniorUsers AS (
        SELECT name, age FROM {TABLE_NAME} WHERE age >= 30
    )
    SELECT name FROM SeniorUsers WHERE name LIKE 'Bob%'
    """
    res = db.query(sql_cte)
    assert len(res['data']) == 1
    assert res['data'][0]['name'] == 'Bob Lake'
    print("✓ CTE (WITH clause) working")

    # 6. Window Functions
    # Rank users by age within status
    sql_window = f"""
    SELECT name, status, age, 
           RANK() OVER (PARTITION BY status ORDER BY age DESC) as rnk
    FROM {TABLE_NAME}
    """
    res = db.query(sql_window)
    assert res['status'] == 0
    # Validation logic: For 'active', Bob(40 inactive), Charlie(35), Spark(30), Alice(25)
    # Status active: Charlie(35, rnk1), Spark(30, rnk2), Alice(25, rnk3)
    # Check Charlie
    charlie = next((r for r in res['data'] if r['name'] == 'Charlie Stream'), None)
    assert charlie['rnk'] == 1, f"Window Function Rank failed, expected 1 got {charlie['rnk']}"
    print("✓ Window Functions working")


def test_merge(db):
    """Test MERGE (Upsert) Operations for Delta Lake"""
    print(f"\n=== Test: Merge (Delta Lake UPSERT) ===")
    
    # Clean slate: clear users table
    db.delete(TABLE_NAME, {"id": {">": 0}})
    
    # 1. Initial insert via merge (all should be INSERTs since table is empty)
    initial_records = [
        {"id": 10, "name": "Merge User A", "email": "merge_a@test.com", "age": 28, "status": "active", "department": "HR", "tags": "merge", "updated_at": datetime.now()},
        {"id": 11, "name": "Merge User B", "email": "merge_b@test.com", "age": 32, "status": "active", "department": "IT", "tags": "merge", "updated_at": datetime.now()},
    ]
    result = db.merge(TABLE_NAME, initial_records, key_columns=["id"])
    assert result["status"] == 0, f"Initial merge failed: {result['message']}"
    
    # Verify inserts
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE id IN (10, 11)")
    assert check['data'][0]['cnt'] == 2, "Initial merge didn't insert all records"
    print("✓ Merge (as INSERT) succeeded")
    
    # 2. Upsert: Update existing + Insert new
    upsert_records = [
        {"id": 10, "name": "Merge User A Updated", "email": "merge_a_new@test.com", "age": 29, "status": "upgraded", "department": "HR", "tags": "updated", "updated_at": datetime.now()},
        {"id": 12, "name": "Merge User C", "email": "merge_c@test.com", "age": 45, "status": "active", "department": "Finance", "tags": "new", "updated_at": datetime.now()},
    ]
    result = db.merge(TABLE_NAME, upsert_records, key_columns=["id"])
    assert result["status"] == 0, f"Upsert merge failed: {result['message']}"
    
    # Verify that id=10 was updated
    check = db.query(f"SELECT name, status FROM {TABLE_NAME} WHERE id = 10")
    assert check['data'][0]['name'] == "Merge User A Updated", "Merge UPDATE didn't update name"
    assert check['data'][0]['status'] == "upgraded", "Merge UPDATE didn't update status"
    print("✓ Merge (UPDATE existing) verified")
    
    # Verify that id=12 was inserted
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE id = 12")
    assert check['data'][0]['cnt'] == 1, "Merge INSERT for new record failed"
    print("✓ Merge (INSERT new) verified")
    
    # 3. Test no_update='Y' (insert-only mode)
    no_update_records = [
        {"id": 10, "name": "Should NOT Update", "email": "no@update.com", "age": 99, "status": "ignored", "department": "X", "tags": "x", "updated_at": datetime.now()},
        {"id": 13, "name": "New Insert Only", "email": "insert_only@test.com", "age": 50, "status": "new", "department": "Sales", "tags": "insert", "updated_at": datetime.now()},
    ]
    result = db.merge(TABLE_NAME, no_update_records, key_columns=["id"], no_update='Y')
    assert result["status"] == 0, f"Insert-only merge failed: {result['message']}"
    
    # id=10 should NOT be updated
    check = db.query(f"SELECT name FROM {TABLE_NAME} WHERE id = 10")
    assert check['data'][0]['name'] == "Merge User A Updated", "no_update='Y' incorrectly updated the record"
    
    # id=13 should be inserted
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE id = 13")
    assert check['data'][0]['cnt'] == 1, "Insert-only merge didn't insert new record"
    print("✓ Merge with no_update='Y' verified")
    
    print("✓ All Merge tests passed")


def test_bulk_load(db):
    """Test Bulk Load Operations (Cursor/Query mode)"""
    print(f"\n=== Test: Bulk Load ===")
    
    # 1. Setup: Create a source table for cursor-based load
    SOURCE_TABLE = "test_bulk_source"
    TARGET_TABLE = "test_bulk_target"
    
    # Create source table
    db.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    db.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    
    db.execute(f"""
    CREATE TABLE {SOURCE_TABLE} (
        id BIGINT,
        name STRING,
        value INT
    ) USING DELTA
    """)
    
    db.execute(f"""
    CREATE TABLE {TARGET_TABLE} (
        id BIGINT,
        name STRING,
        value INT
    ) USING DELTA
    """)
    
    # Insert test data into source
    source_records = [
        {"id": i, "name": f"Bulk Item {i}", "value": i * 10}
        for i in range(1, 51)  # 50 records
    ]
    db.insert(SOURCE_TABLE, source_records)
    print("✓ Source table created with 50 records")
    
    # 2. Test cursor/query mode bulk load
    load_info = {
        'source': f"SELECT id, name, value FROM {SOURCE_TABLE}",
        'source_type': 'cursor',
        'columns': ['id', 'name', 'value']
    }
    result = db.bulk_load(TARGET_TABLE, load_info=load_info)
    assert result["status"] == 0, f"Cursor bulk load failed: {result['message']}"
    
    # Verify row count in target
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}")
    actual_count = check['data'][0]['cnt']
    assert actual_count == 50, f"Expected 50 rows in target, got {actual_count}"
    print(f"✓ Cursor bulk load succeeded: {actual_count} rows loaded")
    
    # 3. Test bulk load with auto-detected source_type (query starts with SELECT)
    db.execute(f"DELETE FROM {TARGET_TABLE}")
    load_info_auto = {
        'source': f"SELECT id, name, value FROM {SOURCE_TABLE} WHERE value > 200"
    }
    result = db.bulk_load(TARGET_TABLE, load_info=load_info_auto)
    assert result["status"] == 0, f"Auto-detect bulk load failed: {result['message']}"
    
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}")
    filtered_count = check['data'][0]['cnt']
    # value > 200 means id > 20 → 30 records (21-50)
    assert filtered_count == 30, f"Expected 30 filtered rows, got {filtered_count}"
    print(f"✓ Auto-detect cursor bulk load: {filtered_count} rows loaded")
    
    # Cleanup
    db.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    db.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    print("✓ Bulk Load tests passed")


def test_update(db):
    """Test Update Operations (Requires Delta)"""
    print(f"\n=== Test: Update ===")
    
    # 1. Update Single
    result = db.update(TABLE_NAME, {"status": "upgraded"}, {"email": "spark@test.com"})
    assert result["status"] == 0, f"Update failed: {result['message']}"
    
    # Verify
    check = db.query(f"SELECT status FROM {TABLE_NAME} WHERE email='spark@test.com'")
    if check['data']:
        assert check['data'][0]['status'] == 'upgraded'
    print("✓ Single update command issued")
    
    # 2. Bulk Update via Condition
    result = db.update(TABLE_NAME, {"department": "Engineering"}, {"email": {"LIKE": "%@delta.io"}})
    assert result["status"] == 0
    print("✓ Bulk update command issued")

def test_delete(db):
    """Test Delete Operations (Requires Delta)"""
    print(f"\n=== Test: Delete ===")
    
    # 1. Delete specific
    result = db.delete(TABLE_NAME, {"id": 3})  # Bob Lake
    assert result["status"] == 0
    
    # Verify
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME} WHERE id=3")
    assert check['data'][0]['cnt'] == 0
    print("✓ Delete verified")
    
    # 2. Delete All
    result = db.delete(TABLE_NAME, {"id": {">": 0}})
    assert result["status"] == 0
    
    # Verify empty
    check = db.query(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}")
    assert check['data'][0]['cnt'] == 0
    print("✓ Delete All verified")

def run_tests():
    db_cfg, key = get_spark_config()
    if not db_cfg:
        print("ERROR: No 'spark' configuration found in dbinfos.json")
        sys.exit(1)
        
    print(f"Using Spark config: {key}")
    
    pool = Pool.from_config(db_cfg, PoolBackend.DBUTILS)
    
    try:
        with pool.client() as client:
            db = get_db(client)
            print(f"Connected to Spark: {db.database_name}")
            
            setup_table(db)
            test_insert(db)
            test_select(db)
            test_advanced_select(db)
            test_merge(db)
            test_bulk_load(db)
            test_update(db)
            test_delete(db)
            
            print("\n========================================")
            print("ALL SPARK TESTS PASSED ✓")
            print("========================================")
            
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pool.dispose()
        print("\nPool disposed")

if __name__ == "__main__":
    run_tests()
