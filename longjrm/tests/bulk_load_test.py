"""
Standalone Bulk Load Test script for longjrm.
This script executes tests manually and aborts immediately on ANY failure.
It does NOT use pytest for control flow.
"""

import sys
import os
import tempfile
import logging
import csv
import io

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from longjrm.config.runtime import get_config, configure
from longjrm.config.config import JrmConfig
from longjrm.connection.pool import Pool
from longjrm.database import get_db
from longjrm.tests.test_utils import get_active_test_configs, drop_table_silently

# Configure logging to stdout
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load configuration
try:
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
except Exception as e:
    print(f"FATAL: Failed to load test config: {e}")
    sys.exit(1)

AVAILABLE_DBS = get_active_test_configs(cfg)

def get_create_bulk_load_table_sql(db_type):
    """Create simple test table for bulk load testing."""
    db_type = db_type.lower()
    if 'postgres' in db_type:
        return "CREATE TABLE IF NOT EXISTS test_bulk_load (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER)"
    elif 'mysql' in db_type:
        return "CREATE TABLE IF NOT EXISTS test_bulk_load (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER)"
    elif 'db2' in db_type:
        return "CREATE TABLE test_bulk_load (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER)"
    elif 'sqlite' in db_type:
        return "CREATE TABLE IF NOT EXISTS test_bulk_load (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(100), email VARCHAR(100), age INTEGER)"
    return "CREATE TABLE test_bulk_load (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INTEGER)"

def create_test_csv_file(rows=50, delimiter=','):
    fd, filepath = tempfile.mkstemp(suffix='.csv')
    with os.fdopen(fd, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=delimiter)
        for i in range(1, rows + 1):
            writer.writerow([f'User {i}', f'user{i}@example.com', 20 + (i % 50)])
    return filepath

class BulkLoadTester:
    def __init__(self, db_key, backend):
        self.db_key = db_key
        self.backend = backend
        self.table_name = 'test_bulk_load'
        self.pool = None
        self.db = None
        self.client_ctx = None

    def setup(self):
        db_info = cfg.require(self.db_key)
        self.pool = Pool.from_config(db_info, self.backend)
        self.client_ctx = self.pool.client()
        client = self.client_ctx.__enter__()
        self.db = get_db(client)
        
        drop_table_silently(self.db, self.table_name)
        create_sql = get_create_bulk_load_table_sql(db_info.type)
        self.db.execute(create_sql)
        self.db.commit()

    def teardown(self):
        try:
            if self.db:
                drop_table_silently(self.db, self.table_name)
                self.db.commit()
            if self.client_ctx:
                self.client_ctx.__exit__(None, None, None)
            if self.pool:
                self.pool.dispose()
        except Exception as e:
            logger.error(f"Teardown error: {e}")

    def verify(self, result, expected_min_rows=None):
        if result['status'] != 0:
            msg = result.get('message', 'Unknown error')
            print(f"\n{'!'*20} FAILURE: {msg} {'!'*20}\n")
            sys.exit(1) # EXPLICIT ABORT
        
        if expected_min_rows is not None:
            res = self.db.query(f"SELECT COUNT(*) as cnt FROM {self.table_name}")
            count = res['data'][0]['cnt']
            if count < expected_min_rows:
                print(f"\n{'!'*20} FAILURE: Expected {expected_min_rows} rows, got {count} {'!'*20}\n")
                sys.exit(1)

    def test_file_load(self):
        db_type = cfg.require(self.db_key).type.lower()
        if db_type in ['oracle', 'sqlserver', 'sqlite', 'db2', 'spark']:
            return # Skip - Spark doesn't implement bulk_load, run spark_test.py instead

        csv_file = create_test_csv_file(rows=50)
        try:
            load_info = {'source': csv_file, 'columns': ['name', 'email', 'age'], 'delimiter': ','}
            if 'postgres' in db_type: load_info['format'] = 'csv'
            
            result = self.db.bulk_load(self.table_name, load_info=load_info)
            self.db.commit()
            self.verify(result, 50)
        finally:
            if os.path.exists(csv_file): os.remove(csv_file)

    def test_query_load(self):
        db_type = cfg.require(self.db_key).type.lower()
        if db_type in ['oracle', 'sqlserver', 'sqlite', 'spark']: return

        # Target table
        target = self.table_name + "_target"
        drop_table_silently(self.db, target)
        create_sql = get_create_bulk_load_table_sql(db_type).replace(self.table_name, target)
        self.db.execute(create_sql)
        
        # Clear source table from previous tests and add fresh data
        self.db.execute(f"DELETE FROM {self.table_name}")
        for i in range(1, 6):
            self.db.insert(self.table_name, {'name': f'U{i}', 'email': f'u{i}@t.com', 'age': 20+i})
        self.db.commit()

        try:
            # Different databases expect different load_info formats but same method call
            load_info = {
                'source': f"SELECT name, email, age FROM {self.table_name}",
                'operation': 'INSERT',
                'columns': ['name', 'email', 'age']
            }
            if db_type == 'db2':
                load_info['filetype'] = 'CURSOR'
            else:
                load_info['source_type'] = 'cursor'

            result = self.db.bulk_load(target, load_info=load_info)
            self.db.commit()
            self.verify(result)
            
            res = self.db.query(f"SELECT COUNT(*) as cnt FROM {target}")
            if res['data'][0]['cnt'] != 5:
                print(f"Query load failed: expected 5, got {res['data'][0]['cnt']}")
                sys.exit(1)
        finally:
            drop_table_silently(self.db, target)

def run_all():
    print(f"\nStarting Bulk Load Standalone Tests (Total Configs: {len(AVAILABLE_DBS)})\n")
    
    for db_key, backend in AVAILABLE_DBS:
        # Skip Spark entirely - Spark tests run separately in spark_test.py
        db_info = cfg.require(db_key)
        if db_info.type.lower() == 'spark':
            print(f"Skipping {db_key} (Spark tests run in spark_test.py)")
            continue
            
        print(f"{'='*30} Testing {db_key} ({backend}) {'='*30}")
        tester = BulkLoadTester(db_key, backend)
        try:
            tester.setup()
            
            print(f"Running test_file_load...")
            tester.test_file_load()
            
            print(f"Running test_query_load...")
            tester.test_query_load()
            
            print(f"SUCCESS: {db_key} ({backend})")
        except Exception as e:
            print(f"\n{'!'*20} UNCAUGHT EXCEPTION: {e} {'!'*20}\n")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            tester.teardown()
            print(f"{'-'*80}\n")

    print("\nALL BULK LOAD TESTS PASSED SUCCESSFULLY!\n")

if __name__ == "__main__":
    run_all()
