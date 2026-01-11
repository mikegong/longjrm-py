
import logging
import os
import tempfile
try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False

from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

class TestScriptExecution:
    """Test suite for script execution functionality"""
    
    @classmethod
    def setup_class(cls):
        """Setup test configuration and pools"""
        cls.cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
        configure(cls.cfg)

        # Setup pools for different databases and backends
        cls.pools = {}

        # Use what's available in typical test env - similar to transaction_test.py
        try:
            pg_cfg = cls.cfg.require("postgres-test")
            cls.pools['postgres-dbutils'] = Pool.from_config(pg_cfg, PoolBackend.DBUTILS)
        except Exception as e:
            logger.warning(f"Could not create PostgreSQL DBUTILS pool: {e}")

        try:
            mysql_cfg = cls.cfg.require("mysql-test")
            cls.pools['mysql-dbutils'] = Pool.from_config(mysql_cfg, PoolBackend.DBUTILS)
        except Exception as e:
            logger.warning(f"Could not create MySQL DBUTILS pool: {e}")
            
    @classmethod
    def teardown_class(cls):
        """Cleanup pools"""
        for db_type, pool in cls.pools.items():
            try:
                pool.dispose()
            except Exception:
                pass

    def setup_method(self):
        """Setup test table before each test"""
        for pool_key, pool in self.pools.items():
            try:
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DROP TABLE IF EXISTS test_script", [])
                    
                    if 'mysql' in pool_key:
                        db.execute("""
                            CREATE TABLE test_script (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                name VARCHAR(100)
                            )
                        """, [])
                    else:
                        db.execute("""
                            CREATE TABLE test_script (
                                id SERIAL PRIMARY KEY,
                                name VARCHAR(100)
                            )
                        """, [])
            except Exception as e:
                logger.error(f"Error setting up test table for {pool_key}: {e}")

    def teardown_method(self):
        """Cleanup test table"""
        for pool_key, pool in self.pools.items():
            try:
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DROP TABLE IF EXISTS test_script", [])
            except Exception:
                pass

    def test_execute_script_success(self):
        """Test executing a valid script with multiple statements"""
        sql_script = """
            INSERT INTO test_script (name) VALUES ('item1');
            INSERT INTO test_script (name) VALUES ('item2');
            INSERT INTO test_script (name) VALUES ('item3');
        """
        
        for pool_key, pool in self.pools.items():
            logger.info(f"Testing execute_script success on {pool_key}")
            with pool.client() as client:
                db = get_db(client)
                
                # Execute script
                result = db.execute_script(sql_script, transaction=True)
                assert result['status'] == 0
                assert "3 statements executed" in result['message']
                
                # Verify data
                result = db.select('test_script')
                assert result['count'] == 3
                names = sorted([r['name'] for r in result['data']])
                assert names == ['item1', 'item2', 'item3']

    def test_execute_script_failure_rollback(self):
        """Test that script failure rolls back changes when transaction=True"""
        sql_script = """
            INSERT INTO test_script (name) VALUES ('valid1');
            INSERT INTO test_script (name) VALUES ('valid2');
            INSERT INTO non_existent_table (col) VALUES (1); -- Error here
        """
        
        for pool_key, pool in self.pools.items():
            logger.info(f"Testing execute_script output rollback on {pool_key}")
            with pool.client() as client:
                db = get_db(client)
                
                # Execute script with transaction=True
                result = db.execute_script(sql_script, transaction=True)
                assert result['status'] == -1
                
                # Verify NO data persisted
                result = db.select('test_script')
                assert result['count'] == 0

    def test_execute_script_mixed_success_no_transaction(self):
        """Test that partial success persists when transaction=False"""
        # Note: This behavior depends on auto-commit settings. 
        # By default our connections might be in auto-commit mode.
        
        sql_script = """
            INSERT INTO test_script (name) VALUES ('persist1');
            INSERT INTO non_existent_table (col) VALUES (1); -- Error
        """
        
        for pool_key, pool in self.pools.items():
            logger.info(f"Testing execute_script no-transaction on {pool_key}")
            with pool.client() as client:
                db = get_db(client)
                
                # Ensure we are in autocommit mode or handle commits per statement
                # execute_script calls self.execute() which typically commits if autocommit is on
                # but db.execute doesn't explicitly commit unless autocommit is on at connection level.
                
                result = db.execute_script(sql_script, transaction=False)
                assert result['status'] == -1
                
                # Verify first item persisted
                # In Postgres, if one statement fails in a block, sometimes it invalidates the whole transaction if not in autocommit.
                # But here we are calling execute() sequentially in python loop.
                # If connection is autocommit, first insert commits immediately.
                
                result = db.select('test_script')
                # If autocommit is on, we expect 1. 
                # Our pool/connections usually default to autocommit=True unless set otherwise.
                if db.get_autocommit():
                   assert result['count'] == 1
                   assert result['data'][0]['name'] == 'persist1'

    def test_run_script_from_file(self):
        """Test running script from a file"""
        for pool_key, pool in self.pools.items():
            logger.info(f"Testing run_script_from_file on {pool_key}")
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp:
                tmp.write("INSERT INTO test_script (name) VALUES ('file1');\n")
                tmp.write("INSERT INTO test_script (name) VALUES ('file2');")
                tmp_path = tmp.name
            
            try:
                with pool.client() as client:
                    db = get_db(client)
                    result = db.run_script_from_file(tmp_path, transaction=True)
                    assert result['status'] == 0
                    
                    rows = db.select('test_script')
                    assert rows['count'] == 2
                    names = sorted([r['name'] for r in rows['data']])
                    assert names == ['file1', 'file2']
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

if __name__ == "__main__":
    t = TestScriptExecution()
    t.setup_class()
    
    t.setup_method()
    try:
        t.test_execute_script_success()
    except Exception as e:
        logger.error(f"test_execute_script_success failed: {e}")
        
    t.setup_method()
    try:
        t.test_execute_script_failure_rollback()
    except Exception as e:
         logger.error(f"test_execute_script_failure_rollback failed: {e}")

    t.setup_method()
    try:
        t.test_execute_script_mixed_success_no_transaction()
    except Exception as e:
         logger.error(f"test_execute_script_mixed_success_no_transaction failed: {e}")

    t.setup_method()
    try:
        t.test_run_script_from_file()
    except Exception as e:
         logger.error(f"test_run_script_from_file failed: {e}")

    t.teardown_class()
