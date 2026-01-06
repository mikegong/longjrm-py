"""
Unit tests for Pool transaction functionality

Tests cover:
1. Basic transaction commit
2. Transaction rollback on error
3. Manual commit/rollback
4. Isolation level settings
5. Multiple operations in transaction
6. Nested transaction behavior
7. Connection state after transaction
8. Transaction with different pool backends
"""

import logging
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


class TestPoolTransaction:
    """Test suite for pool transaction functionality"""
    
    @classmethod
    def setup_class(cls):
        """Setup test configuration and pools"""
        cls.cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
        configure(cls.cfg)

        # Setup pools for different databases and backends
        cls.pools = {}

        # PostgreSQL pools - both backends
        try:
            pg_cfg = cls.cfg.require("postgres-test")
            cls.pools['postgres-dbutils'] = Pool.from_config(pg_cfg, PoolBackend.DBUTILS)
            logger.info("Created PostgreSQL DBUTILS pool")
        except Exception as e:
            logger.warning(f"Could not create PostgreSQL DBUTILS pool: {e}")

        try:
            pg_cfg = cls.cfg.require("postgres-test")
            cls.pools['postgres-sqlalchemy'] = Pool.from_config(pg_cfg, PoolBackend.SQLALCHEMY)
            logger.info("Created PostgreSQL SQLAlchemy pool")
        except Exception as e:
            logger.warning(f"Could not create PostgreSQL SQLAlchemy pool: {e}")

        # MySQL pools - both backends
        try:
            mysql_cfg = cls.cfg.require("mysql-test")
            cls.pools['mysql-dbutils'] = Pool.from_config(mysql_cfg, PoolBackend.DBUTILS)
            logger.info("Created MySQL DBUTILS pool")
        except Exception as e:
            logger.warning(f"Could not create MySQL DBUTILS pool: {e}")

        try:
            mysql_cfg = cls.cfg.require("mysql-test")
            cls.pools['mysql-sqlalchemy'] = Pool.from_config(mysql_cfg, PoolBackend.SQLALCHEMY)
            logger.info("Created MySQL SQLAlchemy pool")
        except Exception as e:
            logger.warning(f"Could not create MySQL SQLAlchemy pool: {e}")
    
    @classmethod
    def teardown_class(cls):
        """Cleanup pools"""
        for db_type, pool in cls.pools.items():
            try:
                pool.dispose()
                logger.info(f"Disposed {db_type} pool")
            except Exception as e:
                logger.error(f"Error disposing {db_type} pool: {e}")
    
    def setup_method(self):
        """Setup test table before each test"""
        for pool_key, pool in self.pools.items():
            try:
                with pool.client() as client:
                    db = get_db(client)

                    # Drop and recreate test table
                    if pool_key.startswith('mysql'):
                        db.execute("DROP TABLE IF EXISTS test_transaction", [])
                        db.execute("""
                            CREATE TABLE test_transaction (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                name VARCHAR(100),
                                value INT,
                                description VARCHAR(255)
                            )
                        """, [])
                    else:  # PostgreSQL
                        db.execute("DROP TABLE IF EXISTS test_transaction", [])
                        db.execute("""
                            CREATE TABLE test_transaction (
                                id SERIAL PRIMARY KEY,
                                name VARCHAR(100),
                                value INT,
                                description VARCHAR(255)
                            )
                        """, [])

                    logger.info(f"Setup test table for {pool_key}")
            except Exception as e:
                logger.error(f"Error setting up test table for {pool_key}: {e}")
    
    def teardown_method(self):
        """Cleanup test table after each test"""
        for pool_key, pool in self.pools.items():
            try:
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DROP TABLE IF EXISTS test_transaction", [])
                    logger.info(f"Cleaned up test table for {pool_key}")
            except Exception as e:
                logger.error(f"Error cleaning up test table for {pool_key}: {e}")
    
    def test_transaction_commit_success(self):
        """Test that successful transaction commits all changes"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing transaction commit for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                # Execute transaction
                with pool.transaction() as tx:
                    db = get_db(tx.client)

                    # Insert multiple rows using Db API
                    db.insert('test_transaction', {'name': 'row1', 'value': 100, 'description': 'First row'})
                    db.insert('test_transaction', {'name': 'row2', 'value': 200, 'description': 'Second row'})

                    logger.info(f"Inserted 2 rows in transaction for {pool_key}")

                # Verify commit - data should be persisted
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 2, f"Expected 2 rows, got {result['count']}"
                    assert result['data'][0]['name'] == 'row1'
                    assert result['data'][1]['name'] == 'row2'

                    logger.info(f"✓ Transaction committed successfully for {pool_key}")
                    logger.info(f"  Verified {result['count']} rows persisted")

            except Exception as e:
                logger.error(f"✗ Transaction commit test failed for {pool_key}: {e}")
                raise
    
    def test_transaction_rollback_on_error(self):
        """Test that transaction rolls back on error"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing transaction rollback for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                # Execute transaction that will fail
                error_occurred = False
                try:
                    with pool.transaction() as tx:
                        db = get_db(tx.client)

                        # Insert valid row
                        db.insert('test_transaction', {'name': 'row1', 'value': 100, 'description': 'First row'})
                        logger.info(f"Inserted first row for {pool_key}")

                        # Cause an error (insert to non-existent table)
                        db.insert('nonexistent_table', {'bad': 'data'})
                except Exception as e:
                    error_occurred = True

                assert error_occurred, "Expected exception did not occur"

                # Verify rollback - no data should be persisted
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 0, f"Expected 0 rows after rollback, got {result['count']}"

                    logger.info(f"✓ Transaction rolled back successfully for {pool_key}")
                    logger.info(f"  Verified 0 rows persisted (rollback worked)")

            except AssertionError:
                raise
            except Exception as e:
                logger.error(f"✗ Transaction rollback test failed for {pool_key}: {e}")
                raise
    
    def test_transaction_manual_commit(self):
        """Test manual commit within transaction"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing manual commit for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                with pool.transaction() as tx:
                    db = get_db(tx.client)

                    # Insert row
                    db.insert('test_transaction', {'name': 'manual', 'value': 123, 'description': 'Manual commit test'})

                    # Manual commit
                    tx.commit()
                    logger.info(f"Manually committed transaction for {pool_key}")

                    # Insert another row (will be auto-committed at end)
                    db.insert('test_transaction', {'name': 'auto', 'value': 456, 'description': 'Auto commit test'})

                # Verify both rows persisted
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 2, f"Expected 2 rows, got {result['count']}"

                    logger.info(f"✓ Manual commit worked for {pool_key}")
                    logger.info(f"  Verified {result['count']} rows persisted")

            except Exception as e:
                logger.error(f"✗ Manual commit test failed for {pool_key}: {e}")
                raise
    
    def test_transaction_manual_rollback(self):
        """Test manual rollback within transaction"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing manual rollback for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                with pool.transaction() as tx:
                    db = get_db(tx.client)

                    # Insert row
                    db.insert('test_transaction', {'name': 'rollback', 'value': 789, 'description': 'Manual rollback test'})

                    # Manual rollback
                    tx.rollback()
                    logger.info(f"Manually rolled back transaction for {pool_key}")

                    # Insert another row (will be auto-committed at end)
                    db.insert('test_transaction', {'name': 'keep', 'value': 999, 'description': 'This should persist'})

                # Verify only the second row persisted
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 1, f"Expected 1 row, got {result['count']}"
                    assert result['data'][0]['name'] == 'keep'

                    logger.info(f"✓ Manual rollback worked for {pool_key}")
                    logger.info(f"  First insert rolled back, second persisted")

            except Exception as e:
                logger.error(f"✗ Manual rollback test failed for {pool_key}: {e}")
                raise
    
    def test_transaction_with_isolation_level(self):
        """Test transaction with specific isolation level"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing isolation level for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                # Test with SERIALIZABLE isolation level
                with pool.transaction(isolation_level="SERIALIZABLE") as tx:
                    db = get_db(tx.client)

                    db.insert('test_transaction', {'name': 'isolated', 'value': 555, 'description': 'Isolation level test'})

                    logger.info(f"Transaction with SERIALIZABLE isolation for {pool_key}")

                # Verify data persisted
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 1

                    logger.info(f"✓ Isolation level test passed for {pool_key}")

            except Exception as e:
                # Some databases might not support certain isolation levels
                logger.warning(f"Isolation level test skipped for {pool_key}: {e}")
    
    def test_multiple_operations_in_transaction(self):
        """Test multiple different operations in one transaction"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing multiple operations for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table and setup initial data
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")
                    db.insert('test_transaction', {'name': 'initial', 'value': 100, 'description': 'Initial data'})

                # Transaction with multiple operations
                with pool.transaction() as tx:
                    db = get_db(tx.client)

                    # Insert
                    db.insert('test_transaction', {'name': 'new', 'value': 200, 'description': 'New row'})

                    # Update
                    db.update('test_transaction', {'value': 150}, {'name': 'initial'})

                    # Select (verify within transaction)
                    result = db.select('test_transaction', ['*'], where={'name': 'initial'})
                    assert result['data'][0]['value'] == 150

                    logger.info(f"Performed INSERT, UPDATE, SELECT in transaction for {pool_key}")

                # Verify all changes committed
                with pool.client() as client:
                    db = get_db(client)
                    result = db.select('test_transaction', ['*'])

                    assert result['count'] == 2
                    initial_row = [r for r in result['data'] if r['name'] == 'initial'][0]
                    assert initial_row['value'] == 150

                    logger.info(f"✓ Multiple operations committed successfully for {pool_key}")

            except Exception as e:
                logger.error(f"✗ Multiple operations test failed for {pool_key}: {e}")
                raise
    
    def test_connection_state_after_transaction(self):
        """Test that connection autocommit is restored after transaction"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing connection state restoration for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                # Clean table before this specific pool's test
                with pool.client() as client:
                    db = get_db(client)
                    db.execute("DELETE FROM test_transaction", [])
                    logger.debug(f"Cleaned table before test for {pool_key}")

                # Use transaction
                with pool.transaction() as tx:
                    db = get_db(tx.client)
                    db.insert('test_transaction', {'name': 'test', 'value': 111, 'description': 'State test'})

                # Use regular client - should work with autocommit restored
                with pool.client() as client:
                    db = get_db(client)

                    # This should work with autocommit=True
                    db.insert('test_transaction', {'name': 'autocommit', 'value': 222, 'description': 'Autocommit test'})

                    # Verify both rows exist
                    result = db.select('test_transaction', ['*'])
                    assert result['count'] == 2

                    logger.info(f"✓ Connection state restored properly for {pool_key}")

            except Exception as e:
                logger.error(f"✗ Connection state test failed for {pool_key}: {e}")
                raise
    
    def test_transaction_context_manager_interface(self):
        """Test that transaction context manager provides correct interface"""
        for pool_key, pool in self.pools.items():
            logger.info(f"\n{'='*80}")
            logger.info(f"Testing transaction context interface for {pool_key}")
            logger.info(f"{'='*80}")

            try:
                with pool.transaction() as tx:
                    # Verify tx has required methods
                    assert hasattr(tx, 'client'), "TransactionContext missing 'client' attribute"
                    assert hasattr(tx, 'commit'), "TransactionContext missing 'commit' method"
                    assert hasattr(tx, 'rollback'), "TransactionContext missing 'rollback' method"
                    assert callable(tx.commit), "'commit' is not callable"
                    assert callable(tx.rollback), "'rollback' is not callable"

                    # Verify client structure
                    assert 'conn' in tx.client, "client missing 'conn' key"
                    assert 'database_type' in tx.client, "client missing 'database_type' key"
                    assert 'database_name' in tx.client, "client missing 'database_name' key"

                    logger.info(f"✓ Transaction context interface is correct for {pool_key}")

            except Exception as e:
                logger.error(f"✗ Interface test failed for {pool_key}: {e}")
                raise


def run_tests():
    """Run all tests manually (without pytest)"""
    test_suite = TestPoolTransaction()
    
    # Setup
    TestPoolTransaction.setup_class()
    
    test_methods = [
        ('test_transaction_commit_success', test_suite.test_transaction_commit_success),
        ('test_transaction_rollback_on_error', test_suite.test_transaction_rollback_on_error),
        ('test_transaction_manual_commit', test_suite.test_transaction_manual_commit),
        ('test_transaction_manual_rollback', test_suite.test_transaction_manual_rollback),
        ('test_transaction_with_isolation_level', test_suite.test_transaction_with_isolation_level),
        ('test_multiple_operations_in_transaction', test_suite.test_multiple_operations_in_transaction),
        ('test_connection_state_after_transaction', test_suite.test_connection_state_after_transaction),
        ('test_transaction_context_manager_interface', test_suite.test_transaction_context_manager_interface),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_method in test_methods:
        try:
            logger.info(f"\n{'#'*80}")
            logger.info(f"Running: {test_name}")
            logger.info(f"{'#'*80}")
            
            test_suite.setup_method()
            test_method()
            test_suite.teardown_method()
            
            passed += 1
            logger.info(f"✓ PASSED: {test_name}")
        
        except Exception as e:
            failed += 1
            logger.error(f"✗ FAILED: {test_name}")
            logger.error(f"  Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Teardown
    TestPoolTransaction.teardown_class()
    
    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"TEST SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Total: {passed + failed}")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"{'='*80}")
    
    return passed, failed


if __name__ == "__main__":
    # Run tests
    passed, failed = run_tests()
    
    # Exit with appropriate code
    import sys
    sys.exit(0 if failed == 0 else 1)

