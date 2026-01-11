"""
Connection Test Script for all database types including Spark SQL.

WINDOWS SPARK NOTE:
On Windows, PySpark may fail with "Python was not found" error due to Windows Store
Python aliases. This script automatically sets PYSPARK_PYTHON to fix this.
For manual fix: Settings → Apps → App execution aliases → Turn off Python entries
"""
import sys
import os

# =============================================================================
# WINDOWS PYSPARK FIX: Set PYSPARK_PYTHON to avoid Windows Store alias issue
# =============================================================================
if sys.platform == 'win32':
    python_exe = sys.executable
    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = python_exe
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
# =============================================================================

import logging
from longjrm.config.config import JrmConfig
from longjrm.connection.connectors import get_connector_class
from longjrm.config.runtime import configure
from longjrm.tests import test_utils

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)




def setup_sql_sample_table(conn, database_type):
    """Create and populate sample table for SQL databases."""
    cursor = conn.cursor()
    try:

        # Drop table if exists
        logger.info("Dropping sample table if it exists...")
        # Since we are using raw cursor here (not wrapper), we can't use db.execute from helper easily
        # but we can try to run standard drop. 
        # This test uses connector classes which return raw driver connections
        # connection_test.py seems to be testing raw connection mostly. 
        # But let's check what 'conn' is. It's from db_connection.connect().
        
        try:
            cursor.execute("DROP TABLE IF EXISTS sample")
        except:
             # Oracle/SQLServer might complain
             try: 
                cursor.execute("DROP TABLE sample")
             except:
                pass
        
        conn.commit()

        # Create table with appropriate syntax for database type
        logger.info("Creating sample table...")
        
        create_sql = test_utils.get_create_table_sql(database_type, "sample")
        if create_sql:
            cursor.execute(create_sql)
            conn.commit()

        # Insert sample data
        logger.info("Inserting sample data...")
        sample_data = [
            ("Alice Johnson", "alice@example.com", 28, "active"),
            ("Bob Smith", "bob@example.com", 35, "active"),
            ("Charlie Brown", "charlie@example.com", 42, "inactive"),
            ("Diana Prince", "diana@example.com", 30, "active"),
            ("Eve Wilson", "eve@example.com", 25, "pending")
        ]

        if database_type in ['sqlserver', 'db2', 'sqlite', 'sqlite3']:
            insert_sql = "INSERT INTO sample (name, email, age, status) VALUES (?, ?, ?, ?)"
        elif database_type == 'oracle':
            insert_sql = "INSERT INTO sample (name, email, age, status) VALUES (:1, :2, :3, :4)"
        else:  # PostgreSQL, MySQL
            insert_sql = "INSERT INTO sample (name, email, age, status) VALUES (%s, %s, %s, %s)"

        cursor.executemany(insert_sql, sample_data)
        conn.commit()
        logger.info(f"✓ Successfully created sample table with {len(sample_data)} records")
    finally:
        cursor.close()




# Run tests for all active configs
cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
configure(cfg)
active_configs = test_utils.get_active_test_configs(cfg)
if not active_configs:
    logger.warning("No active database configurations found.")
else:
    # Deduplicate db_keys (we don't need to test connection for both backends, just once per DB type usually)
    unique_keys = sorted(list(set(k for k, _ in active_configs)))
    
    for db_key in unique_keys:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Testing Connection for: {db_key}")
            logger.info(f"{'='*60}\n")
            
            db_cfg = cfg.require(db_key)
            
            # Skip Spark - Spark tests are run in spark_test.py
            if db_cfg.type == 'spark':
                print(f"Skipping {db_key} (Spark databases are run in spark_test.py)")
                continue

            connector = get_connector_class(db_cfg.type)(db_cfg)
            conn = connector.connect()
            
            # SQL databases
            setup_sql_sample_table(conn, db_cfg.type)

            # Test: Read all records
            logger.info("\nTest 1: Reading all records from sample table...")
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT * FROM sample ORDER BY id")
                results = cursor.fetchall()
                logger.info(f"Found {len(results)} records:")
                for row in results:
                    print(f"  {row}")
            finally:
                cursor.close()

            # Test: Read single record
            logger.info("\nTest 2: Reading single record...")
            cursor = conn.cursor()
            try:
                # Adjust placeholder for read too
                if db_cfg.type in ['sqlserver', 'db2', 'sqlite', 'sqlite3']:
                     placeholder = "?"
                elif db_cfg.type == 'oracle':
                     placeholder = ":1"
                else:
                     placeholder = "%s"
                     
                cursor.execute(f"SELECT * FROM sample WHERE status = {placeholder}", ("active",))
                # Note: Oracle/DB2/Sqlserver fetchone might behave differently if strict
                result = cursor.fetchone()
                logger.info(f"Result: {result}")
            finally:
                cursor.close()

            # Test: Count records
            logger.info("\nTest 3: Counting active users...")
            cursor = conn.cursor()
            try:
                if db_cfg.type in ['sqlserver', 'db2', 'sqlite', 'sqlite3']:
                     placeholder = "?"
                elif db_cfg.type == 'oracle':
                     placeholder = ":1"
                else:
                     placeholder = "%s"

                cursor.execute(f"SELECT COUNT(*) FROM sample WHERE status = {placeholder}", ("active",))
                count = cursor.fetchone()
                logger.info(f"Active users: {count[0]}")
            finally:
                cursor.close()

            logger.info(f"\n{'='*60}")
            logger.info(f"✓ {db_key} connection tests completed successfully!")
            logger.info(f"{'='*60}\n")
            conn.close()
            logger.info("Connection closed.")

        except Exception as e:
            logger.error(f"\n✗ Test failed for {db_key} with error: {e}")
            import traceback
            traceback.print_exc()

