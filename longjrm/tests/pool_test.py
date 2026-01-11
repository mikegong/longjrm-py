import logging
import sys
from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
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

def run_pool_test_for_db(db_key, backend, cfg):
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing Pool: {db_key} with backend {backend}")
        logger.info(f"{'='*60}\n")

        db_cfg = cfg.require(db_key)
        
        # skip spark as it uses its own session management, not traditional connection pooling
        if 'spark' in db_cfg.type:
            logger.info("Skipping Spark for pool test")
            return

        pool = Pool.from_config(db_cfg, backend)

        with pool.client() as client:
            db = get_db(client)

            # Setup table using test_utils for cross-db compatibility
            table_name = "sample"
            
            # Drop logically first? Or just create if not exists
            # test_utils.drop_table_silently(db, table_name) # maybe dangerous if we want to look at data, but for test it's fine.
            # actually connection_test drops it. Let's just try to create if not exists.
            
            create_table_sql = test_utils.get_create_table_sql(db_cfg.type, table_name)
            if create_table_sql:
                # Some DBs might fail creation if exists without "IF NOT EXISTS", 
                # but our test_utils strings usually have it or we handle it.
                # Actually test_utils strings vary. 
                # Let's try to run it, and ignore specific "already exists" errors if we must, 
                # or better, stick to what connection_test does -> Drop then Create.
                
                # For pool test, let's try to just select first. If fail, then create.
                # Or just ensure it exists.
                
                # Check if table exists
                try:
                    db.query(f"SELECT COUNT(*) FROM {table_name}")
                    exists = True
                except:
                    exists = False
                
                if not exists:
                    logger.info("Table 'sample' not found, creating...")
                    db.execute(create_table_sql)
                    
                    # Insert sample data
                    # We need db-specific placeholders for insert?
                    # connection_test handles this manually.
                    # Let's simplify: simply use db.insert which handles placeholders!
                    logger.info("Inserting sample data...")
                    db.insert(table_name, {"name": "Test User", "email": "test@example.com", "age": 30, "status": "active"})
                    
            # Perform Selects
            logger.info("Running SELECT query...")
            results = db.select(table=table_name, columns=['*'], options={'limit': 5})
            logger.info(f"Retrieved {len(results)} rows.")
            for row in results:
                logger.debug(f"  {row}")

        pool.dispose()
        logger.info(f"✓ Pool test passed for {db_key} ({backend})")

    except Exception as e:
        logger.error(f"✗ Pool test failed for {db_key} ({backend}) with error: {e}")
        import traceback
        traceback.print_exc()

def main():
    # Load config
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)
    
    # Get active configs (handles --db= filter)
    active_configs = test_utils.get_active_test_configs(cfg)
    
    if not active_configs:
        logger.warning("No active database configurations found for testing.")
        return

    # active_configs is a list of (db_key, backend)
    # We can deduplicate if we only want to test connection, but for POOL testing, 
    # we specifically might want to test both backends if available.
    
    for db_key, backend in active_configs:
        run_pool_test_for_db(db_key, backend, cfg)

if __name__ == "__main__":
    main()
