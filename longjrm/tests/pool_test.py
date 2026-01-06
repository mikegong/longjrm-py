import logging
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

#db_key = "postgres-test"
#db_key = "mysql-test"
db_key = "sqlite-test"

cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
# inject the configuration into the runtime
configure(cfg)
db_cfg = cfg.require(db_key)

pools = {}
pools[db_key] = Pool.from_config(db_cfg, PoolBackend.DBUTILS)

with pools[db_key].client() as client:
    db = get_db(client)

    # Setup for SQLite (or any fresh DB)
    table_name = "sample"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        age INTEGER,
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    db.execute(create_table_sql)

    # Insert sample data if empty
    count_result = db.query(f"SELECT COUNT(*) as count FROM {table_name}")
    if count_result['data'][0]['count'] == 0:
        insert_sql = f"""
        INSERT INTO {table_name} (id, name, email, age, status) VALUES 
        (1, 'Alice', 'alice@example.com', 25, 'active'),
        (2, 'Bob', 'bob@example.com', 30, 'inactive');
        """
        db.execute(insert_sql)

    
    # result = db.query("SELECT VERSION()")
    
    result1 = db.select(table='sample', columns=['*'], where={'status': 'active'})
    result2 = db.select(table='sample', columns=['*'], where={'status': 'inactive'})
    print(result1)
    print(result2)
    
    # cursor = conn.cursor()
    # cursor.execute("SELECT VERSION()")
    # version = cursor.fetchone()
    # print("Database version:", version)
    # Make sure to close the connection

pools[db_key].dispose()
