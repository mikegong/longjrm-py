import logging
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

db_key = "postgres-test"
#db_key = "mysql-test"
#db_key = "mongodb-test"

cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
# inject the configuration into the runtime
configure(cfg)
db_cfg = cfg.require(db_key)

pools = {}
pools[db_key] = Pool.from_config(db_cfg, PoolBackend.SQLALCHEMY)
client = pools[db_key].get_client()

db = Db(client)

# result = db.query("SELECT VERSION()")

if db.database_type == 'mongodb':
    result = db.select(table='Listing', where={'guestCount': 4, 'roomCount': 2})
    print(result)
else:
    result1 = db.select(table='sample', columns=['*'], where={'c1': 'a'})
    result2 = db.select(table='sample', columns=['*'], where={'c2': 3})
    print(result1)
    print(result2)

# cursor = conn.cursor()
# cursor.execute("SELECT VERSION()")
# version = cursor.fetchone()
# print("Database version:", version)
# Make sure to close the connection

pools[db_key].close_client(client)
pools[db_key].dispose()
