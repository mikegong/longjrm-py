import logging
from dotenv import load_dotenv, find_dotenv
from longjrm.config import JrmConfig

from longjrm.connection.dbconn import DatabaseConnection

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())  

# db_key = "postgres-test"
db_key = "mongodb-test"

cfg = JrmConfig.from_env("JRM_")
db_cfg = cfg.require(db_key)

db_connection = DatabaseConnection(db_cfg)

try:
    conn = db_connection.connect()
    if db_cfg.type not in ['mongodb', 'mongodb+srv']:
        with conn.cursor() as cursor:
            # Read a single record
            sql = "SELECT * from sample"
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
    else:
        # database name has to be defined for the mongodb connection
        result1 = conn[db_cfg.database]['Listing'].find_one()
        print(result1)
        result2 = []
        where = {'guestCount': 4, 'roomCount': 2}
        select_query = {'filter': where, 'limit': 1}
        res_cur = conn[db_cfg.database]['Listing'].find(**select_query)
        for row in res_cur:
            result2.append(row)
        print(result2)
except Exception as e:
    print(e)
