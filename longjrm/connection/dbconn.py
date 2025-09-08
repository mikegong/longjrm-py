import json
import logging
import importlib
import traceback
from pathlib import Path
from longjrm.config import DatabaseConfig
from longjrm.dsn_parts_helper import dsn_to_parts, parts_to_dsn

logger = logging.getLogger(__name__)


class DatabaseConnection(object):

    def __init__(self, db_cfg: DatabaseConfig):

        self.database_type = db_cfg.type

        # support both dsn and parts, dsn has precedence
        if db_cfg.dsn:
            self.dsn = db_cfg.dsn
            dbinfo = dsn_to_parts(db_cfg.dsn)
            self.host = dbinfo.get('host')
            self.port = dbinfo.get('port')
            self.user = dbinfo.get('user')
            self.password = dbinfo.get('password')
            self.database = dbinfo.get('database')
            self.autocommit = dbinfo.get('query', {}).get('autocommit', True)
        else:
            self.host = db_cfg.host
            self.port = db_cfg.port
            self.user = db_cfg.user
            self.password = db_cfg.password
            self.database = db_cfg.database  # database name
            self.autocommit = db_cfg.options.get('autocommit', True)  # By default, autocommit is on

        self.max_conn_pool_size = db_cfg.options.get('MAX_CONN_POOL_SIZE', 5) # for MongoDB only
        self.sqlcode = 0
        self.conn = None  # database connection
        self.pconn = None  # persistent connection
        self.client = None  # database connection client, includes connection object and customized attributes

        # Get the path to db_lib_map.json relative to this file
        db_lib_map_path = Path(__file__).parent / 'db_lib_map.json'
        with open(db_lib_map_path, 'r') as f:
            self.db_lib_map = json.load(f)

    def connect(self):
        # dynamically load database module according to database type
        db_module = importlib.import_module(self.db_lib_map[self.database_type])

        port = f":{self.port}" if self.port else ''
        connection_msg = f"Connected to the {self.database_type} database '{self.database}' at {self.host}{port}"
        connection_error_msg = f"Failed to connect to the {self.database_type} database '{self.database}' at {self.host}{port}"
        
        try:
            if self.database_type == 'mysql':
                # db_module is pymysql for mysql database
                self.conn = db_module.connect(host=self.host,
                                              user=self.user,
                                              password=self.password,
                                              database=self.database,
                                              autocommit=self.autocommit,
                                              cursorclass=db_module.cursors.DictCursor
                                              )
                logger.info(f"{connection_msg}, connection thread: {self.conn.thread_id()}")

            elif self.database_type in ['postgres', 'postgresql']:
                if not self.dsn:
                    sslmode = self.options.get('sslmode', 'prefer')
                    dsn = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} sslmode={sslmode}"
                else:
                    dsn = self.dsn
                self.conn = db_module.connect(dsn)
                self.conn.autocommit = self.autocommit
                logger.info(f"{connection_msg}, connection status: {self.conn.status}")

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                # Note:
                # 1. Atlas MongoDB cluster has to be connected through connection URL
                # 2. When MongoClient instance is created, connection pooling is handled automatically
                # 3. MongoDB Atlas clusters use mongodb+srv protocol that doesn't support explicit port numbers
                
                if not self.dsn:
                    dsn = f"{self.database_type}://{self.user}:{self.password}@{self.host}{port}/{self.database}"
                else:
                    dsn = self.dsn

                self.conn = db_module.MongoClient(dsn, maxPoolSize=self.max_conn_pool_size)
                # An immediate connection can be forced by checking server function
                # self.conn.admin.command('ismaster')
                logger.info(f"{connection_msg}")

            else:
                raise ValueError("Invalid database type")

            return self.conn

        except Exception as e:
            logger.error(f"{connection_error_msg}: {e}")
            logger.error(traceback.format_exc())
            raise JrmConnectionError(e.args)

    def get_client(self):
        # compose connection client, including connection, database type, database name attributes

        self.client = {"conn": self.conn, "database_type": self.database_type, "database_name": self.database,
                       "db_lib": self.db_lib_map[self.database_type]}

        return self.client

    def close(self):
        self.client.conn.close()


class JrmConnectionError(Exception):
    """Raise exception when a connection failed."""

    def __init__(self, message, extra_info=''):
        super().__init__(message)
        self.extra_info = extra_info
