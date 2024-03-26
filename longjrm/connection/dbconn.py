import importlib
import traceback
import longjrm.load_env as jrm_env


class DatabaseConnection(object):

    def __init__(self, dbinfo):
        self.database_type = dbinfo.get('type')
        self.host = dbinfo.get('host')
        self.instance = dbinfo.get('instance')
        self.database = dbinfo.get('database')
        self.user = dbinfo.get('user')
        self.password = dbinfo.get('password')
        self.port = dbinfo.get('port')
        self.autocommit = dbinfo.get('autocommit', True)  # By default, autocommit is on
        self.sqlcode = 0
        self.logger = jrm_env.logger
        self.conn = None  # database connection
        self.pconn = None  # persistent connection
        self.client = None  # database connection client, includes connection object and customized attributes

    def connect(self):
        # dynamically load database module according to database type
        db_module = importlib.import_module(jrm_env.db_lib_map[self.database_type])

        port = f":{self.port}" if self.port else ''
        connection_error_msg = f"Failed to connect to the {self.database_type} database '{self.database}' at {self.host}{port}"
        connection_msg = f"Connected to the {self.database_type} database '{self.database}' at {self.host}{port}"

        try:
            # MongoDB Atlas clusters use mongodb+srv protocol that doesn't support explicit port numbers
            db_url = f"{self.database_type}://{self.user}:{self.password}@{self.host}{port}/{self.database}"

            if self.database_type == 'mysql':
                # db_module is pymysql for mysql database
                self.conn = db_module.connect(host=self.host,
                                              user=self.user,
                                              password=self.password,
                                              database=self.database,
                                              autocommit=self.autocommit,
                                              cursorclass=db_module.cursors.DictCursor
                                              )
                self.logger.info(f"{connection_msg}, connection thread: {self.conn.thread_id()}")

            elif self.database_type in ['postgres', 'postgresql']:
                conn_string = f"host={self.host} dbname={self.database} user={self.user} password={self.password}"
                self.conn = db_module.connect(conn_string)
                self.conn.autocommit = self.autocommit
                self.logger.info(f"{connection_msg}, connection status: {self.conn.status}")

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                # Note:
                # 1. Atlas MongoDB cluster has to be connected through connection URL
                # 2. When MongoClient instance is created, connection pooling is handled automatically
                self.conn = db_module.MongoClient(db_url, maxPoolSize=int(jrm_env.config['POOL']['MAX_CONN_POOL_SIZE']))
                # An immediate connection can be forced by checking server function
                # self.conn.admin.command('ismaster')
                self.logger.info(f"{connection_msg}")

            else:
                raise ValueError("Invalid database type")

            return self.conn

        except Exception as e:
            self.logger.error(f"{connection_error_msg}: {e}")
            self.logger.error(traceback.format_exc())
            raise JrmConnectionError(e.args)

    def get_client(self):
        # compose connection client, including connection, database type, database name attributes

        self.client = {"conn": self.conn, "database_type": self.database_type, "database_name": self.database,
                       "db_lib": jrm_env.db_lib_map[self.database_type]}

        return self.client

    def close(self):
        self.client.conn.close()


class JrmConnectionError(Exception):
    """Raise exception when a connection failed."""

    def __init__(self, message, extra_info=''):
        super().__init__(message)
        self.extra_info = extra_info
