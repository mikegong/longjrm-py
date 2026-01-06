import logging
import importlib
import traceback
from longjrm.config.runtime import get_config
from longjrm.config.config import DatabaseConfig
from longjrm.connection.dsn_parts_helper import dsn_to_parts
from longjrm.connection.driver_registry import load_driver_map

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
            self.options = dbinfo.get('query', {})
        else:
            self.dsn = None  # Set dsn to None when not provided
            self.host = db_cfg.host
            self.port = db_cfg.port
            self.user = db_cfg.user
            self.password = db_cfg.password
            self.database = db_cfg.database  # database name
            self.autocommit = db_cfg.options.get('autocommit', True)  # By default, autocommit is on
            self.options = db_cfg.options

        jrm_cfg = get_config()
        self.connect_timeout = jrm_cfg.connect_timeout

        self.sqlcode = 0
        self.conn = None  # database connection
        self.pconn = None  # persistent connection

        # Load driver map where to get the database module map
        driver_info = load_driver_map().get((db_cfg.type or "").lower())
        self.database_module = driver_info.dbapi if driver_info else None

    def connect(self):
        # dynamically load database module according to database type
        db_module = importlib.import_module(self.database_module)

        port = f":{self.port}" if self.port else ''
        connection_msg = f"Connected to the {self.database_type} database '{self.database}' at {self.host}{port}"
        connection_error_msg = f"Failed to connect to the {self.database_type} database '{self.database}' at {self.host}{port}"
        
        try:
            if self.database_type in ['mysql', 'mariadb']:
                # db_module is pymysql for mysql/mariadb database
                self.conn = db_module.connect(host=self.host,
                                              user=self.user,
                                              password=self.password,
                                              database=self.database,
                                              connect_timeout=self.connect_timeout,
                                              autocommit=self.autocommit,
                                              )
                logger.info(f"{connection_msg}, connection thread: {self.conn.thread_id()}")

            elif self.database_type in ['postgres', 'postgresql']:
                if not self.dsn:
                    sslmode = self.options.get('sslmode', 'prefer')
                    dsn = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} connect_timeout={self.connect_timeout} sslmode={sslmode}"
                else:
                    dsn = self.dsn
                self.conn = db_module.connect(dsn)
                self.conn.autocommit = self.autocommit
                logger.info(f"{connection_msg}, connection status: {self.conn.status}")
            
            elif self.database_type == 'sqlite':
                # SQLite uses file path as database, or ':memory:' for in-memory database
                db_path = self.database or ':memory:'
                self.conn = db_module.connect(db_path)
                # SQLite isolation_level=None enables autocommit
                if self.autocommit:
                    self.conn.isolation_level = None
                logger.info(f"Connected to SQLite database: {db_path}")
            else:
                raise ValueError("Invalid database type")

            return self.conn

        except Exception as e:
            logger.error(f"{connection_error_msg}: {e}")
            logger.error(traceback.format_exc())
            raise JrmConnectionError(e.args)

    def set_autocommit(self, autocommit: bool):
        try:
            if hasattr(self.conn, "autocommit") and not callable(getattr(self.conn, "autocommit")):
                self.conn.autocommit = autocommit        # psycopg/psycopg2
            elif hasattr(self.conn, "autocommit") and callable(getattr(self.conn, "autocommit")):
                self.conn.autocommit(autocommit)         # PyMySQL
            else:
                logger.warning(f"Function is not supported for database type: {self.database_type}")
        except Exception as e:
            raise ValueError(f"Function set_autocommit error for database type: {self.database_type}: {e}")

    def close(self):
        try:
            self.conn.close()
            logger.info(f"Closed connection to {self.database_type} database '{self.database}'")
        except Exception as e:
            logger.info(f"Failed to close connection: {e}")
            pass

    def set_isolation_level(self, isolation_level: str):
        """Set transaction isolation level."""
        try:
            if self.database_type.lower() in ['postgres', 'postgresql']:
                self.conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
            elif self.database_type.lower() == 'mysql':
                self.conn.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            elif self.database_type.lower() == 'sqlite':
                # SQLite has limited isolation level support
                logger.warning("SQLite has limited isolation level support")
            else:
                logger.warning(f"Isolation level setting not implemented for {self.database_type}")
        except Exception as e:
            raise ValueError(f"Failed to set isolation level {isolation_level} for {self.database_type}: {e}")

def set_autocommit(dbapi_conn, autocommit: bool):
    """
    Set autocommit mode on a raw DB-API connection.

    Args:
        dbapi_conn: Raw DB-API connection object
        autocommit: True to enable autocommit, False to disable
    """
    try:
        if hasattr(dbapi_conn, "autocommit") and not callable(getattr(dbapi_conn, "autocommit")):
            dbapi_conn.autocommit = autocommit        # psycopg/psycopg2 (property)
        elif hasattr(dbapi_conn, "autocommit") and callable(getattr(dbapi_conn, "autocommit")):
            dbapi_conn.autocommit(autocommit)         # PyMySQL (method)
        else:
            logger.warning(f"Autocommit not supported for connection type: {type(dbapi_conn).__name__}")
    except Exception as e:
        raise ValueError(f"Function set_autocommit error for this connection: {e}") from e


def get_autocommit(dbapi_conn):
    """
    Get current autocommit state from a raw DB-API connection.

    Args:
        dbapi_conn: Raw DB-API connection object
        
    Returns:
        bool: Current autocommit state
    """
    try:
        if hasattr(dbapi_conn, "autocommit"):
            autocommit_attr = getattr(dbapi_conn, "autocommit")
            if callable(autocommit_attr):
                # PyMySQL: autocommit is a method, need to check get_autocommit
                if hasattr(dbapi_conn, "get_autocommit"):
                    return dbapi_conn.get_autocommit()
                # Fallback: assume True if we can't determine
                return True
            else:
                # psycopg/psycopg2: autocommit is a property
                return autocommit_attr
        else:
            logger.warning(f"get_autocommit not supported for connection type: {type(dbapi_conn).__name__}")
            return True  # Default to True if unknown
    except Exception as e:
        raise ValueError(f"Function get_autocommit error for this connection: {e}") from e


def set_isolation_level(dbapi_conn, database_type, isolation_level: str):
    """Set transaction isolation level."""
    cur = None
    try:
        cur = dbapi_conn.cursor()
        if database_type.lower() in ['postgres', 'postgresql']:
            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        elif database_type.lower() == 'mysql':
            cur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        elif database_type.lower() == 'sqlite':
            # SQLite has limited isolation level support
            logger.warning("SQLite has limited isolation level support")
        else:
            logger.error(f"Isolation level setting not implemented for {database_type}")
            raise ValueError(f"Isolation level setting not implemented for {database_type}")
    except Exception as e:
        raise ValueError(f"Failed to set isolation level {isolation_level} for {database_type}: {e}")
    finally:
        if cur:
            cur.close()

class JrmConnectionError(Exception):
    """Raise exception when a connection failed."""

    def __init__(self, message, extra_info=''):
        super().__init__(message)
        self.extra_info = extra_info

