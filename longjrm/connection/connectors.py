import logging
import importlib
import traceback
from longjrm.connection.driver_registry import fix_ibm_db_dll, load_driver_map
from longjrm.connection.dsn_parts_helper import dsn_to_parts
from longjrm.config.runtime import get_config
from longjrm.config.config import DatabaseConfig


logger = logging.getLogger(__name__)


class JrmConnectionError(Exception):
    """Raised when a connection failed."""
    def __init__(self, message, extra_info=''):
        super().__init__(message)
        self.extra_info = extra_info


class BaseConnector:
    """Base class for database connectors."""

    def __init__(self, db_cfg: DatabaseConfig):
        """
        Initialize connector from DatabaseConfig.
        Handles DSN parsing and config extraction.
        """
        self.database_type = db_cfg.type

        # Support both DSN and parts, DSN has precedence
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
            self.dsn = None
            self.host = db_cfg.host
            self.port = db_cfg.port
            self.user = db_cfg.user
            self.password = db_cfg.password
            self.database = db_cfg.database
            self.autocommit = db_cfg.options.get('autocommit', True)
            self.options = db_cfg.options

        jrm_cfg = get_config()
        self.connect_timeout = jrm_cfg.connect_timeout

        # Load driver info
        driver_info = load_driver_map().get((db_cfg.type or "").lower())
        self.database_module = driver_info.dbapi if driver_info else None

    def connect(self):
        raise NotImplementedError

    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        """Set autocommit on a raw DB-API connection. Subclasses override if needed."""
        conn.autocommit = autocommit

    @staticmethod
    def get_dbapi_autocommit(conn) -> bool:
        """Get autocommit state from a raw DB-API connection. Subclasses override if needed."""
        return conn.autocommit

    @staticmethod
    def set_isolation_level(conn, isolation_level: str):
        """Set transaction isolation level. Subclasses override with database-specific SQL."""
        logger.warning(f"Isolation level setting not implemented for {type(conn).__name__}")

class PostgresConnector(BaseConnector):
    def connect(self):
        import psycopg
        port = f":{self.port}" if self.port else ''
        connection_msg = f"Connected to the postgres database '{self.database}' at {self.host}{port}"
        
        if not self.dsn:
            sslmode = self.options.get('sslmode', 'prefer')
            dsn = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} connect_timeout={self.connect_timeout} sslmode={sslmode}"
        else:
            dsn = self.dsn
            
        conn = psycopg.connect(dsn, autocommit=self.autocommit)
        logger.info(f"{connection_msg}, connection status: {conn.info.status.name}")
        return conn

    @staticmethod
    def set_isolation_level(conn, isolation_level: str):
        cur = conn.cursor()
        try:
            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        finally:
            cur.close()


class MySQLConnector(BaseConnector):
    def connect(self):
        import pymysql
        port = f":{self.port}" if self.port else ''
        connection_msg = f"Connected to the mysql database '{self.database}' at {self.host}{port}"
        
        # PyMySQL 1.1.x requires a callable for local_infile that returns a file handle
        def local_infile_handler(filename):
            return open(filename, 'rb')
        
        conn = pymysql.connect(host=self.host,
                             user=self.user,
                             password=self.password,
                             database=self.database,
                             connect_timeout=self.connect_timeout,
                             autocommit=self.autocommit,
                             local_infile=local_infile_handler)
        logger.info(f"{connection_msg}, connection thread: {conn.thread_id()}")
        return conn

    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        # PyMySQL uses a method
        try:
             conn.autocommit(autocommit)
        except Exception as e:
             raise ValueError(f"Could not set autocommit to {autocommit} for MySQL: {e}")

    @staticmethod
    def get_dbapi_autocommit(conn) -> bool:
        return conn.get_autocommit()

    @staticmethod
    def set_isolation_level(conn, isolation_level: str):
        cur = conn.cursor()
        try:
            cur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        finally:
            cur.close()


class SqliteConnector(BaseConnector):
    def connect(self):
        import sqlite3
        db_path = self.database or ':memory:'
        conn = sqlite3.connect(db_path)
        if self.autocommit:
            conn.isolation_level = None
        logger.info(f"Connected to SQLite database: {db_path}")
        return conn

    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        # SQLite uses isolation_level to control autocommit.
        # isolation_level = None means autocommit is ON.
        # isolation_level = '' or 'DEFERRED' means autocommit is OFF (transaction mode).
        try:
            if autocommit:
                conn.isolation_level = None
            else:
                conn.isolation_level = ''  # Starts transaction mode
        except Exception as e:
            logger.warning(f"Could not set SQLite isolation_level: {e}")

    @staticmethod
    def get_dbapi_autocommit(conn) -> bool:
        return conn.isolation_level is None

    @staticmethod
    def set_isolation_level(conn, isolation_level: str):
        logger.warning("SQLite has limited isolation level support")


class Db2Connector(BaseConnector):
    def connect(self):
        fix_ibm_db_dll()
        import ibm_db_dbi
        port = f":{self.port}" if self.port else ''
        connection_msg = f"Connected to the db2 database '{self.database}' at {self.host}{port}"
        
        # Construct DSN-like string for ibm_db_dbi
        conn_str = (
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL=TCPIP;"
            f"UID={self.user};"
            f"PWD={self.password};"
        )
        
        conn = ibm_db_dbi.connect(conn_str, "", "")
        
        conn.set_autocommit(self.autocommit)
            
        logger.info(f"{connection_msg}")
        return conn

    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        conn.set_autocommit(autocommit)


class OracleConnector(BaseConnector):
    def connect(self):
        import oracledb
        # Fetch LOBs (CLOB, BLOB) as strings/bytes directly
        oracledb.defaults.fetch_lobs = False
        
        # Connection string construction for Oracle
        # Supports both Thin and Thick modes automatically via oracledb
        
        dsn_str = None
        dsn_str = None
        if self.dsn:
            dsn_str = self.dsn
        elif self.host:
            port = self.port if self.port else 1521
            service_name = self.options.get('service_name', self.database)
            # Easy Connect syntax: host:port/service_name
            dsn_str = f"{self.host}:{port}/{service_name}"
            
        logger.info(f"Connecting to Oracle database at {self.host}")
        
        try:
            # Attempt connection
            conn = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=dsn_str
            )
            
            conn.autocommit = self.autocommit
            logger.info(f"Connected to Oracle database via oracledb")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            raise e


class SqlServerConnector(BaseConnector):
    def connect(self):
        import pyodbc
        
        driver = self.options.get('driver', '{ODBC Driver 17 for SQL Server}')
        
        if self.dsn:
            conn_str = self.dsn
        else:
            # Build connection string for pyodbc
            conn_str = (
                f"DRIVER={driver};"
                f"SERVER={self.host},{self.port if self.port else 1433};"
                f"DATABASE={self.database};"
                f"UID={self.user};"
                f"PWD={self.password}"
            )
            # Add extra options
            for k, v in self.options.items():
                if k != 'driver':
                    conn_str += f";{k}={v}"
                    
        logger.info(f"Connecting to SQL Server at {self.host}")
        
        try:
            conn = pyodbc.connect(conn_str, autocommit=self.autocommit)
            logger.info(f"Connected to SQL Server via pyodbc")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise e


class SparkConnector(BaseConnector):
    """Spark SQL connector using PySpark SparkSession singleton."""
    
    def __init__(self, db_cfg: DatabaseConfig):
        super().__init__(db_cfg)
        self.master = self.options.get('master', 'local[*]')
        self.app_name = self.options.get('app_name', 'longjrm-spark')
        self.enable_hive = self.options.get('enable_hive', False)
        self.enable_delta = self.options.get('enable_delta', True)
        self.spark_config = self.options.get('spark_config', {})
        self.existing_session = self.options.get('spark_session', None)
    
    @staticmethod
    def _add_close_method(spark):
        """Add unified close() method to SparkSession if not present."""
        if not hasattr(spark, 'close') or not callable(getattr(spark, 'close', None)):
            import types
            def close(self):
                self.stop()
                logger.info("SparkSession stopped via close()")
            spark.close = types.MethodType(close, spark)
        return spark
    
    def connect(self):
        """Return SparkSession with unified close() interface."""
        if self.existing_session:
            logger.info("Using existing SparkSession")
            return self._add_close_method(self.existing_session)
        
        from pyspark.sql import SparkSession
        builder = SparkSession.builder \
            .master(self.master) \
            .appName(self.app_name)
        
        # Delta Lake configuration
        if self.enable_delta:
            builder = builder.config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Apply custom Spark configurations
        for k, v in self.spark_config.items():
            builder = builder.config(k, v)
        
        if self.enable_hive:
            builder = builder.enableHiveSupport()
        
        # If Delta is enabled, we should use the utility to add JARs if available
        if self.enable_delta:
            try:
                from delta import configure_spark_with_delta_pip
                spark = configure_spark_with_delta_pip(builder).getOrCreate()
            except ImportError:
                logger.warning("delta-spark not installed but enable_delta=True. Delta features may not work.")
                spark = builder.getOrCreate()
        else:
            spark = builder.getOrCreate()
            
        logger.info(f"SparkSession created/retrieved: {self.app_name} @ {self.master}")
        return self._add_close_method(spark)
    
    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        """No-op: Spark has no autocommit concept."""
        pass
    
    @staticmethod
    def get_dbapi_autocommit(conn) -> bool:
        """Spark always behaves as autocommit."""
        return True
    
    @staticmethod
    def set_isolation_level(conn, isolation_level: str):
        """No-op: Spark SQL doesn't support traditional isolation levels."""
        logger.debug("Spark SQL does not support isolation levels")


class GenericConnector(BaseConnector):
    """
    Generic DB-API 2.0 connector handler.
    
    This class attempts to connect to any database with a DB-API 2.0 compliant driver.
    It determines the driver module in the following order:
    1. Looks up the database_type in `longjrm/connection/driver_map.json`.
    2. Uses the database_type string itself as the module name (e.g. 'pyodbc').
    
    If the module cannot be imported, a ValueError is raised.
    """
    def connect(self):
        from longjrm.connection.driver_registry import load_driver_map
        
        db_type = (self.database_type or "").lower()
        driver_info = load_driver_map().get(db_type)
        
        # Determine module name: map or fallback to type name
        module_name = driver_info.dbapi if driver_info else db_type
        
        try:
            db_module = importlib.import_module(module_name)
        except ImportError:
            raise ValueError(
                f"Could not import database module '{module_name}' for type '{db_type}'. "
                f"Ensure the driver is installed or defined in driver_map.json."
            )
            
        connection_msg = f"Connected to the {db_type} database via generic driver '{module_name}'"
        
        # Prepare arguments
        if self.dsn:
            conn = db_module.connect(self.dsn)
        else:
            # Construct kwargs from available config, filtering non-standard ones
            # Common arguments: host, user, password, database, port
            kwargs = {}
            if self.host: kwargs['host'] = self.host
            if self.user: kwargs['user'] = self.user
            if self.password: kwargs['password'] = self.password
            if self.database: kwargs['database'] = self.database
            if self.port: kwargs['port'] = int(self.port)
            if self.connect_timeout: kwargs['connect_timeout'] = self.connect_timeout
            
            # Pass extra options
            kwargs.update(self.options)
            
            conn = db_module.connect(**kwargs)
            
        # Try setting autocommit if supported
        if self.autocommit:
            try:
                if hasattr(conn, 'autocommit'):
                    attr = getattr(conn, 'autocommit')
                    if callable(attr):
                        conn.autocommit(True)
                    else:
                        conn.autocommit = True
            except Exception:
                logger.warning(f"Could not set autocommit for generic driver {module_name}")

        logger.info(connection_msg)
        return conn

    @staticmethod
    def set_dbapi_autocommit(conn, autocommit: bool):
        # Generic try-all approach
        try:
            if hasattr(conn, "autocommit") and not callable(getattr(conn, "autocommit")):
                conn.autocommit = autocommit        # psycopg
            elif hasattr(conn, "autocommit") and callable(getattr(conn, "autocommit")):
                conn.autocommit(autocommit)         # PyMySQL
            elif hasattr(conn, "set_autocommit"):
                conn.set_autocommit(autocommit)
            else:
                logger.warning(f"Autocommit not supported for connection type: {type(conn).__name__}")
        except Exception as e:
            raise ValueError(f"Function set_autocommit error for connection: {e}")


def get_connector_class(database_type):
    """Factory to get the appropriate connector class."""
    db_type = (database_type or "").lower()
    
    if db_type in ['postgres', 'postgresql']:
        return PostgresConnector
    elif db_type in ['mysql', 'mariadb']:
        return MySQLConnector
    elif db_type == 'sqlite':
        return SqliteConnector
    elif db_type == 'db2':
        return Db2Connector
    elif db_type == 'oracle':
        return OracleConnector
    elif db_type == 'sqlserver':
        return SqlServerConnector
    elif db_type == 'spark':
        return SparkConnector
    else:
        # Fallback to generic connector
        return GenericConnector


def unwrap_connection(conn):
    """
    Unwrap pooling library wrappers to get the actual DB-API connection.

    Known nesting scenarios:
    1. DBUtils PooledDB: PooledDedicatedDBConnection._con -> SteadyDBConnection._con -> actual
    2. DBUtils SharedDB: SharedDBConnection.con -> SteadyDBConnection._con -> actual
    3. SQLAlchemy: ConnectionFairy.dbapi_connection -> actual

    Args:
        conn: A connection object that may be wrapped by pooling libraries

    Returns:
        The actual DB-API connection object
    """
    actual_conn = conn
    conn_type = type(actual_conn).__name__
    conn_module = type(actual_conn).__module__
    logger.debug(f"Unwrapping connection: {actual_conn}")
    
    # Scenario 1: DBUtils PooledDB - PooledDedicatedDBConnection
    if 'dbutils' in conn_module.lower() and conn_type == 'PooledDedicatedDBConnection':
        if hasattr(actual_conn, '_con'):
            actual_conn = actual_conn._con  # -> SteadyDBConnection
            # Continue to unwrap SteadyDBConnection
            if hasattr(actual_conn, '_con'):
                logger.debug(f"Unwrapping SteadyDBConnection: {actual_conn}")
                actual_conn = actual_conn._con  # -> actual connection
        return actual_conn

    # Scenario 2: DBUtils SharedDB - SharedDBConnection
    if 'dbutils' in conn_module.lower() and conn_type == 'SharedDBConnection':
        if hasattr(actual_conn, 'con'):
            actual_conn = actual_conn.con  # -> SteadyDBConnection
            # Continue to unwrap SteadyDBConnection
            if hasattr(actual_conn, '_con'):
                actual_conn = actual_conn._con  # -> actual connection
        return actual_conn

    # Scenario 3: Direct SteadyDBConnection (if pool returns it directly)
    if 'dbutils' in conn_module.lower() and conn_type == 'SteadyDBConnection':
        if hasattr(actual_conn, '_con'):
            actual_conn = actual_conn._con  # -> actual connection
        return actual_conn

    # Scenario 4: SQLAlchemy ConnectionFairy
    if 'sqlalchemy' in conn_module.lower():
        if hasattr(actual_conn, 'dbapi_connection'):
            actual_conn = actual_conn.dbapi_connection  # -> actual connection
        elif hasattr(actual_conn, 'connection'):
            actual_conn = actual_conn.connection  # -> actual connection
        return actual_conn

    # No wrapper detected, return as-is (already actual connection)
    return actual_conn
