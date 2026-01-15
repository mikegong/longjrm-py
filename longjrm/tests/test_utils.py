import sys
import os
import logging
from longjrm.connection.pool import PoolBackend

logger = logging.getLogger(__name__)

def get_active_test_configs(cfg):
    """
    Discover available database configurations for testing.
    Filters based on 'TEST_DB' environment variable or '--db=' command line argument.
    
    Returns:
        List of tuples: (db_key, backend, test_function_wrapper)
        Note: The wrapper part is handled by the caller, this returns (db_key, backend)
    """
    
    # Check for filter in args or env
    filter_db = os.environ.get('TEST_DB')
    
    # Simple argv parsing for --db=
    for arg in sys.argv:
        if arg.startswith('--db='):
            filter_db = arg.split('=')[1]
            break
            
    available_dbs = []
    
    # Iterating over the config dictionary is the most reliable way 
    # assuming cfg.config is a dict-like object or has keys()
    # If cfg is JrmConfig, it might not expose keys directly easily without internal access
    # But usually we can iterate or use a known list. 
    # For now, we will inspect the 'dbinfos.json' keys by looking at the config object if possible,
    # or just iterate a known set of potential keys if we can't discover.
    # HOWEVER, JrmConfig usually loads everything into a dictionary. 
    # Let's assume we can iterate cfg._config or similar if available, OR
    # just look at standard keys.
    
    # Better approach: The user wants DYNAMIC discovery.
    # JrmConfig doesn't usually expose a "list all keys" method in standard usage patterns shown so far.
    # But locally we can cheat and look at the keys if we loaded it from validation.
    # Actually, let's look at how cfg is used. `cfg.require(key)`.
    # We might need to pass the raw json dict to this function or let it load dbinfos.json directly to find keys.
    
    # Let's try to load the dbinfos.json file manually to get the keys, 
    # then check if they exist in the loaded config.
    import json
    try:
        with open("test_config/dbinfos.json", "r") as f:
            db_infos = json.load(f)
            potential_keys = list(db_infos.keys())
    except Exception as e:
        logger.warning(f"Could not load test_config/dbinfos.json for discovery: {e}")
        potential_keys = ["postgres-test", "mysql-test", "db2-test", "oracle-test", "sqlserver-test"]

    backends = [PoolBackend.DBUTILS, PoolBackend.SQLALCHEMY]

    for key in potential_keys:
        # Apply filter
        if filter_db and filter_db not in key:
            continue
            
        # Verify it exists in the runtime config
        try:
            cfg.require(key)
        except:
            continue
            
        for backend in backends:
            available_dbs.append((key, backend))
            
    if not available_dbs:
        logger.warning(f"No database configurations found matching filter: {filter_db}")
        
    return available_dbs

def get_create_table_sql(db_type, table_name):
    """
    Returns the appropriate CREATE TABLE SQL for the given database type.
    
    Args:
        db_type (str): 'postgres', 'mysql', 'db2', 'oracle', 'sqlserver'
        table_name (str): Name of the table to create ('test_users' or 'test_transaction')
    """
    db_type = db_type.lower()
    
    # Standardize db_type names
    if db_type == 'postgresql': db_type = 'postgres'
    if 'db2' in db_type: db_type = 'db2'
    if 'oracle' in db_type: db_type = 'oracle'
    if 'sqlserver' in db_type or 'mssql' in db_type: db_type = 'sqlserver'
    if 'sqlite' in db_type: db_type = 'sqlite'
    if 'spark' in db_type: db_type = 'spark'
    
    # --- Table: test_users (Used in select, insert, update, delete tests) ---
    if table_name == 'test_users':
        if db_type == 'postgres':
            return """
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata JSONB,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        elif db_type == 'mysql':
            return """
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata JSON,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        elif db_type == 'db2':
            return """
            CREATE TABLE test_users (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata CLOB(32K),
                tags VARCHAR(1000),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        elif db_type == 'oracle':
            # Oracle 12c+ Identity
            # JSON is usually stored in BLOB or VARCHAR2 with check constraint, or CLOB.
            # We'll use CLOB for metadata to be safe and generic.
            return """
            CREATE TABLE test_users (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name VARCHAR2(100),
                email VARCHAR2(100),
                age NUMBER,
                status VARCHAR2(50) DEFAULT 'active',
                department VARCHAR2(50),
                salary NUMBER(10,2),
                metadata CLOB,
                tags VARCHAR2(1000),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        elif db_type == 'sqlserver':
            # SQL Server uses IDENTITY(1,1)
            # JSON is stored in NVARCHAR(MAX)
            # DATETIME2 is preferred over DATETIME
            return """
            CREATE TABLE test_users (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                status VARCHAR(50) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata NVARCHAR(MAX),
                tags TEXT,
                created_at DATETIME2 DEFAULT GETDATE(),
                updated_at DATETIME2 DEFAULT GETDATE()
            )
            """
        elif db_type == 'sqlite':
            return """
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                age INTEGER,
                status VARCHAR(50) DEFAULT 'active',
                department VARCHAR(50),
                salary DECIMAL(10,2),
                metadata TEXT,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        elif db_type == 'spark':
            # Spark SQL table (PARQUET for basic testing, use DELTA for full MERGE/UPDATE/DELETE)
            return """
            CREATE TABLE IF NOT EXISTS test_users (
                id BIGINT,
                name STRING,
                email STRING,
                age INT,
                status STRING,
                department STRING,
                salary DECIMAL(10,2),
                metadata STRING,
                tags STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
            """

    # --- Table: test_transaction (Used in transaction_test) ---
    elif table_name == 'test_transaction':
        if db_type == 'postgres':
            return """
            CREATE TABLE test_transaction (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                description VARCHAR(255)
            )
            """
        elif db_type == 'mysql':
            return """
            CREATE TABLE test_transaction (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                description VARCHAR(255)
            )
            """
        elif db_type == 'db2':
            return """
            CREATE TABLE test_transaction (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                description VARCHAR(255)
            )
            """
        elif db_type == 'oracle':
            return """
            CREATE TABLE test_transaction (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name VARCHAR2(100),
                value NUMBER,
                description VARCHAR2(255)
            )
            """
        elif db_type == 'sqlserver':
            return """
            CREATE TABLE test_transaction (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                description VARCHAR(255)
            )
            """
        elif db_type == 'sqlite':
            return """
            CREATE TABLE IF NOT EXISTS test_transaction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100),
                value INTEGER,
                description VARCHAR(255)
            )
            """
            
    # --- Table: test_merge_users (Used in merge_test.py) ---
    elif table_name == 'test_merge_users':
        if db_type == 'postgres':
             return """
                CREATE TABLE IF NOT EXISTS test_merge_users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata JSONB,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
             """
        elif db_type == 'mysql':
             return """
                CREATE TABLE IF NOT EXISTS test_merge_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata JSON,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_email (email)
                )
             """
        elif db_type == 'db2':
             return """
                CREATE TABLE test_merge_users (
                    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata CLOB(32K),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
             """
        elif db_type == 'oracle':
             return """
                CREATE TABLE test_merge_users (
                    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    email VARCHAR2(100) NOT NULL UNIQUE,
                    name VARCHAR2(100),
                    age NUMBER,
                    department VARCHAR2(50),
                    status VARCHAR2(20) DEFAULT 'active',
                    metadata CLOB,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
             """
        elif db_type == 'sqlserver':
             return """
                CREATE TABLE test_merge_users (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    name VARCHAR(100),
                    age INT,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata NVARCHAR(MAX),
                    last_updated DATETIME2 DEFAULT GETDATE()
                )
             """
        elif db_type == 'sqlite':
             return """
                CREATE TABLE IF NOT EXISTS test_merge_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    age INTEGER,
                    department VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    metadata TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
             """

    # --- Table: test_user_roles (Used in merge_test.py) ---
    elif table_name == 'test_user_roles':
        if db_type == 'postgres':
             return """
                CREATE TABLE IF NOT EXISTS test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions TEXT,
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
             """
        elif db_type == 'mysql':
             return """
                CREATE TABLE IF NOT EXISTS test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions TEXT,
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
             """
        elif db_type == 'db2':
             return """
                CREATE TABLE test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions VARCHAR(1000),
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
             """
        elif db_type == 'oracle':
             return """
                CREATE TABLE test_user_roles (
                    user_id NUMBER NOT NULL,
                    role_id NUMBER NOT NULL,
                    permissions VARCHAR2(1000),
                    granted_by VARCHAR2(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
             """
        elif db_type == 'sqlserver':
             return """
                CREATE TABLE test_user_roles (
                    user_id INT NOT NULL,
                    role_id INT NOT NULL,
                    permissions VARCHAR(MAX),
                    granted_by VARCHAR(100),
                    granted_at DATETIME2 DEFAULT GETDATE(),
                    PRIMARY KEY (user_id, role_id)
                )
             """
        elif db_type == 'sqlite':
             return """
                CREATE TABLE IF NOT EXISTS test_user_roles (
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    permissions TEXT,
                    granted_by VARCHAR(100),
                    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, role_id)
                )
             """

    # --- Table: sample (Used in connection_test) ---
    elif table_name == 'sample':
         if db_type == 'postgres':
            return """
                CREATE TABLE sample (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
         elif db_type == 'mysql':
            return """
                CREATE TABLE sample (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
         elif db_type == 'db2':
            return """
                CREATE TABLE sample (
                    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
         elif db_type == 'oracle':
            return """
                CREATE TABLE sample (
                    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name VARCHAR2(100) NOT NULL,
                    email VARCHAR2(100),
                    age NUMBER,
                    status VARCHAR2(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
         elif db_type == 'sqlserver':
            return """
                CREATE TABLE sample (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """
         elif db_type == 'sqlite':
            return """
                CREATE TABLE sample (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """

    return None

def drop_table_silently(db, table_name):
    """Executes DROP TABLE IF EXISTS or equivalent"""
    try:
        # Most DBs support this now, but Oracle/DB2/SQLServer might be picky in older versions
        # or require specific syntax.
        # For simplicity we try standar SQL or catch exception.
        
        if db.database_type == 'oracle':
            # Oracle doesn't support DROP TABLE IF EXISTS in older versions
            # But we can try-catch
            try:
                db.execute(f"DROP TABLE {table_name}")
            except:
                pass
        elif db.database_type == 'sqlserver':
             db.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        else:
             db.execute(f"DROP TABLE IF EXISTS {table_name}")
             
    except Exception as e:
        logger.debug(f"Drop table {table_name} ignored error: {e}")

