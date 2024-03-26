import longjrm.load_env as jrm_env
from dbutils.pooled_db import PooledDB
from longjrm.connection.dbconn import DatabaseConnection, JrmConnectionError


class Pools(object):
    # Pools can contain multiple connection pools for various (type) databases
    def __init__(self):
        self.pools = {}
        self.logger = jrm_env.logger

    def start_pool(self, database_name):

        try:
            dbinfo = jrm_env.dbinfos[database_name]
            database_type = dbinfo['type']
            conn_pool_cls = PoolFactory.create_cp_cls(database_type)
            self.pools[database_name] = conn_pool_cls.create_pool(dbinfo)
            self.logger.info(f"Started connection pool for '{database_name}'")
        except Exception as e:
            message = f"Failed to start connection pool for '{database_name}': {e}"
            self.logger.error(message)
            raise JrmConnectionError(message)

    def get_client(self, database_name):
        dbinfo = jrm_env.dbinfos[database_name]
        if dbinfo['type'] in ['mongodb', 'mongodb+srv']:
            # get database/conn object of mongo via pool/connection object
            conn = self.pools[database_name][dbinfo['database']]
            db_lib = self.pools[database_name]['db_lib']
        elif dbinfo['type'] in ['mysql', 'postgres', 'postgresql']:
            # generic pool connection
            conn = self.pools[database_name].connection()
            db_lib = 'dbutils'
        else:
            self.logger.error(f"Invalid database type: {dbinfo.get('type')}")
            raise ValueError("Invalid database type")

        self.logger.info(f"Got connection from pool for {dbinfo.get('type')} '{database_name}'")
        return {"conn": conn, "database_type": dbinfo['type'], "database_name": database_name,
                "db_lib": db_lib}

    def release_connection(self, client):
        # connection pooling of mongodb is managed by database itself,
        # no need to release connection here
        if client['database_type'] not in ['mongodb', 'mongodb+srv']:
            # return the connection to the pool
            client['conn'].close()
            self.logger.info(f"Released connection to {client['database_type']} '{client['database_name']}'")

    def close_connection(self, conn):
        # close connection in connection pool
        pass

    def get_pool_size(self, database_name):
        pass

    def get_pool_available(self, database_name):
        pass

    def drain_database_pool(self, database_name):
        pass


class ConnectionPool(object):
    def __init__(self):
        super().__init__()


class GenericConnectionPool(ConnectionPool):
    def create_pool(self, dbinfo):
        db_connection = DatabaseConnection(dbinfo)
        pool = PooledDB(
            creator=db_connection.connect,
            maxconnections=int(jrm_env.config['POOL']['MAX_CONN_POOL_SIZE']),  # maximum number of connections allowed
        )
        return pool


class MongoConnectionPool(ConnectionPool):

    def create_pool(self, dbinfo):
        db_connection = DatabaseConnection(dbinfo)
        # Mongo's connection is already pooled by Mongo
        pool = db_connection.connect()
        return pool


class PoolFactory:
    # Create ConnectionPool class
    @classmethod
    def create_cp_cls(cls, pool_type):
        if pool_type in ['mongodb', 'mongodb+srv']:
            return MongoConnectionPool()
        elif pool_type in ['mysql', 'postgres', 'postgresql']:
            """
                Note: 
                all the database libraries that support DB-API 2 should work here,
                only above 2 types of database are initially tested at this point
                - MG Feb 10, 2024 
            """
            # generic pool
            return GenericConnectionPool()
        else:
            raise ValueError("Invalid pool type")
