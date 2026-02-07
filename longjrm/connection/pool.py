from __future__ import annotations
from typing import Any, Mapping, Optional, Generator, Dict, Callable, List
import logging
from enum import Enum
from contextlib import contextmanager
from longjrm.config.config import DatabaseConfig
from longjrm.config.runtime import get_config
from longjrm.connection.connectors import get_connector_class, unwrap_connection as _unwrap_connection
from longjrm.connection.driver_registry import load_driver_map, sa_minimal_url
from longjrm.database import get_db


logger = logging.getLogger(__name__)


class TransactionContext:
    """
    Context object for transaction operations.
    Provides access to the client and transaction control methods.
    """
    def __init__(self, client: Dict[str, Any]):
        self.client = client
        self._raw_conn = client['conn']

    def commit(self):
        """Manually commit the current transaction."""
        try:
            self._raw_conn.commit()
            logger.debug("Transaction committed manually")
        except Exception as e:
            logger.error(f"Manual commit failed: {e}")
            raise

    def rollback(self):
        """Manually rollback the current transaction."""
        try:
            self._raw_conn.rollback()
            logger.debug("Transaction rolled back manually")
        except Exception as e:
            logger.error(f"Manual rollback failed: {e}")
            raise


class PoolBackend(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    DBUTILS = "dbutils"
    
    
class _Backend:
    def __init__(self, db_cfg: DatabaseConfig):
        driver_info = load_driver_map().get((db_cfg.type or "").lower())
        self.database_module = driver_info.dbapi if driver_info else None

    def _get_client(self): raise NotImplementedError
    def dispose(self): raise NotImplementedError
    def get_config(self) -> DatabaseConfig: return self._cfg

class _SABackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig, sa_opts: Optional[Mapping[str, Any]]):
        super().__init__(db_cfg)
        from sqlalchemy import create_engine

        self._cfg = db_cfg
        jrm_cfg = get_config()
        self.connect_timeout = jrm_cfg.connect_timeout

        # Minimal URL that only selects the SQLAlchemy dialect+driver.
        # Real DB-API connections are created by dbconn via creator=...
        url = sa_minimal_url(db_cfg.type, override_driver=(db_cfg.options or {}).get("sa_driver"))

        # Ensure driver is loaded/patched (e.g. DB2 DLLs) before SQLAlchemy tries to use it
        from longjrm.connection.driver_registry import load_dbapi_module
        load_dbapi_module(db_cfg.type)

        opts: dict[str, Any] = {
            "pool_size": jrm_cfg.max_pool_size,
            "pool_timeout": jrm_cfg.pool_timeout,
            "future": True,
            "pool_pre_ping": True,
            "pool_reset_on_return": "rollback",  # safe with autocommit-by-default policy
        }
        if sa_opts:
            opts.update(sa_opts)

        if 'sqlite' in db_cfg.type.lower():
            # SQLite default SingletonThreadPool doesn't support pool_size/pool_timeout
            opts.pop("pool_size", None)
            opts.pop("pool_timeout", None)

        self._engine = create_engine(
            url,
            # Fresh connector per checkout -> no shared mutable state
            creator=lambda: get_connector_class(self._cfg.type)(self._cfg).connect(),
            **opts,
        )

        logger.info(f"Created SQLAlchemy engine for {self._cfg.type} '{self._cfg.database}'")

    def _get_client(self):
        fairy = self._engine.raw_connection()  # Returns _ConnectionFairy proxy
        # Set autocommit on the actual connection (unwrap all pooling wrappers)
        get_connector_class(self._cfg.type).set_dbapi_autocommit(_unwrap_connection(fairy), True)
        logger.debug(f"Got SQLAlchemy connection for {self._cfg.type} '{self._cfg.database}'")
        return {"conn": fairy,
                "database_type": self._cfg.type,
                "database_name": self._cfg.database,
                "db_lib": self.database_module}

    def dispose(self) -> None:
        try:
            self._engine.dispose()
            logger.info(f"Disposed SQLAlchemy engine for {self._cfg.type} '{self._cfg.database}'")
        except Exception:
            logger.error(f"Failed to dispose SQLAlchemy engine for {self._cfg.type} '{self._cfg.database}'")


# --------- DBUtils PooledDB backend ---------
class _DBUtilsBackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig, dbutils_opts: Optional[Mapping[str, Any]]):
        super().__init__(db_cfg)
        from dbutils.pooled_db import PooledDB

        self._cfg = db_cfg
        jrm_cfg = get_config()
        self.connect_timeout = jrm_cfg.connect_timeout

        opts: dict[str, Any] = {
            "maxconnections": (jrm_cfg.max_pool_size),
            "mincached":      (jrm_cfg.min_pool_size),
            "maxcached":      (jrm_cfg.max_cached_conn),
            "blocking": True,  # wait when exhausted
            "ping": 1,         # liveness on checkout
            "reset": True      # Always rollback on return to pool
        }
        if dbutils_opts:
            opts.update(dbutils_opts)

        # Single connector instance - PooledDB calls connect() when it needs new connections
        self._connector = get_connector_class(self._cfg.type)(self._cfg)

        # Custom wrapper to allow attaching metadata for DBUtils inspection
        def creator_func():
            return self._connector.connect()

        # Try to import metadata via registry (handles fixes automatically)
        from longjrm.connection.driver_registry import load_dbapi_module
        if self.database_module:
            mod = load_dbapi_module(self._cfg.type)  # Pass db_type for lookup
            
            if mod:
                # 1. Get exceptions via 'failures' option (official DBUtils arg)
                exceptions = (mod.InterfaceError, mod.DatabaseError)
                opts['failures'] = exceptions
                
                # 2. Get threadsafety
                threadsafety = getattr(mod, 'threadsafety', None)
                
                # Attach threadsafety to creator function (DBUtils inspection)
                if threadsafety is not None:
                    creator_func.threadsafety = threadsafety
                    
                logger.debug(f"Attached metadata from '{self.database_module}' to pool creator")
            else:
                logger.warning(f"Could not load metadata for '{self.database_module}'")

        self._pool = PooledDB(creator=creator_func, **opts)

        logger.info(f"Created DBUtils pool for {self._cfg.type} '{self._cfg.database}'")

    def _get_client(self):
        wrapper = self._pool.connection()  # May return various DBUtils wrapper types
        # Set autocommit on the actual connection (unwrap all pooling wrappers)
        get_connector_class(self._cfg.type).set_dbapi_autocommit(_unwrap_connection(wrapper), True)
        logger.debug(f"Got DBUtils connection for {self._cfg.type} '{self._cfg.database}'")
        return {"conn": wrapper,
                "database_type": self._cfg.type,
                "database_name": self._cfg.database,
                "db_lib": self.database_module
                }

    def dispose(self) -> None:
        try:
            self._pool.close()
            logger.info(f"Disposed DBUtils pool for {self._cfg.type} '{self._cfg.database}'")
        except Exception:
            logger.error(f"Failed to dispose DBUtils pool for {self._cfg.type} '{self._cfg.database}'")


# --------- Spark Backend (singleton session) ---------
class _SparkBackend(_Backend):
    """Spark backend using singleton SparkSession - Spark handles concurrency internally."""
    
    def __init__(self, db_cfg: DatabaseConfig, spark_opts: Optional[Mapping[str, Any]]):
        super().__init__(db_cfg)
        self._cfg = db_cfg
        self._session = None
    
    def _get_client(self):
        from longjrm.connection.connectors import SparkConnector
        
        if self._session is None:
            self._session = SparkConnector(self._cfg).connect()
        
        return {
            "conn": self._session,
            "database_type": "spark",
            "database_name": self._cfg.database or "default",
            "db_lib": "pyspark.sql"
        }
    
    def close_client(self, client):
        # No-op for Spark - session is reused (singleton pattern)
        logger.debug("Spark session checkout returned (session remains active)")
    
    def dispose(self) -> None:
        if self._session:
            try:
                self._session.close()  # Uses unified interface (SparkSessionWrapper.close())
                logger.info("Stopped SparkSession")
            except Exception as e:
                logger.error(f"Failed to stop SparkSession: {e}")
            finally:
                self._session = None


class Pool:
    """
    Unified Pool over independent backends (SQLAlchemy / DBUtils).
    """
    def __init__(self, backend_obj: _Backend):
        self._b = backend_obj

    @property
    def config(self) -> DatabaseConfig:
        return self._b._cfg
      
    @classmethod
    def from_config(
        cls,
        db_cfg: DatabaseConfig,
        pool_backend: PoolBackend,
        sa_opts: Optional[Mapping[str, Any]] = None,
        dbutils_opts: Optional[Mapping[str, Any]] = None,
        spark_opts: Optional[Mapping[str, Any]] = None,
    ) -> "Pool":
        # Spark uses singleton session pattern, not traditional pooling
        if db_cfg.type and db_cfg.type.lower() == 'spark':
            return cls(_SparkBackend(db_cfg, spark_opts))

        if pool_backend is PoolBackend.SQLALCHEMY:
            return cls(_SABackend(db_cfg, sa_opts))
        if pool_backend is PoolBackend.DBUTILS:
            return cls(_DBUtilsBackend(db_cfg, dbutils_opts))

        raise ValueError(f"Unknown backend: {pool_backend!r}")

    def _get_client(self):
        return self._b._get_client()

    def close_client(self, client: dict[str, Any]):
        # Delegate to backend if it has a close_client method (e.g., Spark)
        if hasattr(self._b, 'close_client'):
            self._b.close_client(client)
            return
            
        try:
            client["conn"].close()
            logger.info(f"Released {self._b._cfg.type} connection to {self._b._cfg.database} back to the pool")
        except Exception as e:
            logger.warning(f"Failed to close connection {self._b._cfg.type} to '{self._b._cfg.database}'. Error: {e}")

    @contextmanager
    def client(self, **context) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager for automatic client checkout/checkin.

        Usage:
            with pool.client(user_id=123) as client:
                db = Db(client)
                result = db.select("users", ["*"])
                # client automatically returned to pool on exit
        """
        client = None
        try:
            client = self._get_client()
            
            # SESSION SETUP
            if self.config.session_setup:
                try:
                    setup_query = self.config.session_setup.format(**context)
                    if hasattr(client['conn'], 'cursor'):
                       with client['conn'].cursor() as cur:
                           cur.execute(setup_query)
                    elif hasattr(client['conn'], 'execute'): # SQLAlchemy connection
                       client['conn'].execute(setup_query)
                except Exception as e:
                    logger.error(f"Session setup failed: {e}")
                    raise

            yield client
        finally:
            if client is not None:
                # SESSION TEARDOWN (handled before close)
                if self.config.session_teardown:
                    try:
                        if hasattr(client['conn'], 'cursor'):
                           with client['conn'].cursor() as cur:
                               cur.execute(self.config.session_teardown)
                        elif hasattr(client['conn'], 'execute'):
                           client['conn'].execute(self.config.session_teardown)
                    except Exception as e:
                        logger.warning(f"Session teardown failed: {e}")

                self.close_client(client)

    @contextmanager
    def transaction(self, isolation_level: Optional[str] = None) -> Generator[TransactionContext, None, None]:
        """
        Context manager for explicit transaction control.

        Args:
            isolation_level: Optional isolation level (database-specific)

        Usage:
            with pool.transaction() as tx:
                db = Db(tx.client)
                db.insert("users", {"name": "John"})
                db.update("profiles", {"active": True}, {"user_id": 123})
                # Auto-commit on success, auto-rollback on exception

            # Manual control:
            with pool.transaction() as tx:
                db = Db(tx.client)
                db.insert("users", data)
                tx.commit()  # Manual commit
        """
        client = None

        try:
            # Get client connection
            client = self._get_client()

            raw_conn = client['conn']
            # Set autocommit=False on the actual connection (unwrap all pooling wrappers)
            actual_conn = _unwrap_connection(raw_conn)
            get_connector_class(client['database_type']).set_dbapi_autocommit(actual_conn, False)

            # Set isolation level if specified
            if isolation_level:
                try:
                    # Use actual connection for consistency
                    get_connector_class(client['database_type']).set_isolation_level(actual_conn, isolation_level)
                    logger.debug(f"Set isolation level to {isolation_level}")
                except Exception as e:
                    logger.warning(f"Could not set isolation level {isolation_level}: {e}")

            # Create transaction context
            tx = TransactionContext(client)

            # Yield control to user code
            try:
                yield tx
                # Auto-commit on successful completion
                raw_conn.commit()
                logger.debug("Transaction auto-committed successfully")
            except Exception as e:
                # Auto-rollback on exception
                try:
                    raw_conn.rollback()
                    logger.debug("Transaction auto-rolled back due to exception")
                except Exception as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
                raise e

        except Exception as e:
            logger.error(f"Transaction context manager failed: {e}")
            raise e
        finally:
            if client is not None:
                # Always restore autocommit=True before returning to pool
                get_connector_class(client['database_type']).set_dbapi_autocommit(_unwrap_connection(client['conn']), True)

                # Return connection to pool
                self.close_client(client)
            

    def execute_transaction(self,
                          operations_func: Callable[['Db'], Any],
                          isolation_level: Optional[str] = None) -> Any:
        """
        Execute multiple operations in a single transaction.

        Args:
            operations_func: Function that takes a Db instance and performs operations
            isolation_level: Optional isolation level

        Usage:
            def my_operations(db):
                db.insert("users", {"name": "John"})
                db.update("profiles", {"active": True}, {"user_id": 123})
                return {"user_created": True}

            result = pool.execute_transaction(my_operations)
        """
        with self.transaction(isolation_level) as tx:
            db = get_db(tx.client)
            return operations_func(db)

    def execute_batch(self,
                     operations: List[Dict[str, Any]],
                     isolation_level: Optional[str] = None) -> List[Any]:
        """
        Execute multiple CRUD operations in one transaction.

        Args:
            operations: List of operation dictionaries with 'method' and 'params' keys
            isolation_level: Optional isolation level

        Usage:
            operations = [
                {"method": "insert", "params": {"table": "users", "data": {"name": "John"}}},
                {"method": "update", "params": {"table": "profiles", "data": {"active": True}, "where": {"user_id": 123}}}
            ]
            results = pool.execute_batch(operations)
        """
        def batch_ops(db):
            results = []
            for op in operations:
                method_name = op.get('method')
                params = op.get('params', {})

                if not method_name:
                    raise ValueError(f"Operation missing 'method' key: {op}")

                if not hasattr(db, method_name):
                    raise ValueError(f"Db class does not have method '{method_name}'")

                method = getattr(db, method_name)
                result = method(**params)
                results.append(result)

            return results

        return self.execute_transaction(batch_ops, isolation_level)

    def dispose(self) -> None:
        self._b.dispose()