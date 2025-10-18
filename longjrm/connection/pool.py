from __future__ import annotations
from typing import Any, Mapping, Optional, Generator, Dict, Callable, List
import logging
from enum import Enum
from contextlib import contextmanager
from longjrm.config.config import DatabaseConfig
from longjrm.config.runtime import get_config
from longjrm.connection.dbconn import DatabaseConnection, set_autocommit, set_isolation_level
from longjrm.connection.driver_registry import load_driver_map, sa_minimal_url
from longjrm.database.db import Db


logger = logging.getLogger(__name__)


def _unwrap_connection(conn):
    """
    Unwrap pooling library wrappers to get the actual DB-API connection.

    Handles multiple levels of wrapping from DBUtils and SQLAlchemy:
    - DBUtils: PooledDedicatedDBConnection -> SteadyDBConnection -> actual connection
    - DBUtils: SharedDBConnection -> SteadyDBConnection -> actual connection
    - SQLAlchemy: ConnectionFairy -> actual connection

    Args:
        conn: A connection object that may be wrapped by pooling libraries

    Returns:
        The actual DB-API connection object
    """
    actual_conn = conn

    # Unwrap DBUtils wrappers (may have multiple levels)
    while hasattr(actual_conn, 'con') or hasattr(actual_conn, '_con'):
        if hasattr(actual_conn, 'con'):  # SharedDBConnection
            actual_conn = actual_conn.con
        elif hasattr(actual_conn, '_con'):  # PooledDedicatedDBConnection or SteadyDBConnection
            actual_conn = actual_conn._con

    # Handle SQLAlchemy wrapper
    if hasattr(actual_conn, 'dbapi_connection'):  # ConnectionFairy
        actual_conn = actual_conn.dbapi_connection

    return actual_conn


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
    MONGODB = "mongodb"
    
    
class _Backend:
    def __init__(self, db_cfg: DatabaseConfig):
        driver_info = load_driver_map().get((db_cfg.type or "").lower())
        self.database_module = driver_info.dbapi if driver_info else None

    def _get_client(self): raise NotImplementedError
    def close_client(self): raise NotImplementedError
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

        opts: dict[str, Any] = {
            "pool_size": jrm_cfg.max_pool_size,
            "pool_timeout": jrm_cfg.pool_timeout,
            "future": True,
            "pool_pre_ping": True,
            "pool_reset_on_return": "rollback",  # safe with autocommit-by-default policy
        }
        if sa_opts:
            opts.update(sa_opts)

        self._engine = create_engine(
            url,
            creator=lambda: DatabaseConnection(self._cfg).connect(),  # dbconn owns DSN vs parts
            **opts,
        )

        logger.info(f"Created SQLAlchemy engine for {self._cfg.type} '{self._cfg.database}'")

    def _get_client(self):
        fairy = self._engine.raw_connection()  # Returns _ConnectionFairy proxy
        # Set autocommit on the actual connection (unwrap all pooling wrappers)
        set_autocommit(_unwrap_connection(fairy), True)
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

        # Fresh DatabaseConnection per checkout -> no shared mutable state
        # Create ONE DatabaseConnection instance to use as factory
        self._db_connection = DatabaseConnection(self._cfg)
        
        # Use bound method as creator (more efficient than lambda)
        self._pool = PooledDB(creator=self._db_connection.connect, **opts)
        
        logger.info(f"Created DBUtils pool for {self._cfg.type} '{self._cfg.database}'")

    def _get_client(self):
        wrapper = self._pool.connection()  # May return various DBUtils wrapper types
        # Set autocommit on the actual connection (unwrap all pooling wrappers)
        set_autocommit(_unwrap_connection(wrapper), True)
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


class _MongoBackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig):
        self._cfg = db_cfg
        self._client = DatabaseConnection(db_cfg).connect()  # returns a MongoClient
        logger.info(f"Created MongoDB client for {self._cfg.type} '{self._cfg.database}'")

    def _get_client(self):
        # Do NOT close on context exit; MongoClient.close() would tear down the shared pool
        return {
            "conn": self._client,
            "database_type": self._cfg.type,
            "database_name": self._cfg.database or "",
            "db_lib": "pymongo"
        }

    def dispose(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass


class Pool:
    """
    Unified Pool over independent backends (SQLAlchemy / DBUtils / MongoDB).
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
    ) -> "Pool":

        if pool_backend is PoolBackend.SQLALCHEMY:
            return cls(_SABackend(db_cfg, sa_opts))
        if pool_backend is PoolBackend.DBUTILS:
            return cls(_DBUtilsBackend(db_cfg, dbutils_opts))
        if pool_backend is PoolBackend.MONGODB:
            return cls(_MongoBackend(db_cfg))

        raise ValueError(f"Unknown backend: {pool_backend!r}")

    def _get_client(self):
        return self._b._get_client()

    def close_client(self, client: dict[str, Any]):
        try:
            client["conn"].close()
            logger.info(f"Released {self._b._cfg.type} connection to {self._b._cfg.database} back to the pool")
        except Exception as e:
            logger.warning(f"Failed to close connection {self._b._cfg.type} to '{self._b._cfg.database}'. Error: {e}")

    @contextmanager
    def client(self) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager for automatic client checkout/checkin.

        Usage:
            with pool.client() as client:
                db = Db(client)
                result = db.select("users", ["*"])
                # client automatically returned to pool on exit
        """
        client = None
        try:
            client = self._get_client()
            yield client
        finally:
            if client is not None:
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

            # Skip transaction setup for MongoDB (it has its own transaction model)
            if client['database_type'].lower() in ['mongodb', 'mongodb+srv']:
                tx = TransactionContext(client)
                yield tx
                return

            raw_conn = client['conn']
            # Set autocommit=False on the actual connection (unwrap all pooling wrappers)
            actual_conn = _unwrap_connection(raw_conn)
            set_autocommit(actual_conn, False)

            # Set isolation level if specified
            if isolation_level:
                try:
                    # Use actual connection for consistency
                    set_isolation_level(actual_conn, client['database_type'], isolation_level)
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
                set_autocommit(_unwrap_connection(client['conn']), True)

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
            db = Db(tx.client)
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