from __future__ import annotations
from typing import Any, Mapping, Optional
from enum import Enum
import logging
from longjrm.config.config import DatabaseConfig
from longjrm.config.runtime import get_config
from longjrm.connection.dbconn import DatabaseConnection, JrmConnectionError, enforce_autocommit
from longjrm.connection.driver_registry import load_driver_map, sa_minimal_url


logger = logging.getLogger(__name__)


class PoolBackend(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    DBUTILS = "dbutils"
    MONGODB = "mongodb"
    
    
class _Backend:
    def __init__(self, db_cfg: DatabaseConfig):
        driver_info = load_driver_map().get((db_cfg.type or "").lower())
        self.database_module = driver_info.dbapi if driver_info else None

    def get_client(self): raise NotImplementedError
    def close_client(self): raise NotImplementedError
    def dispose(self): raise NotImplementedError
    def get_config(self) -> DatabaseConfig: return self._cfg

class _SABackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig, sa_opts: Optional[Mapping[str, Any]]):
        super().__init__(db_cfg)
        from sqlalchemy import create_engine, event

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
        
        # Normalize connection state such as autocommit=True on checkout in ONE place (dbconn policy).
        @event.listens_for(self._engine, "checkout")
        def _on_checkout(dbapi_conn, conn_record, conn_proxy):
            enforce_autocommit(dbapi_conn, self._cfg.type)

    def get_client(self):
        raw = self._engine.raw_connection() 
        logger.debug(f"Got SQLAlchemy connection for {self._cfg.type} '{self._cfg.database}'")
        return {"conn": raw, 
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
            "reset": True,     # rollback on return; does not change autocommit
        }
        if dbutils_opts:
            opts.update(dbutils_opts)

        # Fresh DatabaseConnection per checkout -> no shared mutable state
        self._pool = PooledDB(creator=lambda: DatabaseConnection(self._cfg).connect(), **opts)
        logger.info(f"Created DBUtils pool for {self._cfg.type} '{self._cfg.database}'")

    def get_client(self):
        raw = self._pool.connection()  # .close() -> returns to DBUtils pool
        # Align state on checkout to match SA behavior (optional but recommended)
        enforce_autocommit(raw, self._cfg)
        logger.debug(f"Got DBUtils connection for {self._cfg.type} '{self._cfg.database}'")
        return {"conn": raw, 
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

    def get_client(self):
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

    def get_client(self):
        return self._b.get_client()

    def close_client(self, client: dict[str, Any]):
        try:
            client["conn"].close()
            logger.info(f"Closed {self._b._cfg.type} connection to {self._b._cfg.database}")
        except Exception as e:
            logger.warning(f"Failed to close connection {self._b._cfg.type} to '{self._b._cfg.database}'. Error: {e}")

    def dispose(self) -> None:
        self._b.dispose()