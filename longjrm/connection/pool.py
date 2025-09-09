from __future__ import annotations
from typing import Any, Mapping, Optional
from dataclasses import dataclass
from enum import Enum
import logging
from longjrm.config import JrmConfig, DatabaseConfig
from longjrm.connection.dbconn import DatabaseConnection, JrmConnectionError, enforce_autocommit
from longjrm.connection.driver_registry import sa_minimal_url


logger = logging.getLogger(__name__)


class PoolBackend(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    DBUTILS = "dbutils"
    MONGODB = "mongodb"
    
    
class _Backend:
    def get_client(self): raise NotImplementedError
    def dispose(self): raise NotImplementedError


class _SABackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig, sa_opts: Optional[Mapping[str, Any]]):
        from sqlalchemy import create_engine, event

        self._cfg = db_cfg

        # Minimal URL that only selects the SQLAlchemy dialect+driver.
        # Real DB-API connections are created by dbconn via creator=...
        url = sa_minimal_url(db_cfg.type, override_driver=(db_cfg.options or {}).get("sa_driver"))

        opts: dict[str, Any] = {
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

        # Normalize connection state such as autocommit=True on checkout in ONE place (dbconn policy).
        @event.listens_for(self._engine, "checkout")
        def _on_checkout(dbapi_conn, conn_record, conn_proxy):
            enforce_autocommit(dbapi_conn, self._cfg.type)

    def get_client(self):
        raw = self._engine.raw_connection()  # .close() -> returns to SA QueuePool
        return {"conn": raw, 
                "database_type": self._cfg.type, 
                "database_name": self._cfg.database,
                "db_lib": "sqlalchemy"}

    def dispose(self) -> None:
        self._engine.dispose()


# --------- DBUtils PooledDB backend ---------
class _DBUtilsBackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig, dbutils_opts: Optional[Mapping[str, Any]]):
        from dbutils.pooled_db import PooledDB

        self._cfg = db_cfg
        opts: dict[str, Any] = {
            "maxconnections": (db_cfg.options or {}).get("MAX_CONN_POOL_SIZE", 10),
            "mincached":      (db_cfg.options or {}).get("MIN_CONN_POOL_SIZE", 1),
            "maxcached":      (db_cfg.options or {}).get("MAX_CACHED_CONN", 5),
            "blocking": True,  # wait when exhausted
            "ping": 1,         # liveness on checkout
            "reset": True,     # rollback on return; does not change autocommit
        }
        if dbutils_opts:
            opts.update(dbutils_opts)

        # Fresh DatabaseConnection per checkout -> no shared mutable state
        self._pool = PooledDB(creator=lambda: DatabaseConnection(self._cfg).connect(), **opts)

    def get_client(self):
        raw = self._pool.connection()  # .close() -> returns to DBUtils pool
        # Align state on checkout to match SA behavior (optional but recommended)
        enforce_autocommit(raw, self._cfg)
        return {"conn": raw, 
                "database_type": self._cfg.type, 
                "database_name": self._cfg.database,
                "db_lib": "dbutils"
                }

    def dispose(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass


class _MongoBackend(_Backend):
    def __init__(self, db_cfg: DatabaseConfig):
        self._cfg = db_cfg
        self._client = DatabaseConnection(db_cfg).connect()  # returns a MongoClient

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

    Example:
        pool = Pool.from_config(cfg, "primary", pool_backend=PoolBackend.DBUTILS)
        with pool.get_client() as db:
            with db.cursor() as cur:
                cur.execute("SELECT 1")
            db.commit()

        mpool = Pool.from_config(cfg, "mongo", pool_backend=PoolBackend.MONGODB)
        with mpool.get_client() as m:
            m.conn["mydb"].users.insert_one({"ok": 1})
    """
    def __init__(self, backend_obj: _Backend):
        self._b = backend_obj

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

    def dispose(self) -> None:
        self._b.dispose()