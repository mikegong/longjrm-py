# =============================================================================
# LongJRM Runtime Config Loader
#
# 12-Factor intentions (https://12factor.net):
# - Config in the environment (Factor III): read OS env (JRM_*) directly.
#   This library NEVER calls load_dotenv(); the application may do that if it
#   wants. Programmatic injection via configure()/using_config() is supported.
# - No I/O at import time: everything is lazy via get_config().
# - Deterministic precedence: documented below; overridable via JRM_SOURCE.
# - Stateless & concurrency-safe: ContextVar keeps per-request defaults (using_db).
# - Disposability: reload_default() clears caches to re-read env/files fast.
# - Dev/prod parity: same code paths across environments (files/env/CI/Docker).
# - Backing services as attached resources: DBs selected/parametrized at runtime.
# - Safety: no cross-source merge by default; explicit/opt-in if needed.
#
# Default precedence (JRM_SOURCE=auto):
#   1) If JRM_CONFIG_PATH or JRM_DBINFOS_PATH is set → load from files
#   2) Else if any JRM_* DB vars exist → load from environment
#   3) Else if ./config/{jrm.config.json, dbinfos.json} exists → load those
#   4) Else → from_env() again (will raise if nothing configured)
#
# You can force precedence with JRM_SOURCE:
#   - JRM_SOURCE=files  → always use files (CONFIG_PATH/DBINFOS_PATH must exist)
#   - JRM_SOURCE=env    → always use OS environment (JRM_*), ignore files
#   - JRM_SOURCE=auto   → use the default precedence above
#
# All loaders fail fast with JrmConfigurationError on conflicts/invalid config.
# =============================================================================
from __future__ import annotations
import os
from contextvars import ContextVar
from functools import lru_cache
from pathlib import Path
from typing import Optional, Tuple

from .config import JrmConfig, DatabaseConfig, JrmConfigurationError  # adjust import path

# Allow overriding the env prefix if you embed multiple copies/configs
_PREFIX = os.getenv("JRM_PREFIX", "JRM_")

# Context-scoped overrides (thread/async safe)
_current_cfg: ContextVar[Optional[JrmConfig]] = ContextVar("jrm_config", default=None)
_current_dbkey: ContextVar[Optional[str]] = ContextVar("jrm_dbkey", default=None)

def _env_fingerprint(prefix: str = _PREFIX) -> Tuple[Tuple[str, str], ...]:
    """Stable key for caching: all JRM_* envs sorted."""
    items = tuple(sorted(
        (k, os.environ.get(k, ""))
        for k in os.environ.keys()
        if k.startswith(prefix)
    ))
    return items

def _has_any_env(prefix: str = _PREFIX) -> bool:
    keys = os.environ.keys()
    # Only consider DB-relevant envs
    interesting = (
        f"{prefix}DATABASES_JSON",
        f"{prefix}DBINFOS_PATH",
        f"{prefix}DB_KEY",
        f"{prefix}DB_DSN",
        f"{prefix}DB_TYPE",
        f"{prefix}DB_HOST",
        f"{prefix}DB_PORT",
        f"{prefix}DB_USER",
        f"{prefix}DB_PASSWORD",
        f"{prefix}DB_NAME",
        f"{prefix}DB_DEFAULT",
    )
    return any(k in keys for k in interesting)


@lru_cache(maxsize=8)
def _load_default_cached(_fp: Tuple[Tuple[str, str], ...]) -> JrmConfig:
    """
    Precedence (JRM_SOURCE=auto):
      1) Files if JRM_CONFIG_PATH or JRM_DBINFOS_PATH is set
      2) Pure env (DATABASES_JSON / DBINFOS_PATH / flat JRM_DB_* vars)
      3) Conventional ./config/{jrm.config.json, dbinfos.json}
      4) As a last resort: from_env() (may raise if nothing is set)
    """
    source = os.getenv(f"{_PREFIX}SOURCE", "auto").lower()
    cfg_path = os.getenv(f"{_PREFIX}CONFIG_PATH")
    dbinfos_path = os.getenv(f"{_PREFIX}DBINFOS_PATH")

    # Forced modes
    if source == "env":
        return JrmConfig.from_env(prefix=_PREFIX)
    if source == "files":
        return JrmConfig.from_files(cfg_path, dbinfos_path)

    # --- auto mode ---
    # 1) explicit file paths via env
    if cfg_path or dbinfos_path:
        return JrmConfig.from_files(cfg_path, dbinfos_path)

    # 2) pure env (supports DATABASES_JSON, DBINFOS_PATH, flat vars)
    if _has_any_env(_PREFIX):
        try:
            return JrmConfig.from_env(prefix=_PREFIX)
        except JrmConfigurationError:
            # fall through to conventional files
            pass

    # 3) conventional fallback in CWD
    default_cfg = Path.cwd() / "config" / "jrm.config.json"
    default_db = Path.cwd() / "config" / "dbinfos.json"
    if default_cfg.exists() or default_db.exists():
        return JrmConfig.from_files(
            str(default_cfg if default_cfg.exists() else None),
            str(default_db if default_db.exists() else None),
        )

    # 4) last resort: env (will raise a clear error if nothing is configured)
    raise JrmConfigurationError(
        "No configuration found. Set JRM_CONFIG_PATH/DBINFOS_PATH, or provide JRM_* "
        "env vars (see 12-Factor Config), or call configure(JrmConfig(...))."
    )
    
def _load_default() -> JrmConfig:
    # include env fingerprint as cache key to auto-refresh when env changes
    return _load_default_cached(_env_fingerprint())

def get_config() -> JrmConfig:
    """Active config (context override > lazily loaded default)."""
    return _current_cfg.get() or _load_default()

def require_db(name: str | None = None) -> DatabaseConfig:
    """Resolve a DatabaseConfig by name or current context default."""
    key = name or _current_dbkey.get()
    return get_config().require(key)

def configure(cfg: JrmConfig) -> None:
    """Set an override for this context (tests/embedded apps)."""
    _current_cfg.set(cfg)

def use_db(key: str | None) -> None:
    """Set a context default DB key (per async task/request)."""
    _current_dbkey.set(key)

def reload_default() -> None:
    """Drop cached auto-loaded config (e.g., after mutating os.environ)."""
    _load_default_cached.cache_clear()

# Ergonomic context managers
from contextlib import contextmanager

@contextmanager
def using_config(cfg: JrmConfig):
    tok = _current_cfg.set(cfg)
    try:
        yield
    finally:
        _current_cfg.reset(tok)

@contextmanager
def using_db(key: str):
    tok = _current_dbkey.set(key)
    try:
        yield
    finally:
        _current_dbkey.reset(tok)
