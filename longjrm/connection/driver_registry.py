from __future__ import annotations
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files  # Python 3.9+
from typing import Dict, Optional, Any
from types import ModuleType
import os
import importlib
import importlib.util
import logging

logger = logging.getLogger(__name__)

@dataclass(frozen=True, slots=True)
class DriverInfo:
    dbapi: Optional[str] = None        # e.g. "psycopg", "pymysql"
    sa_dialect: Optional[str] = None   # e.g. "postgresql", "mysql", "sqlite"
    sa_driver: Optional[str] = None    # e.g. "psycopg", "pymysql" (None for sqlite)

@lru_cache(maxsize=1)
def load_driver_map(extra_path: Optional[str] = None) -> Dict[str, DriverInfo]:
    # Load the JSON that sits right next to this file (same package)
    text = files(__package__).joinpath("driver_map.json").read_text(encoding="utf-8")
    base = json.loads(text)

    # Optional: allow an external override file (env/config), if you want
    if extra_path:
        try:
            with open(extra_path, "r", encoding="utf-8") as f:
                base.update(json.load(f))
        except FileNotFoundError:
            pass

    # Normalize keys and coerce to DriverInfo
    out: Dict[str, DriverInfo] = {}
    for k, v in base.items():
        out[k.lower()] = DriverInfo(
            dbapi=v.get("dbapi"),
            sa_dialect=v.get("sa_dialect"),
            sa_driver=v.get("sa_driver"),
        )
    return out

def sa_minimal_url(db_type: str, *, override_driver: Optional[str] = None) -> str:
    info = load_driver_map().get((db_type or "").lower())
    if not info or not info.sa_dialect:
        raise ValueError(f"No SQLAlchemy dialect for type={db_type!r}")
    driver = override_driver if override_driver is not None else info.sa_driver
    return f"{info.sa_dialect}+{driver}://" if driver else f"{info.sa_dialect}://"

def fix_ibm_db_dll() -> None:
    """
    Pre-emptive fix for Windows DLL load failure (Python 3.8+).
    Adds ibm_db clidriver/bin to the DLL search path.
    """
    if os.name != 'nt':
        return

    try:
        # Check if already fixed to avoid repeated work
        try:
            import ibm_db
            return
        except ImportError:
            pass

        # Find where ibm_db is installed
        spec = importlib.util.find_spec('ibm_db')
        if spec and spec.origin:
            ibm_db_dir = os.path.dirname(spec.origin)
            
            # Helper to try adding path
            def try_add_path(path: str) -> bool:
                if os.path.isdir(path):
                    logger.debug(f"Adding DB2 driver path to DLL search: {path}")
                    if hasattr(os, 'add_dll_directory'):
                        os.add_dll_directory(path)
                    os.environ['PATH'] = path + os.pathsep + os.environ['PATH']
                    return True
                return False

            # Try standard locations
            # 1. .../site-packages/ibm_db-X.X.X/clidriver/bin
            if try_add_path(os.path.join(ibm_db_dir, 'clidriver', 'bin')):
                return
            # 2. .../site-packages/clidriver/bin (flat structure)
            site_packages = os.path.dirname(ibm_db_dir)
            if try_add_path(os.path.join(site_packages, 'clidriver', 'bin')):
                return
                
    except Exception as e:
        logger.warning(f"Failed to auto-fix ibm_db import: {e}")

def load_dbapi_module(db_type: str) -> Optional[ModuleType]:
    """
    Load the DB-API module for the given database type.
    Handles database-specific environment patches (e.g. DB2 DLLs).
    """
    driver_info = load_driver_map().get((db_type or "").lower())
    if not driver_info or not driver_info.dbapi:
        logger.warning(f"No dbapi driver defined for database type: {db_type}")
        return None

    module_name = driver_info.dbapi

    # Apply specific fixes before import
    if module_name == 'ibm_db_dbi':
        fix_ibm_db_dll()

    try:
        mod = importlib.import_module(module_name)
        
        # Post-import patches
        # Fix for DB2 which might report 0 or None despite being usable in pools
        if module_name == 'ibm_db_dbi':
             if not getattr(mod, 'threadsafety', None):
                 logger.warning(f"Forcing threadsafety=1 for {module_name}")
                 mod.threadsafety = 1
                 
        return mod
    except ImportError as e:
        # Don't log error here, let the caller handle it (e.g. connectors might want to raise better error)
        # But we can log debug
        logger.debug(f"Could not import dbapi module '{module_name}': {e}")
        return None
