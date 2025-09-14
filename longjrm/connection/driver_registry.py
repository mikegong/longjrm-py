from __future__ import annotations
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files  # Python 3.9+
from typing import Dict, Optional

@dataclass(frozen=True, slots=True)
class DriverInfo:
    dbapi: Optional[str] = None        # e.g. "psycopg2", "pymysql", "pymongo"
    sa_dialect: Optional[str] = None   # e.g. "postgresql", "mysql", "sqlite"
    sa_driver: Optional[str] = None    # e.g. "psycopg2", "pymysql" (None for sqlite)

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
