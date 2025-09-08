# drivers.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Callable
from urllib.parse import urlparse, urlencode, quote


@dataclass(frozen=True)
class Driver:
    name: str
    default_port: int | None
    validate: Callable[[Mapping[str, Any]], None]          # raise on invalid config
    build_dsn: Callable[[Mapping[str, Any]], str] | None   # may be None (opaque type)

_REGISTRY: dict[str, Driver] = {}

def register_driver(drv: Driver) -> None:
    _REGISTRY[drv.name] = drv

def get_driver(name: str) -> Driver | None:
    return _REGISTRY.get(name)

def type_from_dsn(dsn: str) -> str | None:
    scheme = urlparse(dsn).scheme
    return scheme.split("+", 1)[0] if scheme else None

# ---- helpers ---------------------------------------------------------------

def _req(keys: list[str], cfg: Mapping[str, Any]) -> None:
    missing = [k for k in keys if not cfg.get(k)]
    if missing:
        raise ValueError(f"missing fields: {missing}")

def _i(v: Any) -> int | None:
    if v is None or v == "": return None
    return int(v)

def _cred(user: str | None, password: str | None) -> str:
    if not user:
        return ""
    if password is None:
        return f"{quote(user, safe='')}@"
    return f"{quote(user, safe='')}:{quote(password, safe='')}@"

def _qs(q: Any) -> str:
    if not q or not isinstance(q, Mapping):
        return ""
    return "?" + urlencode(q, doseq=True)

# ---- built-in drivers ------------------------------------------------------

# PostgreSQL
postgres = Driver(
    name="postgresql",
    default_port=5432,
    validate=lambda c: _req(["host", "user", "password", "database"], c),
    build_dsn=lambda c: (
        f"postgresql://{_cred(c.get('user'), c.get('password'))}"
        f"{c['host']}{f':{_i(c.get('port'))}' if _i(c.get('port')) else ''}"
        f"/{c['database']}{_qs(c.get('query'))}"
    ),
)
register_driver(postgres)
# common aliases
register_driver(Driver(**{**postgres.__dict__, "name": "postgres"}))
register_driver(Driver(**{**postgres.__dict__, "name": "postgresql+psycopg"}))

# MySQL
mysql = Driver(
    name="mysql",
    default_port=3306,
    validate=lambda c: _req(["host", "user", "password", "database"], c),
    build_dsn=lambda c: (
        f"mysql://{_cred(c.get('user'), c.get('password'))}"
        f"{c['host']}{f':{_i(c.get('port'))}' if _i(c.get('port')) else ''}"
        f"/{c['database']}{_qs(c.get('query'))}"
    ),
)
register_driver(mysql)
# common aliases
register_driver(Driver(**{**mysql.__dict__, "name": "mysql+pymysql"}))
register_driver(Driver(**{**mysql.__dict__, "name": "mariadb"}))

# SQLite
def _sqlite_build(c: Mapping[str, Any]) -> str:
    db = (c.get("database") or "").strip()
    if not db:
        raise ValueError("sqlite requires 'database' (file path or URI)")
    # If user already provided a URI (e.g., 'file:...','sqlite://...'), pass through
    if "://" in db or db.startswith("file:"):
        return db
    # Otherwise, build a standard three-slash URI. For Windows, prefer passing a proper URI.
    return f"sqlite:///{db}{_qs(c.get('query'))}"

sqlite = Driver(
    name="sqlite",
    default_port=None,
    validate=lambda c: _req(["database"], c),
    build_dsn=_sqlite_build,
)
register_driver(sqlite)
# common alias
register_driver(Driver(**{**sqlite.__dict__, "name": "sqlite3"}))

# MongoDB
def _mongodb_build(c: Mapping[str, Any]) -> str:
    # hosts: support either "host" (comma-separated) or list under "hosts"
    hosts: str
    if "hosts" in c and isinstance(c["hosts"], (list, tuple)):
        hosts = ",".join(str(h) for h in c["hosts"])
    else:
        hosts = str(c.get("host", "")).strip()
    if not hosts:
        raise ValueError("mongodb requires 'host' or 'hosts'")
    port = _i(c.get("port"))
    # If user already included :port in host(s), don't add port again
    if port and ":" not in hosts and "," not in hosts:
        hosts = f"{hosts}:{port}"
    cred = _cred(c.get("user"), c.get("password"))
    db = c.get("database") or ""   # db is optional in mongodb URIs
    return f"mongodb://{cred}{hosts}/{db}{_qs(c.get('query'))}"

mongodb = Driver(
    name="mongodb",
    default_port=27017,
    validate=lambda c: _req(["host"], c),  # allow no user/password/database
    build_dsn=_mongodb_build,
)
register_driver(mongodb)
# SRV alias (note: SRV typically ignores port and uses DNS)
register_driver(Driver(**{**mongodb.__dict__, "name": "mongodb+srv"}))

# ---- optional: third-party plugin discovery via entry points ---------------
def load_entry_point_drivers(group: str = "jrm.drivers") -> None:
    """
    Third-party packages can expose a `Driver` via entry points:
      setup.cfg / pyproject:
        [project.entry-points."jrm.drivers"]
        cockroach = mypkg.drivers:cockroach_driver
    """
    try:
        from importlib.metadata import entry_points
    except Exception:
        return
    for ep in entry_points().select(group=group):
        try:
            drv: Driver = ep.load()
            register_driver(drv)
        except Exception:
            # Intentionally silent; application can log this if desired
            pass
