from __future__ import annotations
from typing import Any, Mapping, Dict, List
from urllib.parse import urlparse, parse_qs, urlencode, quote, unquote

def _scheme_base(s: str) -> str:
    # e.g., "postgresql+psycopg" -> "postgresql"
    return s.split("+", 1)[0].lower()

def _flatten_qs(qs: Dict[str, List[str]]) -> Dict[str, Any]:
    # Turn {"a": ["1"], "b": ["x","y"]} -> {"a": "1", "b": ["x","y"]}
    return {k: (v[0] if len(v) == 1 else v) for k, v in qs.items()}

def parts_to_dsn(parts: Mapping[str, Any]) -> str:
    """
    Build a DSN string from 'parts'. Supported shapes:
      SQL (postgres/mysql/...):
        {"type","host","port?","user?","password?","database?","query?":{...}}
      SQLite:
        {"type":"sqlite","database": "/abs/path.db" | "relative.db" | "file:...","query?":{...}}
    """
    t = (parts.get("type") or "").lower().strip()
    if not t:
        raise ValueError("parts_to_dsn: 'type' is required")

    # Query string
    q = parts.get("query") or {}
    if not isinstance(q, Mapping):
        raise ValueError("parts_to_dsn: 'query' must be a mapping if provided")
    qs = ("?" + urlencode(q, doseq=True)) if q else ""

    # Credentials
    user = parts.get("user")
    pwd = parts.get("password")
    cred = ""
    if user:
        cred = quote(str(user), safe="")
        if pwd is not None:
            cred += ":" + quote(str(pwd), safe="")
        cred += "@"

    # SQLite
    if t == "sqlite":
        db = parts.get("database")
        if not db:
            raise ValueError("sqlite requires 'database' (path or 'file:...' URI)")
        db = str(db)
        # If user passed an explicit URI (file:... or sqlite://...), pass it through
        if db.startswith("file:") or "://" in db:
            return db + qs
        # Normal path form
        return f"sqlite:///{db}{qs}"

    # Generic SQL (postgresql, mysql, mariadb, mssql, etc.)
    host = parts.get("host")
    if not host:
        raise ValueError(f"{t} requires 'host'")
    port = parts.get("port")
    portseg = f":{int(port)}" if port not in (None, "") else ""
    dbname = parts.get("database")
    path = f"/{dbname}" if dbname else ""
    return f"{t}://{cred}{host}{portseg}{path}{qs}"


def dsn_to_parts(dsn: str) -> Dict[str, Any]:
    """
    Parse a DSN string into parts. Returns a dict with keys:
      type, user?, password?, host?, port?, database?, query? (dict)
    Handles: generic SQL DSNs and sqlite.
    """
    if not isinstance(dsn, str) or "://" not in dsn:
        # Treat bare sqlite path as sqlite database
        return {"type": "sqlite", "database": dsn, "query": {}}

    u = urlparse(dsn)
    scheme = _scheme_base(u.scheme)
    out: Dict[str, Any] = {"type": scheme}

    # Query
    out["query"] = _flatten_qs(parse_qs(u.query)) if u.query else {}

    # Credentials
    if u.username is not None:
        out["user"] = unquote(u.username)
    if u.password is not None:
        out["password"] = unquote(u.password)

    # SQLite
    if scheme == "sqlite" or u.scheme == "file":
        # sqlite:///path/to.db  -> path="///path/to.db"
        # file:./local.db       -> scheme="file", path="./local.db"
        if u.scheme == "file":
            out["type"] = "sqlite"
            out["database"] = f"file:{u.path}"
        else:
            # Drop leading "/" added by urlparse for absolute paths
            db = u.path.lstrip("/")
            out["database"] = db
        return out

    # Generic SQL
    out["host"] = u.hostname or None
    out["port"] = int(u.port) if u.port is not None else None
    out["database"] = u.path.lstrip("/") or None
    return out


def type_from_dsn(dsn: str) -> str | None:
    scheme = urlparse(dsn).scheme
    return scheme.split("+", 1)[0] if scheme else None