from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping, Dict
from pathlib import Path
import os, json, re
from types import MappingProxyType
from typing import Mapping
from longjrm.connection.dsn_parts_helper import dsn_to_parts, type_from_dsn



class JrmConfigurationError(ValueError): ...

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

def _expand_env_str(v: str) -> str:
    # Expand ${VAR} from os.environ; missing vars become ""
    return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), ""), v)

def _expand_env(obj: Any) -> Any:
    if isinstance(obj, str): return _expand_env_str(obj)
    if isinstance(obj, dict): return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_expand_env(x) for x in obj]
    return obj

def _read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    p = Path(path).expanduser()
    
    # If path doesn't exist and is relative, try resolving from project root
    if not p.exists() and not p.is_absolute():
        # Get project root (assuming this file is in longjrm/ directory)
        project_root = Path(__file__).parent.parent
        alt_path = project_root / p
        if alt_path.exists():
            p = alt_path
    
    if not p.exists():
        raise JrmConfigurationError(f"Config file not found: {p}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise JrmConfigurationError(f"Invalid JSON in {p}: {e}") from e

def _to_int(name: str, v: Any) -> int:
    try:
        return int(v)
    except Exception:
        raise JrmConfigurationError(f"Invalid integer for {name}: {v!r}")

@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    # DSN-first; parts are optional
    type: str
    dsn: str | None = None
    host: str | None = None
    user: str | None = None
    password: str | None = field(default=None, repr=False)
    port: int | None = None
    database: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatabaseConfig":
        d = _expand_env(dict(data))  # expand ${ENV} inside JSON strings
        dsn = d.get("dsn")
        if dsn:
            t = (d.get("type") or type_from_dsn(dsn) or "")
            if not t:
                raise JrmConfigurationError("Could not infer database type from DSN; set 'type'.")

            # Extract options from DSN query parameters
            dsn_parts = dsn_to_parts(dsn)
            dsn_options = dsn_parts.get('query', {})
            
            # Merge explicit options with DSN options (explicit takes precedence)
            merged_options = {**dsn_options, **(d.get("options", {}) or {})}
    
            return cls(
                type=t.split("+", 1)[0],
                dsn=dsn,
                options=merged_options
            )

        db_type = d.get("type")
        if not db_type:
            raise JrmConfigurationError("Either 'dsn' or 'type' is required for a database entry.")

        return cls(
            type=db_type,
            dsn=dsn,
            host=d.get("host"),
            user=d.get("user"),
            password=d.get("password"),
            port=d.get("port"),
            database=d.get("database"),
            options=d.get("options", {}) or {},
        )

@dataclass(frozen=True, slots=True)
class JrmConfig:
    _databases: Dict[str, DatabaseConfig]
    default_db: str | None = None
    connect_timeout: float = 40.0
    data_fetch_limit: int = 1000
    min_pool_size: int = 1
    max_pool_size: int = 10
    max_cached_conn: int = 5

    def require(self, name: str | None = None) -> DatabaseConfig:
        dbmap = self._databases
        key = name or self.default_db or next(iter(dbmap ))
        try:
            return dbmap [key]
        except KeyError as e:
            raise JrmConfigurationError(f"Unknown database key: {key!r}") from e

    def databases(self) -> Mapping[str, DatabaseConfig]:
        """Read-only view of all configured databases."""
        return MappingProxyType(self._databases)
    
    @classmethod
    def from_files(cls, config_path: str | None = None, dbinfos_path: str | None = None) -> "JrmConfig":
        """
        Reads:
          - config_path JSON: may contain {"default_db": "...", "databases": {...}, timeout fields}
          - dbinfos_path JSON: plain {"name": {...}} map of databases
        Merge rule: dbinfos first, then config['databases'] overrides on key conflicts.
        Supports per-entry 'dsn' OR 'type'+parts in both files. Expands ${ENV_VAR}.
        """
        cfg: dict[str, Any] = {}
        if config_path:
            cfg = _read_json(config_path)

        dbs_map: dict[str, Any] = {}
        if dbinfos_path:
            dbs_map = _read_json(dbinfos_path)

        merged = {}
        merged.update(dbs_map or {})
        merged.update((cfg.get("databases") or {}))

        if not isinstance(merged, dict) or not merged:
            raise JrmConfigurationError("No database configurations found in files")

        # Parse each DB entry via DSN-first logic
        parsed = {name: DatabaseConfig.from_dict(d) for name, d in merged.items()}

        # Pull common tuning (keep units = seconds)
        def _f(k: str, default: float) -> float:
            v = cfg.get(k)
            return float(v) if v is not None else default
        def _i(k: str, default: int) -> int:
            v = cfg.get(k)
            return int(v) if v is not None else default

        return cls(
            _databases=parsed,
            default_db=cfg.get("default_db") or cfg.get("db_default"),
            connect_timeout=_i("jrm_db_timeout", 40.0),
            data_fetch_limit=_i("data_fetch_limit", 1000),
            min_pool_size=_i("min_conn_pool_size", 1),
            max_pool_size=_i("max_conn_pool_size", 10),
            max_cached_conn=_i("max_cached_conn", 5),
        )

    @classmethod
    def from_env(cls, prefix: str = "JRM_") -> "JrmConfig":
        """
        Env contract (all optional; any one path must provide at least one DB):
          - {P}DATABASES_JSON : inline JSON object of databases (supports ${VAR} inside)
          - {P}DBINFOS_PATH   : path to JSON file with databases
          - Single-DB (flat):
              {P}DB_KEY, and either:
                * {P}DB_DSN
                * or parts: {P}DB_TYPE {P}DB_HOST {P}DB_PORT {P}DB_USER {P}DB_PASSWORD {P}DB_NAME
          - Selector / tuning:
              {P}DB_DEFAULT, {P}JRM_DB_TIMEOUT, {P}DATA_FETCH_LIMIT, {P}MIN_CONN_POOL_SIZE,
              {P}MAX_CONN_POOL_SIZE, {P}MAX_CACHED_CONN
        Precedence: DATABASES_JSON > DBINFOS_PATH > single-DB vars.
        """
        get = os.getenv

        # 1) Many DBs via inline JSON
        dbs_map: dict[str, Any] = {}
        inline = get(prefix + "DATABASES_JSON")
        if inline:
            try:
                dbs_map = _expand_env(json.loads(inline))
            except json.JSONDecodeError as e:
                raise JrmConfigurationError(f"{prefix}DATABASES_JSON is not valid JSON: {e}") from e

        # 2) Or via file
        if not dbs_map:
            dbinfos_path = get(prefix + "DBINFOS_PATH")
            if dbinfos_path:
                dbs_map = _expand_env(_read_json(dbinfos_path))

        # 3) Optional single DB via flat env (merged in)
        key = get(prefix + "DB_KEY")
        single: dict[str, Any] | None = None
        if key:
            dsn = get(prefix + "DB_DSN")
            if dsn:
                single = {"dsn": _expand_env_str(dsn)}
            else:
                # Parse DB_OPTIONS as JSON if present
                options_str = get(prefix + "DB_OPTIONS")
                options = {}
                if options_str:
                    try:
                        options = json.loads(options_str)
                    except json.JSONDecodeError as e:
                        raise JrmConfigurationError(f"{prefix}DB_OPTIONS is not valid JSON: {e}") from e
                
                parts = {
                    "type": get(prefix + "DB_TYPE"),
                    "host": get(prefix + "DB_HOST"),
                    "port": get(prefix + "DB_PORT"),
                    "user": get(prefix + "DB_USER"),
                    "password": get(prefix + "DB_PASSWORD"),
                    "database": get(prefix + "DB_NAME"),
                    "options": options
                }
                # drop Nones; validation happens in DatabaseConfig.from_dict
                single = {k: v for k, v in parts.items() if v is not None}

            if single:
                # merge: explicit single DB overrides any same-named entry from map
                dbs_map = {**dbs_map, key: single}

        if not dbs_map:
            raise JrmConfigurationError("No database configurations found in environment")

        parsed = {name: DatabaseConfig.from_dict(cfg) for name, cfg in dbs_map.items()}

        def _f(name: str, default: float) -> float:
            v = get(prefix + name)
            return float(v) if v is not None else default
        def _i(name: str, default: int) -> int:
            v = get(prefix + name)
            return int(v) if v is not None else default

        return cls(
            _databases=parsed,
            default_db=get(prefix + "DB_DEFAULT") or (key if key in parsed else None),
            connect_timeout=_i("JRM_DB_TIMEOUT", 40.0),
            data_fetch_limit=_i("DATA_FETCH_LIMIT", 1000),
            min_pool_size=_i("MIN_CONN_POOL_SIZE", 1),
            max_pool_size=_i("MAX_CONN_POOL_SIZE", 10),
            max_cached_conn=_i("MAX_CACHED_CONN", 5),
        )
