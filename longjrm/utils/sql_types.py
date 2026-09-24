"""Column types across database engines.

A column's type as one engine names it -- what its catalog reports, such as
``character varying``, ``varchar2`` or ``int4`` -- maps to a canonical token
(``STRING``, ``INT``, ``DECIMAL`` ...), and a canonical token renders as the type
another engine writes in DDL. Going through the token describes each engine once,
instead of once for every pair of engines.

Rendering branches on length and scale (``DECIMAL(p,s)`` vs ``DECIMAL(p)``,
``VARCHAR(n)`` vs a default width), which is why the maps are code, not data. A
type the maps do not know has no token; a column of that type is created as
``fallback_type``, a wide text type, so an unknown type never narrows silently.

Engines are named by their longjrm database type, and the aliases ``get_db``
accepts (``postgresql``, ``mariadb``, ``mssql``) name the same engines here. No
driver is imported, so the maps work where an engine's driver is not installed.
"""

# An engine's type name (lowercased, parametric suffix stripped) -> canonical token
_TO_CANONICAL = {
    "postgres": {
        "character varying": "STRING", "varchar": "STRING",
        "character": "STRING", "char": "STRING", "bpchar": "STRING",
        "name": "STRING", "uuid": "STRING",
        "text": "TEXT", "citext": "TEXT",
        "smallint": "SMALLINT", "int2": "SMALLINT", "smallserial": "SMALLINT",
        "integer": "INT", "int": "INT", "int4": "INT", "serial": "INT",
        "bigint": "BIGINT", "int8": "BIGINT", "bigserial": "BIGINT",
        "numeric": "DECIMAL", "decimal": "DECIMAL", "money": "DECIMAL",
        "real": "FLOAT", "float4": "FLOAT",
        "double precision": "DOUBLE", "float8": "DOUBLE",
        "boolean": "BOOL", "bool": "BOOL",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP",
        "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMP",
        "date": "DATE",
        "time without time zone": "TIME", "time with time zone": "TIME",
        "time": "TIME",
        "json": "JSON", "jsonb": "JSON",
        # Every array column reports data_type='ARRAY' (the element type lives in
        # udt_name, which data_type does not carry), so JSON is the one token that
        # holds any of them: element list and order survive, and MySQL -- which has no
        # array type at all -- gets a column it can validate and query with
        # JSON_CONTAINS / JSON_TABLE instead of an opaque TEXT blob. The cost is that a
        # postgres array rendered for postgres becomes JSONB rather than an array.
        "array": "JSON",
        "bytea": "BLOB",
    },
    "mysql": {
        "varchar": "STRING", "char": "STRING",
        "text": "TEXT", "tinytext": "TEXT", "mediumtext": "TEXT", "longtext": "TEXT",
        # MySQL TINYINT is signed -128..127 -- its OWN token, not a SMALLINT. Calling it
        # SMALLINT would say the column is wider than it is: a SMALLINT value (up to
        # 32767) judged to fit would overflow it at write time.
        "tinyint": "TINYINT", "smallint": "SMALLINT", "mediumint": "INT",
        "int": "INT", "integer": "INT",
        "bigint": "BIGINT",
        "decimal": "DECIMAL", "numeric": "DECIMAL",
        "float": "FLOAT", "double": "DOUBLE",
        "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP",
        "date": "DATE", "time": "TIME",
        "json": "JSON",
        "blob": "BLOB", "tinyblob": "BLOB", "mediumblob": "BLOB", "longblob": "BLOB",
    },
    # IBM Db2 (SYSCAT.COLUMNS TYPENAME, lowercased here). FLOAT(n>24)/DOUBLE are 8-byte;
    # REAL is 4-byte. GRAPHIC/VARGRAPHIC are double-byte char.
    "db2": {
        "varchar": "STRING", "character": "STRING", "char": "STRING",
        "graphic": "STRING", "vargraphic": "STRING", "nvarchar": "STRING", "nchar": "STRING",
        "clob": "TEXT", "dbclob": "TEXT", "long varchar": "TEXT",
        "smallint": "SMALLINT",
        "integer": "INT", "int": "INT",
        "bigint": "BIGINT",
        "decimal": "DECIMAL", "numeric": "DECIMAL", "decfloat": "DECIMAL",
        "real": "FLOAT",
        "double": "DOUBLE", "double precision": "DOUBLE", "float": "DOUBLE",
        "boolean": "BOOL",
        "timestamp": "TIMESTAMP", "date": "DATE", "time": "TIME",
        "blob": "BLOB", "binary": "BLOB", "varbinary": "BLOB", "xml": "TEXT",
    },
    # Oracle (USER_TAB_COLUMNS DATA_TYPE). NUMBER is the catch-all numeric; DATE carries
    # a time component, so it canonicalizes to TIMESTAMP.
    "oracle": {
        "varchar2": "STRING", "nvarchar2": "STRING", "varchar": "STRING",
        "char": "STRING", "nchar": "STRING", "rowid": "STRING", "urowid": "STRING",
        "clob": "TEXT", "nclob": "TEXT", "long": "TEXT",
        "number": "DECIMAL", "decimal": "DECIMAL", "numeric": "DECIMAL",
        "integer": "INT", "int": "INT", "smallint": "SMALLINT",
        "float": "FLOAT", "binary_float": "FLOAT", "binary_double": "DOUBLE",
        "date": "TIMESTAMP", "timestamp": "TIMESTAMP",
        "blob": "BLOB", "raw": "BLOB", "long raw": "BLOB", "bfile": "BLOB",
    },
    # Microsoft SQL Server (sys types). FLOAT is 8-byte (DOUBLE); REAL is 4-byte (FLOAT).
    "sqlserver": {
        "varchar": "STRING", "nvarchar": "STRING", "char": "STRING", "nchar": "STRING",
        "uniqueidentifier": "STRING", "sysname": "STRING",
        "text": "TEXT", "ntext": "TEXT",
        # SQL Server TINYINT is UNSIGNED 0..255, so it does NOT fit MySQL's signed
        # TINYINT: same name, different range. Kept at SMALLINT -- saying it is wider
        # than it is only costs a needless widen, never a silent overflow.
        "tinyint": "SMALLINT", "smallint": "SMALLINT",
        "int": "INT", "integer": "INT", "bigint": "BIGINT",
        "decimal": "DECIMAL", "numeric": "DECIMAL", "money": "DECIMAL", "smallmoney": "DECIMAL",
        "real": "FLOAT", "float": "DOUBLE",
        "bit": "BOOL",
        "datetime": "TIMESTAMP", "datetime2": "TIMESTAMP", "smalldatetime": "TIMESTAMP",
        "datetimeoffset": "TIMESTAMP", "date": "DATE", "time": "TIME",
        "binary": "BLOB", "varbinary": "BLOB", "image": "BLOB",
    },
}

# canonical token -> (length, scale) -> the type an engine writes in DDL
_RENDER = {
    "mysql": {
        "STRING": lambda L, S: f"VARCHAR({L})" if L else "VARCHAR(255)",
        "TEXT": lambda L, S: "TEXT",
        "TINYINT": lambda L, S: "TINYINT",
        "SMALLINT": lambda L, S: "SMALLINT",
        "INT": lambda L, S: "INT",
        "BIGINT": lambda L, S: "BIGINT",
        "DECIMAL": lambda L, S: (f"DECIMAL({L},{S})" if (L and S is not None)
                                 else f"DECIMAL({L})" if L else "DECIMAL(38,10)"),
        "FLOAT": lambda L, S: "FLOAT",
        "DOUBLE": lambda L, S: "DOUBLE",
        # MySQL TIMESTAMP has the 2038 limit; DATETIME is safe and stores no tz.
        "TIMESTAMP": lambda L, S: "DATETIME(6)",
        "DATE": lambda L, S: "DATE",
        "TIME": lambda L, S: "TIME(6)",
        "BOOL": lambda L, S: "TINYINT(1)",
        "JSON": lambda L, S: "JSON",
        "BLOB": lambda L, S: "LONGBLOB",
    },
    "postgres": {
        "STRING": lambda L, S: f"VARCHAR({L})" if L else "VARCHAR(255)",
        "TEXT": lambda L, S: "TEXT",
        # Postgres has no one-byte integer; the next size up holds it losslessly.
        "TINYINT": lambda L, S: "SMALLINT",
        "SMALLINT": lambda L, S: "SMALLINT",
        "INT": lambda L, S: "INTEGER",
        "BIGINT": lambda L, S: "BIGINT",
        "DECIMAL": lambda L, S: (f"NUMERIC({L},{S})" if (L and S is not None)
                                 else f"NUMERIC({L})" if L else "NUMERIC(38,10)"),
        "FLOAT": lambda L, S: "REAL",
        "DOUBLE": lambda L, S: "DOUBLE PRECISION",
        "TIMESTAMP": lambda L, S: "TIMESTAMP",
        "DATE": lambda L, S: "DATE",
        "TIME": lambda L, S: "TIME",
        "BOOL": lambda L, S: "BOOLEAN",
        "JSON": lambda L, S: "JSONB",
        "BLOB": lambda L, S: "BYTEA",
    },
}

# The wide text type an engine creates a column of unknown type as.
_FALLBACK = {"mysql": "TEXT", "postgres": "TEXT"}

# The aliases get_db accepts for the same engine.
_ALIASES = {"postgresql": "postgres", "mariadb": "mysql", "mssql": "sqlserver"}


def _engine(database_type):
    name = str(database_type or "").strip().lower()
    return _ALIASES.get(name, name)


def canonical_type(database_type, raw_type):
    """The canonical token for a column type as ``database_type`` names it, or None
    when the type is not known. A parametric suffix is ignored (``numeric(10,2)`` is
    ``numeric``), and so is an array marker (``text[]``)."""
    if not raw_type:
        return None
    t = str(raw_type).strip().lower()
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    t = t.rstrip("[]").strip()
    return _TO_CANONICAL.get(_engine(database_type), {}).get(t)


def render_type(canonical, database_type, length=None, scale=None):
    """The type ``database_type`` writes in DDL for a canonical token, sized by
    ``length`` and ``scale`` where the type takes them; None when the engine has no
    rendering for the token."""
    renderer = _RENDER.get(_engine(database_type), {}).get(canonical)
    return None if renderer is None else renderer(length, scale)


def fallback_type(database_type):
    """The wide text type ``database_type`` creates a column of unknown type as; None
    when the engine has no renderings."""
    return _FALLBACK.get(_engine(database_type))
