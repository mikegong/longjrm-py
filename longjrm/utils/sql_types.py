"""Column types across database engines.

A column's type as one engine names it -- what its catalog reports, such as
``character varying``, ``varchar2`` or ``int4`` -- maps to a canonical token
(``STRING``, ``INT``, ``DECIMAL`` ...), and a canonical token renders as the type
another engine writes in DDL. Going through the token describes each engine once,
instead of once for every pair of engines.

Every engine longjrm connects to is described on both sides: Postgres, MySQL/MariaDB,
DB2, Oracle, SQL Server, SQLite and Spark. A rendering never narrows silently: a size
past what the engine's type holds takes the engine's widest type of the same kind, and
a type the maps do not know has no token -- a column of that type is created as
``fallback_type``, the engine's widest text type.

Engines are named by their longjrm database type, and the aliases ``get_db`` accepts
(``postgresql``, ``mariadb``, ``mssql``) name the same engines here. No driver is
imported, so the maps work where an engine's driver is not installed.
"""

import re

# An engine's type name (lowercased, sizes removed) -> canonical token
_TO_CANONICAL = {
    "postgres": {
        "character varying": "STRING", "varchar": "STRING",
        "character": "STRING", "char": "STRING", "bpchar": "STRING",
        "name": "STRING", "uuid": "STRING",
        "inet": "STRING", "cidr": "STRING", "macaddr": "STRING", "macaddr8": "STRING",
        "text": "TEXT", "citext": "TEXT", "xml": "TEXT",
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
        # udt_name, which data_type does not carry), and a declared type such as
        # ``text[]`` is read as the same array: JSON is the one token that holds any of
        # them. Element list and order survive, and MySQL -- which has no array type at
        # all -- gets a column it can validate and query with JSON_CONTAINS / JSON_TABLE
        # instead of an opaque TEXT blob. The cost is that a postgres array rendered for
        # postgres becomes JSONB rather than an array.
        "array": "JSON",
        "bytea": "BLOB",
    },
    "mysql": {
        "varchar": "STRING", "char": "STRING", "enum": "STRING", "set": "STRING",
        "text": "TEXT", "tinytext": "TEXT", "mediumtext": "TEXT", "longtext": "TEXT",
        # MySQL TINYINT is signed -128..127 -- its OWN token, not a SMALLINT. Calling it
        # SMALLINT would say the column is wider than it is: a SMALLINT value (up to
        # 32767) judged to fit would overflow it at write time.
        "tinyint": "TINYINT", "smallint": "SMALLINT", "mediumint": "INT",
        "int": "INT", "integer": "INT",
        "bigint": "BIGINT",
        # column_type keeps UNSIGNED, which moves an integer up one size: 0..4294967295
        # does not fit a signed INT, and 0..2^64-1 fits no integer at all.
        "tinyint unsigned": "SMALLINT", "smallint unsigned": "INT",
        "mediumint unsigned": "INT", "int unsigned": "BIGINT", "integer unsigned": "BIGINT",
        "bigint unsigned": "DECIMAL", "decimal unsigned": "DECIMAL",
        "float unsigned": "FLOAT", "double unsigned": "DOUBLE",
        "year": "SMALLINT",
        "decimal": "DECIMAL", "numeric": "DECIMAL",
        "float": "FLOAT", "double": "DOUBLE", "double precision": "DOUBLE", "real": "DOUBLE",
        "bool": "BOOL", "boolean": "BOOL",
        "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP",
        "date": "DATE", "time": "TIME",
        "json": "JSON",
        "blob": "BLOB", "tinyblob": "BLOB", "mediumblob": "BLOB", "longblob": "BLOB",
        "binary": "BLOB", "varbinary": "BLOB", "bit": "BLOB",
    },
    # IBM Db2 (SYSCAT.COLUMNS TYPENAME, lowercased here). FLOAT(n>24)/DOUBLE are 8-byte;
    # REAL is 4-byte. GRAPHIC/VARGRAPHIC are double-byte char.
    "db2": {
        "varchar": "STRING", "character": "STRING", "char": "STRING",
        "graphic": "STRING", "vargraphic": "STRING", "nvarchar": "STRING", "nchar": "STRING",
        "clob": "TEXT", "dbclob": "TEXT", "long varchar": "TEXT", "long vargraphic": "TEXT",
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
        "clob": "TEXT", "nclob": "TEXT", "long": "TEXT", "xmltype": "TEXT",
        "number": "DECIMAL", "decimal": "DECIMAL", "numeric": "DECIMAL",
        "integer": "INT", "int": "INT", "smallint": "SMALLINT",
        # FLOAT is a NUMBER with up to 126 bits of binary precision, not a 4-byte float:
        # DOUBLE is the widest floating token.
        "float": "DOUBLE", "binary_float": "FLOAT", "binary_double": "DOUBLE",
        "boolean": "BOOL",
        "date": "TIMESTAMP", "timestamp": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP", "timestamp with local time zone": "TIMESTAMP",
        "json": "JSON",
        "blob": "BLOB", "raw": "BLOB", "long raw": "BLOB", "bfile": "BLOB",
    },
    # Microsoft SQL Server (sys types). FLOAT is 8-byte (DOUBLE); REAL is 4-byte (FLOAT).
    "sqlserver": {
        "varchar": "STRING", "nvarchar": "STRING", "char": "STRING", "nchar": "STRING",
        "uniqueidentifier": "STRING", "sysname": "STRING",
        "text": "TEXT", "ntext": "TEXT", "xml": "TEXT",
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
        "json": "JSON",
        # TIMESTAMP here is ROWVERSION, an 8-byte binary counter -- not a date and time.
        "binary": "BLOB", "varbinary": "BLOB", "image": "BLOB",
        "timestamp": "BLOB", "rowversion": "BLOB",
    },
    # SQLite declared types (PRAGMA table_info). An integer is stored in up to 8 bytes
    # whatever size the declaration names, so every integer type is BIGINT; REAL is 8-byte.
    "sqlite": {
        "varchar": "STRING", "character varying": "STRING", "varying character": "STRING",
        "char": "STRING", "character": "STRING", "nchar": "STRING", "nvarchar": "STRING",
        "native character": "STRING",
        "text": "TEXT", "clob": "TEXT",
        "integer": "BIGINT", "int": "BIGINT", "tinyint": "BIGINT", "smallint": "BIGINT",
        "mediumint": "BIGINT", "bigint": "BIGINT", "int2": "BIGINT", "int8": "BIGINT",
        "unsigned big int": "BIGINT",
        "numeric": "DECIMAL", "decimal": "DECIMAL",
        "real": "DOUBLE", "double": "DOUBLE", "double precision": "DOUBLE", "float": "DOUBLE",
        "boolean": "BOOL",
        "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP", "date": "DATE", "time": "TIME",
        "json": "JSON",
        "blob": "BLOB",
    },
    # Spark SQL type names (DESCRIBE). STRING has no length, so it is TEXT; the complex
    # types keep their structure as JSON.
    "spark": {
        "varchar": "STRING", "char": "STRING",
        "string": "TEXT",
        "tinyint": "TINYINT", "byte": "TINYINT",
        "smallint": "SMALLINT", "short": "SMALLINT",
        "int": "INT", "integer": "INT",
        "bigint": "BIGINT", "long": "BIGINT",
        "decimal": "DECIMAL", "dec": "DECIMAL", "numeric": "DECIMAL",
        "float": "FLOAT", "real": "FLOAT", "double": "DOUBLE",
        "boolean": "BOOL",
        "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP", "timestamp_ltz": "TIMESTAMP",
        "date": "DATE",
        "array": "JSON", "map": "JSON", "struct": "JSON", "variant": "JSON",
        "binary": "BLOB",
    },
}


def _sized(pattern, most, wider, unsized):
    """A type sized by length: ``pattern`` up to ``most`` (None: no limit); ``wider`` past
    it, or for a negative length, which is how SQL Server reports MAX; ``unsized`` when
    the length is not known."""
    def render(L, S):
        if not L:
            return unsized
        return wider if L < 0 or (most is not None and L > most) else pattern.format(L)
    return render


def _decimal(name, most, unsized):
    """DECIMAL(p,s) up to ``most`` digits (None: no limit), else the engine's widest."""
    return lambda L, S: (f"{name}({L},{S})" if L and (most is None or L <= most) and S is not None
                         else f"{name}({L})" if L and (most is None or L <= most) else unsized)


# canonical token -> the type an engine writes in DDL: a string, or a function of
# (length, scale) for the types sized by them
_RENDER = {
    "postgres": {
        "STRING": _sized("VARCHAR({})", 10485760, "TEXT", "VARCHAR(255)"),
        "TEXT": "TEXT",
        # Postgres has no one-byte integer; the next size up holds it losslessly.
        "TINYINT": "SMALLINT", "SMALLINT": "SMALLINT", "INT": "INTEGER", "BIGINT": "BIGINT",
        "DECIMAL": _decimal("NUMERIC", 1000, "NUMERIC"),
        "FLOAT": "REAL", "DOUBLE": "DOUBLE PRECISION",
        "BOOL": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "TIME": "TIME",
        "JSON": "JSONB", "BLOB": "BYTEA",
    },
    "mysql": {
        "STRING": _sized("VARCHAR({})", 16383, "LONGTEXT", "VARCHAR(255)"),
        # TEXT holds 64 KB and MEDIUMTEXT 16 MB; only LONGTEXT holds any text.
        "TEXT": "LONGTEXT",
        "TINYINT": "TINYINT", "SMALLINT": "SMALLINT", "INT": "INT", "BIGINT": "BIGINT",
        "DECIMAL": _decimal("DECIMAL", 65, "DECIMAL(65,30)"),
        "FLOAT": "FLOAT", "DOUBLE": "DOUBLE",
        "BOOL": "TINYINT(1)",
        # MySQL TIMESTAMP has the 2038 limit; DATETIME is safe and stores no tz.
        "TIMESTAMP": "DATETIME(6)", "DATE": "DATE", "TIME": "TIME(6)",
        "JSON": "JSON", "BLOB": "LONGBLOB",
    },
    "db2": {
        # A LOB over 1 GB must be NOT LOGGED, so 1G is the widest a plain column declares.
        "STRING": _sized("VARCHAR({})", 32672, "CLOB(1G)", "VARCHAR(255)"),
        "TEXT": "CLOB(1G)",
        "TINYINT": "SMALLINT", "SMALLINT": "SMALLINT", "INT": "INTEGER", "BIGINT": "BIGINT",
        # DECIMAL stops at 31 digits; DECFLOAT(34) holds 34.
        "DECIMAL": _decimal("DECIMAL", 31, "DECFLOAT(34)"),
        "FLOAT": "REAL", "DOUBLE": "DOUBLE",
        "BOOL": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "TIME": "TIME",
        "JSON": "CLOB(1G)", "BLOB": "BLOB(1G)",
    },
    "oracle": {
        "STRING": _sized("VARCHAR2({} CHAR)", 4000, "CLOB", "VARCHAR2(255 CHAR)"),
        "TEXT": "CLOB",
        "TINYINT": "NUMBER(3)", "SMALLINT": "NUMBER(5)", "INT": "NUMBER(10)", "BIGINT": "NUMBER(19)",
        "DECIMAL": _decimal("NUMBER", 38, "NUMBER"),
        "FLOAT": "BINARY_FLOAT", "DOUBLE": "BINARY_DOUBLE",
        # A BOOLEAN column arrived in 23ai; NUMBER(1) works on every release.
        "BOOL": "NUMBER(1)",
        "TIMESTAMP": "TIMESTAMP", "DATE": "DATE",
        # No TIME type: a time of day is the interval since midnight.
        "TIME": "INTERVAL DAY(0) TO SECOND(6)",
        # A JSON column arrived in 21c; CLOB works on every release.
        "JSON": "CLOB", "BLOB": "BLOB",
    },
    "sqlserver": {
        "STRING": _sized("NVARCHAR({})", 4000, "NVARCHAR(MAX)", "NVARCHAR(255)"),
        "TEXT": "NVARCHAR(MAX)",
        # TINYINT is unsigned 0..255 here, so a signed one needs SMALLINT.
        "TINYINT": "SMALLINT", "SMALLINT": "SMALLINT", "INT": "INT", "BIGINT": "BIGINT",
        "DECIMAL": _decimal("DECIMAL", 38, "DECIMAL(38,10)"),
        # FLOAT is 8-byte here; REAL is 4-byte.
        "FLOAT": "REAL", "DOUBLE": "FLOAT",
        "BOOL": "BIT",
        "TIMESTAMP": "DATETIME2(6)", "DATE": "DATE", "TIME": "TIME(6)",
        "JSON": "NVARCHAR(MAX)", "BLOB": "VARBINARY(MAX)",
    },
    "sqlite": {
        # SQLite enforces no length or precision; a declared size still tells a reader.
        "STRING": _sized("VARCHAR({})", None, "TEXT", "TEXT"),
        "TEXT": "TEXT",
        "TINYINT": "INTEGER", "SMALLINT": "INTEGER", "INT": "INTEGER", "BIGINT": "INTEGER",
        "DECIMAL": _decimal("NUMERIC", None, "NUMERIC"),
        "FLOAT": "REAL", "DOUBLE": "REAL",
        "BOOL": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "TIME": "TIME",
        # A declared JSON type would take NUMERIC affinity and turn '123' into a number.
        "JSON": "TEXT", "BLOB": "BLOB",
    },
    "spark": {
        "STRING": "STRING", "TEXT": "STRING",
        "TINYINT": "TINYINT", "SMALLINT": "SMALLINT", "INT": "INT", "BIGINT": "BIGINT",
        "DECIMAL": _decimal("DECIMAL", 38, "DECIMAL(38,18)"),
        "FLOAT": "FLOAT", "DOUBLE": "DOUBLE",
        "BOOL": "BOOLEAN",
        # No TIME type: a time of day is kept as its text.
        "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "TIME": "STRING",
        "JSON": "STRING", "BLOB": "BINARY",
    },
}

# The aliases get_db accepts for the same engine.
_ALIASES = {"postgresql": "postgres", "mariadb": "mysql", "mssql": "sqlserver"}


def _engine(database_type):
    name = str(database_type or "").strip().lower()
    return _ALIASES.get(name, name)


def canonical_type(database_type, raw_type):
    """The canonical token for a column type as ``database_type`` names it, or None
    when the type is not known. Sizes are ignored and the words around them kept
    (``numeric(10,2)`` is ``numeric``, ``int(10) unsigned`` is ``int unsigned``), and a
    declared array (``text[]``, ``array<string>``) is read as the engine's array."""
    if not raw_type:
        return None
    t = " ".join(re.sub(r"\([^)]*\)|<.*>", " ", str(raw_type).lower()).split())
    if t.endswith("[]"):
        t = "array"
    return _TO_CANONICAL.get(_engine(database_type), {}).get(t)


def render_type(canonical, database_type, length=None, scale=None):
    """The type ``database_type`` writes in DDL for a canonical token, sized by
    ``length`` and ``scale`` where the type takes them -- a negative length is no limit,
    as SQL Server reports MAX; None when the engine has no rendering for the token."""
    rendering = _RENDER.get(_engine(database_type), {}).get(canonical)
    return rendering(length, scale) if callable(rendering) else rendering


def fallback_type(database_type):
    """The type ``database_type`` creates a column of unknown type as: its widest text
    type, the rendering of TEXT. None for an engine these maps do not describe."""
    return render_type("TEXT", database_type)
