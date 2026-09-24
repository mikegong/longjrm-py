"""DB-free unit tests for longjrm.utils.sql_types: a column type as an engine names it
maps to a canonical token, and a token renders as the type an engine writes in DDL."""

import pytest

from longjrm.utils.sql_types import canonical_type, fallback_type, render_type


def test_a_parametric_suffix_and_an_array_marker_are_ignored():
    assert canonical_type("postgres", "numeric(10,2)") == "DECIMAL"
    assert canonical_type("postgres", "character varying") == "STRING"
    assert canonical_type("postgres", "timestamp without time zone") == "TIMESTAMP"
    assert canonical_type("postgres", "text[]") == "TEXT"


def test_an_unknown_type_has_no_token():
    assert canonical_type("postgres", "totally unknown type") is None
    assert canonical_type("postgres", None) is None
    assert canonical_type("no_such_engine", "integer") is None


@pytest.mark.parametrize("engine,raw,canonical", [
    ("db2", "INTEGER", "INT"), ("db2", "VARCHAR", "STRING"), ("db2", "DECIMAL", "DECIMAL"),
    ("db2", "DECFLOAT", "DECIMAL"), ("db2", "VARGRAPHIC", "STRING"), ("db2", "CLOB", "TEXT"),
    ("oracle", "VARCHAR2", "STRING"), ("oracle", "NUMBER", "DECIMAL"),
    ("oracle", "DATE", "TIMESTAMP"), ("oracle", "BINARY_DOUBLE", "DOUBLE"),
    ("sqlserver", "NVARCHAR", "STRING"), ("sqlserver", "BIT", "BOOL"),
    ("sqlserver", "FLOAT", "DOUBLE"), ("sqlserver", "REAL", "FLOAT"),
    ("sqlserver", "DATETIME2", "TIMESTAMP"), ("sqlserver", "MONEY", "DECIMAL"),
    ("mysql", "TINYINT", "TINYINT"), ("sqlserver", "TINYINT", "SMALLINT"),
])
def test_native_types_canonicalize(engine, raw, canonical):
    assert canonical_type(engine, raw) == canonical


def test_the_aliases_get_db_accepts_name_the_same_engines():
    assert canonical_type("postgresql", "int4") == "INT"
    assert canonical_type("mariadb", "mediumint") == "INT"
    assert canonical_type("MSSQL", "bit") == "BOOL"
    assert render_type("INT", "postgresql") == "INTEGER"


def test_render_for_mysql():
    assert render_type("STRING", "mysql", 40, None) == "VARCHAR(40)"
    assert render_type("STRING", "mysql", None, None) == "VARCHAR(255)"
    assert render_type("DECIMAL", "mysql", 10, 2) == "DECIMAL(10,2)"
    assert render_type("DECIMAL", "mysql", 10, None) == "DECIMAL(10)"
    assert render_type("TIMESTAMP", "mysql") == "DATETIME(6)"
    assert render_type("BOOL", "mysql") == "TINYINT(1)"
    assert render_type("JSON", "mysql") == "JSON"


def test_render_for_postgres():
    assert render_type("INT", "postgres") == "INTEGER"
    assert render_type("TINYINT", "postgres") == "SMALLINT"
    assert render_type("DECIMAL", "postgres", 12, 2) == "NUMERIC(12,2)"
    assert render_type("DECIMAL", "postgres") == "NUMERIC(38,10)"
    assert render_type("JSON", "postgres") == "JSONB"


def test_an_engine_with_no_renderings_renders_nothing():
    assert render_type("INT", "db2") is None
    assert render_type("NO_SUCH_TOKEN", "postgres") is None
    assert fallback_type("db2") is None
    assert (fallback_type("postgres"), fallback_type("mysql")) == ("TEXT", "TEXT")
