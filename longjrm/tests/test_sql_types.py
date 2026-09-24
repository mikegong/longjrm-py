"""DB-free unit tests for longjrm.utils.sql_types: a column type as an engine names it
maps to a canonical token, and a token renders as the type an engine writes in DDL."""

import pytest

from longjrm.utils.sql_types import (
    _RENDER, _TO_CANONICAL, canonical_type, engine_name, fallback_type, render_type,
)

ENGINES = ("postgres", "mysql", "db2", "oracle", "sqlserver", "sqlite", "spark")
TOKENS = ("STRING", "TEXT", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE",
          "BOOL", "TIMESTAMP", "DATE", "TIME", "JSON", "BLOB")


def test_every_engine_is_described_on_both_sides():
    assert set(_TO_CANONICAL) == set(_RENDER) == set(ENGINES)


@pytest.mark.parametrize("engine", ENGINES)
def test_every_engine_renders_every_token(engine):
    assert set(_RENDER[engine]) == set(TOKENS)
    assert all(render_type(token, engine) for token in TOKENS)
    assert all(render_type(token, engine, 10, 2) for token in TOKENS)


def test_every_token_a_source_type_reaches_is_a_token_the_engines_render():
    assert {token for names in _TO_CANONICAL.values() for token in names.values()} <= set(TOKENS)


def test_sizes_are_ignored_and_the_words_around_them_kept():
    assert canonical_type("postgres", "numeric(10,2)") == "DECIMAL"
    assert canonical_type("postgres", "character varying(100)") == "STRING"
    assert canonical_type("postgres", "timestamp(3) without time zone") == "TIMESTAMP"
    assert canonical_type("mysql", "int(10) unsigned") == "BIGINT"
    assert canonical_type("oracle", "TIMESTAMP(6) WITH TIME ZONE") == "TIMESTAMP"


def test_a_declared_array_is_the_engines_array():
    assert canonical_type("postgres", "text[]") == "JSON"
    assert canonical_type("postgres", "ARRAY") == "JSON"
    assert canonical_type("spark", "array<string>") == "JSON"
    assert canonical_type("spark", "map<string,array<int>>") == "JSON"


def test_an_unknown_type_has_no_token():
    assert canonical_type("postgres", "totally unknown type") is None
    assert canonical_type("postgres", None) is None
    assert canonical_type("no_such_engine", "integer") is None


@pytest.mark.parametrize("engine,raw,canonical", [
    ("db2", "INTEGER", "INT"), ("db2", "VARCHAR", "STRING"), ("db2", "DECIMAL", "DECIMAL"),
    ("db2", "DECFLOAT", "DECIMAL"), ("db2", "VARGRAPHIC", "STRING"), ("db2", "CLOB", "TEXT"),
    ("oracle", "VARCHAR2", "STRING"), ("oracle", "NUMBER", "DECIMAL"),
    ("oracle", "DATE", "TIMESTAMP"), ("oracle", "BINARY_DOUBLE", "DOUBLE"),
    ("oracle", "FLOAT", "DOUBLE"), ("oracle", "JSON", "JSON"),
    ("sqlserver", "NVARCHAR", "STRING"), ("sqlserver", "BIT", "BOOL"),
    ("sqlserver", "FLOAT", "DOUBLE"), ("sqlserver", "REAL", "FLOAT"),
    ("sqlserver", "DATETIME2", "TIMESTAMP"), ("sqlserver", "MONEY", "DECIMAL"),
    ("sqlserver", "TIMESTAMP", "BLOB"),
    ("mysql", "TINYINT", "TINYINT"), ("sqlserver", "TINYINT", "SMALLINT"),
    ("mysql", "bigint unsigned", "DECIMAL"), ("mysql", "enum", "STRING"), ("mysql", "year", "SMALLINT"),
    ("sqlite", "INTEGER", "BIGINT"), ("sqlite", "VARCHAR(20)", "STRING"), ("sqlite", "REAL", "DOUBLE"),
    ("spark", "string", "TEXT"), ("spark", "long", "BIGINT"), ("spark", "decimal(10,2)", "DECIMAL"),
])
def test_native_types_canonicalize(engine, raw, canonical):
    assert canonical_type(engine, raw) == canonical


def test_the_aliases_get_db_accepts_name_the_same_engines():
    assert canonical_type("postgresql", "int4") == "INT"
    assert canonical_type("mariadb", "mediumint") == "INT"
    assert canonical_type("MSSQL", "bit") == "BOOL"
    assert render_type("INT", "postgresql") == "INTEGER"
    assert [engine_name(t) for t in ("PostgreSQL", "mariadb", "mssql", "db2", None)] == [
        "postgres", "mysql", "sqlserver", "db2", ""]


def test_a_negative_length_is_no_limit_as_sql_server_reports_max():
    assert render_type("STRING", "postgres", -1) == "TEXT"
    assert render_type("STRING", "mysql", -1) == "LONGTEXT"
    assert render_type("STRING", "sqlserver", -1) == "NVARCHAR(MAX)"
    assert render_type("STRING", "sqlite", -1) == "TEXT"


def test_render_for_mysql():
    assert render_type("STRING", "mysql", 40) == "VARCHAR(40)"
    assert render_type("STRING", "mysql") == "VARCHAR(255)"
    assert render_type("STRING", "mysql", 20000) == "LONGTEXT"
    assert render_type("TEXT", "mysql") == "LONGTEXT"
    assert render_type("DECIMAL", "mysql", 10, 2) == "DECIMAL(10,2)"
    assert render_type("DECIMAL", "mysql", 10) == "DECIMAL(10)"
    assert render_type("DECIMAL", "mysql") == "DECIMAL(65,30)"
    assert render_type("TIMESTAMP", "mysql") == "DATETIME(6)"
    assert render_type("BOOL", "mysql") == "TINYINT(1)"
    assert render_type("JSON", "mysql") == "JSON"


def test_render_for_postgres():
    assert render_type("INT", "postgres") == "INTEGER"
    assert render_type("TINYINT", "postgres") == "SMALLINT"
    assert render_type("DECIMAL", "postgres", 12, 2) == "NUMERIC(12,2)"
    assert render_type("DECIMAL", "postgres") == "NUMERIC"
    assert render_type("JSON", "postgres") == "JSONB"


def test_render_for_db2():
    assert render_type("STRING", "db2", 100) == "VARCHAR(100)"
    assert render_type("STRING", "db2", 40000) == "CLOB(1G)"
    assert render_type("DECIMAL", "db2", 31, 2) == "DECIMAL(31,2)"
    assert render_type("DECIMAL", "db2", 38, 2) == "DECFLOAT(34)"
    assert render_type("DECIMAL", "db2") == "DECFLOAT(34)"
    assert render_type("TINYINT", "db2") == "SMALLINT"
    assert render_type("TEXT", "db2") == "CLOB(1G)"
    assert render_type("BLOB", "db2") == "BLOB(1G)"


def test_render_for_oracle():
    assert render_type("STRING", "oracle", 100) == "VARCHAR2(100 CHAR)"
    assert render_type("STRING", "oracle", 5000) == "CLOB"
    assert render_type("INT", "oracle") == "NUMBER(10)"
    assert render_type("BIGINT", "oracle") == "NUMBER(19)"
    assert render_type("DECIMAL", "oracle", 12, 2) == "NUMBER(12,2)"
    assert render_type("DECIMAL", "oracle") == "NUMBER"
    assert render_type("DOUBLE", "oracle") == "BINARY_DOUBLE"
    assert render_type("TIME", "oracle") == "INTERVAL DAY(0) TO SECOND(6)"


def test_render_for_sqlserver():
    assert render_type("STRING", "sqlserver", 100) == "NVARCHAR(100)"
    assert render_type("STRING", "sqlserver", 5000) == "NVARCHAR(MAX)"
    assert render_type("TINYINT", "sqlserver") == "SMALLINT"
    assert render_type("DOUBLE", "sqlserver") == "FLOAT"
    assert render_type("BOOL", "sqlserver") == "BIT"
    assert render_type("TIMESTAMP", "sqlserver") == "DATETIME2(6)"
    assert render_type("BLOB", "sqlserver") == "VARBINARY(MAX)"


def test_render_for_sqlite_and_spark():
    assert render_type("BIGINT", "sqlite") == "INTEGER"
    assert render_type("STRING", "sqlite") == "TEXT"
    assert render_type("DECIMAL", "sqlite", 12, 2) == "NUMERIC(12,2)"
    assert render_type("JSON", "sqlite") == "TEXT"
    assert render_type("STRING", "spark", 100) == "STRING"
    assert render_type("DECIMAL", "spark") == "DECIMAL(38,18)"
    assert render_type("BLOB", "spark") == "BINARY"


def test_the_fallback_is_the_engines_widest_text():
    assert {engine: fallback_type(engine) for engine in ENGINES} == {
        "postgres": "TEXT", "mysql": "LONGTEXT", "db2": "CLOB(1G)", "oracle": "CLOB",
        "sqlserver": "NVARCHAR(MAX)", "sqlite": "TEXT", "spark": "STRING"}
    assert fallback_type("generic") is None
    assert render_type("NO_SUCH_TOKEN", "postgres") is None
