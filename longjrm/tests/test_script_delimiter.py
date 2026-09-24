"""DB-free unit tests: execute_script splits a script on its delimiter -- ';' unless it is
given another, which leaves a semicolon in a comment or a literal alone."""

import sqlite3

import pytest

from longjrm.config.config import DatabaseConfig, JrmConfig
from longjrm.config.runtime import configure
from longjrm.database.sqlite import SqliteDb


def _db():
    configure(JrmConfig(_databases={"mem": DatabaseConfig(type="sqlite", database=":memory:")}))
    return SqliteDb({"conn": sqlite3.connect(":memory:"), "database_type": "sqlite", "database_name": ":memory:"})


def _values(db):
    return [row[0] for row in db.conn.execute("SELECT a FROM t").fetchall()]


def test_a_script_splits_on_semicolons_by_default():
    db = _db()
    assert db.execute_script("CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('x');")["status"] == 0
    assert _values(db) == ["x"]


def test_another_delimiter_leaves_a_semicolon_in_a_comment_or_a_literal_alone():
    db = _db()
    script = ("-- a comment; with a semicolon\nCREATE TABLE t (a TEXT)\n@\n"
              "INSERT INTO t VALUES ('one; two')\n@\n")
    assert db.execute_script(script, delimiter="@")["status"] == 0
    assert _values(db) == ["one; two"]


def test_a_script_file_takes_the_delimiter(tmp_path):
    db = _db()
    path = tmp_path / "script.sql"
    path.write_text("CREATE TABLE t (a TEXT)@INSERT INTO t VALUES ('a;b')@", encoding="utf-8")
    db.run_script_from_file(str(path), delimiter="@")
    assert _values(db) == ["a;b"]


def test_an_empty_delimiter_is_refused():
    with pytest.raises(ValueError):
        _db().execute_script("SELECT 1", delimiter="")
