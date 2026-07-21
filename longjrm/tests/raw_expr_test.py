"""
Tests for the Raw SQL expression sentinel and backward compatibility of the
legacy backtick CURRENT-keyword strings.

Self-contained: runs against in-memory SQLite, no test_config required.

    python -m unittest longjrm.tests.raw_expr_test -v
"""
import datetime
import sqlite3
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm import Raw, CURRENT_TIMESTAMP, CURRENT_DATE
from longjrm.config.config import JrmConfig, DatabaseConfig
from longjrm.config.runtime import configure
from longjrm.database.db import Db
from longjrm.database.sqlite import SqliteDb
from longjrm.utils import sql as sql_utils
from longjrm.utils.sql import where_parser, inject_current, build_where

TS_PATTERN = r"\d{4}-\d{2}-\d{2}"  # sqlite CURRENT_TIMESTAMP / CURRENT_DATE prefix


def _make_db():
    configure(JrmConfig(_databases={"mem": DatabaseConfig(type="sqlite", database=":memory:")}))
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    return SqliteDb({"conn": conn, "database_type": "sqlite", "database_name": ":memory:"})


class _PyformatDb(Db):
    """Minimal concrete Db with a %s placeholder, for exercising the pyformat
    (_escape_sql / inject_current) path without a live Postgres/MySQL server.
    Only _prepare_sql and the SQL constructors are used — the conn is never
    touched."""

    def get_cursor(self):  # pragma: no cover - never called
        raise NotImplementedError

    def get_stream_cursor(self):  # pragma: no cover - never called
        raise NotImplementedError

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        cols = ', '.join(key_columns)
        if update_columns:
            sets = ', '.join(f"{c} = EXCLUDED.{c}" for c in update_columns)
            return f"ON CONFLICT ({cols}) DO UPDATE SET {sets}"
        return f"ON CONFLICT ({cols}) DO NOTHING"


def _make_pyformat_db():
    configure(JrmConfig(_databases={"mem": DatabaseConfig(type="sqlite", database=":memory:")}))
    db = _PyformatDb({"conn": object(), "database_type": "postgres", "database_name": "x"})
    db.placeholder = '%s'
    return db


class RawClassTests(unittest.TestCase):
    def test_validation(self):
        with self.assertRaises(TypeError):
            Raw(123)
        with self.assertRaises(TypeError):
            Raw("")
        with self.assertRaises(TypeError):
            Raw(None)

    def test_equality_and_repr(self):
        self.assertEqual(Raw("NOW()"), Raw("NOW()"))
        self.assertNotEqual(Raw("NOW()"), "NOW()")
        self.assertEqual(repr(Raw("NOW()")), "Raw('NOW()')")
        self.assertEqual(Raw(Raw("NOW()")).text, "NOW()")

    def test_constants(self):
        self.assertEqual(CURRENT_TIMESTAMP.text, "CURRENT_TIMESTAMP")
        self.assertEqual(CURRENT_DATE.text, "CURRENT_DATE")


class WhereParserRawTests(unittest.TestCase):
    def test_simple_condition(self):
        clause, values = where_parser({"ts": Raw("CURRENT_TIMESTAMP")}, "?")
        self.assertEqual(clause, " where ts = CURRENT_TIMESTAMP")
        self.assertEqual(values, [])

    def test_regular_condition(self):
        clause, values = where_parser({"ts": {"<=": Raw("CURRENT_DATE - 7")}}, "?")
        self.assertEqual(clause, " where ts <= CURRENT_DATE - 7")
        self.assertEqual(values, [])

    def test_comprehensive_condition(self):
        clause, values = where_parser(
            {"ts": {"operator": ">", "value": Raw("CURRENT_DATE"), "placeholder": "Y"}}, "?")
        self.assertEqual(clause, " where ts > CURRENT_DATE")
        self.assertEqual(values, [])

    def test_raw_mixed_with_bound_values(self):
        clause, values = where_parser({"ts": {"<": Raw("CURRENT_TIMESTAMP")}, "name": "x"}, "?")
        self.assertEqual(clause, " where ts < CURRENT_TIMESTAMP and name = ?")
        self.assertEqual(values, ["x"])

    def test_inject_current_handles_raw(self):
        sql, values = inject_current(
            "INSERT INTO t (a, b) VALUES (?, ?)", [1, Raw("CURRENT_TIMESTAMP")], "?")
        self.assertEqual(sql, "INSERT INTO t (a, b) VALUES (?, CURRENT_TIMESTAMP)")
        self.assertEqual(values, [1])


class RawCrudTests(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()
        self.db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, ts TEXT)")

    def test_insert_with_raw(self):
        self.db.insert("t", {"id": 1, "name": "a", "ts": Raw("CURRENT_TIMESTAMP")})
        row = self.db.query("SELECT * FROM t")["data"][0]
        self.assertRegex(row["ts"], TS_PATTERN)
        self.assertEqual(row["name"], "a")

    def test_insert_with_constant(self):
        self.db.insert("t", {"id": 1, "ts": CURRENT_DATE})
        row = self.db.query("SELECT ts FROM t")["data"][0]
        self.assertRegex(row["ts"], TS_PATTERN)

    def test_insert_all_raw_values(self):
        self.db.insert("t", {"id": Raw("1 + 1"), "ts": CURRENT_TIMESTAMP})
        row = self.db.query("SELECT * FROM t")["data"][0]
        self.assertEqual(row["id"], 2)
        self.assertRegex(row["ts"], TS_PATTERN)

    def test_update_with_raw(self):
        self.db.insert("t", {"id": 1, "name": "a", "ts": "old"})
        self.db.update("t", {"ts": Raw("CURRENT_TIMESTAMP"), "name": "b"}, {"id": 1})
        row = self.db.query("SELECT * FROM t")["data"][0]
        self.assertRegex(row["ts"], TS_PATTERN)
        self.assertEqual(row["name"], "b")

    def test_delete_with_raw_where(self):
        self.db.insert("t", {"id": 1, "ts": "1990-01-01"})
        self.db.insert("t", {"id": 2, "ts": "9999-01-01"})
        result = self.db.delete("t", {"ts": {"<": Raw("CURRENT_TIMESTAMP")}})
        self.assertEqual(result["count"], 1)
        self.assertEqual(self.db.query("SELECT id FROM t")["data"], [{"id": 2}])

    def test_select_with_raw_where(self):
        self.db.insert("t", {"id": 1, "ts": "1990-01-01"})
        rows = self.db.select("t", ["id"], {"ts": {"<": Raw("CURRENT_DATE")}})
        self.assertEqual(rows["count"], 1)

    def test_single_merge_with_raw(self):
        self.db.merge("t", {"id": 1, "name": "a", "ts": Raw("CURRENT_TIMESTAMP")}, ["id"])
        first = self.db.query("SELECT ts FROM t")["data"][0]["ts"]
        self.assertRegex(first, TS_PATTERN)
        self.db.merge("t", {"id": 1, "name": "b", "ts": Raw("CURRENT_TIMESTAMP")}, ["id"])
        row = self.db.query("SELECT * FROM t")["data"][0]
        self.assertEqual(row["name"], "b")

    def test_bulk_merge_with_consistent_raw(self):
        self.db.merge("t", [{"id": 1, "ts": CURRENT_TIMESTAMP},
                            {"id": 2, "ts": CURRENT_TIMESTAMP}], ["id"])
        rows = self.db.query("SELECT * FROM t ORDER BY id")["data"]
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertRegex(row["ts"], TS_PATTERN)

    def test_bulk_merge_with_inconsistent_raw_raises(self):
        with self.assertRaises(ValueError):
            self.db.merge("t", [{"id": 1, "ts": CURRENT_TIMESTAMP},
                                {"id": 2, "ts": "literal"}], ["id"])

    def test_execute_with_raw_value(self):
        self.db.execute("INSERT INTO t (id, ts) VALUES (?, ?)", [5, Raw("CURRENT_TIMESTAMP")])
        row = self.db.query("SELECT ts FROM t WHERE id = 5")["data"][0]
        self.assertRegex(row["ts"], TS_PATTERN)

    def test_bulk_insert_with_raw_raises(self):
        with self.assertRaises(TypeError):
            self.db.insert("t", [{"id": 1, "ts": Raw("CURRENT_TIMESTAMP")}])

    def test_bulk_update_with_raw_raises(self):
        self.db.insert("t", {"id": 1, "ts": "old"})
        with self.assertRaises(TypeError):
            self.db.bulk_update("t", [{"id": 1, "ts": Raw("CURRENT_TIMESTAMP")}], ["id"])


class BackwardCompatibilityTests(unittest.TestCase):
    """The legacy backtick string form must keep working unchanged."""

    def setUp(self):
        self.db = _make_db()
        self.db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TEXT)")

    def test_backtick_insert_still_works(self):
        self.db.insert("t", {"id": 1, "ts": "`CURRENT_TIMESTAMP`"})
        row = self.db.query("SELECT ts FROM t")["data"][0]
        self.assertRegex(row["ts"], TS_PATTERN)

    def test_backtick_where_still_works(self):
        clause, values = where_parser({"ts": {"<": "`CURRENT_TIMESTAMP`"}}, "?")
        self.assertEqual(clause, " where ts < CURRENT_TIMESTAMP")
        self.assertEqual(values, [])

    def test_plain_string_is_data_not_sql(self):
        # Without backticks (or Raw), keyword-looking strings are plain data.
        self.db.insert("t", {"id": 1, "ts": "CURRENT_TIMESTAMP"})
        row = self.db.query("SELECT ts FROM t")["data"][0]
        self.assertEqual(row["ts"], "CURRENT_TIMESTAMP")

    def test_json_data_cannot_produce_raw(self):
        # The security property: deserialized JSON is strings all the way down.
        import json
        data = json.loads('{"ts": "Raw(\'CURRENT_TIMESTAMP\')"}')
        self.db.insert("t", {"id": 1, **data})
        row = self.db.query("SELECT ts FROM t")["data"][0]
        self.assertEqual(row["ts"], "Raw('CURRENT_TIMESTAMP')")


class LiteralPercentTests(unittest.TestCase):
    """Regression for the _escape_sql gating fix (review finding #1): a literal
    '%' must survive on qmark backends and round-trip on pyformat backends."""

    def test_qmark_backend_preserves_literal_percent(self):
        # SQLite uses '?'; _escape_sql must NOT fire, so '50%' stays '50%'.
        db = _make_db()
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        db.insert("t", {"id": 1, "name": "50%"})
        self.assertEqual(db.query("SELECT name FROM t")["data"][0]["name"], "50%")
        # a bound value containing % is data, not a placeholder: exact match works
        self.assertEqual(db.query("SELECT * FROM t WHERE name = ?", ["50%"])["count"], 1)

    def test_qmark_backend_no_param_statement_keeps_percent(self):
        # No bound values -> _escape_sql must not run even conceptually.
        db = _make_db()
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO t (id, name) VALUES (1, '90%')")
        self.assertEqual(db.query("SELECT name FROM t")["data"][0]["name"], "90%")

    def test_pyformat_escapes_literal_percent_only_with_params(self):
        db = _make_pyformat_db()
        # With a bound value, a stray % is doubled so the driver's pyformat
        # pass collapses it back to a single % (server sees the original).
        sql, vals = db._prepare_sql(
            "SELECT * FROM t WHERE ratio > %s AND note LIKE 'x%'", [10])
        self.assertEqual(sql, "SELECT * FROM t WHERE ratio > %s AND note LIKE 'x%%'")
        self.assertEqual(vals, [10])
        # The %s placeholder itself is never doubled.
        self.assertIn("> %s ", sql)

    def test_pyformat_no_escape_without_params(self):
        db = _make_pyformat_db()
        # No values bound -> nothing to substitute -> % left untouched.
        sql, vals = db._prepare_sql("SELECT * FROM t WHERE note LIKE 'x%'", None)
        self.assertEqual(sql, "SELECT * FROM t WHERE note LIKE 'x%'")
        self.assertEqual(vals, None)


class RawPyformatBackendTests(unittest.TestCase):
    """Raw on a %s (pyformat) backend: the escaping branch only fires here, so
    it is otherwise unexercised by the SQLite-based tests."""

    def setUp(self):
        self.db = _make_pyformat_db()

    def test_insert_renders_raw_inline_no_placeholder(self):
        sql, vals = self.db._single_insert_constructor(
            "t", {"name": "a", "created_at": Raw("CURRENT_TIMESTAMP")})
        self.assertEqual(sql, "INSERT INTO t (name, created_at) VALUES (%s, CURRENT_TIMESTAMP)")
        self.assertEqual(vals, ["a"])

    def test_update_renders_raw_inline(self):
        sql, vals = self.db._update_constructor(
            "t", {"updated_at": Raw("CURRENT_TIMESTAMP"), "name": "b"}, {"id": 1})
        # where_parser emits a lowercase ' where ' prefix in this path.
        self.assertEqual(sql, "UPDATE t SET updated_at = CURRENT_TIMESTAMP, name = %s where id = %s")
        self.assertEqual(vals, ["b", 1])

    def test_merge_renders_raw_inline(self):
        sql, vals = self.db._merge_constructor(
            "t", {"id": 1, "ts": Raw("CURRENT_TIMESTAMP")}, ["id"])
        self.assertIn("VALUES (%s, CURRENT_TIMESTAMP)", sql)
        self.assertEqual(vals, [1])

    def test_raw_with_literal_percent_roundtrips(self):
        # Raw("x % 2") injected, then a bound value forces _escape_sql; the
        # doubled %% collapses back in the driver's pyformat pass.
        sql, vals = self.db._prepare_sql(
            "UPDATE t SET ratio = %s, note = %s", [Raw("x % 2"), "done"])
        self.assertEqual(sql, "UPDATE t SET ratio = x %% 2, note = %s")
        self.assertEqual(vals, ["done"])

    def test_prepare_sql_injects_raw_via_inject_current(self):
        sql, vals = self.db._prepare_sql(
            "INSERT INTO t (a, b) VALUES (%s, %s)", [1, CURRENT_TIMESTAMP])
        self.assertEqual(sql, "INSERT INTO t (a, b) VALUES (%s, CURRENT_TIMESTAMP)")
        self.assertEqual(vals, [1])


class MergeSelectRawConditionTests(unittest.TestCase):
    """merge_select conditions flow through build_where (a different entry
    point than the CRUD constructors), so Raw needs explicit coverage there."""

    def test_build_where_renders_raw(self):
        clause, values = build_where({"ts": {">=": Raw("CURRENT_DATE")}}, "?")
        self.assertEqual(clause, " WHERE ts >= CURRENT_DATE")
        self.assertEqual(values, [])

    def test_build_where_raw_mixed_with_bound(self):
        clause, values = build_where(
            {"ts": {">=": Raw("CURRENT_DATE")}, "active": True}, "?")
        self.assertEqual(clause, " WHERE ts >= CURRENT_DATE and active = ?")
        self.assertEqual(values, [True])

    def test_build_where_raw_inline_mode(self):
        # inline=True must still treat Raw as an expression (not quote it).
        clause, values = build_where({"ts": {">=": Raw("CURRENT_DATE")}}, "?", inline=True)
        self.assertEqual(clause, " WHERE ts >= CURRENT_DATE")
        self.assertEqual(values, [])

    def test_merge_select_with_raw_condition_end_to_end(self):
        db = _make_db()
        db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, ts TEXT)")
        db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, ts TEXT)")
        db.insert("src", {"id": 1, "ts": "1990-01-01"})
        db.insert("src", {"id": 2, "ts": "9999-01-01"})
        # Only the past row should be merged.
        db.merge_select(
            source_table="src", target_table="dst",
            insert_columns=["id", "ts"], key_columns=["id"],
            conditions={"ts": {"<": Raw("CURRENT_DATE")}},
        )
        rows = db.query("SELECT id FROM dst ORDER BY id")["data"]
        self.assertEqual(rows, [{"id": 1}])


if __name__ == '__main__':
    unittest.main(verbosity=2)
