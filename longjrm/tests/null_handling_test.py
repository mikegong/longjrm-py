"""
Tests for NULL handling in WHERE conditions: None values translate to
IS NULL / IS NOT NULL, and None members in IN / NOT IN lists are handled
correctly (no NOT-IN-NULL trap, no literal 'None' in inline SQL).

Self-contained: runs against in-memory SQLite, no test_config required.

    python -m unittest longjrm.tests.null_handling_test -v
"""
import sqlite3
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig, DatabaseConfig
from longjrm.config.runtime import configure
from longjrm.database.sqlite import SqliteDb
from longjrm.utils.sql import where_parser, build_where, null_operator_clause


def _make_db():
    configure(JrmConfig(_databases={"mem": DatabaseConfig(type="sqlite", database=":memory:")}))
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    db = SqliteDb({"conn": conn, "database_type": "sqlite", "database_name": ":memory:"})
    db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    db.execute("INSERT INTO t (id, name) VALUES (1, 'alice')")
    db.execute("INSERT INTO t (id, name) VALUES (2, NULL)")
    db.execute("INSERT INTO t (id, name) VALUES (3, 'bob')")
    return db


class NullOperatorClauseTests(unittest.TestCase):
    def test_equality_ops_map_to_is_null(self):
        for op in ("=", "==", "IS", "is"):
            self.assertEqual(null_operator_clause("c", op), "c IS NULL")

    def test_inequality_ops_map_to_is_not_null(self):
        for op in ("!=", "<>", "IS NOT", "is not", "NOT"):
            self.assertEqual(null_operator_clause("c", op), "c IS NOT NULL")

    def test_undefined_ops_raise(self):
        for op in (">", "<", ">=", "LIKE"):
            with self.assertRaises(ValueError):
                null_operator_clause("c", op)


class WhereParserNullTests(unittest.TestCase):
    def test_simple_none_is_null(self):
        clause, values = where_parser({"name": None}, "?")
        self.assertEqual(clause, " where name is null")
        self.assertEqual(values, [])

    def test_regular_eq_none(self):
        clause, values = where_parser({"name": {"=": None}}, "?")
        self.assertEqual(clause, " where name IS NULL")
        self.assertEqual(values, [])

    def test_regular_neq_none_is_not_null(self):
        clause, values = where_parser({"name": {"!=": None}}, "?")
        self.assertEqual(clause, " where name IS NOT NULL")
        self.assertEqual(values, [])

    def test_regular_neq_none_inline(self):
        # Previously emitted the literal 'None' -> SQL error.
        clause, values = where_parser({"name": {"<>": None}}, "?", inline=True)
        self.assertEqual(clause, " where name IS NOT NULL")
        self.assertEqual(values, [])

    def test_comprehensive_neq_none(self):
        clause, values = where_parser(
            {"name": {"operator": "!=", "value": None, "placeholder": "Y"}}, "?")
        self.assertEqual(clause, " where name IS NOT NULL")
        self.assertEqual(values, [])

    def test_undefined_operator_with_none_raises(self):
        with self.assertRaises(ValueError):
            where_parser({"age": {">": None}}, "?")

    def test_in_list_with_none_bind(self):
        clause, values = where_parser({"name": {"IN": ["alice", None]}}, "?")
        self.assertEqual(clause, " where (name IN (?) OR name IS NULL)")
        self.assertEqual(values, ["alice"])

    def test_in_list_with_none_inline(self):
        clause, values = where_parser({"name": {"IN": ["alice", None]}}, "?", inline=True)
        self.assertEqual(clause, " where (name IN ('alice') OR name IS NULL)")
        self.assertEqual(values, [])

    def test_in_list_only_none(self):
        clause, values = where_parser({"name": {"IN": [None]}}, "?")
        self.assertEqual(clause, " where name IS NULL")
        self.assertEqual(values, [])

    def test_nin_with_none_bind(self):
        clause, values = where_parser({"$nin": {"name": ["alice", None]}}, "?")
        self.assertEqual(clause, " where (name NOT IN (?) AND name IS NOT NULL)")
        self.assertEqual(values, ["alice"])

    def test_nin_with_none_inline(self):
        clause, values = where_parser({"$nin": {"name": ["alice", None]}}, "?", inline=True)
        self.assertEqual(clause, " where (name NOT IN ('alice') AND name IS NOT NULL)")
        self.assertEqual(values, [])

    def test_nin_only_none(self):
        clause, values = where_parser({"$nin": {"name": [None]}}, "?")
        self.assertEqual(clause, " where name IS NOT NULL")
        self.assertEqual(values, [])

    def test_build_where_normalizes_keyword(self):
        clause, values = build_where({"name": {"!=": None}}, "?")
        self.assertEqual(clause, " WHERE name IS NOT NULL")
        self.assertEqual(values, [])


class NullEndToEndTests(unittest.TestCase):
    """Verify the generated SQL actually returns the right rows on SQLite."""

    def setUp(self):
        self.db = _make_db()

    def _ids(self, where):
        rows = self.db.select("t", ["id"], where, options={"order_by": ["id"]})["data"]
        return [r["id"] for r in rows]

    def test_is_null(self):
        self.assertEqual(self._ids({"name": None}), [2])

    def test_is_not_null_via_neq(self):
        self.assertEqual(self._ids({"name": {"!=": None}}), [1, 3])

    def test_is_not_null_comprehensive(self):
        self.assertEqual(
            self._ids({"name": {"operator": "<>", "value": None, "placeholder": "Y"}}),
            [1, 3])

    def test_in_with_none_matches_value_or_null(self):
        # alice OR null -> rows 1 and 2
        self.assertEqual(self._ids({"name": {"IN": ["alice", None]}}), [1, 2])

    def test_not_in_with_none_excludes_null_rows(self):
        # NOT alice AND NOT NULL -> only bob (row 3); the NULL row is excluded,
        # which the naive `NOT IN (?, NULL)` form would wrongly drop entirely.
        self.assertEqual(self._ids({"$nin": {"name": ["alice", None]}}), [3])

    def test_delete_is_not_null(self):
        self.db.delete("t", {"name": {"!=": None}})
        self.assertEqual([r["id"] for r in self.db.query("SELECT id FROM t")["data"]], [2])

    def test_logical_or_with_null_branches(self):
        # $or composing an IS NULL and an equality
        ids = self._ids({"$or": [{"name": None}, {"name": "bob"}]})
        self.assertEqual(ids, [2, 3])


class InOperatorParserTests(unittest.TestCase):
    """All the ways IN / NOT IN can be expressed produce valid, bound SQL."""

    def test_regular_in(self):
        clause, values = where_parser({"id": {"IN": [1, 2, 3]}}, "?")
        self.assertEqual(clause, " where id IN (?, ?, ?)")
        self.assertEqual(values, [1, 2, 3])

    def test_regular_in_inline(self):
        clause, values = where_parser({"id": {"IN": [1, 2, 3]}}, "?", inline=True)
        self.assertEqual(clause, " where id IN (1, 2, 3)")
        self.assertEqual(values, [])

    def test_regular_not_in_bind(self):
        clause, values = where_parser({"id": {"NOT IN": [1, 2, 3]}}, "?")
        self.assertEqual(clause, " where id NOT IN (?, ?, ?)")
        self.assertEqual(values, [1, 2, 3])

    def test_regular_not_in_inline(self):
        clause, values = where_parser({"id": {"NOT IN": [1, 2, 3]}}, "?", inline=True)
        self.assertEqual(clause, " where id NOT IN (1, 2, 3)")
        self.assertEqual(values, [])

    def test_regular_not_in_whitespace_and_case(self):
        clause, values = where_parser({"id": {"not  in": [1, 2]}}, "?")
        self.assertEqual(clause, " where id not  in (?, ?)")
        self.assertEqual(values, [1, 2])

    def test_comprehensive_in_bind(self):
        clause, values = where_parser(
            {"id": {"operator": "IN", "value": [1, 2, 3], "placeholder": "Y"}}, "?")
        self.assertEqual(clause, " where id IN (?, ?, ?)")
        self.assertEqual(values, [1, 2, 3])

    def test_comprehensive_in_inline(self):
        clause, values = where_parser(
            {"id": {"operator": "IN", "value": [1, 2, 3], "placeholder": "N"}}, "?")
        self.assertEqual(clause, " where id IN (1, 2, 3)")
        self.assertEqual(values, [])

    def test_comprehensive_not_in_bind(self):
        clause, values = where_parser(
            {"id": {"operator": "NOT IN", "value": [1, 2], "placeholder": "Y"}}, "?")
        self.assertEqual(clause, " where id NOT IN (?, ?)")
        self.assertEqual(values, [1, 2])

    def test_dollar_in(self):
        clause, values = where_parser({"$in": {"id": [1, 2, 3]}}, "?")
        self.assertEqual(clause, " where id IN (?, ?, ?)")
        self.assertEqual(values, [1, 2, 3])

    def test_dollar_in_inline(self):
        clause, values = where_parser({"$in": {"id": [1, 2, 3]}}, "?", inline=True)
        self.assertEqual(clause, " where id IN (1, 2, 3)")
        self.assertEqual(values, [])

    def test_dollar_in_requires_list(self):
        with self.assertRaises(ValueError):
            where_parser({"$in": {"id": 5}}, "?")

    def test_empty_in_is_false(self):
        clause, _ = where_parser({"id": {"IN": []}}, "?")
        self.assertEqual(clause, " where 1=0")

    def test_empty_not_in_is_true(self):
        clause, _ = where_parser({"id": {"NOT IN": []}}, "?")
        self.assertEqual(clause, " where 1=1")


class InOperatorEndToEndTests(unittest.TestCase):
    """IN / NOT IN forms return the right rows on SQLite (table: 1=alice,
    2=NULL, 3=bob)."""

    def setUp(self):
        self.db = _make_db()

    def _ids(self, where):
        rows = self.db.select("t", ["id"], where, options={"order_by": ["id"]})["data"]
        return [r["id"] for r in rows]

    def test_regular_in(self):
        self.assertEqual(self._ids({"id": {"IN": [1, 3]}}), [1, 3])

    def test_regular_not_in(self):
        # rows whose id is not 1 -> 2 and 3 (NULL name is irrelevant; id is set)
        self.assertEqual(self._ids({"id": {"NOT IN": [1]}}), [2, 3])

    def test_comprehensive_in(self):
        self.assertEqual(
            self._ids({"id": {"operator": "IN", "value": [2], "placeholder": "Y"}}), [2])

    def test_dollar_in(self):
        self.assertEqual(self._ids({"$in": {"id": [1, 2]}}), [1, 2])

    def test_in_on_text_column(self):
        self.assertEqual(self._ids({"name": {"IN": ["alice", "bob"]}}), [1, 3])

    def test_not_in_on_text_column_inline(self):
        # inline path must quote string members
        rows = self.db.select(
            "t", ["id"], {"name": {"NOT IN": ["alice"]}},
            options={"order_by": ["id"], "dynamic_param": "N"})["data"]
        # NULL-name row excluded by NOT IN semantics; only bob remains
        self.assertEqual([r["id"] for r in rows], [3])


if __name__ == '__main__':
    unittest.main(verbosity=2)
