"""Tests for time-zone-aware datetime values on the write path.

An aware datetime denotes an *instant*. Serializing it without its UTC offset
does not fail anywhere: the server reads the wall-clock digits in its own
session time zone and stores a different instant, silently. These tests pin the
offset to the serialized value on every path that turns a datetime into SQL.

Self-contained: runs against in-memory SQLite, no test_config required.

    python -m unittest longjrm.tests.datetime_tz_test -v
"""
import datetime
import sqlite3
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig, DatabaseConfig
from longjrm.config.runtime import configure
from longjrm.database.sqlite import SqliteDb
from longjrm.database.spark import SparkDb
from longjrm.utils.data import datalist_to_dataseq, serialize_datetime

UTC = datetime.timezone.utc
PLUS8 = datetime.timezone(datetime.timedelta(hours=8))

AWARE_UTC = datetime.datetime(2026, 8, 12, 11, 26, 53, 525447, tzinfo=UTC)
AWARE_PLUS8 = datetime.datetime(2026, 8, 12, 19, 26, 53, 525447, tzinfo=PLUS8)
NAIVE = datetime.datetime(2026, 8, 12, 11, 26, 53, 525447)


def _make_db():
    configure(JrmConfig(_databases={"mem": DatabaseConfig(type="sqlite", database=":memory:")}))
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    return SqliteDb({"conn": conn, "database_type": "sqlite", "database_name": ":memory:"})


class SerializeDatetimeTests(unittest.TestCase):
    def test_aware_keeps_offset(self):
        self.assertEqual(serialize_datetime(AWARE_UTC), "2026-08-12 11:26:53.525447+00:00")
        self.assertEqual(serialize_datetime(AWARE_PLUS8), "2026-08-12 19:26:53.525447+08:00")

    def test_aware_values_of_the_same_instant_serialize_to_the_same_instant(self):
        """+08:00 19:26 and UTC 11:26 are one instant; both must survive as such."""
        self.assertEqual(
            datetime.datetime.fromisoformat(serialize_datetime(AWARE_UTC)),
            datetime.datetime.fromisoformat(serialize_datetime(AWARE_PLUS8)),
        )

    def test_naive_keeps_the_historical_format(self):
        """Naive values carry no offset to preserve -- nothing about them changes."""
        self.assertEqual(serialize_datetime(NAIVE), "2026-08-12 11:26:53.525447")

    def test_zero_microseconds_still_serializes(self):
        aware = datetime.datetime(2026, 8, 12, 11, 26, 53, tzinfo=UTC)
        self.assertEqual(serialize_datetime(aware), "2026-08-12 11:26:53+00:00")
        naive = datetime.datetime(2026, 8, 12, 11, 26, 53)
        self.assertEqual(serialize_datetime(naive), "2026-08-12 11:26:53.000000")


class ProcessValueTests(unittest.TestCase):
    """The single-row insert/update path (Db._process_value)."""

    def setUp(self):
        self.db = _make_db()

    def test_aware_datetime_keeps_offset(self):
        self.assertEqual(self.db._process_value(AWARE_UTC), "2026-08-12 11:26:53.525447+00:00")
        self.assertEqual(self.db._process_value(AWARE_PLUS8), "2026-08-12 19:26:53.525447+08:00")

    def test_naive_datetime_unchanged(self):
        self.assertEqual(self.db._process_value(NAIVE), "2026-08-12 11:26:53.525447")

    def test_plain_date_unchanged(self):
        """date is a datetime superclass -- it has no offset and must not gain one."""
        self.assertEqual(self.db._process_value(datetime.date(2026, 8, 12)), "2026-08-12")


class BulkPathTests(unittest.TestCase):
    """The bulk insert path (data_utils.datalist_to_dataseq), both variants."""

    def test_builtin_conversion_keeps_offset(self):
        batches = list(datalist_to_dataseq([{"ts": AWARE_PLUS8}]))
        self.assertEqual(batches[0][0][0], "2026-08-12 19:26:53.525447+08:00")

    def test_process_value_fn_keeps_offset(self):
        db = _make_db()
        batches = list(datalist_to_dataseq([{"ts": AWARE_PLUS8}],
                                           process_value_fn=db._process_value))
        self.assertEqual(batches[0][0][0], "2026-08-12 19:26:53.525447+08:00")

    def test_both_variants_agree(self):
        """Bulk and single-row must serialize identically on the same backend."""
        db = _make_db()
        builtin = list(datalist_to_dataseq([{"ts": AWARE_UTC}]))[0][0][0]
        per_row = db._process_value(AWARE_UTC)
        self.assertEqual(builtin, per_row)


class SqliteRoundTripTests(unittest.TestCase):
    """End to end through insert(): the offset must reach storage."""

    def setUp(self):
        self.db = _make_db()
        self.db.conn.execute("CREATE TABLE ev (id INTEGER, ts TEXT)")

    def test_single_insert_stores_the_offset(self):
        self.db.insert("ev", {"id": 1, "ts": AWARE_PLUS8})
        stored = self.db.conn.execute("SELECT ts FROM ev WHERE id = 1").fetchone()[0]
        self.assertEqual(stored, "2026-08-12 19:26:53.525447+08:00")
        self.assertEqual(datetime.datetime.fromisoformat(stored), AWARE_UTC)

    def test_bulk_insert_stores_the_offset(self):
        self.db.insert("ev", [{"id": 2, "ts": AWARE_UTC}, {"id": 3, "ts": AWARE_PLUS8}])
        rows = dict(self.db.conn.execute("SELECT id, ts FROM ev").fetchall())
        self.assertEqual(datetime.datetime.fromisoformat(rows[2]), AWARE_UTC)
        self.assertEqual(datetime.datetime.fromisoformat(rows[3]), AWARE_UTC)

    def test_update_stores_the_offset(self):
        self.db.insert("ev", {"id": 4, "ts": NAIVE})
        self.db.update("ev", {"ts": AWARE_PLUS8}, where={"id": 4})
        stored = self.db.conn.execute("SELECT ts FROM ev WHERE id = 4").fetchone()[0]
        self.assertEqual(datetime.datetime.fromisoformat(stored), AWARE_UTC)


class SparkLiteralTests(unittest.TestCase):
    """Spark builds SQL literals instead of binding parameters."""

    def _prepare(self, value):
        # Only _prepare_sql is exercised; no SparkSession is touched.
        db = object.__new__(SparkDb)
        sql, _ = db._prepare_sql("INSERT INTO ev VALUES (%s)", [value])
        return sql

    def test_aware_literal_keeps_offset(self):
        self.assertIn("TIMESTAMP '2026-08-12 19:26:53.525447+08:00'",
                      self._prepare(AWARE_PLUS8))

    def test_naive_literal_has_no_offset(self):
        self.assertIn("TIMESTAMP '2026-08-12 11:26:53.525447'", self._prepare(NAIVE))


if __name__ == '__main__':
    unittest.main(verbosity=2)
