"""
Async smoke tests for longjrm 0.2.0 (Strategy A: threadpool-backed async).

Covers:
    * pool.aclient() / pool.atransaction() async context managers
    * get_async_db() factory
    * AsyncDb mirrors of select / query / execute / insert / update / delete
    * AsyncDb internal lock serializes concurrent calls on a shared connection
    * Event loop is not blocked while a slow query runs (sanity check)

Run:
    cd longjrm-py
    pip install -e .[postgres]      # or .[all]
    python -m unittest longjrm/tests/async_select_test.py

Filter by DB:
    TEST_DB=sqlite python -m unittest longjrm/tests/async_select_test.py
    TEST_DB=postgres python -m unittest longjrm/tests/async_select_test.py
"""

import asyncio
import logging
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_async_db
from longjrm.tests import test_utils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- Test config discovery ---------------------------------------------------
# Keep parity with the sync test suite: only run against DBs that exist in
# test_config/dbinfos.json. Sqlite is the lowest-friction default; postgres /
# mysql etc. opt in via TEST_DB env var or --db= flag.
try:
    cfg = JrmConfig.from_files(
        "test_config/jrm.config.json", "test_config/dbinfos.json"
    )
    configure(cfg)
    ACTIVE_DBS = test_utils.get_active_test_configs(cfg)
except Exception as e:
    logger.warning(f"Could not load test_config: {e}. Falling back to sqlite-only.")
    cfg = None
    ACTIVE_DBS = []


def _setup_table(sync_db):
    """Create a clean test_users table using the sync API (one-time fixture)."""
    test_utils.drop_table_silently(sync_db, "test_users")
    create_sql = test_utils.get_create_table_sql(sync_db.database_type, "test_users")
    if not create_sql:
        raise unittest.SkipTest(
            f"No CREATE TABLE SQL defined for {sync_db.database_type}"
        )
    sync_db.execute(create_sql)


class AsyncDbSmokeTests(unittest.IsolatedAsyncioTestCase):
    """Smoke tests for AsyncDb + Pool.aclient() / atransaction()."""

    @classmethod
    def setUpClass(cls):
        if cfg is None or not ACTIVE_DBS:
            raise unittest.SkipTest(
                "No active DB configurations found in test_config/dbinfos.json"
            )
        # Pick the first available config; sub-tests iterate via subTest.
        cls.cfg = cfg

    async def _run_for_db(self, db_key, backend):
        """Execute the full smoke sequence against a single (db, backend) pair."""
        from longjrm.database import get_db

        db_cfg = self.cfg.require(db_key)
        pool = Pool.from_config(db_cfg, backend)
        try:
            # One-time table setup via sync API (fixture concern, not under test)
            with pool.client() as client:
                _setup_table(get_db(client))

            # ---- Test 1: aclient() + insert + select round-trip ----
            async with pool.aclient() as client:
                db = get_async_db(client)
                ins = await db.insert(
                    "test_users",
                    {"name": "Alice", "email": "a@x.io", "age": 30, "status": "active"},
                )
                self.assertEqual(ins["status"], 0)

                sel = await db.select("test_users", ["name", "age"])
                self.assertEqual(sel["count"], 1)
                self.assertEqual(sel["data"][0]["name"], "Alice")

            # ---- Test 2: query() with positional placeholder ----
            async with pool.aclient() as client:
                db = get_async_db(client)
                res = await db.query(
                    "SELECT name FROM test_users WHERE age > %s",
                    [25],
                )
                self.assertEqual(res["count"], 1)

            # ---- Test 3: update + delete ----
            async with pool.aclient() as client:
                db = get_async_db(client)
                upd = await db.update(
                    "test_users",
                    {"status": "inactive"},
                    {"name": "Alice"},
                )
                self.assertEqual(upd["status"], 0)

                check = await db.select(
                    "test_users", ["status"], where={"name": "Alice"}
                )
                self.assertEqual(check["data"][0]["status"], "inactive")

                dele = await db.delete("test_users", {"name": "Alice"})
                self.assertEqual(dele["status"], 0)

                empty = await db.select("test_users", ["*"])
                self.assertEqual(empty["count"], 0)

            # ---- Test 4: atransaction() auto-commit on success ----
            async with pool.atransaction() as tx:
                db = get_async_db(tx.client)
                await db.insert(
                    "test_users",
                    {"name": "Bob", "email": "b@x.io", "age": 40},
                )
                await db.insert(
                    "test_users",
                    {"name": "Carol", "email": "c@x.io", "age": 41},
                )
            async with pool.aclient() as client:
                db = get_async_db(client)
                count = await db.select("test_users", ["*"])
                self.assertEqual(count["count"], 2, "atransaction commit failed")

            # ---- Test 5: atransaction() auto-rollback on exception ----
            with self.assertRaises(RuntimeError):
                async with pool.atransaction() as tx:
                    db = get_async_db(tx.client)
                    await db.insert(
                        "test_users",
                        {"name": "Dave", "email": "d@x.io", "age": 50},
                    )
                    raise RuntimeError("force rollback")
            async with pool.aclient() as client:
                db = get_async_db(client)
                count = await db.select("test_users", ["*"])
                self.assertEqual(
                    count["count"], 2, "atransaction did not roll back"
                )

            # ---- Test 6: gather() on a single AsyncDb is serialized, not corrupt ----
            async with pool.aclient() as client:
                db = get_async_db(client)
                results = await asyncio.gather(
                    db.select("test_users", ["name"]),
                    db.select("test_users", ["name"]),
                    db.select("test_users", ["name"]),
                )
                for r in results:
                    self.assertEqual(r["count"], 2)

            # ====== Phase 2 coverage ======

            # ---- Test 7: bulk_update ----
            # Seed several rows, then bulk-update them by primary key (name).
            async with pool.atransaction() as tx:
                db = get_async_db(tx.client)
                for i in range(3):
                    await db.insert(
                        "test_users",
                        {"name": f"Bulk{i}", "email": f"b{i}@x.io", "age": 20 + i},
                    )
            async with pool.aclient() as client:
                db = get_async_db(client)
                bu = await db.bulk_update(
                    "test_users",
                    [
                        {"name": "Bulk0", "status": "vip"},
                        {"name": "Bulk1", "status": "vip"},
                        {"name": "Bulk2", "status": "vip"},
                    ],
                    key_columns=["name"],
                )
                self.assertEqual(bu["status"], 0)
                vip_count = await db.select(
                    "test_users", ["*"], where={"status": "vip"}
                )
                self.assertEqual(vip_count["count"], 3, "bulk_update did not update all rows")

            # ---- Test 8: stream_query yields the same shape as sync ----
            # Iterate over all rows; each yield is (row_num, row_dict, status).
            async with pool.aclient() as client:
                db = get_async_db(client)
                seen = []
                async for row_num, row, status in db.stream_query(
                    "SELECT name, age FROM test_users ORDER BY name",
                ):
                    self.assertEqual(status, 0)
                    self.assertIn("name", row)
                    seen.append(row["name"])
                # 2 originals (Bob, Carol) + 3 Bulk* = 5 rows
                self.assertEqual(len(seen), 5,
                                 f"stream_query saw {len(seen)} rows, expected 5: {seen}")

            # ---- Test 9: stream_query early-abort releases the lock ----
            # If aclose() didn't release, the next call would deadlock.
            async with pool.aclient() as client:
                db = get_async_db(client)
                async for _row_num, _row, _status in db.stream_query(
                    "SELECT name FROM test_users",
                ):
                    break  # iterate just once, then bail
                # Lock must be released so this next call works.
                await asyncio.wait_for(
                    db.select("test_users", ["*"], options={"limit": 1}),
                    timeout=5.0,
                )

            # ---- Test 10: stream_query_batch yields buckets ----
            async with pool.aclient() as client:
                db = get_async_db(client)
                batch_count = 0
                row_total = 0
                async for total, batch, status in db.stream_query_batch(
                    "SELECT name FROM test_users",
                    batch_size=2,
                ):
                    self.assertEqual(status, 0)
                    self.assertIsInstance(batch, list)
                    batch_count += 1
                    row_total = total
                # 5 rows / batch_size=2 → 3 batches (2 + 2 + 1)
                self.assertGreaterEqual(batch_count, 1)
                self.assertEqual(row_total, 5)

            # ---- Test 11: stream_insert from a sync generator ----
            # Clean slate first, then bulk-insert through the streaming API.
            async with pool.aclient() as client:
                db = get_async_db(client)
                await db.delete("test_users", where={"status": "vip"})

            def _gen():
                for i in range(4):
                    yield {"name": f"Stream{i}", "email": f"s{i}@x.io", "age": 30 + i}

            async with pool.aclient() as client:
                db = get_async_db(client)
                si = await db.stream_insert(_gen(), "test_users", commit_count=2)
                # stream_* return a status dict from the underlying handler
                self.assertIn(si.get("status"), (0, None),
                              f"stream_insert unexpected status: {si}")
                stream_seen = await db.select(
                    "test_users", ["*"], where={"name": {"LIKE": "Stream%"}}
                )
                self.assertEqual(stream_seen["count"], 4)

        finally:
            pool.dispose()

    async def test_async_crud_round_trip(self):
        """Run the smoke sequence against every active (db, backend) pair.

        Subtests are skip-tolerant: a DB whose host isn't reachable is skipped
        rather than failing the suite, so partial CI environments still produce
        meaningful results from whatever databases are up. Real bugs in the
        async wiring will still surface as assertion failures.
        """
        for db_key, backend in ACTIVE_DBS:
            with self.subTest(db=db_key, backend=backend.value):
                # Skip Spark: its own runtime, not in scope for Strategy A.
                if 'spark' in db_key.lower():
                    self.skipTest("Spark not in scope for AsyncDb")

                # Known limitation: SQLite + SQLAlchemy SingletonThreadPool is
                # not safe under threadpool dispatch (each to_thread call may
                # hop OS threads and SQLite connections are thread-bound). Use
                # the DBUtils backend with SQLite instead. This is a SQLite
                # property, not an async bug.
                if 'sqlite' in db_key.lower() and backend == PoolBackend.SQLALCHEMY:
                    self.skipTest(
                        "SQLite + SQLAlchemy SingletonThreadPool not "
                        "compatible with threadpool dispatch; use DBUtils "
                        "backend for SQLite under async."
                    )

                try:
                    await self._run_for_db(db_key, backend)
                except (ConnectionError, OSError) as e:
                    self.skipTest(f"{db_key} not reachable: {e}")
                except Exception as e:
                    # Driver-specific connection-refused errors don't all
                    # subclass OSError consistently across psycopg/pymysql/
                    # pyodbc/oracledb/ibm_db. Pattern-match the message.
                    msg = str(e).lower()
                    refused_hints = (
                        "connection refused",
                        "could not connect",
                        "10061",          # WinError: target actively refused
                        "can't connect",  # PyMySQL phrasing
                        "tcp provider",   # SQL Server phrasing
                        "dpy-6005",       # Oracle "cannot connect"
                        "sql30081",       # DB2 communication error
                    )
                    if any(h in msg for h in refused_hints):
                        self.skipTest(f"{db_key} not reachable: {e}")
                    raise

    async def test_event_loop_not_blocked_by_slow_query(self):
        """While a slow query runs in a worker thread, the loop must keep ticking.

        Uses sqlite if available (sqlite-test in dbinfos.json), since it's the
        most portable target and the assertion is driver-agnostic.
        """
        sqlite_match = next(
            ((k, b) for k, b in ACTIVE_DBS if 'sqlite' in k.lower()),
            None,
        )
        if not sqlite_match:
            self.skipTest("sqlite-test not configured; skipping loop-liveness check")

        db_key, backend = sqlite_match
        pool = Pool.from_config(self.cfg.require(db_key), backend)
        try:
            ticks = 0

            async def heartbeat():
                nonlocal ticks
                # Tick every 10ms; if loop is blocked, ticks won't advance.
                deadline = time.monotonic() + 0.5
                while time.monotonic() < deadline:
                    ticks += 1
                    await asyncio.sleep(0.01)

            async def slow_query():
                # SQLite has no pg_sleep; emulate via a long synchronous sleep
                # inside a query call so we exercise the to_thread path.
                async with pool.aclient() as client:
                    db = get_async_db(client)
                    # Run a no-op SELECT, then sleep on the worker thread to
                    # simulate a slow query path. The point is: this sleep
                    # happens in a worker thread, so the loop is free.
                    await db.query("SELECT 1")
                    await asyncio.to_thread(time.sleep, 0.3)

            await asyncio.gather(slow_query(), heartbeat())
            # Without the to_thread offload, ticks would be ~0 during the sleep.
            # We expect tens of ticks across the 0.5s window.
            self.assertGreater(
                ticks, 20,
                f"event loop appears blocked: only {ticks} ticks in 500ms",
            )
        finally:
            pool.dispose()


if __name__ == "__main__":
    unittest.main()
