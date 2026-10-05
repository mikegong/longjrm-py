"""After checkout the DBUtils pool never replaces a connection or re-runs a statement.

No server needed: SQLite in a temp file, through the real DBUtils backend.
pool_contract_test.py holds the same contract, and the autocommit rule, against
every configured database on both backends.

Before 0.4.0 the pool told DBUtils that every database error was a broken
connection. On an ordinary error inside a transaction DBUtils then swapped the
connection, and the caller carried on in autocommit on a new one with the
transaction gone.
"""

import sqlite3
from contextlib import contextmanager

import pytest

from longjrm.config.config import DatabaseConfig, JrmConfig
from longjrm.config.runtime import using_config
from longjrm.connection.connectors import unwrap_connection
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db


@contextmanager
def _sqlite_pool(tmp_path, dbutils_opts=None):
    db_cfg = DatabaseConfig(type="sqlite", database=str(tmp_path / "contract.db"))
    with using_config(JrmConfig(_databases={"contract": db_cfg})):
        p = Pool.from_config(db_cfg, PoolBackend.DBUTILS, dbutils_opts=dbutils_opts)
        try:
            with p.client() as c:
                get_db(c).execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            yield p
        finally:
            p.dispose()


@pytest.fixture
def pool(tmp_path):
    with _sqlite_pool(tmp_path) as p:
        yield p


def _rows(pool):
    with pool.client() as c:
        return [r["id"] for r in get_db(c).query("SELECT id FROM t ORDER BY id")["data"]]


def _error_keeps_the_connection_and_the_transaction(pool):
    with pool.client() as c:
        db = get_db(c)
        before = id(unwrap_connection(db.conn))
        db.set_autocommit(False)
        db.execute("INSERT INTO t (id) VALUES (1)")
        with pytest.raises(Exception):
            db.execute("INSERT INTO t (id) VALUES (1)")
        assert id(unwrap_connection(db.conn)) == before
        db.execute("INSERT INTO t (id) VALUES (2)")
        db.commit()
    assert _rows(pool) == [1, 2]


def test_an_error_in_a_manual_transaction_keeps_the_connection_and_the_transaction(pool):
    _error_keeps_the_connection_and_the_transaction(pool)


def test_the_caller_cannot_switch_failover_back_on(tmp_path):
    # These are the classes the pool itself used to pass. Handed in by a caller
    # they must change nothing.
    old_failures = (sqlite3.InterfaceError, sqlite3.DatabaseError)
    with _sqlite_pool(tmp_path, dbutils_opts={"failures": old_failures}) as p:
        _error_keeps_the_connection_and_the_transaction(p)


def test_a_rollback_after_a_caught_error_undoes_everything(pool):
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        db.execute("INSERT INTO t (id) VALUES (1)")
        with pytest.raises(Exception):
            db.execute("INSERT INTO missing_table (id) VALUES (9)")
        db.execute("INSERT INTO t (id) VALUES (2)")
        db.rollback()
    assert _rows(pool) == []


def test_an_error_in_a_transaction_block_rolls_the_block_back(pool):
    with pytest.raises(Exception):
        with pool.transaction() as tx:
            db = get_db(tx.client)
            db.execute("INSERT INTO t (id) VALUES (1)")
            db.execute("INSERT INTO t (id) VALUES (1)")
    assert _rows(pool) == []


def test_checkout_puts_autocommit_back_on(pool):
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        db.execute("INSERT INTO t (id) VALUES (1)")
    with pool.client() as c:
        assert get_db(c).get_autocommit() is True
    assert _rows(pool) == []
