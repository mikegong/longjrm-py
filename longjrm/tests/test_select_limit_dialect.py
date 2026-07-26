"""DB-free unit tests for the per-dialect LIMIT rendering in _construct_select_sql.

The base implementation emits ``limit N``; backends whose SQL dialect does not
accept it override the method. A falsy limit (0/None) must render no clause at
all -- that is how callers ask for an unbounded read.
"""

import pytest


def _sql(cls, limit):
    # _construct_select_sql only formats strings, so call it unbound: no driver
    # connection is needed to assert the dialect's clause.
    return cls._construct_select_sql(None, "t", "a, b", " where a = ?", " order by a", limit)


def test_base_uses_limit():
    from longjrm.database.db import Db
    assert _sql(Db, 10).endswith(" limit 10")
    assert "limit" not in _sql(Db, 0)
    assert "limit" not in _sql(Db, None)


def test_db2_uses_fetch_first():
    pytest.importorskip("ibm_db")
    from longjrm.database.db2 import Db2Db
    assert _sql(Db2Db, 10).endswith(" FETCH FIRST 10 ROWS ONLY")
    assert "FETCH FIRST" not in _sql(Db2Db, 0)
    assert "FETCH FIRST" not in _sql(Db2Db, None)
