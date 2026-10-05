"""
What the pool guarantees, checked against live databases on both backends.

1. Autocommit. Every connection is handed out with autocommit ON. What the caller
   does with it afterwards -- including turning it off -- is the caller's
   business; the pool puts it back to ON at the next checkout, and rolls back
   whatever was left uncommitted.

2. No failover after checkout. Once handed out, a connection is never replaced
   and a statement is never re-run. A database error reaches the caller as that
   error, and the transaction it happened in stays intact. (A dead pooled
   connection is replaced AT checkout; liveness_test.py covers that.)

Run from the repository root:

    python -m longjrm.tests.pool_contract_test
    python -m longjrm.tests.pool_contract_test --db=db2

The checks provoke database errors on purpose, so library logging below
CRITICAL is switched off to keep the output readable.
"""

import logging
import os
import sys

# Add the project root to Python path for development testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from longjrm.config.config import JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.connectors import get_connector_class, unwrap_connection
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db
from longjrm.tests import test_utils
from longjrm.tests.liveness_test import BACKEND_PID_SQL, KILL_SQL

logging.disable(logging.ERROR)

TABLE = "test_pool_contract"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ins(db, i):
    return db.execute(f"INSERT INTO {TABLE} (id) VALUES ({i})")


def _rows(pool):
    with pool.client() as c:
        return [r["id"] for r in get_db(c).query(f"SELECT id FROM {TABLE} ORDER BY id")["data"]]


def _clear(pool):
    with pool.client() as c:
        get_db(c).execute(f"DELETE FROM {TABLE}")


def _autocommit_is_on(db):
    """Judge by behavior, not by a flag: with autocommit on, an insert survives a rollback."""
    _ins(db, 999)
    db.rollback()
    n = db.query(f"SELECT COUNT(*) AS cnt FROM {TABLE} WHERE id = 999")["data"][0]["cnt"]
    if n:
        db.execute(f"DELETE FROM {TABLE} WHERE id = 999")
    else:
        db.rollback()
    return bool(n)


def _next_checkout_is_on(pool):
    with pool.client() as c:
        return _autocommit_is_on(get_db(c))


def _raises(fn):
    """The exception fn raises, or None."""
    try:
        fn()
    except Exception as e:
        return e
    return None


def _stream(ids):
    return iter([{"id": i} for i in ids])


def _aborts_on_error(db_type):
    """Postgres aborts the whole transaction when a statement fails; the others
    roll back only the failed statement and let the transaction carry on."""
    return db_type in ('postgres', 'postgresql')


# ---------------------------------------------------------------------------
# 1. Autocommit
# ---------------------------------------------------------------------------

def check_fresh_checkout_is_autocommit(pool, db_type):
    assert _next_checkout_is_on(pool), "a fresh checkout must have autocommit on"


def check_returned_with_autocommit_off_and_uncommitted_work(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        _ins(db, 1)
    assert _next_checkout_is_on(pool), "next checkout must have autocommit on"
    assert _rows(pool) == [], "the uncommitted row must be rolled back on return"


def check_committed_work_is_kept_and_the_rest_rolled_back(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        _ins(db, 1)
        db.commit()
        _ins(db, 2)
    assert _next_checkout_is_on(pool)
    assert _rows(pool) == [1], f"row 1 was committed, row 2 was not: {_rows(pool)}"


def check_turned_off_on_the_raw_driver_connection(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        get_connector_class(db.database_type).set_dbapi_autocommit(unwrap_connection(db.conn), False)
        _ins(db, 1)
        db.commit()
        _ins(db, 2)
    assert _next_checkout_is_on(pool)
    assert _rows(pool) == [1], f"row 1 was committed, row 2 was not: {_rows(pool)}"


def check_transaction_block_commits_and_restores(pool, db_type):
    with pool.transaction() as tx:
        db = get_db(tx.client)
        assert not _autocommit_is_on(db), "autocommit must be off inside pool.transaction()"
        _ins(db, 1)
        db.commit()
        _ins(db, 2)
    assert _next_checkout_is_on(pool)
    assert _rows(pool) == [1, 2]


def check_failed_transaction_block_rolls_back_and_restores(pool, db_type):
    def block():
        with pool.transaction() as tx:
            _ins(get_db(tx.client), 1)
            raise RuntimeError("caller's code fails")
    assert isinstance(_raises(block), RuntimeError)
    assert _next_checkout_is_on(pool)
    assert _rows(pool) == []


def check_several_connections_turned_off_at_once(pool, db_type):
    with pool.client() as c1, pool.client() as c2, pool.client() as c3:
        for c in (c1, c2, c3):
            get_db(c).set_autocommit(False)
    with pool.client() as c1, pool.client() as c2, pool.client() as c3:
        states = [_autocommit_is_on(get_db(c)) for c in (c1, c2, c3)]
    assert states == [True, True, True], states


def check_autocommit_reads_back_what_was_set(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        assert db.get_autocommit() is True
        db.set_autocommit(False)
        assert db.get_autocommit() is False
        db.set_autocommit(True)
        assert db.get_autocommit() is True


def check_managed_stream_hands_the_connection_back_as_found(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        r = db.stream_insert(_stream([10, 11, 12]), TABLE, commit_count=2)
        assert r["status"] == 0, r
        assert _autocommit_is_on(db), "found on, must be on afterwards"
    assert _rows(pool) == [10, 11, 12]
    _clear(pool)
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        r = db.stream_insert(_stream([10, 11, 12]), TABLE, commit_count=2)
        assert r["status"] == 0, r
        assert db.get_autocommit() is False, "found off, must still be off afterwards"
    assert _rows(pool) == [10, 11, 12]


def check_transactional_script_hands_the_connection_back_as_found(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        r = db.execute_script(
            f"INSERT INTO {TABLE} (id) VALUES (1); INSERT INTO {TABLE} (id) VALUES (2)", transaction=True)
        assert r["status"] == 0, r
        assert _autocommit_is_on(db)
    assert _rows(pool) == [1, 2]


# ---------------------------------------------------------------------------
# 2. No failover after checkout
# ---------------------------------------------------------------------------

def check_error_in_manual_transaction_raises_and_keeps_the_connection(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        before = id(unwrap_connection(db.conn))
        db.set_autocommit(False)
        _ins(db, 1)
        _ins(db, 2)
        assert _raises(lambda: _ins(db, 1)) is not None, "a duplicate key must raise"
        assert id(unwrap_connection(db.conn)) == before, "the connection must not be replaced"
        db.rollback()
    assert _rows(pool) == [], f"everything was rolled back: {_rows(pool)}"


def check_error_in_transaction_block_rolls_everything_back(pool, db_type):
    def block():
        with pool.transaction() as tx:
            db = get_db(tx.client)
            _ins(db, 1)
            _ins(db, 2)
            _ins(db, 1)
    assert _raises(block) is not None, "a duplicate key must raise out of the block"
    assert _rows(pool) == [], f"the block must roll back as a whole: {_rows(pool)}"


def check_caller_catches_an_error_then_rolls_back(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        before = id(unwrap_connection(db.conn))
        db.set_autocommit(False)
        _ins(db, 1)
        assert _raises(lambda: db.execute(f"INSERT INTO {TABLE}_missing (id) VALUES (9)")) is not None
        carried_on = _raises(lambda: _ins(db, 2)) is None
        assert carried_on != _aborts_on_error(db_type), "statement after the error behaved unexpectedly"
        assert id(unwrap_connection(db.conn)) == before, "the connection must not be replaced"
        db.rollback()
    assert _rows(pool) == [], f"the rollback must undo everything: {_rows(pool)}"


def check_caller_catches_an_error_then_commits(pool, db_type):
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        _ins(db, 1)
        assert _raises(lambda: _ins(db, 1)) is not None
        _raises(lambda: _ins(db, 2))
        db.commit()
    expected = [] if _aborts_on_error(db_type) else [1, 2]
    assert _rows(pool) == expected, f"expected {expected}, found {_rows(pool)}"


def check_turning_autocommit_off_again_keeps_pending_work_pending(pool, db_type):
    if _aborts_on_error(db_type):
        return "skipped: the driver refuses to change autocommit inside a transaction"
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        _ins(db, 1)
        db.set_autocommit(False)
        db.rollback()
    assert _rows(pool) == [], f"the pending row must not have been committed: {_rows(pool)}"


def check_managed_stream_with_a_duplicate_in_the_batch(pool, db_type):
    with pool.client() as c:
        r = get_db(c).stream_insert(_stream([1, 2, 3, 2]), TABLE, commit_count=10)
    assert r["status"] == -1, r
    assert _rows(pool) == [], f"the batch must roll back as a whole: {_rows(pool)}"


def check_managed_stream_fails_after_a_committed_batch(pool, db_type):
    with pool.client() as c:
        r = get_db(c).stream_insert(_stream([1, 2, 3, 1]), TABLE, commit_count=2)
    assert r["status"] == -1, r
    assert _rows(pool) == [1, 2], f"only the committed batch remains: {_rows(pool)}"


def check_tolerant_stream_keeps_the_good_rows(pool, db_type):
    with pool.client() as c:
        r = get_db(c).stream_insert(_stream([1, 2, 2, 3]), TABLE, commit_count=10, max_error_count=5)
    assert r["status"] == 0 and r["reject_count"] == 1, r
    assert _rows(pool) == [1, 2, 3], _rows(pool)


def check_tolerant_stream_inside_a_transaction_block(pool, db_type):
    with pool.transaction() as tx:
        r = get_db(tx.client).stream_insert(_stream([1, 2, 2, 3]), TABLE, commit_count=0, max_error_count=5)
    assert r["status"] == 0 and r["reject_count"] == 1, r
    assert _rows(pool) == [1, 2, 3], _rows(pool)


def _kill(db, db_type, killer):
    pid = db.query(BACKEND_PID_SQL[db_type], [])["data"][0]["pid"]
    with killer.client() as k:
        get_db(k).execute(KILL_SQL[db_type], [pid])


def check_session_killed_while_checked_out(pool, db_type, killer):
    if db_type not in KILL_SQL:
        return f"skipped: no way to kill a session from outside on {db_type}"
    with pool.client() as c:
        db = get_db(c)
        _kill(db, db_type, killer)
        assert _raises(lambda: _ins(db, 1)) is not None, "the statement must raise, not be re-run"
    assert _rows(pool) == [], f"nothing may have been applied: {_rows(pool)}"


def check_session_killed_inside_a_manual_transaction(pool, db_type, killer):
    if db_type not in KILL_SQL:
        return f"skipped: no way to kill a session from outside on {db_type}"
    with pool.client() as c:
        db = get_db(c)
        db.set_autocommit(False)
        _ins(db, 1)
        _kill(db, db_type, killer)
        assert _raises(lambda: _ins(db, 2)) is not None, "the statement must raise, not be re-run"
        _raises(db.commit)
    assert _rows(pool) == [], f"no part of the lost transaction may be committed: {_rows(pool)}"


AUTOCOMMIT_CHECKS = [
    check_fresh_checkout_is_autocommit,
    check_returned_with_autocommit_off_and_uncommitted_work,
    check_committed_work_is_kept_and_the_rest_rolled_back,
    check_turned_off_on_the_raw_driver_connection,
    check_transaction_block_commits_and_restores,
    check_failed_transaction_block_rolls_back_and_restores,
    check_several_connections_turned_off_at_once,
    check_autocommit_reads_back_what_was_set,
    check_managed_stream_hands_the_connection_back_as_found,
    check_transactional_script_hands_the_connection_back_as_found,
]

NO_FAILOVER_CHECKS = [
    check_error_in_manual_transaction_raises_and_keeps_the_connection,
    check_error_in_transaction_block_rolls_everything_back,
    check_caller_catches_an_error_then_rolls_back,
    check_caller_catches_an_error_then_commits,
    check_turning_autocommit_off_again_keeps_pending_work_pending,
    check_managed_stream_with_a_duplicate_in_the_batch,
    check_managed_stream_fails_after_a_committed_batch,
    check_tolerant_stream_keeps_the_good_rows,
    check_tolerant_stream_inside_a_transaction_block,
]

KILL_CHECKS = [
    check_session_killed_while_checked_out,
    check_session_killed_inside_a_manual_transaction,
]


def run_for(db_key, backend, cfg):
    """Run every check for one database and backend. Returns (passed, failed, skipped)."""
    db_cfg = cfg.require(db_key)
    db_type = (db_cfg.type or '').lower()
    print(f"\n>>> {db_key} using {backend.value} backend")

    pool = Pool.from_config(db_cfg, backend)
    killer = Pool.from_config(db_cfg, backend)
    passed = failed = skipped = 0
    try:
        with pool.client() as c:
            db = get_db(c)
            test_utils.drop_table_silently(db, TABLE)
            db.execute(f"CREATE TABLE {TABLE} (id INT NOT NULL PRIMARY KEY)")

        checks = ([(f, ()) for f in AUTOCOMMIT_CHECKS + NO_FAILOVER_CHECKS]
                  + [(f, (killer,)) for f in KILL_CHECKS])
        for check, extra in checks:
            name = check.__name__[len("check_"):].replace("_", " ")
            try:
                note = check(pool, db_type, *extra)
                if note:
                    print(f"  skip    {name}  ({note})")
                    skipped += 1
                else:
                    print(f"  ok      {name}")
                    passed += 1
            except Exception as e:
                print(f"  FAILED  {name}: {type(e).__name__}: {e}")
                failed += 1
            try:
                _clear(pool)
            except Exception as e:
                print(f"  FAILED  could not clear {TABLE} after '{name}': {e}")
                failed += 1
    finally:
        try:
            with pool.client() as c:
                test_utils.drop_table_silently(get_db(c), TABLE)
        finally:
            pool.dispose()
            killer.dispose()
    return passed, failed, skipped


def main():
    print("=== JRM Pool Contract Test Suite ===")
    cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
    configure(cfg)

    total_passed = total_failed = total_skipped = combinations = 0
    for db_key, backend in test_utils.get_active_test_configs(cfg):
        if cfg.require(db_key).type == 'spark':
            print(f"Skipping {db_key} (Spark has no transactions to pool)")
            continue
        try:
            passed, failed, skipped = run_for(db_key, backend, cfg)
        except Exception as e:
            print(f"  FAILED  {db_key} ({backend.value}) could not run: {type(e).__name__}: {e}")
            passed, failed, skipped = 0, 1, 0
        total_passed += passed
        total_failed += failed
        total_skipped += skipped
        combinations += 1

    if combinations == 0:
        print("ERROR: No database configurations found")
        return 1
    print(f"\n=== Pool Contract Test Suite Complete: {total_passed} passed, {total_failed} failed, "
          f"{total_skipped} skipped across {combinations} database/backend combinations ===")
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
