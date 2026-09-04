"""
Pool liveness test: does a DBUtils pool notice that a cached connection died?

Background. DBUtils checks a connection when it hands it back out, and the
check is a call to ping() on the connection object. ping() is a MySQLdb
interface -- psycopg, pyodbc, sqlite3 and ibm_db_dbi do not have one, and when
DBUtils hits AttributeError it reads that as "this driver cannot be pinged",
disables the check for good and says nothing. A connection that died while
idle in the pool is then handed out as healthy, and the caller only finds out
on their next statement.

BaseConnector.attach_liveness supplies the missing method. These tests prove
it is wired in, that it recovers a dead connection, and -- the part most worth
guarding -- that a bug in the probe cannot silently switch the check back off.

Run:
    python -m longjrm.tests.liveness_test            # offline checks only
    python -m longjrm.tests.liveness_test --db=postgres
"""
import logging
import time

from longjrm.config.config import DatabaseConfig, JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.connectors import get_connector_class
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db
from longjrm.tests import test_utils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# How the live half kills a connection out from under the pool, per database.
# Only databases that can execute this from a second session are covered.
KILL_SQL = {
    'postgres': "SELECT pg_terminate_backend(%s)",
    'mysql': "KILL %s",
}

BACKEND_PID_SQL = {
    'postgres': "SELECT pg_backend_pid() AS pid",
    'mysql': "SELECT CONNECTION_ID() AS pid",
}


# ---------------------------------------------------------------------------
# Offline checks: no database, no driver. These are the ones that must never
# be skipped, because they cover the failure mode that hides itself.
# ---------------------------------------------------------------------------

class _Attachable:
    """Stands in for a driver connection that accepts new attributes."""


class _Sealed:
    """Stands in for a C-extension connection type (pyodbc) that does not."""
    __slots__ = ()


def _connector(db_type='postgres'):
    # BaseConnector.__init__ reads the runtime config for connect_timeout, so
    # the offline checks need one even though they never open a connection.
    configure(JrmConfig(_databases={}))
    return get_connector_class(db_type)(
        DatabaseConfig(type=db_type, host='h', port=1, user='u',
                       password='p', database='d')
    )


def test_probe_bug_does_not_disable_the_check():
    """A broken probe must look "dead", never "unsupported".

    DBUtils treats AttributeError, IndexError, TypeError and ValueError from
    ping() as "this driver has no ping()" and turns the check off permanently.
    If our probe could leak one of those, a typo in it would silently restore
    the blindness this whole mechanism removes -- and leave no trace.
    """
    passed = True
    for boom in (AttributeError, IndexError, TypeError, ValueError):
        connector = _connector()
        type(connector).ping_dbapi = staticmethod(
            lambda _conn, exc=boom: (_ for _ in ()).throw(exc("probe bug"))
        )
        try:
            conn = _Attachable()
            connector.attach_liveness(conn)
            try:
                conn.ping(False)
                logger.error(f"✗ probe raising {boom.__name__} did not surface at all")
                passed = False
            except boom:
                logger.error(
                    f"✗ probe raising {boom.__name__} leaked it -- DBUtils would "
                    f"disable liveness checking permanently"
                )
                passed = False
            except Exception as exc:
                if isinstance(exc, (AttributeError, IndexError, TypeError, ValueError)):
                    logger.error(f"✗ probe bug surfaced as {type(exc).__name__}, which DBUtils ignores")
                    passed = False
        finally:
            del type(connector).ping_dbapi

    if passed:
        logger.info("✓ every probe failure surfaces as an error DBUtils acts on")
    return passed


def test_native_ping_is_left_alone():
    """A driver with its own ping() keeps it -- ours is less well informed."""
    class HasPing:
        def ping(self, reconnect=False):
            return "native"

    conn = HasPing()
    _connector().attach_liveness(conn)
    if conn.ping(False) != "native":
        logger.error("✗ overwrote the driver's own ping()")
        return False
    logger.info("✓ driver's own ping() left in place")
    return True


def test_unattachable_connection_is_not_silent():
    """pyodbc's connection type takes no attributes. That must be said out loud."""
    import longjrm.connection.connectors as connectors

    connectors._LIVENESS_UNSUPPORTED.discard('sqlserver')
    connector = _connector('sqlserver')
    with_warning = []
    original = connectors.logger.warning
    connectors.logger.warning = lambda msg, *a, **k: with_warning.append(msg)
    try:
        connector.attach_liveness(_Sealed())
    finally:
        connectors.logger.warning = original

    if not with_warning:
        logger.error("✗ a pool with no liveness check was created without a warning")
        return False
    logger.info(f"✓ warned: {with_warning[0][:80]}...")
    return True


def test_sqlite_needs_no_check():
    """A local file connection cannot die in the pool -- and must not warn."""
    import longjrm.connection.connectors as connectors

    connectors._LIVENESS_UNSUPPORTED.discard('sqlite')
    warnings = []
    original = connectors.logger.warning
    connectors.logger.warning = lambda msg, *a, **k: warnings.append(msg)
    try:
        _connector('sqlite').attach_liveness(_Sealed())
    finally:
        connectors.logger.warning = original

    if warnings:
        logger.error(f"✗ sqlite warned about a check it does not need: {warnings[0]}")
        return False
    logger.info("✓ sqlite: no check, no warning")
    return True


# ---------------------------------------------------------------------------
# Live check: kill a pooled connection, then use the pool again.
# ---------------------------------------------------------------------------

def run_liveness_test_for_db(db_key, cfg):
    """Kill a cached connection from a second session, then write through the pool."""
    db_config = cfg.require(db_key)
    db_type = (db_config.type or '').lower()

    if db_type not in KILL_SQL:
        logger.info(f"- {db_key}: no way to kill a session from outside on {db_type}, skipped")
        return None

    pool = Pool.from_config(db_config, PoolBackend.DBUTILS)
    killer = Pool.from_config(db_config, PoolBackend.DBUTILS)
    try:
        with pool.client() as client:
            pid = get_db(client).query(BACKEND_PID_SQL[db_type], [])['data'][0]['pid']
        logger.info(f"{db_key}: checked out session {pid}, killing it")

        with killer.client() as client:
            get_db(client).execute(KILL_SQL[db_type], [pid])

        # The connection is now back in the idle cache and dead. Checking it
        # out inside a transaction is the exact shape that used to fail:
        # DBUtils does not retry statements inside a declared transaction, so
        # nothing downstream can paper over a corpse handed out here.
        started = time.time()
        with pool.transaction() as tx:
            new_pid = get_db(tx.client).query(BACKEND_PID_SQL[db_type], [])['data'][0]['pid']
        elapsed = time.time() - started

        if new_pid == pid:
            logger.error(f"✗ {db_key}: still on the killed session {pid}")
            return False
        logger.info(f"✓ {db_key}: recovered onto session {new_pid} in {elapsed:.2f}s")
        return True
    except Exception as exc:
        logger.error(f"✗ {db_key}: pool handed out the dead connection -- {type(exc).__name__}: {exc}")
        return False
    finally:
        pool.dispose()
        killer.dispose()


def main():
    results = [
        test_probe_bug_does_not_disable_the_check(),
        test_native_ping_is_left_alone(),
        test_unattachable_connection_is_not_silent(),
        test_sqlite_needs_no_check(),
    ]

    try:
        cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
        configure(cfg)
    except Exception as exc:
        logger.warning(f"No test database config, offline checks only: {exc}")
    else:
        seen = set()
        for db_key, _backend in test_utils.get_active_test_configs(cfg):
            if db_key in seen:
                continue
            seen.add(db_key)
            outcome = run_liveness_test_for_db(db_key, cfg)
            if outcome is not None:
                results.append(outcome)

    failed = results.count(False)
    logger.info(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
