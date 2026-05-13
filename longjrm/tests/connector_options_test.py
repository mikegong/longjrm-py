"""
Unit tests for connector option passthrough and connect_timeout resolution.

These tests do NOT require any real database. They fake the driver modules
in sys.modules and assert what kwargs were forwarded to the driver's connect().
"""
from __future__ import annotations
import sys
import unittest
from unittest.mock import MagicMock, patch

from longjrm.config.config import DatabaseConfig, JrmConfig
from longjrm.config.runtime import configure
from longjrm.connection.connectors import (
    PostgresConnector, MySQLConnector, SqliteConnector,
    OracleConnector, SqlServerConnector, Db2Connector,
)


def _set_global_timeout(secs: int) -> None:
    """Install a JrmConfig with the given global connect_timeout."""
    configure(JrmConfig(_databases={}, connect_timeout=secs))


def _install_fake_module(name: str) -> MagicMock:
    """Replace sys.modules[name] with a MagicMock for the duration of a test."""
    mod = MagicMock()
    sys.modules[name] = mod
    return mod


# -----------------------------------------------------------------------------
# BaseConnector: connect_timeout resolution
# -----------------------------------------------------------------------------

class TestConnectTimeoutResolution(unittest.TestCase):
    def test_global_default_used_when_no_options(self):
        _set_global_timeout(33)
        db = DatabaseConfig(type="postgres", host="h", database="d")
        c = PostgresConnector(db)
        self.assertEqual(c.connect_timeout, 33)

    def test_options_override_global(self):
        _set_global_timeout(33)
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"connect_timeout": 7})
        c = PostgresConnector(db)
        self.assertEqual(c.connect_timeout, 7)

    def test_invalid_options_falls_back_to_global(self):
        _set_global_timeout(33)
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"connect_timeout": "not-an-int"})
        c = PostgresConnector(db)
        self.assertEqual(c.connect_timeout, 33)

    def test_string_int_options_is_coerced(self):
        _set_global_timeout(33)
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"connect_timeout": "12"})
        c = PostgresConnector(db)
        self.assertEqual(c.connect_timeout, 12)


# -----------------------------------------------------------------------------
# Postgres
# -----------------------------------------------------------------------------

class TestPostgresOptions(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(40)
        self.psycopg = _install_fake_module("psycopg")
        self.psycopg.connect.return_value.info.status.name = "READY"

    def tearDown(self):
        sys.modules.pop("psycopg", None)

    def test_passthrough_keepalives_and_app_name(self):
        db = DatabaseConfig(type="postgres", host="h", port=5432, database="d",
                            user="u", password="p",
                            options={
                                "keepalives": 1,
                                "keepalives_idle": 30,
                                "keepalives_interval": 10,
                                "keepalives_count": 3,
                                "tcp_user_timeout": 5000,
                                "application_name": "longjrm-test",
                            })
        PostgresConnector(db).connect()
        kw = self.psycopg.connect.call_args.kwargs
        self.assertEqual(kw["keepalives"], 1)
        self.assertEqual(kw["keepalives_idle"], 30)
        self.assertEqual(kw["keepalives_interval"], 10)
        self.assertEqual(kw["keepalives_count"], 3)
        self.assertEqual(kw["tcp_user_timeout"], 5000)
        self.assertEqual(kw["application_name"], "longjrm-test")

    def test_default_sslmode_is_prefer(self):
        db = DatabaseConfig(type="postgres", host="h", database="d")
        PostgresConnector(db).connect()
        self.assertEqual(self.psycopg.connect.call_args.kwargs["sslmode"], "prefer")

    def test_options_sslmode_overrides_default(self):
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"sslmode": "require"})
        PostgresConnector(db).connect()
        self.assertEqual(self.psycopg.connect.call_args.kwargs["sslmode"], "require")

    def test_options_connect_timeout_overrides_global(self):
        _set_global_timeout(99)
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"connect_timeout": 7})
        PostgresConnector(db).connect()
        self.assertEqual(self.psycopg.connect.call_args.kwargs["connect_timeout"], 7)

    def test_unknown_option_is_dropped(self):
        db = DatabaseConfig(type="postgres", host="h", database="d",
                            options={"weird_unknown": "x"})
        PostgresConnector(db).connect()
        self.assertNotIn("weird_unknown", self.psycopg.connect.call_args.kwargs)

    def test_dsn_path_does_not_strip_options(self):
        """When DSN is given, libpq parses query params; we just pass DSN through."""
        db = DatabaseConfig(type="postgres",
                            dsn="postgresql://u:p@h:5432/d?sslmode=require&keepalives_idle=30")
        PostgresConnector(db).connect()
        # First positional arg should be the DSN string verbatim.
        args, kw = self.psycopg.connect.call_args
        self.assertEqual(args[0],
                         "postgresql://u:p@h:5432/d?sslmode=require&keepalives_idle=30")


# -----------------------------------------------------------------------------
# MySQL
# -----------------------------------------------------------------------------

class TestMySQLOptions(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(40)
        self.pymysql = _install_fake_module("pymysql")

    def tearDown(self):
        sys.modules.pop("pymysql", None)

    def test_port_is_passed(self):
        """Regression: PyMySQL was never receiving port."""
        db = DatabaseConfig(type="mysql", host="h", port=3307, database="d",
                            user="u", password="p")
        MySQLConnector(db).connect()
        self.assertEqual(self.pymysql.connect.call_args.kwargs["port"], 3307)

    def test_passthrough_charset_ssl_init_command(self):
        db = DatabaseConfig(type="mysql", host="h", port=3306, database="d",
                            options={
                                "charset": "utf8mb4",
                                "ssl_ca": "/etc/ssl/ca.pem",
                                "init_command": "SET time_zone='+00:00'",
                                "read_timeout": 10,
                                "write_timeout": 10,
                            })
        MySQLConnector(db).connect()
        kw = self.pymysql.connect.call_args.kwargs
        self.assertEqual(kw["charset"], "utf8mb4")
        self.assertEqual(kw["ssl_ca"], "/etc/ssl/ca.pem")
        self.assertEqual(kw["init_command"], "SET time_zone='+00:00'")
        self.assertEqual(kw["read_timeout"], 10)
        self.assertEqual(kw["write_timeout"], 10)


# -----------------------------------------------------------------------------
# SQLite
# -----------------------------------------------------------------------------

class TestSqliteOptions(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(40)
        # Save+swap real sqlite3 module so connect() returns a mock
        self._real_sqlite3 = sys.modules.get("sqlite3")
        self.sqlite3 = _install_fake_module("sqlite3")
        self.sqlite3.connect.return_value.isolation_level = ""

    def tearDown(self):
        sys.modules.pop("sqlite3", None)
        if self._real_sqlite3 is not None:
            sys.modules["sqlite3"] = self._real_sqlite3

    def test_passthrough_busy_timeout_and_check_same_thread(self):
        db = DatabaseConfig(type="sqlite", database=":memory:",
                            options={"timeout": 30, "check_same_thread": False})
        SqliteConnector(db).connect()
        args, kwargs = self.sqlite3.connect.call_args
        self.assertEqual(args[0], ":memory:")
        self.assertEqual(kwargs["timeout"], 30)
        self.assertEqual(kwargs["check_same_thread"], False)

    def test_global_connect_timeout_not_mapped_to_sqlite(self):
        """sqlite3 'timeout' is busy-lock, not connect timeout — must not auto-map."""
        _set_global_timeout(99)
        db = DatabaseConfig(type="sqlite", database=":memory:")
        SqliteConnector(db).connect()
        kwargs = self.sqlite3.connect.call_args.kwargs
        self.assertNotIn("timeout", kwargs)


# -----------------------------------------------------------------------------
# Oracle
# -----------------------------------------------------------------------------

class TestOracleOptions(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(40)
        self.oracledb = _install_fake_module("oracledb")
        # oracledb.defaults.fetch_lobs is set inside connect()
        self.oracledb.defaults = MagicMock()

    def tearDown(self):
        sys.modules.pop("oracledb", None)

    def test_connect_timeout_maps_to_tcp_connect_timeout(self):
        _set_global_timeout(15)
        db = DatabaseConfig(type="oracle", host="h", port=1521, database="ORCL",
                            user="u", password="p")
        OracleConnector(db).connect()
        kw = self.oracledb.connect.call_args.kwargs
        self.assertEqual(kw["tcp_connect_timeout"], 15)

    def test_passthrough_mode_wallet_expire_time(self):
        db = DatabaseConfig(type="oracle", host="h", database="ORCL",
                            options={"mode": 2,
                                     "wallet_location": "/opt/wallet",
                                     "expire_time": 60})
        OracleConnector(db).connect()
        kw = self.oracledb.connect.call_args.kwargs
        self.assertEqual(kw["mode"], 2)
        self.assertEqual(kw["wallet_location"], "/opt/wallet")
        self.assertEqual(kw["expire_time"], 60)

    def test_service_name_used_in_dsn_not_forwarded(self):
        db = DatabaseConfig(type="oracle", host="h", port=1521, database="ORCL",
                            options={"service_name": "MYPDB"})
        OracleConnector(db).connect()
        kw = self.oracledb.connect.call_args.kwargs
        self.assertEqual(kw["dsn"], "h:1521/MYPDB")
        self.assertNotIn("service_name", kw)


# -----------------------------------------------------------------------------
# SQL Server
# -----------------------------------------------------------------------------

class TestSqlServerOptions(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(40)
        self.pyodbc = _install_fake_module("pyodbc")

    def tearDown(self):
        sys.modules.pop("pyodbc", None)

    def test_connect_timeout_maps_to_pyodbc_timeout(self):
        _set_global_timeout(12)
        db = DatabaseConfig(type="sqlserver", host="h", port=1433, database="d",
                            user="u", password="p")
        SqlServerConnector(db).connect()
        self.assertEqual(self.pyodbc.connect.call_args.kwargs["timeout"], 12)

    def test_extra_options_inlined_in_conn_str(self):
        db = DatabaseConfig(type="sqlserver", host="h", database="d",
                            options={"Encrypt": "yes",
                                     "TrustServerCertificate": "yes"})
        SqlServerConnector(db).connect()
        conn_str = self.pyodbc.connect.call_args.args[0]
        self.assertIn("Encrypt=yes", conn_str)
        self.assertIn("TrustServerCertificate=yes", conn_str)

    def test_pyodbc_only_kwarg_not_inlined(self):
        """`readonly` is a pyodbc.connect kwarg, must not appear in conn string."""
        db = DatabaseConfig(type="sqlserver", host="h", database="d",
                            options={"readonly": True})
        SqlServerConnector(db).connect()
        conn_str = self.pyodbc.connect.call_args.args[0]
        self.assertNotIn("readonly", conn_str)
        self.assertEqual(self.pyodbc.connect.call_args.kwargs["readonly"], True)


# -----------------------------------------------------------------------------
# DB2
# -----------------------------------------------------------------------------

class TestDb2Options(unittest.TestCase):
    def setUp(self):
        _set_global_timeout(20)
        self._patcher = patch("longjrm.connection.connectors.fix_ibm_db_dll", lambda: None)
        self._patcher.start()
        self.ibm = _install_fake_module("ibm_db_dbi")

    def tearDown(self):
        self._patcher.stop()
        sys.modules.pop("ibm_db_dbi", None)

    def test_connect_timeout_in_conn_str(self):
        db = DatabaseConfig(type="db2", host="h", port=50000, database="SAMPLE",
                            user="u", password="p")
        Db2Connector(db).connect()
        conn_str = self.ibm.connect.call_args.args[0]
        self.assertIn("ConnectTimeout=20", conn_str)

    def test_passthrough_security_and_currentschema(self):
        db = DatabaseConfig(type="db2", host="h", port=50000, database="SAMPLE",
                            options={"SECURITY": "SSL",
                                     "CURRENTSCHEMA": "MYSCHEMA"})
        Db2Connector(db).connect()
        conn_str = self.ibm.connect.call_args.args[0]
        self.assertIn("SECURITY=SSL", conn_str)
        self.assertIn("CURRENTSCHEMA=MYSCHEMA", conn_str)


if __name__ == "__main__":
    unittest.main()
