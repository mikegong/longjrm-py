"""A connector must be able to read back the autocommit state it sets (DB-free).

The base connector writes and reads a plain ``autocommit`` attribute. A connector
that writes it another way -- because its driver has no such attribute -- has to
read it another way too. Db2Connector did not: ibm_db_dbi has set_autocommit()
and nothing to read, so every batched stream and transactional script on DB2
failed at the read. GenericConnector had the same shape.
"""

import inspect

import pytest

from longjrm.connection import connectors
from longjrm.connection.connectors import GenericConnector


def test_every_connector_that_writes_autocommit_its_own_way_reads_it_its_own_way():
    one_sided = [
        cls.__name__
        for _, cls in inspect.getmembers(connectors, inspect.isclass)
        if issubclass(cls, connectors.BaseConnector)
        and "set_dbapi_autocommit" in cls.__dict__
        and "get_dbapi_autocommit" not in cls.__dict__
    ]
    assert one_sided == []


def test_db2_getter_reads_the_driver_state_from_the_handle(monkeypatch):
    ibm_db = pytest.importorskip("ibm_db")
    from longjrm.connection.connectors import Db2Connector

    class _Conn:                      # stands in for ibm_db_dbi.Connection
        conn_handler = object()

    state = {}
    monkeypatch.setattr(ibm_db, "autocommit", lambda handle: state[handle])
    for driver_value, expected in ((1, True), (0, False)):
        state[_Conn.conn_handler] = driver_value
        assert Db2Connector.get_dbapi_autocommit(_Conn()) is expected


def test_generic_getter_reads_an_attribute():
    class _Conn:                      # psycopg shape
        autocommit = False

    conn = _Conn()
    assert GenericConnector.get_dbapi_autocommit(conn) is False
    GenericConnector.set_dbapi_autocommit(conn, True)
    assert GenericConnector.get_dbapi_autocommit(conn) is True


def test_generic_getter_asks_the_driver_when_autocommit_is_a_method():
    class _Conn:                      # PyMySQL shape
        def __init__(self):
            self._mode = True

        def autocommit(self, value):
            self._mode = value

        def get_autocommit(self):
            return self._mode

    conn = _Conn()
    GenericConnector.set_dbapi_autocommit(conn, False)
    assert GenericConnector.get_dbapi_autocommit(conn) is False


def test_generic_getter_refuses_to_guess():
    class _Conn:                      # can be set, cannot be read
        def set_autocommit(self, value):
            pass

    with pytest.raises(ValueError):
        GenericConnector.get_dbapi_autocommit(_Conn())
