"""Focused tests for the additive reject_sink hook in _stream_transaction_handler.

DB-free: drives the handler through a minimal stub that supplies only the
connection-control methods it calls. Verifies that (a) a reject_sink receives the
failing row + reason, (b) the result carries reject_count, and (c) the default
(no sink) path is unchanged.
"""

from longjrm.database.db import Db


class _StubDb:
    """Minimal stand-in exposing just what _stream_transaction_handler calls."""

    def __init__(self):
        self.committed = 0
        self.rolled_back = 0
        self._autocommit = True
        self.executed = []           # SAVEPOINT / RELEASE / ROLLBACK TO statements

    def get_autocommit(self):
        return self._autocommit

    def set_autocommit(self, v):
        self._autocommit = v

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def execute(self, sql, arr_values=None):
        # The tolerant path issues SAVEPOINT/RELEASE/ROLLBACK TO via execute().
        self.executed.append(sql)
        return {"status": 0, "count": 0}

    # Call the real handler as an unbound method with this stub as self.
    def run(self, stream, op, **kw):
        return Db._stream_transaction_handler(self, stream, op, **kw)


def _ok(row, _):
    return {"status": 0}


def _fail_on(bad_keys):
    def op(row, _):
        if row.get("id") in bad_keys:
            return {"status": -1, "message": f"bad row {row.get('id')}"}
        return {"status": 0}
    return op


def test_reject_sink_receives_failing_row_and_reason():
    captured = []
    stream = [(1, {"id": 1}), (2, {"id": 2}), (3, {"id": 3})]
    db = _StubDb()
    result = db.run(stream, _fail_on({2}), commit_count=0, max_error_count=5,
                    reject_sink=lambda n, row, reason: captured.append((n, row, reason)))
    assert result["status"] == 0
    assert result["reject_count"] == 1
    assert len(captured) == 1
    n, row, reason = captured[0]
    assert n == 2 and row == {"id": 2}
    assert "bad row 2" in reason


def test_final_aborting_row_is_captured_before_rollback():
    captured = []
    stream = [(1, {"id": 1}), (2, {"id": 2})]
    db = _StubDb()
    result = db.run(stream, _fail_on({2}), commit_count=10, max_error_count=0,
                    reject_sink=lambda n, row, reason: captured.append((n, row)))
    assert result["status"] == -1                 # tolerance 0 -> aborts on the bad row
    assert result["reject_count"] == 1
    assert captured == [(2, {"id": 2})]           # captured even though it aborts
    assert db.rolled_back == 1


def test_per_row_raise_is_captured_as_reject_via_savepoint():
    # A row whose operation RAISES (longjrm write ops raise on DB error) is
    # isolated by a savepoint, rolled back, and recorded as a per-row reject --
    # not a fatal stream abort -- when reject handling is on.
    captured = []

    def op(row, _):
        if row.get("id") == 2:
            raise RuntimeError("data too long")
        return {"status": 0}

    db = _StubDb()
    result = db.run([(1, {"id": 1}), (2, {"id": 2}), (3, {"id": 3})], op,
                    commit_count=5, max_error_count=5,
                    reject_sink=lambda n, row, reason: captured.append((n, row, reason)))
    assert result["status"] == 0
    assert result["reject_count"] == 1
    assert captured[0][0] == 2 and captured[0][1] == {"id": 2}
    assert "data too long" in captured[0][2]
    # Savepoint set then rolled back for the bad row.
    assert any("SAVEPOINT jrm_sp_2" in s for s in db.executed)
    assert any("ROLLBACK TO SAVEPOINT jrm_sp_2" in s for s in db.executed)


def test_raise_without_reject_handling_is_fatal():
    # No sink and max_error_count=0 -> original behavior: a raising row aborts.
    def op(row, _):
        raise RuntimeError("boom")

    db = _StubDb()
    result = db.run([(1, {"id": 1})], op, commit_count=0)
    assert result["status"] == -1            # fatal, caught by the handler's except
    assert db.executed == []                 # no savepoint overhead on the default path


def test_no_sink_path_unchanged_and_reports_reject_count():
    db = _StubDb()
    result = db.run([(1, {"id": 1}), (2, {"id": 2})], _ok, commit_count=0)
    assert result["status"] == 0
    assert result["record_count"] == 2
    assert result["reject_count"] == 0            # additive key, no rejects


def test_empty_stream_reports_zero_reject_count():
    db = _StubDb()
    result = db.run([], _ok, commit_count=0)
    assert result["status"] == 0
    assert result["record_count"] == 0
    assert result["reject_count"] == 0
