"""Row shapes _stream_transaction_handler accepts (DB-free).

A stream item is either a bare row dict -- what a hand-written generator
yields, and what README / docs/database.md show -- or the tuple stream_query
yields, ``(row_number, row[, status])``, so a query can be piped into a write.
Before 0.4.0 only the tuple was understood, and a dict was unpacked into its
own keys.
"""

from longjrm.tests.test_reject_sink import _StubDb


def _collect(seen):
    def op(row, row_number):
        seen.append((row_number, row))
        return {"status": 0}
    return op


def test_bare_dict_rows_are_rows_numbered_from_one():
    # 1, 2 and 3 keys: the three lengths the tuple unpacking mistook for
    # (row_number, row[, status]).
    rows = [{"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2, "c": 3}]
    seen, db = [], _StubDb()
    result = db.run(iter(rows), _collect(seen), commit_count=0)
    assert result["status"] == 0
    assert result["record_count"] == 3
    assert seen == [(1, rows[0]), (2, rows[1]), (3, rows[2])]


def test_tuple_rows_keep_their_upstream_numbers():
    seen, db = [], _StubDb()
    result = db.run(iter([(7, {"a": 1}), (8, {"a": 2}, 0)]), _collect(seen), commit_count=0)
    assert result["status"] == 0
    assert result["record_count"] == 8
    assert seen == [(7, {"a": 1}), (8, {"a": 2})]


def test_upstream_error_status_still_rejects():
    seen, db = [], _StubDb()
    result = db.run(iter([(1, {"a": 1}, 0), (2, {}, -1)]), _collect(seen), commit_count=0)
    assert result["status"] == -1
    assert result["record_count"] == 2
    assert seen == [(1, {"a": 1})]


def test_dict_rows_commit_every_commit_count_rows():
    db = _StubDb()
    result = db.run(iter([{"a": i} for i in range(5)]), _collect([]), commit_count=2)
    assert result["status"] == 0
    assert result["record_count"] == 5
    assert db.committed == 3          # after rows 2 and 4, then the final commit
