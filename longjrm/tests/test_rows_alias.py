"""DB-free unit tests for the rows_* result-key aliasing (longjrm.database.db.rows_alias).

The aliasing is additive and non-breaking: old keys (count / record_count /
reject_count / row_count) stay; the standard rows_* names are exposed alongside.
"""

from longjrm.database.db import Db, rows_alias


def test_alias_adds_new_key_mirroring_old():
    @rows_alias(count="rows_inserted")
    def f():
        return {"status": 0, "count": 5}
    r = f()
    assert r["count"] == 5 and r["rows_inserted"] == 5      # old kept, new added


def test_alias_multiple_keys():
    @rows_alias(record_count="rows_read", reject_count="rows_rejected")
    def f():
        return {"status": 0, "record_count": 10, "reject_count": 2}
    r = f()
    assert r["rows_read"] == 10 and r["rows_rejected"] == 2
    assert r["record_count"] == 10 and r["reject_count"] == 2   # originals untouched


def test_alias_skipped_when_source_absent():
    @rows_alias(count="rows_inserted")
    def f():
        return {"status": 0}
    assert "rows_inserted" not in f()


def test_alias_does_not_overwrite_existing_new_key():
    @rows_alias(count="rows_read")
    def f():
        return {"count": 1, "rows_read": 99}
    assert f()["rows_read"] == 99


def test_alias_passes_through_non_dict():
    @rows_alias(count="rows_read")
    def f():
        return None
    assert f() is None


def test_public_methods_are_decorated():
    # functools.wraps sets __wrapped__ on the alias wrapper; assert the standard
    # data methods carry it (cheap structural check, no DB needed).
    for name in ("query", "insert", "update", "bulk_update", "delete", "merge",
                 "merge_select", "stream_insert", "stream_merge"):
        assert hasattr(getattr(Db, name), "__wrapped__"), f"{name} not aliased"
