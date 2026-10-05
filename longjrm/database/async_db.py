"""
Async wrapper around the synchronous Db class.

Background
----------
longjrm's underlying drivers (psycopg, pymysql, oracledb, ...) and pool
backends (DBUtils, SQLAlchemy) are all blocking. To support async frameworks
(FastAPI / aiohttp / Sanic) without forcing users to sprinkle
``run_in_threadpool`` over every call site, this module provides ``AsyncDb``:

  * Same public method names and signatures as ``Db``.
  * Each method dispatches the synchronous call through ``asyncio.to_thread``
    so the event loop stays unblocked.
  * I/O is still performed by the underlying sync driver in a worker thread.
    This is *threadpool-backed async*, not native async I/O. For C10K-class
    throughput requirements, consider a native async driver (asyncpg,
    psycopg.AsyncConnection, aiomysql) instead.

Concurrency rules
-----------------
A single ``AsyncDb`` instance wraps a single sync ``Db`` (and therefore a
single DB-API connection). DB-API connections are NOT safe to use from
multiple threads/coroutines concurrently. ``AsyncDb`` enforces this with an
internal ``asyncio.Lock``:

    async with pool.aclient() as client:
        db = get_async_db(client)
        # OK -- serialized by the internal lock
        a, b = await asyncio.gather(db.query(sql1), db.query(sql2))

The lock makes accidental concurrency safe (serialized) rather than corrupt.
For real concurrency, check out one ``AsyncDb`` per branch:

    async def fetch(sql):
        async with pool.aclient() as client:
            return await get_async_db(client).query(sql)

    a, b = await asyncio.gather(fetch(sql1), fetch(sql2))
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Optional

from longjrm.database import get_db

logger = logging.getLogger(__name__)


async def _aiter_in_thread(gen, lock: asyncio.Lock) -> AsyncIterator:
    """Drive a synchronous generator from a worker thread, one ``next()`` per
    ``__anext__``, holding ``lock`` for the lifetime of the iteration.

    Holding the lock for the whole iteration is intentional: the underlying
    DB-API connection has an open cursor and cannot be shared with another
    coroutine until the iterator is exhausted or closed.

    This is a native async generator on purpose. ``async for`` never calls
    ``aclose()`` on an iterator when the loop exits early, so a hand-written
    ``__anext__`` class holds the lock forever after a ``break``. A native
    async generator is finalized by asyncio when it is dropped: the
    ``finally`` below closes the sync generator (releasing its cursor) and
    the ``async with`` releases the lock. After an early exit that release is
    scheduled on the loop, so the next ``await`` on the same ``AsyncDb`` sees
    it; ``aclose()`` or ``contextlib.aclosing(...)`` releases immediately.
    A reference kept to the iterator keeps the lock held: iterate it with
    ``async for`` directly.
    """
    sentinel = object()
    async with lock:
        try:
            while True:
                value = await asyncio.to_thread(next, gen, sentinel)
                if value is sentinel:
                    return
                yield value
        finally:
            await asyncio.to_thread(gen.close)


class AsyncDb:
    """
    Async-friendly facade over the synchronous ``Db`` class.

    All public methods mirror their sync counterparts 1:1 in name, parameters,
    and return shape. The only difference is each method returns an awaitable.
    """

    def __init__(self, client: dict):
        # client dict is the same shape produced by Pool.aclient() / Pool.client()
        self._sync = get_db(client)
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Underlying-instance access (escape hatches)
    # ------------------------------------------------------------------
    @property
    def sync(self):
        """Return the wrapped synchronous Db. Use only when you really know
        the call is non-blocking (e.g. attribute reads). Avoid invoking sync
        I/O methods through this property from inside ``async def`` code."""
        return self._sync

    @property
    def database_type(self) -> str:
        return self._sync.database_type

    @property
    def database_name(self) -> str:
        return self._sync.database_name

    # ------------------------------------------------------------------
    # CRUD: select / query / execute
    # ------------------------------------------------------------------
    async def select(self, table, columns=None, where=None, options=None):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.select, table, columns, where, options
            )

    async def query(self, sql, arr_values=None):
        async with self._lock:
            return await asyncio.to_thread(self._sync.query, sql, arr_values)

    async def execute(self, sql, arr_values=None):
        async with self._lock:
            return await asyncio.to_thread(self._sync.execute, sql, arr_values)

    # ------------------------------------------------------------------
    # CRUD: insert / update / delete / merge
    # ------------------------------------------------------------------
    async def insert(self, table, data, return_columns=None, bulk_size=1000):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.insert, table, data, return_columns, bulk_size
            )

    async def update(self, table, data, where=None):
        async with self._lock:
            return await asyncio.to_thread(self._sync.update, table, data, where)

    async def delete(self, table, where=None):
        async with self._lock:
            return await asyncio.to_thread(self._sync.delete, table, where)

    async def merge(self, table, data, key_columns, no_update=None):
        async with self._lock:
            # Keyword form so the flag can never bind to a backend-specific
            # positional extra if an override ever diverges from the base.
            return await asyncio.to_thread(
                self._sync.merge, table, data, key_columns, no_update=no_update
            )

    # ------------------------------------------------------------------
    # Bulk / merge_select / bulk_load (Phase 2)
    # ------------------------------------------------------------------
    async def bulk_update(self, table, data_list, key_columns, bulk_size=1000):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.bulk_update, table, data_list, key_columns, bulk_size
            )

    async def merge_select(
        self,
        source_table,
        target_table,
        insert_columns,
        key_columns,
        order_by=None,
        conditions=None,
        source_select=None,
        update_columns=None,
        isolation_clause='',
        dynamic_param='Y',
    ):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.merge_select,
                source_table,
                target_table,
                insert_columns,
                key_columns,
                order_by,
                conditions,
                source_select,
                update_columns,
                isolation_clause,
                dynamic_param,
            )

    async def bulk_load(self, table, load_info=None, *, command=None):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.bulk_load, table, load_info, command=command
            )

    # ------------------------------------------------------------------
    # Streaming queries (Phase 2)
    #
    # ``stream_query`` and ``stream_query_batch`` return async iterators
    # that wrap the underlying sync generator. The lock is held for the
    # entire iteration lifetime, since the underlying DB-API cursor cannot
    # be shared. Use ``async for ... in db.stream_query(sql):``; for explicit
    # cleanup wrap it in ``contextlib.aclosing(db.stream_query(sql))``.
    #
    # NOTE: returning a coroutine that yields an async iterator would mean
    # callers write ``async for r in await db.stream_query(...):``. We keep
    # the more idiomatic shape: ``async for r in db.stream_query(...):`` —
    # so these are regular methods returning the async generator directly.
    # ------------------------------------------------------------------
    def stream_query(self, sql, arr_values=None, *, max_error_count=0) -> AsyncIterator:
        """Async-iterate rows from a query without buffering the whole result.

        Yields the same tuples as ``Db.stream_query``: ``(row_number, row_dict, status)``.

        Usage:
            async for row_num, row, status in db.stream_query(
                "SELECT * FROM big_table"
            ):
                ...
        """
        gen = self._sync.stream_query(sql, arr_values, max_error_count=max_error_count)
        return _aiter_in_thread(gen, self._lock)

    def stream_query_batch(
        self, sql, arr_values=None, *, batch_size=1000, max_error_count=0
    ) -> AsyncIterator:
        """Async-iterate batched rows. Yields ``(row_number, batch_data, status)``."""
        gen = self._sync.stream_query_batch(
            sql, arr_values, batch_size=batch_size, max_error_count=max_error_count
        )
        return _aiter_in_thread(gen, self._lock)

    def stream_select(self, table, columns=None, where=None, options=None, *, max_error_count=0) -> AsyncIterator:
        """Async-iterate a SELECT without buffering -- the streaming counterpart of
        select(). Same SQL as select() (data_fetch_limit default applies; pass
        options={"limit": 0} to stream all). Yields (row_number, row_dict, status)."""
        gen = self._sync.stream_select(
            table, columns=columns, where=where, options=options,
            max_error_count=max_error_count)
        return _aiter_in_thread(gen, self._lock)

    # ------------------------------------------------------------------
    # Streaming writes (Phase 2)
    #
    # These accept a SYNC iterable / generator as the ``stream`` argument
    # and consume it within a worker thread. Async iterables are NOT
    # supported here; if your data source is async, materialize a list
    # first or use ``asyncio.to_thread`` yourself.
    # ------------------------------------------------------------------
    async def stream_insert(self, stream, table, *, commit_count=10000, max_error_count=0):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.stream_insert,
                stream, table,
                commit_count=commit_count, max_error_count=max_error_count,
            )

    async def stream_update(self, stream, table, *, commit_count=10000, max_error_count=0):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.stream_update,
                stream, table,
                commit_count=commit_count, max_error_count=max_error_count,
            )

    async def stream_merge(self, stream, table, key_columns, *, commit_count=10000, max_error_count=0):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.stream_merge,
                stream, table, key_columns,
                commit_count=commit_count, max_error_count=max_error_count,
            )

    # ------------------------------------------------------------------
    # File / script helpers (Phase 2)
    # ------------------------------------------------------------------
    async def run_query_from_file(self, sql_file, values=None):
        async with self._lock:
            return await asyncio.to_thread(self._sync.run_query_from_file, sql_file, values)

    async def execute_script(self, sql_script, transaction=False, delimiter=';'):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.execute_script, sql_script, transaction, delimiter
            )

    async def run_script_from_file(self, sql_file, transaction=False, delimiter=';'):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.run_script_from_file, sql_file, transaction, delimiter
            )

    async def stream_to_csv(self, sql, csv_file, values=None, options=None):
        async with self._lock:
            return await asyncio.to_thread(
                self._sync.stream_to_csv, sql, csv_file, values, options
            )

    # ------------------------------------------------------------------
    # Transaction helpers (cheap calls, but kept async for API symmetry)
    # ------------------------------------------------------------------
    async def commit(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync.rollback)

    async def set_autocommit(self, value: bool) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync.set_autocommit, value)

    async def get_autocommit(self) -> Optional[bool]:
        async with self._lock:
            return await asyncio.to_thread(self._sync.get_autocommit)


__all__ = ["AsyncDb"]
