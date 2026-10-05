"""DB-free checks for the lock discipline of AsyncDb's streaming reads.

``_aiter_in_thread`` is what ``stream_query`` / ``stream_query_batch`` /
``stream_select`` return. It must hold the AsyncDb lock while the sync
generator (and so the DB-API cursor) is open, and give it back however the
iteration ends: exhaustion, ``break``, an exception in the loop body, or an
explicit ``aclose()``. The ``break`` and exception cases are the ones that
deadlocked every later call on the same AsyncDb before 0.4.0, and the only
test for them needed a live database and filed the deadlock as a skip.
"""

import asyncio
import contextlib

import pytest

from longjrm.database.async_db import _aiter_in_thread


def _rows(n, closed):
    """A sync generator that records its own close() in ``closed``."""
    try:
        for i in range(n):
            yield i
    finally:
        closed.append(True)


async def _lock_is_free(lock):
    # A held lock would make acquire() wait for a release that never comes;
    # the timeout turns that into a failure instead of a hung test.
    await asyncio.wait_for(lock.acquire(), timeout=2.0)
    lock.release()


def test_lock_held_while_iterating_and_released_on_exhaustion():
    async def main():
        lock, closed, seen = asyncio.Lock(), [], []
        async for v in _aiter_in_thread(_rows(3, closed), lock):
            assert lock.locked(), "lock must be held between rows"
            seen.append(v)
        assert seen == [0, 1, 2]
        assert not lock.locked()
        assert closed == [True]
    asyncio.run(main())


def test_break_releases_the_lock_and_closes_the_generator():
    async def main():
        lock, closed = asyncio.Lock(), []
        async for _ in _aiter_in_thread(_rows(3, closed), lock):
            break
        await _lock_is_free(lock)
        assert closed == [True]
    asyncio.run(main())


def test_exception_in_the_loop_body_releases_the_lock():
    async def main():
        lock, closed = asyncio.Lock(), []
        with pytest.raises(RuntimeError):
            async for _ in _aiter_in_thread(_rows(3, closed), lock):
                raise RuntimeError("consumer failed mid-stream")
        await _lock_is_free(lock)
        assert closed == [True]
    asyncio.run(main())


def test_aclosing_releases_immediately():
    async def main():
        lock, closed = asyncio.Lock(), []
        async with contextlib.aclosing(_aiter_in_thread(_rows(3, closed), lock)) as it:
            async for _ in it:
                break
        # No turn of the loop needed: aclosing() awaited aclose() itself.
        assert not lock.locked()
        assert closed == [True]
    asyncio.run(main())
