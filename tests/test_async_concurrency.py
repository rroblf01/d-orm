"""Bug-hunting tests for the async path under concurrency.

The single-coroutine async tests cover the happy path. Real apps run
many coroutines through the same connection pool simultaneously, and
that's where pool starvation, race conditions in the descriptor
caches, and cross-task transaction leakage tend to show up. This file
drives those scenarios via ``asyncio.gather`` and friends.

Postgres-only because SQLite's async (``aiosqlite``) serialises
operations on a single thread anyway — concurrency bugs in the pool
machinery are PG-shaped.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.models import Author


pytestmark = pytest.mark.asyncio


@pytest.fixture
def _postgres_only(db_config):
    """Async concurrency tests target the PG pool. SQLite serialises
    aiosqlite ops in a single worker thread, so ``gather`` of many
    coroutines doesn't actually exercise the pool — the tests would
    pass trivially without proving anything."""
    if db_config.get("ENGINE") != "postgresql":
        pytest.skip("Async concurrency tests target PostgreSQL.")


# ── gather() of N concurrent saves ──────────────────────────────────────────


class TestGatherCreate:
    async def test_gather_acreate_lands_every_row(self, _postgres_only):
        """Spam the pool with concurrent inserts and verify nothing
        falls through the cracks. Catches connection re-use bugs that
        would lose rows or duplicate them."""
        N = 20
        results = await asyncio.gather(
            *(
                Author.objects.acreate(name=f"gather-{i}", age=i)
                for i in range(N)
            )
        )
        assert len(results) == N
        # Every PK is distinct, every name is distinct.
        assert len({a.pk for a in results}) == N
        # Every row landed in the DB.
        count = await Author.objects.filter(name__startswith="gather-").acount()
        assert count == N

    async def test_gather_mixed_create_and_query(self, _postgres_only):
        """Interleave writes and reads on the same model. Connection
        pool must not deadlock and reads must see no half-committed
        rows."""
        # Seed.
        for i in range(5):
            await Author.objects.acreate(name=f"seed-{i}", age=i)

        async def writer(j: int) -> None:
            await Author.objects.acreate(name=f"w-{j}", age=100 + j)

        async def reader() -> int:
            # Each reader does a fresh count.
            return await Author.objects.acount()

        # Mix 10 writes and 10 reads concurrently.
        coros = [writer(i) for i in range(10)] + [reader() for _ in range(10)]
        results = await asyncio.gather(*coros)
        # Final count: 5 seeds + 10 writers.
        final = await Author.objects.acount()
        assert final == 15
        # All readers got something between 5 (only seeds visible) and
        # 15 (all writes visible). The transition is monotonic — no
        # reader should see fewer rows than the seeds we inserted
        # before the gather started.
        for r in results:
            if isinstance(r, int):
                assert 5 <= r <= 15, f"reader saw out-of-range count {r}"


# ── Concurrent updates on the same row ──────────────────────────────────────


class TestConcurrentUpdate:
    async def test_atomic_increments_under_gather_with_F(self, _postgres_only):
        """``UPDATE … SET age = age + 1`` is atomic at the SQL level —
        N concurrent increments must produce exactly N. Catches a
        regression where dorm fetched-then-saved (read/modify/write)
        inside ``acreate``/``aupdate`` paths."""
        from dorm import F

        author = await Author.objects.acreate(name="counter", age=0)

        N = 25
        await asyncio.gather(
            *(
                Author.objects.filter(pk=author.pk).aupdate(age=F("age") + 1)
                for _ in range(N)
            )
        )

        refreshed = await Author.objects.aget(pk=author.pk)
        assert refreshed.age == N


# ── Connection pool: many independent transactions ─────────────────────────


class TestPoolStress:
    async def test_many_aatomic_blocks_complete(self, _postgres_only):
        """Each task opens its own transaction. Pool must hand out
        connections, accept them back, and never leave a task hanging
        waiting for one. Caps at the configured ``MAX_POOL_SIZE``
        worth of concurrent active tx; overflow waits for a slot."""
        from dorm.transaction import aatomic

        async def work(i: int) -> int:
            async with aatomic():
                a = await Author.objects.acreate(name=f"tx-{i}", age=i)
                # Touch the row from inside the same tx — exercises
                # read-after-write within a single connection.
                fetched = await Author.objects.aget(pk=a.pk)
                return fetched.age

        results = await asyncio.gather(*(work(i) for i in range(20)))
        assert sorted(results) == list(range(20))
        # All rows landed.
        assert await Author.objects.filter(name__startswith="tx-").acount() == 20


# ── Cancellation safety ────────────────────────────────────────────────────


class TestAsyncCancellation:
    async def test_cancel_before_query_starts(self, _postgres_only):
        """Cancelling a task before its first await on a DB op must
        leave the pool in a usable state. The next task should get a
        clean connection without hanging."""

        async def slow_query():
            # Simulate a coroutine that's about to do a DB call but
            # gets cancelled first. The mere creation of the coroutine
            # shouldn't have side effects on the pool.
            await asyncio.sleep(0)
            return await Author.objects.acount()

        task = asyncio.create_task(slow_query())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Pool still works for the next caller.
        count = await Author.objects.acount()
        assert isinstance(count, int)

    async def test_cancel_mid_long_query_does_not_leak_connection(
        self, _postgres_only
    ):
        """Cancellation while a query is in flight (use
        ``pg_sleep`` to make the query take long enough to interrupt)
        — the pool must reclaim the connection rather than leak it.

        We can't directly observe pool state, but if the connection
        leaks we'll eventually exhaust ``MAX_POOL_SIZE`` and a
        subsequent gather of more tasks than the pool can handle will
        hang. So: cancel, then run a follow-up gather that exceeds
        the pool size to confirm the slots came back.
        """
        from dorm.db.connection import get_async_connection

        async def long_query() -> None:
            conn = get_async_connection()
            # ``pg_sleep`` blocks server-side for the given seconds.
            await conn.execute("SELECT pg_sleep(2)")

        task = asyncio.create_task(long_query())
        await asyncio.sleep(0.1)  # let the query reach the server
        task.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await task

        # Follow-up gather that exceeds the configured pool size
        # (the test PG fixture caps MAX_POOL_SIZE=3). If the cancelled
        # task leaked its connection, we'd hang here.
        await asyncio.wait_for(
            asyncio.gather(
                *(Author.objects.acount() for _ in range(10))
            ),
            timeout=5,
        )


# ── Multiple aatomic in same task: nested savepoints ─────────────────────


class TestNestedAatomic:
    async def test_inner_rollback_does_not_undo_outer_writes(
        self, _postgres_only
    ):
        from dorm.transaction import aatomic

        async with aatomic():
            await Author.objects.acreate(name="outer", age=1)
            try:
                async with aatomic():
                    await Author.objects.acreate(name="inner-doomed", age=2)
                    raise RuntimeError("inner fails")
            except RuntimeError:
                pass

        # Outer survived; inner was rolled back.
        assert await Author.objects.filter(name="outer").aexists()
        assert not await Author.objects.filter(name="inner-doomed").aexists()


# ── Mixing sync and async on the same model ─────────────────────────────


class TestSyncAsyncInterleaving:
    async def test_async_sees_sync_committed_data(self, _postgres_only):
        """Sync code commits a row, async code reads it via ``aget``.
        The two use separate connection pools but share the DB —
        committed data must be visible immediately."""
        # Sync write outside any tx.
        Author.objects.create(name="sync-then-async", age=42)

        # Async read.
        author = await Author.objects.aget(name="sync-then-async")
        assert author.age == 42

    async def test_sync_sees_async_committed_data(self, _postgres_only):
        # Async write.
        await Author.objects.acreate(name="async-then-sync", age=1)

        # Sync read.
        a = Author.objects.get(name="async-then-sync")
        assert a.age == 1


# ── Async signals fire under gather ─────────────────────────────────────


class TestAsyncSignalsUnderGather:
    async def test_post_save_async_receivers_fire_for_every_row(
        self, _postgres_only
    ):
        """Many concurrent ``acreate`` → many concurrent
        ``post_save.asend``. Every receiver invocation must run; none
        get dropped under contention."""
        from dorm.signals import post_save

        seen: list[str] = []

        async def receiver(sender, instance, **_):
            # tiny await so multiple invocations can interleave.
            await asyncio.sleep(0)
            seen.append(instance.name)

        post_save.connect(receiver, sender=Author, weak=False)
        try:
            N = 15
            await asyncio.gather(
                *(
                    Author.objects.acreate(name=f"sigc-{i}", age=i)
                    for i in range(N)
                )
            )
        finally:
            post_save.disconnect(receiver)

        assert sorted(seen) == sorted(f"sigc-{i}" for i in range(N))


# ── Connection-pool leak: hammer create + read in series ────────────────


class TestPoolDoesNotLeak:
    async def test_serial_loop_of_300_aget_calls_does_not_exhaust_pool(
        self, _postgres_only
    ):
        """If each ``aget`` leaked its connection back to the pool
        slowly (e.g. via a finalizer), running many of them in serial
        would eventually starve. 300 sequential calls is enough to
        force a full pool turnover several times."""
        seed = await Author.objects.acreate(name="pool-loop", age=0)
        for _ in range(300):
            got = await Author.objects.aget(pk=seed.pk)
            assert got.name == "pool-loop"
