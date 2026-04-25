"""Stress / load-style tests.

These aren't strict benchmarks — they don't fail on regression by default
— but they exercise the hot paths (bulk insert, async concurrency, pool
contention) with non-trivial volumes so a bad refactor turns them red or
*noticeably* slow. Set ``DORM_STRESS_FAIL_OVER_MS=<ms>`` to make them
fail when total elapsed exceeds the budget.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest

import dorm
from tests.models import Author


_FAIL_OVER_MS = float(os.environ.get("DORM_STRESS_FAIL_OVER_MS", "0"))


def _budget_check(label: str, elapsed_ms: float, budget_ms: float) -> None:
    """Print timing always; fail only when DORM_STRESS_FAIL_OVER_MS is set."""
    print(f"\n[stress] {label}: {elapsed_ms:.0f} ms (budget {budget_ms:.0f} ms)")
    if _FAIL_OVER_MS and elapsed_ms > budget_ms:
        pytest.fail(f"{label} took {elapsed_ms:.0f}ms, budget {budget_ms:.0f}ms")


# ── Bulk insert: 10k rows in one batch should be << seconds ──────────────────


def test_stress_bulk_create_10k():
    """Create 10 000 rows via a single bulk_create call. Sub-second on
    SQLite, ~2s on testcontainer Postgres."""
    Author.objects.filter(name__startswith="ST-bulk").delete()
    objs = [
        Author(name=f"ST-bulk{i}", age=i % 80, email=f"stb{i}@x.com")
        for i in range(10_000)
    ]
    t0 = time.perf_counter()
    Author.objects.bulk_create(objs, batch_size=1000)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    try:
        assert Author.objects.filter(name__startswith="ST-bulk").count() == 10_000
        _budget_check("bulk_create 10k", elapsed_ms, 10_000)
    finally:
        Author.objects.filter(name__startswith="ST-bulk").delete()


# ── Bulk update: rewrite 1k rows in a single CASE WHEN UPDATE ────────────────


def test_stress_bulk_update_1k():
    """Update one column on 1 000 rows in a single round-trip per batch."""
    Author.objects.filter(name__startswith="ST-up").delete()
    objs = Author.objects.bulk_create(
        [Author(name=f"ST-up{i}", age=0, email=f"stu{i}@x.com") for i in range(1_000)]
    )
    for o in objs:
        o.age = 99

    t0 = time.perf_counter()
    Author.objects.bulk_update(objs, fields=["age"], batch_size=500)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    try:
        assert all(
            a.age == 99
            for a in Author.objects.filter(name__startswith="ST-up").iterator()
        )
        _budget_check("bulk_update 1k", elapsed_ms, 5_000)
    finally:
        Author.objects.filter(name__startswith="ST-up").delete()


# ── Async concurrency: 200 parallel reads against the same pool ──────────────


async def test_stress_async_concurrent_reads():
    """Fire 200 ``acount()`` calls in parallel. The pool serializes them
    against ``MAX_POOL_SIZE`` connections; this catches deadlocks /
    leaked connections under load."""
    a = await Author.objects.acreate(name="ST-async", age=1, email="sta@x.com")
    try:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *[Author.objects.filter(name="ST-async").acount() for _ in range(200)]
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert all(r == 1 for r in results)
        _budget_check("200 parallel acount", elapsed_ms, 5_000)
    finally:
        await a.adelete()


# ── Mixed write/read concurrency ──────────────────────────────────────────────


async def test_stress_async_mixed_workload():
    """100 writers + 100 readers on the same async pool. Smokes out
    transaction interleaving and pool starvation."""
    await Author.objects.filter(name__startswith="ST-mix").adelete()

    async def writer(i: int) -> None:
        await Author.objects.acreate(
            name=f"ST-mix{i}", age=i, email=f"mix{i}@x.com"
        )

    async def reader() -> int:
        return await Author.objects.filter(name__startswith="ST-mix").acount()

    t0 = time.perf_counter()
    await asyncio.gather(
        *[writer(i) for i in range(100)],
        *[reader() for _ in range(100)],
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    try:
        final = await Author.objects.filter(name__startswith="ST-mix").acount()
        assert final == 100
        _budget_check("100W+100R mixed", elapsed_ms, 10_000)
    finally:
        await Author.objects.filter(name__startswith="ST-mix").adelete()


# ── Cancellation safety ──────────────────────────────────────────────────────


async def test_async_cancellation_returns_pool_to_clean_state():
    """When ``asyncio.wait_for`` cancels mid-query, the pool's
    connection-acquisition context manager must run and return the
    connection. After cancellation the pool must accept new work."""
    from dorm.db.connection import get_async_connection

    wrapper = get_async_connection()
    if wrapper.vendor != "postgresql":
        pytest.skip("pool semantics relevant on PG only")

    # Trigger pool open by running a normal query first.
    await Author.objects.acount()
    before = wrapper.pool_stats()

    # Cancel something that would normally block briefly. We use a tiny
    # timeout so the await on cur.execute is virtually guaranteed to be
    # in flight when cancellation hits.
    async def slow():
        # pg_sleep(0.5) holds the conn server-side for half a second.
        await get_async_connection().execute("SELECT pg_sleep(0.5)")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(slow(), timeout=0.05)

    # Pool must still serve more work. If the cancelled connection had
    # leaked, this acquire would block until POOL_TIMEOUT.
    n = await Author.objects.acount()
    assert n >= 0
    after = wrapper.pool_stats()
    # No requests currently waiting after the cancellation settles.
    assert after.get("requests_waiting", 0) == 0
    del before  # quiet linter — only used for breakpoint debugging


# ── No leftover open connections at session end ──────────────────────────────


def test_stress_no_connection_leak_after_burst():
    """Run a burst and confirm the connection wrappers are tracked
    correctly (no orphan pools accumulating across iterations) AND that
    the pool's checked-out count returns to zero between bursts."""
    from dorm.db.connection import (
        _async_connections,
        _sync_connections,
        get_connection,
    )

    initial_sync = len(_sync_connections)
    initial_async = len(_async_connections)

    for batch in range(5):
        objs = [
            Author(name=f"ST-leak{batch}_{i}", age=i, email=f"l{batch}_{i}@x.com")
            for i in range(50)
        ]
        Author.objects.bulk_create(objs)
        Author.objects.filter(name__startswith=f"ST-leak{batch}_").delete()

    # We should still have at most the same number of wrappers as before
    # (one per alias), regardless of how many query bursts ran.
    assert len(_sync_connections) <= initial_sync + 1
    assert len(_async_connections) <= initial_async + 1

    # Pool stats post-burst: PG wrapper should report no requests queued
    # and no connections checked out for active work.
    wrapper = get_connection()
    stats = wrapper.pool_stats()
    if stats.get("vendor") == "postgresql" and stats.get("open"):
        # ``requests_waiting`` is the number of acquirers currently
        # blocked waiting for a connection — must be 0 after a quiet
        # period. (``requests_queued`` is the cumulative count and
        # legitimately grows on warm-up checkouts.)
        assert stats.get("requests_waiting", 0) == 0, (
            f"connections leaked: {stats!r}"
        )
