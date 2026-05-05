"""Tests for ``dorm.contrib.task_pool``.

PostgreSQL is the only backend with a real async pool; on other
backends ``pinned_connection`` is a no-op that the test verifies via
the ``vendor`` short-circuit.
"""

from __future__ import annotations

import asyncio

import pytest

from dorm.contrib.task_pool import (
    assert_no_concurrent_gather,
    get_pinned_connection,
    pinned_connection,
)
from tests.models import Author


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


@pytest.mark.asyncio
async def test_pinned_connection_noop_on_sqlite(db_config):
    if _is_postgres(db_config):
        pytest.skip("non-PG path")
    async with pinned_connection() as raw:
        assert raw is None


@pytest.mark.asyncio
async def test_pinned_connection_runs_queries_pg(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    async with pinned_connection() as raw:
        assert raw is not None
        # Pinned conn is reused for the whole block.
        await Author.objects.acreate(name="x", age=1)
        n = await Author.objects.acount()
        assert n >= 1
        assert get_pinned_connection() is raw


@pytest.mark.asyncio
async def test_pin_does_not_leak_outside_block(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    async with pinned_connection():
        assert get_pinned_connection() is not None
    # After the block, no pin survives on the same task.
    assert get_pinned_connection() is None


@pytest.mark.asyncio
async def test_sibling_tasks_have_independent_pins(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")

    pins: list[object] = []

    async def _worker():
        async with pinned_connection() as raw:
            pins.append(raw)
            await asyncio.sleep(0.05)

    await asyncio.gather(_worker(), _worker())
    assert len(pins) == 2
    assert pins[0] is not pins[1]


@pytest.mark.asyncio
async def test_assert_no_concurrent_gather_raises_when_shared(db_config):
    """When two gather siblings share a pinned connection the helper
    must complain. We simulate the in-flight count manually instead of
    racing real queries."""
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    from dorm.contrib import task_pool

    async with pinned_connection():
        # Pretend a query is in flight.
        state = task_pool._PINNED_STATE.get()
        assert state is not None
        _wrapper, _conn, counter = state
        counter[0] = 1
        try:
            with pytest.raises(RuntimeError, match="gather"):
                assert_no_concurrent_gather()
        finally:
            counter[0] = 0


@pytest.mark.asyncio
async def test_assert_no_concurrent_gather_quiet_outside_pin(db_config):
    # Should be a no-op without a pin.
    assert_no_concurrent_gather()
