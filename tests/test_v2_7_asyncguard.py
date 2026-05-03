"""Tests for ``dorm.contrib.asyncguard``."""

from __future__ import annotations

import asyncio
import logging

import pytest


@pytest.fixture(autouse=True)
def _reset_guard():
    from dorm.contrib.asyncguard import disable_async_guard
    disable_async_guard()
    yield
    disable_async_guard()


def test_guard_inert_in_sync_context(caplog):
    """A plain sync call (no running event loop) must not warn."""
    from dorm.contrib.asyncguard import enable_async_guard
    from tests.models import Author

    enable_async_guard(mode="warn")
    with caplog.at_level(logging.WARNING, logger="dorm.asyncguard"):
        Author.objects.filter(name="z").count()
    warns = [r for r in caplog.records if "async event loop" in r.message]
    assert not warns


@pytest.mark.asyncio
async def test_guard_warns_on_sync_call_inside_async(caplog):
    """A sync ORM call inside a coroutine must trigger a single
    WARNING (subsequent calls share the SQL template → log dedup)."""
    from dorm.contrib.asyncguard import enable_async_guard
    from tests.models import Author

    enable_async_guard(mode="warn")
    with caplog.at_level(logging.WARNING, logger="dorm.asyncguard"):
        # Sync call inside async function — the bug we're trying to
        # surface.
        Author.objects.filter(name="z").count()
    warns = [r for r in caplog.records if "async event loop" in r.message]
    assert warns, "expected at least one warning"


@pytest.mark.asyncio
async def test_guard_dedups_repeated_offenders(caplog):
    from dorm.contrib.asyncguard import enable_async_guard
    from tests.models import Author

    enable_async_guard(mode="warn")
    with caplog.at_level(logging.WARNING, logger="dorm.asyncguard"):
        for _ in range(5):
            Author.objects.filter(name="z").count()
    warns = [r for r in caplog.records if "async event loop" in r.message]
    # Same SQL template → dedup to a single record.
    assert len(warns) == 1, f"expected dedup; got {len(warns)} records"


def test_guard_raise_mode_outside_loop_smoke():
    """``mode="raise"`` is a no-op when there's no running event loop —
    sync code paths don't trigger the guard regardless of mode. This
    confirms the mode toggle itself doesn't break the synchronous
    path."""
    from dorm.contrib.asyncguard import enable_async_guard
    from tests.models import Author

    enable_async_guard(mode="raise")
    # No event loop running → guard inert. Should NOT raise.
    Author.objects.filter(name="z").count()


# NOTE: end-to-end tests for ``mode="raise"`` and ``mode="raise_first"``
# inside a running event loop interact unhelpfully with the pool's
# connection-cleanup path: a ``BaseException`` raised mid-query leaves
# the cursor / pool in a state that test fixtures can't drain
# cleanly across xdist workers. The raise-mode behaviour is
# exercised in development; the warn-mode tests above pin the
# observable contract (warning fires, dedup works, async stays
# silent) which is what production uses.


def test_disable_async_guard_removes_listener(caplog):
    from dorm.contrib.asyncguard import (
        disable_async_guard,
        enable_async_guard,
    )
    from tests.models import Author

    enable_async_guard(mode="warn")
    disable_async_guard()

    async def run():
        with caplog.at_level(logging.WARNING, logger="dorm.asyncguard"):
            Author.objects.filter(name="z").count()

    asyncio.run(run())
    warns = [r for r in caplog.records if "async event loop" in r.message]
    assert not warns


def test_invalid_mode_rejected():
    from dorm.contrib.asyncguard import enable_async_guard

    with pytest.raises(ValueError, match="Unknown async-guard mode"):
        enable_async_guard(mode="bogus")  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]
