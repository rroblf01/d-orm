"""Tests for ``dorm.contrib.listen_notify``.

PostgreSQL-only feature; on other backends the helper raises
NotImplementedError. The async LISTEN test publishes from a separate
task so we exercise the cross-connection delivery path.
"""

from __future__ import annotations

import asyncio

import pytest

from dorm.contrib.listen_notify import anotify, listen, notify


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_notify_not_supported_on_sqlite(db_config):
    if _is_postgres(db_config):
        pytest.skip("test targets non-PG path")
    with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
        notify("ch", "payload")


def test_notify_pg(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    # Bare smoke test: NOTIFY without a listener is a no-op but must
    # not raise.
    notify("smoke_ch", "hi")


@pytest.mark.asyncio
async def test_listen_receives_notifications(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    received = []

    async def _publisher():
        # Give the listener time to run LISTEN. PG buffers nothing
        # before LISTEN — without this delay the message is dropped.
        await asyncio.sleep(0.2)
        await anotify("dorm_test_chan", "first")
        await anotify("dorm_test_chan", "second")

    async def _listener():
        async with listen("dorm_test_chan") as ch:
            async for n in ch:
                received.append(n.payload)
                if len(received) >= 2:
                    break

    pub = asyncio.create_task(_publisher())
    try:
        await asyncio.wait_for(_listener(), timeout=10.0)
    finally:
        await pub

    assert received == ["first", "second"]


@pytest.mark.asyncio
async def test_listen_requires_channel(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    with pytest.raises(ValueError, match="at least one channel"):
        async with listen():
            pass
