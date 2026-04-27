"""Tests for async signal receivers (Signal.asend / connect coroutine fns)."""
from __future__ import annotations

import asyncio
import logging

import pytest

from dorm.signals import Signal, post_save, pre_save
from tests.models import Author


pytestmark = pytest.mark.asyncio


# ── Async Signal directly ────────────────────────────────────────────────────


async def test_asend_runs_async_receiver():
    sig = Signal()
    seen: list[tuple[str, int]] = []

    async def receiver(sender, **kwargs):
        # Yield the loop so we know we actually went through asyncio,
        # not just executed synchronously.
        await asyncio.sleep(0)
        seen.append(("async", kwargs.get("value", -1)))

    sig.connect(receiver, weak=False)
    await sig.asend(sender="x", value=7)

    assert seen == [("async", 7)]


async def test_asend_runs_sync_and_async_in_order():
    sig = Signal()
    seen: list[str] = []

    def sync_recv(sender, **kwargs):
        seen.append("sync")

    async def async_recv(sender, **kwargs):
        await asyncio.sleep(0)
        seen.append("async")

    sig.connect(sync_recv, weak=False)
    sig.connect(async_recv, weak=False)

    await sig.asend(sender="x")

    assert seen == ["sync", "async"]


async def test_send_skips_async_receiver_with_warning(caplog):
    sig = Signal()
    sync_calls: list[int] = []

    def sync_recv(sender, **kwargs):
        sync_calls.append(1)

    async def async_recv(sender, **kwargs):
        sync_calls.append(99)  # should never run via send()

    sig.connect(sync_recv, weak=False)
    sig.connect(async_recv, weak=False)

    with caplog.at_level(logging.WARNING, logger="dorm.signals"):
        responses = sig.send(sender="x")

    assert sync_calls == [1]
    assert len(responses) == 1  # only the sync receiver replied
    assert any(
        "skipped" in rec.message and "async receiver" in rec.message
        for rec in caplog.records
    )


async def test_asend_propagates_exception_when_strict():
    sig = Signal(raise_exceptions=True)

    async def bad(sender, **kwargs):
        raise ValueError("nope")

    sig.connect(bad, weak=False)

    with pytest.raises(ValueError):
        await sig.asend(sender="x")


async def test_asend_logs_and_continues_when_lenient(caplog):
    sig = Signal(raise_exceptions=False)
    after: list[int] = []

    async def bad(sender, **kwargs):
        raise RuntimeError("boom")

    async def good(sender, **kwargs):
        after.append(1)

    sig.connect(bad, weak=False)
    sig.connect(good, weak=False)

    with caplog.at_level(logging.ERROR, logger="dorm.signals"):
        await sig.asend(sender="x")

    assert after == [1]
    assert any("boom" in rec.message or "boom" in str(rec.exc_info)
               for rec in caplog.records)


async def test_asend_awaits_coroutine_returned_by_sync_receiver():
    sig = Signal()
    seen: list[int] = []

    async def helper():
        await asyncio.sleep(0)
        seen.append(42)

    def wrapping_sync(sender, **kwargs):
        return helper()

    sig.connect(wrapping_sync, weak=False)
    await sig.asend(sender="x")
    assert seen == [42]


async def test_disconnect_async_receiver():
    sig = Signal()
    seen: list[int] = []

    async def recv(sender, **kwargs):
        seen.append(1)

    sig.connect(recv, weak=False)
    sig.disconnect(recv)
    await sig.asend(sender="x")
    assert seen == []


# ── Integration: Model.asave / Model.adelete fire async receivers ────────────


async def test_async_pre_post_save_fire_on_asave():
    seen: list[tuple[str, str]] = []

    async def on_pre(sender, instance, **kwargs):
        await asyncio.sleep(0)
        seen.append(("pre", instance.name))

    async def on_post(sender, instance, created, **kwargs):
        await asyncio.sleep(0)
        seen.append(("post", instance.name + ("/new" if created else "/upd")))

    pre_save.connect(on_pre, sender=Author, weak=False)
    post_save.connect(on_post, sender=Author, weak=False)
    try:
        author = Author(name="Async-Bob", age=33)
        await author.asave()
        author.age = 34
        await author.asave()
    finally:
        pre_save.disconnect(on_pre)
        post_save.disconnect(on_post)

    assert seen == [
        ("pre", "Async-Bob"),
        ("post", "Async-Bob/new"),
        ("pre", "Async-Bob"),
        ("post", "Async-Bob/upd"),
    ]


async def test_async_post_delete_fires_on_adelete():
    seen: list[int] = []

    async def on_delete(sender, instance, **kwargs):
        await asyncio.sleep(0)
        seen.append(instance.pk or -1)

    post_save_disconnect = post_save  # silence linter; only delete here
    del post_save_disconnect

    from dorm.signals import post_delete

    post_delete.connect(on_delete, sender=Author, weak=False)
    try:
        author = await Author.objects.acreate(name="ToDelete", age=20)
        original_pk = author.pk
        await author.adelete()
    finally:
        post_delete.disconnect(on_delete)

    assert seen == [original_pk]


async def test_sync_save_skips_async_receiver_silently_for_data(caplog):
    """Connecting an async receiver and then using the *sync* save path
    should not write the sync row twice or error — it just skips the
    async receiver with a warning. Regression guard: a previous draft
    awaited the coroutine via asyncio.run and deadlocked under -n 4."""
    fired_sync = []

    def sync_recv(sender, instance, **kwargs):
        fired_sync.append(instance.name)

    async def async_recv(sender, instance, **kwargs):  # never runs here
        fired_sync.append("ASYNC-RAN-IN-SYNC")  # pragma: no cover

    post_save.connect(sync_recv, sender=Author, weak=False)
    post_save.connect(async_recv, sender=Author, weak=False)
    try:
        with caplog.at_level(logging.WARNING, logger="dorm.signals"):
            # Note: from inside an async test, ``Author.objects.create`` is
            # the sync path even though our event loop is running.
            Author.objects.create(name="SyncOnly", age=10)
    finally:
        post_save.disconnect(sync_recv)
        post_save.disconnect(async_recv)

    assert fired_sync == ["SyncOnly"]
    assert any(
        "skipped" in r.message and "async receiver" in r.message
        for r in caplog.records
    )
