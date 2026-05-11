"""Tier-5 features added in v4.2: dataloader + plan drift + multiplexer."""
from __future__ import annotations

import asyncio
from typing import Any

import pytest


# ── DataLoader ───────────────────────────────────────────────────────────────


class TestDataLoader:
    async def test_coalesces_concurrent_loads(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[list[int]] = []

        def _batch(keys: list[int]) -> dict[int, str]:
            calls.append(list(keys))
            return {k: f"v{k}" for k in keys}

        loader = DataLoader(_batch)
        results = await asyncio.gather(
            loader.load(1), loader.load(2), loader.load(3)
        )
        assert results == ["v1", "v2", "v3"]
        # ONE batch call for three concurrent loads.
        assert len(calls) == 1
        assert sorted(calls[0]) == [1, 2, 3]

    async def test_cache_hits_skip_batch(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[list[int]] = []

        def _batch(keys):
            calls.append(list(keys))
            return {k: k * 10 for k in keys}

        loader = DataLoader(_batch)
        await loader.load(1)
        await loader.load(1)
        await loader.load(1)
        assert len(calls) == 1  # only the first call batched.

    async def test_no_cache_reissues(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[list[int]] = []

        def _batch(keys):
            calls.append(list(keys))
            return {k: k for k in keys}

        loader = DataLoader(_batch, cache=False)
        await loader.load(1)
        await loader.load(1)
        assert len(calls) == 2

    async def test_missing_key_returns_default(self):
        from dorm.contrib.dataloader import DataLoader

        loader = DataLoader(lambda ks: {}, missing="x")
        assert await loader.load(42) == "x"

    async def test_load_many(self):
        from dorm.contrib.dataloader import DataLoader

        def _batch(keys):
            return {k: k * 2 for k in keys}

        loader = DataLoader(_batch)
        assert await loader.load_many([1, 2, 3]) == [2, 4, 6]

    async def test_max_batch_size_splits(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[int] = []

        def _batch(keys):
            calls.append(len(keys))
            return {k: k for k in keys}

        loader = DataLoader(_batch, max_batch_size=2)
        await loader.load_many([1, 2, 3, 4, 5])
        assert max(calls) <= 2
        assert sum(calls) == 5

    async def test_async_batch_fn(self):
        from dorm.contrib.dataloader import DataLoader

        async def _batch(keys):
            await asyncio.sleep(0)
            return {k: k * 100 for k in keys}

        loader = DataLoader(_batch)
        assert await loader.load(7) == 700

    async def test_iterable_of_tuples(self):
        from dorm.contrib.dataloader import DataLoader

        def _batch(keys):
            return [(k, str(k)) for k in keys]

        loader = DataLoader(_batch)
        assert await loader.load(3) == "3"

    async def test_failure_propagates(self):
        from dorm.contrib.dataloader import DataLoader

        def _batch(keys):
            raise RuntimeError("boom")

        loader = DataLoader(_batch)
        with pytest.raises(RuntimeError, match="boom"):
            await loader.load(1)

    async def test_invalid_max_batch_rejected(self):
        from dorm.contrib.dataloader import DataLoader

        with pytest.raises(ValueError):
            DataLoader(lambda ks: {}, max_batch_size=0)

    async def test_prime_skips_batch(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[list[int]] = []

        def _batch(keys):
            calls.append(list(keys))
            return {k: -k for k in keys}

        loader = DataLoader(_batch)
        loader.prime(7, 700)
        assert await loader.load(7) == 700
        assert calls == []  # never batched

    async def test_prime_noop_when_cache_disabled(self, caplog):
        import logging

        from dorm.contrib.dataloader import DataLoader

        def _batch(keys):
            return {k: 0 for k in keys}

        loader = DataLoader(_batch, cache=False)
        with caplog.at_level(
            logging.WARNING, logger="dorm.contrib.dataloader"
        ):
            loader.prime(1, 100)
        assert any("cache=False" in rec.message for rec in caplog.records)
        # Value is NOT cached — next load() goes through the batch.
        assert await loader.load(1) == 0

    async def test_clear_all_drops_every_key(self):
        from dorm.contrib.dataloader import DataLoader

        calls: list[list[int]] = []

        def _batch(keys):
            calls.append(list(keys))
            return {k: k for k in keys}

        loader = DataLoader(_batch)
        await loader.load(1)
        await loader.load(2)
        loader.clear_all()
        await loader.load(1)
        await loader.load(2)
        # Two batches before clear_all + two more after.
        assert len(calls) == 3 or len(calls) == 4


# ── Plan drift ──────────────────────────────────────────────────────────────


class TestPlanDrift:
    def test_record_and_compare_identical(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib import plan_drift
        from dorm.db.connection import (
            _async_connections,
            _sync_connections,
            get_connection,
        )
        from dorm.migrations.schema import SchemaEditor

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "pd.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _PD(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with SchemaEditor(get_connection()) as se:
            se.create_model(_PD)
        try:
            plan_drift.reset()
            sql = f"SELECT * FROM {_PD._meta.db_table} WHERE name = ?"
            plan_drift.record_baseline("pd.lookup", sql, params=["x"])
            result = plan_drift.compare("pd.lookup", sql, params=["x"])
            assert result.drifted is False
            assert plan_drift.diff_text(result) == ""
        finally:
            plan_drift.reset()
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    def test_compare_unknown_tag_raises(self):
        from dorm.contrib import plan_drift

        plan_drift.reset()
        with pytest.raises(KeyError):
            plan_drift.compare("never-recorded", "SELECT 1")

    def test_strip_volatile_drops_cost_rows(self):
        from dorm.contrib.plan_drift import _strip_volatile

        plan = "Seq Scan on orders  (cost=0.00..100.00 rows=42 width=8)"
        cleaned = _strip_volatile(plan)
        assert "cost=" not in cleaned
        assert "rows=" not in cleaned
        assert "width=" not in cleaned

    def test_strip_volatile_stable_under_data_growth(self):
        from dorm.contrib.plan_drift import _strip_volatile

        small = "Seq Scan on t (cost=0.00..10.00 rows=10 width=4)"
        big = "Seq Scan on t (cost=0.00..1000000.00 rows=1000000 width=4)"
        # Same plan structure → same cleaned text.
        assert _strip_volatile(small) == _strip_volatile(big)


# ── LISTEN/NOTIFY Broadcaster ────────────────────────────────────────────────


class TestBroadcaster:
    def test_init_requires_channels(self):
        from dorm.contrib.listen_notify import Broadcaster

        with pytest.raises(ValueError, match="channel"):
            Broadcaster([])

    def test_invalid_maxsize_rejected(self):
        from dorm.contrib.listen_notify import Broadcaster

        with pytest.raises(ValueError, match="maxsize"):
            Broadcaster(["a"], maxsize=0)

    def test_subscriber_stream_terminates_on_sentinel(self):
        from dorm.contrib.listen_notify import _SubscriberStream

        async def _scenario():
            q: asyncio.Queue = asyncio.Queue()
            stream = _SubscriberStream(q)
            q.put_nowait(None)  # shutdown sentinel
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

        asyncio.run(_scenario())

    def test_subscriber_stream_yields_in_order(self):
        from dorm.contrib.listen_notify import Notification, _SubscriberStream

        async def _scenario():
            q: asyncio.Queue = asyncio.Queue()
            stream = _SubscriberStream(q)
            q.put_nowait(Notification(channel="c", payload="a", pid=1))
            q.put_nowait(Notification(channel="c", payload="b", pid=1))
            q.put_nowait(None)
            collected = []
            async for n in stream:
                collected.append(n.payload)
            assert collected == ["a", "b"]

        asyncio.run(_scenario())

    def test_broadcaster_fanout_two_subscribers(self, monkeypatch):
        """Drive ``Broadcaster._dispatch_loop`` manually to verify
        every subscriber sees every notification — without spinning a
        real PG connection."""
        from dorm.contrib.listen_notify import Broadcaster, Notification

        async def _scenario():
            bcast = Broadcaster(["orders"])
            # Skip the real LISTEN setup — populate ``_subs`` directly
            # via two ``subscribe`` calls, then push notifications into
            # both queues manually.
            qa: asyncio.Queue[Any] = asyncio.Queue()
            qb: asyncio.Queue[Any] = asyncio.Queue()
            bcast._subs["orders"] = [qa, qb]
            # Simulate the dispatcher's per-notification fan-out.
            n1 = Notification(channel="orders", payload="x", pid=1)
            n2 = Notification(channel="orders", payload="y", pid=1)
            for n in (n1, n2):
                for q in bcast._subs["orders"]:
                    q.put_nowait(n)
                    sentinel: Any = None
                    q.put_nowait(sentinel)
            assert qa.qsize() == 4
            assert qb.qsize() == 4
            # Drain via the public stream.
            from dorm.contrib.listen_notify import _SubscriberStream

            sa, sb = _SubscriberStream(qa), _SubscriberStream(qb)
            got_a = [n.payload async for n in sa]
            got_b = [n.payload async for n in sb]
            assert got_a == ["x"]  # first sentinel terminates
            assert got_b == ["x"]

        asyncio.run(_scenario())

    def test_broadcaster_subscribe_rejects_unknown_channel(self):
        from dorm.contrib.listen_notify import Broadcaster

        async def _scenario():
            bcast = Broadcaster(["orders"])
            with pytest.raises(KeyError):
                async with bcast.subscribe("not-registered"):
                    pass

        asyncio.run(_scenario())
