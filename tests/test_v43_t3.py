"""Tier-3 features for v4.3: sync DataLoader + BackgroundTasks."""
from __future__ import annotations


import pytest


# ── SyncDataLoader ──────────────────────────────────────────────────────────


class TestSyncDataLoader:
    def test_load_returns_none_before_flush(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {k: k for k in ks})
        assert loader.load(1) is None
        assert loader.load(2) is None
        # Pending list captured.
        assert 1 in loader._pending and 2 in loader._pending

    def test_flush_resolves_pending(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {k: k * 10 for k in ks})
        loader.load(1)
        loader.load(2)
        out = loader.flush()
        assert out == {1: 10, 2: 20}
        # Cache populated.
        assert loader.load(1) == 10
        assert loader.load(2) == 20

    def test_flush_empty_returns_empty_dict(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {})
        assert loader.flush() == {}

    def test_max_batch_size_chunks(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        calls: list[int] = []

        def _batch(ks):
            calls.append(len(ks))
            return {k: k for k in ks}

        loader = SyncDataLoader(_batch, max_batch_size=2)
        for k in (1, 2, 3, 4, 5):
            loader.load(k)
        loader.flush()
        assert max(calls) <= 2
        assert sum(calls) == 5

    def test_prime_seeds_cache(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {})
        loader.prime(99, "primed")
        assert loader.load(99) == "primed"

    def test_clear_all(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {k: k for k in ks})
        loader.load(1)
        loader.flush()
        loader.clear_all()
        assert loader.load(1) is None

    def test_missing_sentinel(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: {}, missing="N/A")
        # `get` of unresolved key returns the sentinel.
        assert loader.get(42) == "N/A"

    def test_iterable_of_tuples(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: [(k, str(k)) for k in ks])
        loader.load(3)
        loader.flush()
        assert loader.load(3) == "3"

    def test_invalid_max_batch_size_rejected(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        with pytest.raises(ValueError):
            SyncDataLoader(lambda ks: {}, max_batch_size=0)

    def test_non_iterable_result_rejected(self):
        from dorm.contrib.sync_dataloader import SyncDataLoader

        loader = SyncDataLoader(lambda ks: 42)  # type: ignore[return-value, arg-type]
        loader.load(1)
        with pytest.raises(TypeError, match="batch_fn"):
            loader.flush()


# ── BackgroundTasks ─────────────────────────────────────────────────────────


class TestBackgroundTasks:
    async def test_runs_scheduled_tasks(self):
        from dorm.contrib.background import BackgroundTasks

        bg = BackgroundTasks(concurrency=4)
        results: list[int] = []

        async def _job(n: int) -> int:
            results.append(n)
            return n

        bg.add(_job, 1)
        bg.add(_job, 2)
        bg.add(_job, 3)
        out = await bg.run()
        assert sorted(results) == [1, 2, 3]
        assert sorted(out) == [1, 2, 3]

    async def test_swallows_exceptions_by_default(self, caplog):
        import logging

        from dorm.contrib.background import BackgroundTasks

        bg = BackgroundTasks(concurrency=2)

        async def _ok():
            return "ok"

        async def _bad():
            raise RuntimeError("nope")

        bg.add(_ok)
        bg.add(_bad)
        with caplog.at_level(logging.WARNING, logger="dorm.contrib.background"):
            out = await bg.run()
        assert out == ["ok"]
        assert any("background task failed" in r.message for r in caplog.records)

    async def test_raises_when_swallow_false(self):
        from dorm.contrib.background import BackgroundTasks

        bg = BackgroundTasks()

        async def _bad():
            raise RuntimeError("boom")

        bg.add(_bad)
        with pytest.raises(RuntimeError, match="boom"):
            await bg.run(swallow_exceptions=False)

    async def test_concurrency_respected(self):
        import asyncio as _asyncio

        from dorm.contrib.background import BackgroundTasks

        bg = BackgroundTasks(concurrency=2)
        in_flight = {"current": 0, "peak": 0}

        async def _slow():
            in_flight["current"] += 1
            in_flight["peak"] = max(in_flight["peak"], in_flight["current"])
            await _asyncio.sleep(0.01)
            in_flight["current"] -= 1

        for _ in range(10):
            bg.add(_slow)
        await bg.run()
        assert in_flight["peak"] <= 2

    def test_invalid_concurrency_rejected(self):
        from dorm.contrib.background import BackgroundTasks

        with pytest.raises(ValueError):
            BackgroundTasks(concurrency=0)
