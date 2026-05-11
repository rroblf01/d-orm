"""Tier-5 obs/perf features for v4.3."""
from __future__ import annotations

import pytest


# ── Plan-drift history ──────────────────────────────────────────────────────


class TestPlanDriftHistory:
    def test_history_records_comparisons(self, tmp_path):
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
        db = tmp_path / "pdh.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _PDH(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with SchemaEditor(get_connection()) as se:
            se.create_model(_PDH)

        try:
            plan_drift.reset()
            plan_drift.clear_history()
            sql = f"SELECT * FROM {_PDH._meta.db_table} WHERE name = ?"
            plan_drift.record_baseline("h.tag", sql, params=["a"])
            for _ in range(3):
                plan_drift.compare("h.tag", sql, params=["a"])
            hist = plan_drift.history("h.tag")
            assert len(hist) == 3
            assert all(r.tag == "h.tag" for r in hist)
        finally:
            plan_drift.reset()
            plan_drift.clear_history()
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    def test_clear_history_per_tag(self):
        from dorm.contrib import plan_drift

        # Build minimal history without DB by injecting via helper.
        plan_drift.clear_history()
        plan_drift._HISTORY.setdefault("a", []).append(
            plan_drift.CompareResult(tag="a", baseline="x", current="x", drifted=False)
        )
        plan_drift._HISTORY.setdefault("b", []).append(
            plan_drift.CompareResult(tag="b", baseline="x", current="x", drifted=False)
        )
        assert len(plan_drift.history("a")) == 1
        assert len(plan_drift.history("b")) == 1
        plan_drift.clear_history("a")
        assert plan_drift.history("a") == []
        assert len(plan_drift.history("b")) == 1
        plan_drift.clear_history()
        assert plan_drift.history() == []


# ── Pool warmup metric ──────────────────────────────────────────────────────


class TestPoolWarmupMetric:
    def test_record_and_render(self):
        from dorm.contrib import prometheus

        prometheus.record_pool_warmup("alias-w", 0.25)
        text = prometheus.metrics_response()
        assert "dorm_pool_warmup_seconds" in text
        assert "alias-w" in text


# ── Row-level cache ─────────────────────────────────────────────────────────


class TestRowCache:
    def test_cache_returns_same_instance_on_hit(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.row_cache import RowCache
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
        db = tmp_path / "rc.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _RC(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with SchemaEditor(get_connection()) as se:
            se.create_model(_RC)

        try:
            cache = RowCache(_RC, maxsize=10)
            try:
                row = _RC.objects.create(name="x")
                # First fetch populates the cache.
                a = cache.get(row.pk)
                b = cache.get(row.pk)
                assert a is b
            finally:
                cache.detach()
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    def test_invalidate_on_save(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.row_cache import RowCache
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
        db = tmp_path / "rcs.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _RCS(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with SchemaEditor(get_connection()) as se:
            se.create_model(_RCS)

        try:
            cache = RowCache(_RCS)
            try:
                row = _RCS.objects.create(name="x")
                cache.get(row.pk)
                assert len(cache) == 1
                # Save fires post_save → cache invalidated.
                row.name = "y"
                row.save()
                assert len(cache) == 0
            finally:
                cache.detach()
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    def test_maxsize_evicts_lru(self):
        import dorm
        from dorm.contrib.row_cache import RowCache

        class _LRU(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        cache = RowCache(_LRU, maxsize=2, invalidate_on_write=False)
        cache._put(1, _LRU(name="a"))
        cache._put(2, _LRU(name="b"))
        cache._put(3, _LRU(name="c"))  # evicts 1
        assert len(cache) == 2
        cache.detach()

    def test_invalid_maxsize_rejected(self):
        import dorm
        from dorm.contrib.row_cache import RowCache

        class _M(dorm.Model):
            class Meta:
                app_label = "tests"

        with pytest.raises(ValueError):
            RowCache(_M, maxsize=0)


# ── N+1 suggestions ─────────────────────────────────────────────────────────


class TestNPlusOneSuggest:
    def test_fk_suggestion(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix('SELECT "x"."id" FROM "x" WHERE "x"."author_id" = ?')
        assert "select_related" in msg
        assert "'author'" in msg

    def test_reverse_in_suggestion(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix(
            'SELECT "x"."id" FROM "x" WHERE "x"."author_id" IN (?, ?, ?)'
        )
        assert "prefetch_related" in msg
        assert "'author'" in msg

    def test_unrecognised_template_returns_generic(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix("SELECT 1")
        assert "select_related" in msg or "prefetch_related" in msg

    def test_model_hint_included(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix(
            'SELECT "x"."id" FROM "x" WHERE "x"."author_id" = ?',
            model_hint="Book",
        )
        assert "Book" in msg
