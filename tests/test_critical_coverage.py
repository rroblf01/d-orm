"""Tests for critical untested code paths identified in coverage audit.

1. Signal._live_receivers() — weak reference pruning
2. cache.invalidation._ado_drop() — async cache invalidation
3. db.connection._atexit_close() — process shutdown
4. ExclusionConstraint — SQL generation
"""
from __future__ import annotations

import gc
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── Signal._live_receivers() ────────────────────────────────────────────────


class TestSignalLiveReceivers:
    """Tests for weak-reference pruning in Signal._live_receivers()."""

    def test_weak_ref_pruned_after_gc(self):
        """A weakly-connected receiver should be pruned after the object
        it references is garbage-collected."""
        from dorm.signals import Signal

        signal = Signal()

        class Handler:
            def __call__(self, **kwargs):
                return "kept"

        obj = Handler()
        signal.connect(obj, weak=True)
        assert len(signal._receivers) == 1

        # Drop the reference and force GC
        del obj
        gc.collect()

        callables, live = signal._live_receivers(sender=None)
        assert callables == []
        assert live == []  # pruned from _receivers

    def test_weak_ref_survives_with_reference_held(self):
        """A weakly-connected receiver should survive when the object
        is still referenced."""
        from dorm.signals import Signal

        signal = Signal()

        class Handler:
            def __call__(self, **kwargs):
                return "kept"

        obj = Handler()
        signal.connect(obj, weak=True)
        assert len(signal._receivers) == 1

        callables, live = signal._live_receivers(sender=None)
        assert len(callables) == 1
        assert callables[0]() == "kept"
        assert len(live) == 1

    def test_strong_ref_never_pruned(self):
        """A strongly-connected receiver should never be pruned by GC."""
        from dorm.signals import Signal

        signal = Signal()

        def handler(**kwargs):
            return "strong"

        signal.connect(handler, weak=False)
        assert len(signal._receivers) == 1

        callables, live = signal._live_receivers(sender=None)
        assert len(callables) == 1
        assert callables[0] is handler
        assert len(live) == 1

    def test_sender_filter_skips_non_matching(self):
        """Receivers registered for a specific sender should be skipped
        when dispatching for a different sender."""
        from dorm.signals import Signal

        signal = Signal()

        def handler_a(**kwargs):
            return "a"

        def handler_b(**kwargs):
            return "b"

        class SenderA:
            pass

        class SenderB:
            pass

        signal.connect(handler_a, sender=SenderA, weak=False)
        signal.connect(handler_b, sender=SenderB, weak=False)

        # Dispatch for SenderA — only handler_a should fire
        callables, live = signal._live_receivers(sender=SenderA)
        assert len(callables) == 1
        assert callables[0] is handler_a
        # Both should remain in live (both survive the filter)
        assert len(live) == 2

    def test_mixed_weak_strong_with_gc(self):
        """Mixed weak + strong receivers: only the weak one is pruned."""
        from dorm.signals import Signal

        signal = Signal()

        class WeakHandler:
            def __call__(self, **kwargs):
                return "weak"

        def strong_handler(**kwargs):
            return "strong"

        obj = WeakHandler()
        signal.connect(obj, weak=True)
        signal.connect(strong_handler, weak=False)

        assert len(signal._receivers) == 2

        # Drop the weak reference
        del obj
        gc.collect()

        callables, live = signal._live_receivers(sender=None)
        assert len(callables) == 1
        assert callables[0] is strong_handler
        assert len(live) == 1

    def test_live_receivers_updates_internal_state(self):
        """_live_receivers should prune dead entries from _receivers
        when called via send()."""
        from dorm.signals import Signal

        signal = Signal()

        class Handler:
            def __call__(self, **kwargs):
                return "ok"

        obj = Handler()
        signal.connect(obj, weak=True)
        assert len(signal._receivers) == 1

        del obj
        gc.collect()

        # send() calls _live_receivers and updates self._receivers
        responses = signal.send(sender=None)
        assert responses == []
        assert len(signal._receivers) == 0  # pruned


# ── cache.invalidation._ado_drop() ──────────────────────────────────────────


class TestAdoDrop:
    """Tests for the async cache invalidation path."""

    @pytest.mark.asyncio
    async def test_ado_drop_bumps_version_and_deletes_pattern(self):
        """_ado_drop should bump the cache version and call
        adelete_pattern for each configured cache."""
        from dorm.cache import invalidation

        with patch.object(invalidation, "model_cache_namespace", return_value="test.Model"), \
             patch("dorm.cache.bump_model_cache_version") as mock_bump, \
             patch("dorm.cache.invalidation.get_cache") as mock_get_cache, \
             patch("dorm.conf.settings") as mock_settings:

            mock_settings.CACHES = {"default": {}}
            mock_backend = AsyncMock()
            mock_get_cache.return_value = mock_backend

            await invalidation._ado_drop(sender=MagicMock)

            mock_bump.assert_called_once()
            mock_backend.adelete_pattern.assert_awaited_once_with("test.Model:*")

    @pytest.mark.asyncio
    async def test_ado_drop_handles_missing_caches(self):
        """_ado_drop should not crash when CACHES is empty or missing."""
        from dorm.cache import invalidation

        with patch.object(invalidation, "model_cache_namespace", return_value="test.Model"), \
             patch("dorm.cache.bump_model_cache_version"), \
             patch("dorm.conf.settings") as mock_settings:

            mock_settings.CACHES = {}
            # Should not raise
            await invalidation._ado_drop(sender=MagicMock())

    @pytest.mark.asyncio
    async def test_ado_drop_tolerates_backend_failure(self):
        """_ado_drop should swallow exceptions from adelete_pattern
        so a Redis outage never crashes a save."""
        from dorm.cache import invalidation

        with patch.object(invalidation, "model_cache_namespace", return_value="test.Model"), \
             patch("dorm.cache.bump_model_cache_version"), \
             patch("dorm.cache.invalidation.get_cache") as mock_get_cache, \
             patch("dorm.conf.settings") as mock_settings:

            mock_settings.CACHES = {"default": {}}
            mock_backend = AsyncMock()
            mock_backend.adelete_pattern.side_effect = RuntimeError("Redis down")
            mock_get_cache.return_value = mock_backend

            # Should not raise
            await invalidation._ado_drop(sender=MagicMock())

    @pytest.mark.asyncio
    async def test_ado_drop_tolerates_version_bump_failure(self):
        """_ado_drop should swallow exceptions during version bump."""
        from dorm.cache import invalidation

        with patch("dorm.cache.bump_model_cache_version", side_effect=RuntimeError("boom")), \
             patch("dorm.conf.settings") as mock_settings:

            mock_settings.CACHES = {"default": {}}
            # Should not raise
            await invalidation._ado_drop(sender=MagicMock())


# ── db.connection._atexit_close() ───────────────────────────────────────────


class TestAtexitClose:
    """Tests for process shutdown cleanup."""

    def test_atexit_close_clears_async_connections(self):
        """_atexit_close should clear the async connections dict."""
        from dorm.db import connection

        original = dict(connection._async_connections)
        try:
            connection._async_connections["test"] = MagicMock()
            connection._atexit_close()
            assert len(connection._async_connections) == 0
        finally:
            connection._async_connections.update(original)

    def test_atexit_close_calls_force_close_sync(self):
        """_atexit_close should call force_close_sync on async wrappers
        that have it."""
        from dorm.db import connection

        original = dict(connection._async_connections)
        try:
            mock_conn = MagicMock()
            mock_conn.force_close_sync = MagicMock()
            connection._async_connections["test"] = mock_conn
            connection._atexit_close()
            mock_conn.force_close_sync.assert_called_once()
            assert len(connection._async_connections) == 0
        finally:
            connection._async_connections.update(original)

    def test_atexit_close_tolerates_force_close_failure(self):
        """_atexit_close should swallow exceptions from force_close_sync
        so cleanup continues for other connections."""
        from dorm.db import connection

        original = dict(connection._async_connections)
        try:
            mock_conn1 = MagicMock()
            mock_conn1.force_close_sync = MagicMock(side_effect=RuntimeError("boom"))
            mock_conn2 = MagicMock()
            mock_conn2.force_close_sync = MagicMock()
            connection._async_connections["fail"] = mock_conn1
            connection._async_connections["ok"] = mock_conn2

            connection._atexit_close()

            mock_conn1.force_close_sync.assert_called_once()
            mock_conn2.force_close_sync.assert_called_once()
            assert len(connection._async_connections) == 0
        finally:
            connection._async_connections.update(original)

    def test_atexit_close_handles_wrappers_without_force_close_sync(self):
        """_atexit_close should skip async wrappers that don't have
        force_close_sync."""
        from dorm.db import connection

        original = dict(connection._async_connections)
        try:
            mock_conn = MagicMock(spec=[])  # no force_close_sync attribute
            connection._async_connections["test"] = mock_conn
            connection._atexit_close()
            assert len(connection._async_connections) == 0
        finally:
            connection._async_connections.update(original)


# ── ExclusionConstraint ─────────────────────────────────────────────────────


class TestExclusionConstraint:
    """Tests for ExclusionConstraint SQL generation and validation."""

    def test_basic_pg_sql(self):
        """Basic EXCLUDE constraint generates correct PostgreSQL SQL."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="no_overlap",
            expressions=[("room_id", "="), ("slot", "&&")],
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("reservations", mock_conn)
        assert "EXCLUDE USING gist" in sql
        assert '"room_id" WITH =' in sql
        assert '"slot" WITH &&' in sql
        assert 'ALTER TABLE "reservations"' in sql
        assert 'ADD CONSTRAINT "no_overlap"' in sql

    def test_non_pg_returns_empty(self):
        """Non-PostgreSQL backends return empty string."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="no_overlap",
            expressions=[("room_id", "="), ("slot", "&&")],
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "sqlite"
        assert c.constraint_sql("reservations", mock_conn) == ""

    def test_remove_sql_pg(self):
        """remove_sql returns correct DROP for PostgreSQL."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="no_overlap",
            expressions=[("room_id", "="), ("slot", "&&")],
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.remove_sql("reservations", mock_conn)
        assert sql == 'ALTER TABLE "reservations" DROP CONSTRAINT IF EXISTS "no_overlap"'

    def test_remove_sql_non_pg(self):
        """Non-PostgreSQL backends return empty string for remove_sql."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="no_overlap",
            expressions=[("room_id", "=")],
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "sqlite"
        assert c.remove_sql("reservations", mock_conn) == ""

    def test_spgist_index_type(self):
        """spgist index type is accepted and rendered."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="spgist_test",
            expressions=[("range_col", "&&")],
            index_type="spgist",
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("t", mock_conn)
        assert "USING spgist" in sql

    def test_btree_index_type(self):
        """btree index type is accepted and rendered."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="btree_test",
            expressions=[("col1", "=")],
            index_type="btree",
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("t", mock_conn)
        assert "USING btree" in sql

    def test_deferred_deferrable(self):
        """DEFERRABLE INITIALLY DEFERRED is rendered."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="def_test",
            expressions=[("col", "=")],
            deferrable="deferred",
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("t", mock_conn)
        assert "DEFERRABLE INITIALLY DEFERRED" in sql

    def test_immediate_deferrable(self):
        """DEFERRABLE INITIALLY IMMEDIATE is rendered."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="imm_test",
            expressions=[("col", "=")],
            deferrable="immediate",
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("t", mock_conn)
        assert "DEFERRABLE INITIALLY IMMEDIATE" in sql

    def test_condition_with_q_object(self):
        """A Q condition adds a WHERE clause."""
        from dorm.constraints import ExclusionConstraint
        from dorm.expressions import Q

        c = ExclusionConstraint(
            name="cond_test",
            expressions=[("col", "=")],
            condition=Q(active=True),
        )
        mock_conn = MagicMock()
        mock_conn.vendor = "postgresql"
        sql = c.constraint_sql("t", mock_conn)
        assert "WHERE" in sql

    def test_empty_expressions_raises(self):
        """Empty expressions list raises ImproperlyConfigured."""
        from dorm.constraints import ExclusionConstraint
        from dorm.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured, match="at least one"):
            ExclusionConstraint(name="bad", expressions=[])

    def test_invalid_index_type_raises(self):
        """Invalid index_type raises ImproperlyConfigured."""
        from dorm.constraints import ExclusionConstraint
        from dorm.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured, match="gist"):
            ExclusionConstraint(
                name="bad", expressions=[("col", "=")], index_type="hash"
            )

    def test_invalid_operator_raises(self):
        """Operator with unexpected characters raises ImproperlyConfigured."""
        from dorm.constraints import ExclusionConstraint
        from dorm.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured, match="unexpected characters"):
            ExclusionConstraint(
                name="bad", expressions=[("col", "'; DROP TABLE")]
            )

    def test_invalid_condition_type_raises(self):
        """Non-Q condition raises ImproperlyConfigured."""
        from dorm.constraints import ExclusionConstraint
        from dorm.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured, match="Q object"):
            ExclusionConstraint(
                name="bad",
                expressions=[("col", "=")],
                condition="not a q",  # ty: ignore[invalid-argument-type]
            )

    def test_invalid_deferrable_raises(self):
        """Invalid deferrable value raises ImproperlyConfigured."""
        from dorm.constraints import ExclusionConstraint
        from dorm.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured, match="deferred"):
            ExclusionConstraint(
                name="bad",
                expressions=[("col", "=")],
                deferrable="wrong",
            )

    def test_describe(self):
        """describe() returns a readable summary."""
        from dorm.constraints import ExclusionConstraint

        c = ExclusionConstraint(
            name="desc_test",
            expressions=[("a", "="), ("b", "&&")],
        )
        d = c.describe()
        assert "desc_test" in d
        assert "[('a', '=')]" in d or "expressions=" in d

    def test_eq_same(self):
        """Two identical constraints are equal."""
        from dorm.constraints import ExclusionConstraint

        a = ExclusionConstraint(name="x", expressions=[("c", "=")])
        b = ExclusionConstraint(name="x", expressions=[("c", "=")])
        assert a == b

    def test_eq_different_name(self):
        """Different names means not equal."""
        from dorm.constraints import ExclusionConstraint

        a = ExclusionConstraint(name="x", expressions=[("c", "=")])
        b = ExclusionConstraint(name="y", expressions=[("c", "=")])
        assert a != b

    def test_eq_different_expressions_same_name(self):
        """Same name but different expressions: BaseConstraint.__eq__
        only compares name (existing behavior)."""
        from dorm.constraints import ExclusionConstraint

        a = ExclusionConstraint(name="x", expressions=[("c", "=")])
        b = ExclusionConstraint(name="x", expressions=[("d", "=")])
        # BaseConstraint.__eq__ only checks type + name, so these ARE equal
        assert a == b

    def test_hash_consistent_with_eq(self):
        """hash is consistent with __eq__ (same name = same hash)."""
        from dorm.constraints import ExclusionConstraint

        a = ExclusionConstraint(name="x", expressions=[("c", "=")])
        b = ExclusionConstraint(name="x", expressions=[("d", "=")])
        assert hash(a) == hash(b)
