"""End-to-end integration tests for v4.2 Tier-3 features.

These exercise the actual side-effects (triggers fire, DDL applies)
rather than just verifying the SQL shape — locks in behaviour that a
``_FakeConn`` test cannot catch.
"""
from __future__ import annotations

import pytest

import dorm
from dorm.migrations.operations import MakeTableAppendOnly
from dorm.migrations.schema import SchemaEditor


class _Audit(dorm.Model):
    payload = dorm.CharField(max_length=64)

    class Meta:
        app_label = "tests"


@pytest.fixture
def sqlite_env(tmp_path):
    from dorm.conf import settings
    from dorm.db.connection import (
        _async_connections,
        _sync_connections,
        get_connection,
    )

    saved = {a: dict(c) for a, c in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "ti.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    with SchemaEditor(get_connection()) as se:
        se.create_model(_Audit)
    yield get_connection()
    dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()


class TestMakeTableAppendOnlySQLite:
    def test_update_rejected_after_op(self, sqlite_env):
        conn = sqlite_env
        op = MakeTableAppendOnly(_Audit._meta.db_table)
        op.database_forwards("tests", conn, None, None)

        row = _Audit.objects.create(payload="initial")
        with pytest.raises(Exception, match="append-only"):
            _Audit.objects.filter(pk=row.pk).update(payload="changed")

    def test_delete_rejected_after_op(self, sqlite_env):
        conn = sqlite_env
        op = MakeTableAppendOnly(_Audit._meta.db_table)
        op.database_forwards("tests", conn, None, None)

        row = _Audit.objects.create(payload="initial")
        with pytest.raises(Exception, match="append-only"):
            _Audit.objects.filter(pk=row.pk).delete()

    def test_allow_delete_lets_delete_through(self, sqlite_env):
        conn = sqlite_env
        op = MakeTableAppendOnly(_Audit._meta.db_table, allow_delete=True)
        op.database_forwards("tests", conn, None, None)

        row = _Audit.objects.create(payload="initial")
        # DELETE allowed.
        _Audit.objects.filter(pk=row.pk).delete()
        assert not _Audit.objects.filter(pk=row.pk).exists()
        # UPDATE still blocked.
        new_row = _Audit.objects.create(payload="x")
        with pytest.raises(Exception, match="append-only"):
            _Audit.objects.filter(pk=new_row.pk).update(payload="y")

    def test_reverse_drops_triggers(self, sqlite_env):
        conn = sqlite_env
        op = MakeTableAppendOnly(_Audit._meta.db_table)
        op.database_forwards("tests", conn, None, None)
        op.database_backwards("tests", conn, None, None)

        # UPDATE and DELETE work again after reverse.
        row = _Audit.objects.create(payload="x")
        _Audit.objects.filter(pk=row.pk).update(payload="y")
        assert _Audit.objects.get(pk=row.pk).payload == "y"
        _Audit.objects.filter(pk=row.pk).delete()
        assert not _Audit.objects.filter(pk=row.pk).exists()

    def test_insert_still_allowed(self, sqlite_env):
        conn = sqlite_env
        op = MakeTableAppendOnly(_Audit._meta.db_table)
        op.database_forwards("tests", conn, None, None)

        # INSERT must remain permitted — only UPDATE/DELETE are blocked.
        _Audit.objects.create(payload="event-1")
        _Audit.objects.create(payload="event-2")
        assert _Audit.objects.count() == 2


# ── Slow EXPLAIN end-to-end (auto-collect from log_query) ────────────────────


class _SE(dorm.Model):
    text = dorm.CharField(max_length=64)

    class Meta:
        app_label = "tests"


class TestSlowExplainEndToEnd:
    def test_explain_attached_when_threshold_crossed(self, tmp_path, caplog):
        import logging

        from dorm.conf import settings
        from dorm.db.connection import (
            _async_connections,
            _sync_connections,
            get_connection,
        )
        from dorm.db.utils import (
            _SLOW_QUERY_EXPLAIN_SETTING,
            _SLOW_QUERY_MS_SETTING,
        )

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        # Snapshot membership + raw attribute for the two settings we're
        # about to flip; the conftest's session-scoped configure_dorm
        # does NOT touch them, so the canonical pre-state is "not in
        # _explicit_settings, attribute absent". Snapshotting anyway
        # protects against future conftest changes.
        _SENTINEL = object()
        slow_ms_attr = settings.__dict__.get("SLOW_QUERY_MS", _SENTINEL)
        slow_ms_explicit = "SLOW_QUERY_MS" in settings._explicit_settings
        slow_explain_attr = settings.__dict__.get("SLOW_QUERY_EXPLAIN", _SENTINEL)
        slow_explain_explicit = "SLOW_QUERY_EXPLAIN" in settings._explicit_settings
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "se.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
            # Force every query slow, force EXPLAIN.
            SLOW_QUERY_MS=0.0,
            SLOW_QUERY_EXPLAIN=True,
        )
        _SLOW_QUERY_MS_SETTING.invalidate()
        _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
        try:
            with SchemaEditor(get_connection()) as se:
                se.create_model(_SE)
            _SE.objects.create(text="x")
            with caplog.at_level(
                logging.WARNING, logger="dorm.db.slow_explain"
            ):
                # Issue a SELECT — must trigger the slow path + EXPLAIN.
                list(_SE.objects.all())
            assert any(
                "slow query plan" in rec.message for rec in caplog.records
            )
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            # Restore SLOW_QUERY_MS attribute + _explicit membership.
            if slow_ms_attr is _SENTINEL:
                try:
                    delattr(settings, "SLOW_QUERY_MS")
                except AttributeError:
                    pass
            else:
                settings.__dict__["SLOW_QUERY_MS"] = slow_ms_attr
            if slow_ms_explicit:
                settings._explicit_settings.add("SLOW_QUERY_MS")
            else:
                settings._explicit_settings.discard("SLOW_QUERY_MS")
            if slow_explain_attr is _SENTINEL:
                try:
                    delattr(settings, "SLOW_QUERY_EXPLAIN")
                except AttributeError:
                    pass
            else:
                settings.__dict__["SLOW_QUERY_EXPLAIN"] = slow_explain_attr
            if slow_explain_explicit:
                settings._explicit_settings.add("SLOW_QUERY_EXPLAIN")
            else:
                settings._explicit_settings.discard("SLOW_QUERY_EXPLAIN")
            _sync_connections.clear()
            _async_connections.clear()
            _SLOW_QUERY_MS_SETTING.invalidate()
            _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
