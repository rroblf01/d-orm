"""Tests for the slow-query EXPLAIN auto-collect path."""
from __future__ import annotations

import logging

import pytest

import dorm
from dorm.db.utils import (
    _EXPLAIN_REENTRY,
    _maybe_capture_explain_plan,
    _SLOW_QUERY_EXPLAIN_SETTING,
    _SLOW_QUERY_MS_SETTING,
)
from dorm.migrations.schema import SchemaEditor


class _Note(dorm.Model):
    text = dorm.CharField(max_length=64)

    class Meta:
        app_label = "tests"


@pytest.fixture(autouse=True)
def fresh_db(tmp_path):
    from dorm.conf import settings
    from dorm.db.connection import _async_connections, _sync_connections, get_connection

    saved_db = {alias: dict(cfg) for alias, cfg in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "sex.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    with SchemaEditor(get_connection()) as se:
        se.create_model(_Note)
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()
    _SLOW_QUERY_MS_SETTING.invalidate()
    _SLOW_QUERY_EXPLAIN_SETTING.invalidate()


class TestExplainPlanCapture:
    def test_select_emits_plan(self, caplog):
        _Note.objects.create(text="x")
        table = _Note._meta.db_table
        with caplog.at_level(logging.WARNING, logger="dorm.db.slow_explain"):
            _maybe_capture_explain_plan(
                "sqlite", f"SELECT * FROM {table}", []
            )
        assert any(
            "slow query plan" in rec.message for rec in caplog.records
        )

    def test_skips_non_select(self, caplog):
        table = _Note._meta.db_table
        with caplog.at_level(logging.WARNING, logger="dorm.db.slow_explain"):
            _maybe_capture_explain_plan(
                "sqlite", f"INSERT INTO {table} (text) VALUES ('x')", []
            )
        assert not any(
            "slow query plan" in rec.message for rec in caplog.records
        )

    def test_skips_when_reentrant(self, caplog):
        table = _Note._meta.db_table
        token = _EXPLAIN_REENTRY.set(True)
        try:
            with caplog.at_level(logging.WARNING, logger="dorm.db.slow_explain"):
                _maybe_capture_explain_plan(
                    "sqlite", f"SELECT * FROM {table}", []
                )
            assert not any(
                "slow query plan" in rec.message for rec in caplog.records
            )
        finally:
            _EXPLAIN_REENTRY.reset(token)

    def test_unknown_vendor_skipped(self, caplog):
        with caplog.at_level(logging.WARNING, logger="dorm.db.slow_explain"):
            _maybe_capture_explain_plan(
                "exotic-vendor", "SELECT 1", []
            )
        assert not any(
            "slow query plan" in rec.message for rec in caplog.records
        )

    def test_with_cte_recognised(self, caplog):
        _Note.objects.create(text="x")
        table = _Note._meta.db_table
        sql = f"WITH x AS (SELECT * FROM {table}) SELECT * FROM x"
        with caplog.at_level(logging.WARNING, logger="dorm.db.slow_explain"):
            _maybe_capture_explain_plan("sqlite", sql, [])
        assert any(
            "slow query plan" in rec.message for rec in caplog.records
        )

    def test_caught_exception_does_not_propagate(self, caplog):
        # Feed a SQL string that EXPLAIN can't parse — captures should
        # swallow the error and log at DEBUG.
        with caplog.at_level(logging.DEBUG, logger="dorm.db.slow_explain"):
            _maybe_capture_explain_plan(
                "sqlite", "SELECT garbage_table_does_not_exist", []
            )
        # No WARNING — the capture didn't fire.
        assert not any(
            rec.levelno == logging.WARNING and "slow query plan" in rec.message
            for rec in caplog.records
        )


class TestSettings:
    def test_slow_query_explain_setting_off_by_default(self):
        _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
        from dorm.db.utils import _slow_query_explain

        assert _slow_query_explain() is False

    def test_setting_on_via_env(self, monkeypatch):
        monkeypatch.setenv("DORM_SLOW_QUERY_EXPLAIN", "true")
        _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
        from dorm.db.utils import _slow_query_explain

        assert _slow_query_explain() is True

    @pytest.mark.parametrize("val", ["1", "yes", "on", "TRUE"])
    def test_setting_truthy_values(self, monkeypatch, val):
        monkeypatch.setenv("DORM_SLOW_QUERY_EXPLAIN", val)
        _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
        from dorm.db.utils import _slow_query_explain

        assert _slow_query_explain() is True

    @pytest.mark.parametrize("val", ["0", "no", "false", "off"])
    def test_setting_falsy_values(self, monkeypatch, val):
        monkeypatch.setenv("DORM_SLOW_QUERY_EXPLAIN", val)
        _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
        from dorm.db.utils import _slow_query_explain

        assert _slow_query_explain() is False
