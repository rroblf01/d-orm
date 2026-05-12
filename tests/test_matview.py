"""Tests for ``dorm.contrib.matview``.

Identifier validation + non-PG guard are exercised purely; the
CREATE / REFRESH / DROP integration paths require a live PG —
those are covered by the smoke test below (skipped on SQLite).
"""
from __future__ import annotations

import pytest

from dorm.contrib import matview


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)

    def execute(self, sql: str, params=None):
        return []


class TestIdentValidation:
    def test_quotes_plain_name(self):
        assert matview._quote_ident("dashboard_v") == '"dashboard_v"'

    @pytest.mark.parametrize(
        "bad",
        ["1bad", "name with space", 'name"quote', "name;DROP", ""],
    )
    def test_rejects_invalid(self, bad):
        with pytest.raises(ValueError, match="invalid identifier"):
            matview._quote_ident(bad)


class TestNonPGGuard:
    def test_require_pg_rejects_sqlite(self, monkeypatch):
        with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
            matview._require_pg(_FakeConn(vendor="sqlite"))

    def test_create_matview_rejects_non_pg(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            matview.create_matview("v", "SELECT 1")

    def test_refresh_rejects_non_pg(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            matview.refresh_matview("v")

    def test_drop_rejects_non_pg(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            matview.drop_matview("v")

    def test_list_matviews_rejects_non_pg(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            matview.list_matviews()


class TestSQLEmission:
    def test_create_default_emits_with_data(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        matview.create_matview("v", "SELECT 1")
        assert any("CREATE MATERIALIZED VIEW " in s for s in fake.scripts)
        assert any('"v"' in s for s in fake.scripts)
        assert any("WITH DATA" in s for s in fake.scripts)

    def test_create_with_no_data_and_if_not_exists(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        matview.create_matview(
            "v", "SELECT 1", with_data=False, if_not_exists=True
        )
        sql = fake.scripts[-1]
        assert "IF NOT EXISTS" in sql
        assert "WITH NO DATA" in sql

    def test_refresh_concurrently(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        matview.refresh_matview("v", concurrently=True)
        assert "REFRESH MATERIALIZED VIEW CONCURRENTLY " in fake.scripts[-1]

    def test_drop_with_cascade(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        matview.drop_matview("v", cascade=True)
        sql = fake.scripts[-1]
        assert "DROP MATERIALIZED VIEW IF EXISTS " in sql
        assert "CASCADE" in sql


class TestRefreshTaskHelper:
    def test_returns_zero_arg_closure(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        job = matview.matview_refresh_task("v")
        # Closure must be callable with no args + emit a refresh.
        job()
        assert any(
            s.startswith("REFRESH MATERIALIZED VIEW CONCURRENTLY")
            for s in fake.scripts
        )


# ── live PG smoke (skipped on SQLite) ───────────────────────────────────────


class TestLivePG:
    @pytest.fixture(autouse=True)
    def _skip_non_pg(self, db_config):
        if db_config.get("ENGINE") != "postgresql":
            pytest.skip("matview needs PostgreSQL")

    def test_create_refresh_list_drop_roundtrip(self):
        from dorm.db.connection import get_connection

        name = "_mv_smoke"
        # Ensure clean slate.
        try:
            matview.drop_matview(name, if_exists=True)
        except Exception:
            pass
        matview.create_matview(name, "SELECT 1 AS n")
        assert name in matview.list_matviews()
        matview.refresh_matview(name)
        matview.drop_matview(name)
        assert name not in matview.list_matviews()
        # Re-use the connection to ensure no transaction leaks.
        get_connection().execute("SELECT 1")
