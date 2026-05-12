"""Tests for ``dorm.contrib.prepared_stmts``."""
from __future__ import annotations

import pytest

from dorm.contrib import prepared_stmts


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.scripts: list[str] = []
        self._rows: list[dict] = []

    def execute(self, sql, params=None):
        return self._rows

    def execute_script(self, sql):
        self.scripts.append(sql)


class TestSetThreshold:
    def test_unknown_alias_raises(self):
        with pytest.raises(KeyError, match="not in DATABASES"):
            prepared_stmts.set_threshold(5, alias="ghost")

    def test_non_pg_alias_raises(self, tmp_path):
        import dorm
        from dorm.conf import settings

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        try:
            dorm.configure(
                DATABASES={
                    "default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "t.db")}
                },
                INSTALLED_APPS=[],
            )
            with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
                prepared_stmts.set_threshold(5)
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=list(settings.INSTALLED_APPS))


class TestRequirePG:
    def test_rejects_sqlite(self):
        with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
            prepared_stmts._require_pg(_FakeConn(vendor="sqlite"))


class TestActivePreparedMock:
    def test_returns_rows_from_view(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        fake._rows = [
            {
                "name": "_pg_stmt_1",
                "statement": "SELECT 1",
                "prepare_time": "2026-05-12 00:00:00+00",
                "parameter_types": [],
                "from_sql": True,
            }
        ]
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        rows = prepared_stmts.active_prepared()
        assert rows[0]["name"] == "_pg_stmt_1"

    def test_non_pg_rejected(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            prepared_stmts.active_prepared()


class TestDeallocateAllMock:
    def test_emits_deallocate_all(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        prepared_stmts.deallocate_all()
        assert fake.scripts == ["DEALLOCATE ALL"]

    def test_non_pg_rejected(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        with pytest.raises(NotImplementedError):
            prepared_stmts.deallocate_all()


# ── live PG smoke ─────────────────────────────────────────────────────────


class TestLivePG:
    @pytest.fixture(autouse=True)
    def _skip_non_pg(self, db_config):
        if db_config.get("ENGINE") != "postgresql":
            pytest.skip("prepared_stmts smoke requires PostgreSQL")

    def test_active_prepared_returns_list(self):
        # The view always exists on PG; rows may be empty.
        rows = prepared_stmts.active_prepared()
        assert isinstance(rows, list)

    def test_set_threshold_updates_settings(self):
        import dorm
        from dorm.conf import settings

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        try:
            prepared_stmts.set_threshold(0)
            assert settings.DATABASES["default"]["PREPARE_THRESHOLD"] == 0
            prepared_stmts.set_threshold(None)
            assert settings.DATABASES["default"]["PREPARE_THRESHOLD"] is None
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=list(settings.INSTALLED_APPS))

    def test_deallocate_all_smoke(self):
        # No assertion beyond "doesn't raise".
        prepared_stmts.deallocate_all()
