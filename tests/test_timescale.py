"""Tests for ``dorm.contrib.timescale``.

The live PG smoke is skipped when the ``timescaledb`` extension
isn't installed in the test database — the test container ships
plain ``postgres:16-alpine`` without the extension, so the
identifier-validation / non-PG guard / SQL-emission tests carry
the coverage load.
"""
from __future__ import annotations

import pytest

from dorm.contrib import timescale


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.calls: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or []))
        return []


class TestIdentifierAndIntervalValidation:
    def test_quote_ident_accepts_plain(self):
        assert timescale._quote_ident("events") == '"events"'

    @pytest.mark.parametrize(
        "bad",
        ["1bad", " spaces ", "name;DROP", 'oops"', ""],
    )
    def test_quote_ident_rejects_invalid(self, bad):
        with pytest.raises(ValueError, match="invalid identifier"):
            timescale._quote_ident(bad)

    @pytest.mark.parametrize(
        "good",
        ["1 day", "7 days", "12 hours", "30 minutes", "1 month", "5 years"],
    )
    def test_interval_allowed(self, good):
        assert timescale._validate_interval(good) == good

    @pytest.mark.parametrize(
        "bad",
        ["forever", "1", "drop table x", "1 day; SELECT 1", ""],
    )
    def test_interval_rejected(self, bad):
        with pytest.raises(ValueError, match="invalid interval"):
            timescale._validate_interval(bad)


class TestNonPGGuard:
    def test_require_pg_rejects_sqlite(self):
        with pytest.raises(NotImplementedError, match="PostgreSQL"):
            timescale._require_pg(_FakeConn(vendor="sqlite"))

    @pytest.mark.parametrize(
        "fn,args",
        [
            (timescale.create_hypertable, ("t", "ts")),
            (timescale.add_retention_policy, ("t",)),
            (timescale.remove_retention_policy, ("t",)),
            (timescale.add_compression_policy, ("t",)),
            (timescale.hypertables, ()),
        ],
    )
    def test_helpers_reject_non_pg(self, monkeypatch, fn, args):
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(
            _conn_mod, "get_connection", lambda _a: _FakeConn(vendor="sqlite")
        )
        kwargs: dict = {}
        if fn is timescale.add_retention_policy:
            kwargs = {"drop_after": "30 days"}
        elif fn is timescale.add_compression_policy:
            kwargs = {"compress_after": "1 day"}
        with pytest.raises(NotImplementedError):
            fn(*args, **kwargs)


class TestSQLEmission:
    def _patched_pg(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        fake = _FakeConn()
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        return fake

    def test_create_hypertable_emits_expected_sql(self, monkeypatch):
        fake = self._patched_pg(monkeypatch)
        timescale.create_hypertable("events", "ts")
        sql, params = fake.calls[-1]
        assert "create_hypertable(" in sql
        assert "INTERVAL '1 day'" in sql
        assert "if_not_exists => TRUE" in sql
        # Table goes in quoted (regclass cast strips the quotes);
        # column goes in unquoted because the helper's NAME slot
        # would otherwise look for a column literally named ``"ts"``.
        assert params == ['"events"', "ts"]

    def test_create_hypertable_custom_interval(self, monkeypatch):
        fake = self._patched_pg(monkeypatch)
        timescale.create_hypertable("events", "ts", chunk_time_interval="6 hours")
        assert "INTERVAL '6 hours'" in fake.calls[-1][0]

    def test_add_retention_policy(self, monkeypatch):
        fake = self._patched_pg(monkeypatch)
        timescale.add_retention_policy("events", drop_after="90 days")
        sql, params = fake.calls[-1]
        assert "add_retention_policy(" in sql
        assert "INTERVAL '90 days'" in sql
        assert params == ['"events"']

    def test_remove_retention_policy(self, monkeypatch):
        fake = self._patched_pg(monkeypatch)
        timescale.remove_retention_policy("events")
        sql, _ = fake.calls[-1]
        assert "remove_retention_policy(" in sql
        assert "if_exists => TRUE" in sql

    def test_add_compression_policy(self, monkeypatch):
        fake = self._patched_pg(monkeypatch)
        timescale.add_compression_policy("events", compress_after="7 days")
        sql, _ = fake.calls[-1]
        assert "add_compression_policy(" in sql
        assert "INTERVAL '7 days'" in sql

    def test_create_hypertable_invalid_interval_rejected(self, monkeypatch):
        self._patched_pg(monkeypatch)
        with pytest.raises(ValueError, match="invalid interval"):
            timescale.create_hypertable(
                "events", "ts", chunk_time_interval="forever"
            )


class TestHypertablesListing:
    def test_returns_empty_when_catalog_missing(self, monkeypatch, caplog):
        """When the ``timescaledb_information.hypertables`` view
        is absent (extension not installed), ``hypertables()`` logs
        and returns an empty list."""
        from dorm.db import connection as _conn_mod

        class _Broken(_FakeConn):
            def execute(self, sql, params=None):
                raise RuntimeError(
                    'relation "timescaledb_information.hypertables" does not exist'
                )

        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: _Broken())
        with caplog.at_level("WARNING", logger="dorm.contrib.timescale"):
            assert timescale.hypertables() == []
        assert any(
            "catalog read failed" in rec.message for rec in caplog.records
        )

    def test_returns_names_when_catalog_present(self, monkeypatch):
        from dorm.db import connection as _conn_mod

        class _OK(_FakeConn):
            def execute(self, sql, params=None):
                return [
                    {"hypertable_name": "events"},
                    {"hypertable_name": "metrics"},
                ]

        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: _OK())
        assert timescale.hypertables() == ["events", "metrics"]
