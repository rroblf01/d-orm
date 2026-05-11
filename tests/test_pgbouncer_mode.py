"""Tests for the PgBouncer transaction-mode compatibility shim."""
from __future__ import annotations

import logging

import pytest

from dorm.db.backends.postgresql import (
    PostgreSQLAsyncDatabaseWrapper,
    PostgreSQLDatabaseWrapper,
    _coerce_pgbouncer_mode,
)


class TestCoercePgbouncerMode:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (False, None),
            (None, None),
            ("", None),
            ("session", None),
            (True, "transaction"),
            ("transaction", "transaction"),
            ("TRANSACTION", "transaction"),
            ("statement", "statement"),
            ("  statement  ", "statement"),
        ],
    )
    def test_valid_inputs(self, value, expected):
        assert _coerce_pgbouncer_mode(value) == expected

    @pytest.mark.parametrize("value", ["foo", 42, ["transaction"]])
    def test_invalid_inputs_raise(self, value):
        with pytest.raises(ValueError):
            _coerce_pgbouncer_mode(value)


class TestSyncWrapperPgbouncer:
    def test_pgbouncer_off_keeps_default_prepare(self):
        wrapper = PostgreSQLDatabaseWrapper(
            {"ENGINE": "postgresql", "NAME": "x"}
        )
        assert wrapper._pgbouncer_mode is None
        # Default ``None`` means "defer to psycopg's default" — the
        # original behaviour where ``prepare_threshold`` is not passed.
        assert wrapper._prepare_threshold is None

    def test_pgbouncer_true_forces_none(self):
        wrapper = PostgreSQLDatabaseWrapper(
            {"ENGINE": "postgresql", "NAME": "x", "PGBOUNCER_MODE": True}
        )
        assert wrapper._pgbouncer_mode == "transaction"
        assert wrapper._prepare_threshold is None

    def test_pgbouncer_overrides_prepare_threshold_with_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="dorm.db.lifecycle.postgresql"):
            wrapper = PostgreSQLDatabaseWrapper(
                {
                    "ENGINE": "postgresql",
                    "NAME": "x",
                    "PGBOUNCER_MODE": "transaction",
                    "PREPARE_THRESHOLD": 5,
                }
            )
        assert wrapper._prepare_threshold is None
        assert any(
            "PGBOUNCER_MODE" in rec.message for rec in caplog.records
        )

    def test_prepare_threshold_respected_without_pgbouncer(self):
        wrapper = PostgreSQLDatabaseWrapper(
            {"ENGINE": "postgresql", "NAME": "x", "PREPARE_THRESHOLD": 0}
        )
        assert wrapper._prepare_threshold == 0

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            PostgreSQLDatabaseWrapper(
                {"ENGINE": "postgresql", "NAME": "x", "PGBOUNCER_MODE": "lol"}
            )


class TestAsyncWrapperPgbouncer:
    def test_pgbouncer_true_forces_none(self):
        wrapper = PostgreSQLAsyncDatabaseWrapper(
            {"ENGINE": "postgresql", "NAME": "x", "PGBOUNCER_MODE": True}
        )
        assert wrapper._pgbouncer_mode == "transaction"
        assert wrapper._prepare_threshold is None

    def test_pgbouncer_statement_mode(self):
        wrapper = PostgreSQLAsyncDatabaseWrapper(
            {
                "ENGINE": "postgresql",
                "NAME": "x",
                "PGBOUNCER_MODE": "statement",
            }
        )
        assert wrapper._pgbouncer_mode == "statement"
        assert wrapper._prepare_threshold is None
