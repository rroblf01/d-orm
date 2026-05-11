"""Unit tests for CockroachDB backend wiring.

The Cockroach backend reuses the PostgreSQL wrappers wholesale — we
don't spin up a real CockroachDB container here. These tests verify the
glue: engine resolution picks the Cockroach subclass, the default port
gets patched, ``vendor`` stays ``"postgresql"`` (so PG-only ORM code
paths still apply), and the serialization-retry helpers behave under
both retryable and non-retryable failures.

Integration tests against a live ``cockroach demo`` cluster live in a
separate file (``test_cockroach_integration.py``) gated on a CI flag.
"""
from __future__ import annotations

import pytest

from dorm.contrib.cockroach import (
    _is_serialization_failure,
    aretry_on_serialization,
    retry_on_serialization,
    with_retry,
)


# ── Engine resolution ─────────────────────────────────────────────────────────


class TestEngineResolution:
    def test_cockroach_sync_picks_subclass(self):
        from dorm.db.backends.cockroach import CockroachDBDatabaseWrapper
        from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper
        from dorm.db.connection import _create_sync_connection

        # Don't actually open a pool — just confirm the wrapper class.
        wrapper = _create_sync_connection(
            "default",
            {"ENGINE": "cockroachdb", "NAME": "defaultdb", "HOST": "x"},
        )
        assert isinstance(wrapper, CockroachDBDatabaseWrapper)
        # Cockroach subclasses PostgreSQL — every PG code path applies.
        assert isinstance(wrapper, PostgreSQLDatabaseWrapper)

    def test_cockroach_async_picks_subclass(self):
        from dorm.db.backends.cockroach import CockroachDBAsyncDatabaseWrapper
        from dorm.db.backends.postgresql import PostgreSQLAsyncDatabaseWrapper
        from dorm.db.connection import _create_async_connection

        wrapper = _create_async_connection(
            "default",
            {"ENGINE": "cockroachdb", "NAME": "defaultdb", "HOST": "x"},
        )
        assert isinstance(wrapper, CockroachDBAsyncDatabaseWrapper)
        assert isinstance(wrapper, PostgreSQLAsyncDatabaseWrapper)

    def test_vendor_stays_postgresql(self):
        """vendor must read ``"postgresql"`` so every PG-only ORM
        branch (CreatePGEnum, copy_from, ARRAY DDL, …) still fires."""
        from dorm.db.backends.cockroach import CockroachDBDatabaseWrapper

        wrapper = CockroachDBDatabaseWrapper(
            {"ENGINE": "cockroachdb", "NAME": "defaultdb"}
        )
        assert wrapper.vendor == "postgresql"
        assert wrapper.dialect == "cockroachdb"

    def test_default_port_is_26257(self):
        from dorm.db.backends.cockroach import CockroachDBDatabaseWrapper

        wrapper = CockroachDBDatabaseWrapper({"ENGINE": "cockroachdb"})
        assert wrapper._dsn["port"] == 26257

    def test_explicit_port_is_respected(self):
        from dorm.db.backends.cockroach import CockroachDBDatabaseWrapper

        wrapper = CockroachDBDatabaseWrapper(
            {"ENGINE": "cockroachdb", "PORT": 5555}
        )
        assert wrapper._dsn["port"] == 5555


# ── Retry detection ───────────────────────────────────────────────────────────


class _FakeSqlstateError(Exception):
    def __init__(self, msg: str, sqlstate: str | None = None) -> None:
        super().__init__(msg)
        self.sqlstate = sqlstate


class TestIsSerializationFailure:
    def test_sqlstate_40001_is_retryable(self):
        assert _is_serialization_failure(
            _FakeSqlstateError("conflict", sqlstate="40001")
        )

    def test_sqlstate_40003_is_retryable(self):
        assert _is_serialization_failure(
            _FakeSqlstateError("unknown", sqlstate="40003")
        )

    def test_other_sqlstate_is_not_retryable(self):
        assert not _is_serialization_failure(
            _FakeSqlstateError("syntax error", sqlstate="42601")
        )

    def test_message_fallback_matches_restart(self):
        # Some psycopg builds drop sqlstate on follow-up
        # InFailedSqlTransaction errors — fall back to the message text.
        assert _is_serialization_failure(
            Exception("restart transaction: write conflict")
        )

    def test_ordinary_exception_is_not_retryable(self):
        assert not _is_serialization_failure(ValueError("nope"))


# ── Sync retry helper ────────────────────────────────────────────────────────


class TestRetryOnSerialization:
    def test_success_first_attempt(self):
        calls: list[int] = []

        def _fn() -> str:
            calls.append(1)
            return "ok"

        assert retry_on_serialization(_fn, max_attempts=3) == "ok"
        assert len(calls) == 1

    def test_retries_then_succeeds(self):
        calls = {"n": 0}

        def _fn() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _FakeSqlstateError("conflict", sqlstate="40001")
            return "ok"

        result = retry_on_serialization(
            _fn,
            max_attempts=5,
            base_backoff=0.001,
            max_backoff=0.001,
            jitter=False,
        )
        assert result == "ok"
        assert calls["n"] == 3

    def test_non_retryable_propagates(self):
        def _fn() -> str:
            raise ValueError("nope")

        with pytest.raises(ValueError):
            retry_on_serialization(_fn, max_attempts=5, base_backoff=0.001)

    def test_budget_exhausted_raises_last(self):
        def _fn() -> str:
            raise _FakeSqlstateError("conflict", sqlstate="40001")

        with pytest.raises(_FakeSqlstateError):
            retry_on_serialization(
                _fn,
                max_attempts=3,
                base_backoff=0.001,
                max_backoff=0.001,
                jitter=False,
            )

    def test_max_attempts_zero_rejected(self):
        with pytest.raises(ValueError):
            retry_on_serialization(lambda: None, max_attempts=0)


# ── Async retry helper ────────────────────────────────────────────────────────


class TestAsyncRetryOnSerialization:
    async def test_async_retries_then_succeeds(self):
        calls = {"n": 0}

        async def _fn() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _FakeSqlstateError("conflict", sqlstate="40001")
            return "ok"

        result = await aretry_on_serialization(
            _fn,
            max_attempts=5,
            base_backoff=0.001,
            max_backoff=0.001,
            jitter=False,
        )
        assert result == "ok"
        assert calls["n"] == 3

    async def test_async_non_retryable_propagates(self):
        async def _fn() -> str:
            raise ValueError("nope")

        with pytest.raises(ValueError):
            await aretry_on_serialization(
                _fn, max_attempts=5, base_backoff=0.001
            )


# ── Decorator ────────────────────────────────────────────────────────────────


class TestWithRetryDecorator:
    def test_decorator_with_args(self):
        calls = {"n": 0}

        @with_retry(max_attempts=3, base_backoff=0.001, jitter=False)
        def transfer() -> str:
            calls["n"] += 1
            if calls["n"] < 2:
                raise _FakeSqlstateError("conflict", sqlstate="40001")
            return "done"

        assert transfer() == "done"
        assert calls["n"] == 2

    def test_decorator_bare(self):
        @with_retry
        def transfer() -> str:
            return "done"

        assert transfer() == "done"

    async def test_decorator_async(self):
        calls = {"n": 0}

        @with_retry(max_attempts=3, base_backoff=0.001, jitter=False)
        async def transfer() -> str:
            calls["n"] += 1
            if calls["n"] < 2:
                raise _FakeSqlstateError("conflict", sqlstate="40001")
            return "done"

        result = await transfer()
        assert result == "done"
        assert calls["n"] == 2
