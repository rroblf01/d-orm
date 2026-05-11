"""Tests for the PG advisory-lock helpers.

The blocking / xact paths require a live PG connection, so the bulk of
the lock-acquisition tests run inside a real psycopg pool when the
conftest ``db_config`` resolves to a PG backend. The pure-logic tests
(key encoding, vendor gating) run regardless.
"""
from __future__ import annotations

import pytest

from dorm.contrib.advisory import (
    _key_to_bigint,
    advisory_lock,
    advisory_xact_lock,
    try_advisory_lock,
    try_advisory_xact_lock,
)


# ── Pure-logic tests ─────────────────────────────────────────────────────────


class TestKeyToBigint:
    def test_int_passthrough(self):
        assert _key_to_bigint(0) == (0,)
        assert _key_to_bigint(-1) == (-1,)
        assert _key_to_bigint(2**40) == (2**40,)

    def test_str_hashed_deterministically(self):
        # Same string → same key on every call (deterministic, unlike
        # Python's hash()).
        first = _key_to_bigint("nightly-report")
        second = _key_to_bigint("nightly-report")
        assert first == second
        assert isinstance(first[0], int)

    def test_str_hash_fits_signed_64bit(self):
        signed = _key_to_bigint("a very long key name here" * 10)[0]
        assert -(2**63) <= signed <= 2**63 - 1

    def test_tuple_two_arg_form(self):
        assert _key_to_bigint((1, 2)) == (1, 2)

    def test_tuple_wrong_length_rejected(self):
        with pytest.raises(ValueError, match="exactly"):
            _key_to_bigint((1, 2, 3))  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]

    def test_tuple_non_int_rejected(self):
        with pytest.raises(TypeError, match="int"):
            _key_to_bigint(("a", 1))  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]

    def test_invalid_type_rejected(self):
        with pytest.raises(TypeError, match="key must"):
            _key_to_bigint([1, 2])  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]


# ── Vendor gate ─────────────────────────────────────────────────────────────


def _vendor() -> str:
    from dorm.db.connection import get_connection

    return getattr(get_connection(), "vendor", "sqlite")


class TestVendorGate:
    def test_advisory_lock_rejects_non_pg(self):
        if _vendor() == "postgresql":
            pytest.skip("PG backend exercised in the integration tests below")
        with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
            with advisory_lock("test-key"):
                pass

    def test_try_advisory_lock_rejects_non_pg(self):
        if _vendor() == "postgresql":
            pytest.skip("PG backend exercised in the integration tests below")
        with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
            with try_advisory_lock("test-key"):
                pass


# ── PG integration ──────────────────────────────────────────────────────────


@pytest.fixture
def pg_only():
    if _vendor() != "postgresql":
        pytest.skip("PG-only — advisory locks have no portable analogue")


class TestAdvisoryLockPG:
    def test_lock_and_release(self, pg_only):
        # Smoke test: acquire + release in one process. Two passes prove
        # the unlock landed (a leaked lock would block the second wait).
        with advisory_lock("dorm-test-lock-A"):
            pass
        with advisory_lock("dorm-test-lock-A"):
            pass

    def test_try_lock_acquires_when_free(self, pg_only):
        with try_advisory_lock("dorm-test-lock-B") as acquired:
            assert acquired is True

    def test_xact_lock_inside_atomic(self, pg_only):
        import dorm

        with dorm.transaction.atomic():
            with advisory_xact_lock("dorm-test-xact-A"):
                pass

    def test_xact_lock_requires_atomic(self, pg_only):
        with pytest.raises(RuntimeError, match="atomic"):
            with advisory_xact_lock("dorm-test-xact-B"):
                pass

    def test_try_xact_lock_inside_atomic(self, pg_only):
        import dorm

        with dorm.transaction.atomic():
            with try_advisory_xact_lock("dorm-test-xact-C") as acquired:
                assert acquired is True
