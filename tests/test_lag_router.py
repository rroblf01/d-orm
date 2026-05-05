"""Unit tests for ``LagAwareReadRouter``.

The lag-measurement path is mocked so the tests don't need real
streaming replicas — that infrastructure can be tested separately
in an integration suite.
"""

from __future__ import annotations

import random

import pytest

from dorm.contrib.lag_router import LagAwareReadRouter


class _Stub:
    pass


@pytest.fixture
def deterministic_router():
    """A router with a seeded RNG so the choice between two healthy
    replicas is reproducible."""
    rng = random.Random(0)
    return LagAwareReadRouter(
        primary="primary",
        replicas=["r1", "r2"],
        max_lag_seconds=2.0,
        cache_seconds=5.0,
        rng=rng,
    )


def test_validation():
    with pytest.raises(ValueError):
        LagAwareReadRouter(replicas=[])
    with pytest.raises(ValueError):
        LagAwareReadRouter(replicas=["r"], max_lag_seconds=0)
    with pytest.raises(ValueError):
        LagAwareReadRouter(replicas=["r"], cache_seconds=0)


def test_db_for_write_always_primary(deterministic_router):
    assert deterministic_router.db_for_write(_Stub()) == "primary"


def test_db_for_read_picks_healthy_replica(deterministic_router, monkeypatch):
    # Force every probe to return 0 s lag.
    monkeypatch.setattr(
        deterministic_router, "_measure_lag", lambda alias: 0.0
    )
    chosen = deterministic_router.db_for_read(_Stub())
    assert chosen in {"r1", "r2"}


def test_unhealthy_replicas_deflect_to_primary(deterministic_router, monkeypatch):
    # All replicas above threshold.
    monkeypatch.setattr(
        deterministic_router, "_measure_lag", lambda alias: 10.0
    )
    assert deterministic_router.db_for_read(_Stub()) == "primary"


def test_partial_health_skips_lagging(deterministic_router, monkeypatch):
    monkeypatch.setattr(
        deterministic_router,
        "_measure_lag",
        lambda alias: 0.5 if alias == "r1" else 99.0,
    )
    # Only r1 is healthy.
    chosen = {deterministic_router.db_for_read(_Stub()) for _ in range(20)}
    assert chosen == {"r1"}


def test_cache_avoids_re_probe(deterministic_router, monkeypatch):
    calls = [0]

    def _probe(alias):
        calls[0] += 1
        return 0.1

    monkeypatch.setattr(deterministic_router, "_measure_lag", _probe)
    deterministic_router.db_for_read(_Stub())
    deterministic_router.db_for_read(_Stub())
    deterministic_router.db_for_read(_Stub())
    # 1 call per replica on first hit; subsequent reads are cached.
    assert calls[0] <= 2


def test_reset_clears_cache(deterministic_router, monkeypatch):
    calls = [0]
    monkeypatch.setattr(
        deterministic_router,
        "_measure_lag",
        lambda alias: (calls.__setitem__(0, calls[0] + 1) or 0.1),
    )
    deterministic_router.db_for_read(_Stub())
    cached = calls[0]
    deterministic_router.reset()
    deterministic_router.db_for_read(_Stub())
    assert calls[0] > cached


def test_snapshot_reflects_state(deterministic_router, monkeypatch):
    monkeypatch.setattr(
        deterministic_router,
        "_measure_lag",
        lambda alias: 0.5,
    )
    deterministic_router.db_for_read(_Stub())
    snap = deterministic_router.snapshot()
    assert any(v["healthy"] for v in snap.values())


def test_allow_relation_returns_true(deterministic_router):
    assert deterministic_router.allow_relation(object(), object()) is True


def test_allow_migrate_only_primary(deterministic_router):
    assert deterministic_router.allow_migrate("primary", "any") is True
    assert deterministic_router.allow_migrate("r1", "any") is False
