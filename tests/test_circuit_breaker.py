"""Tests for ``dorm.contrib.circuit_breaker``.

State-machine tests use a deterministic mock clock so the tests don't
sleep — every transition is exercised via explicit time advancement.
"""

from __future__ import annotations

import asyncio

import pytest

from dorm.contrib.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    circuit_breaker,
    get_state,
    reset_circuit_breakers,
)


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture(autouse=True)
def _clean_breakers():
    reset_circuit_breakers()
    yield
    reset_circuit_breakers()


def test_validation():
    with pytest.raises(ValueError):
        CircuitBreaker("x", failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker("x", open_window_s=0)


def test_starts_closed():
    cb = CircuitBreaker("x")
    assert cb.state is CircuitState.CLOSED
    assert cb.allow() is True


def test_trips_open_after_threshold():
    cb = CircuitBreaker("x", failure_threshold=3)
    cb.record_failure()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED
    cb.record_failure()
    assert cb.state is CircuitState.OPEN


def test_open_blocks_calls():
    cb = CircuitBreaker("x", failure_threshold=1)
    cb.record_failure()
    with pytest.raises(CircuitOpenError):
        with cb:
            pass


def test_half_open_after_window_then_success_closes():
    clock = _Clock()
    cb = CircuitBreaker("x", failure_threshold=2, open_window_s=10.0, clock=clock)
    cb.record_failure()
    cb.record_failure()
    assert cb.state is CircuitState.OPEN

    clock.advance(11.0)
    # First read after the cooldown should promote to HALF_OPEN.
    assert cb.state is CircuitState.HALF_OPEN
    # A successful probe closes the circuit and resets the failure
    # counter.
    with cb:
        pass
    assert cb.state is CircuitState.CLOSED
    assert cb.failures == 0


def test_half_open_failure_reopens_with_new_window():
    clock = _Clock()
    cb = CircuitBreaker("x", failure_threshold=1, open_window_s=10.0, clock=clock)
    cb.record_failure()
    clock.advance(11.0)
    # Half-open probe fails — back to OPEN with a fresh timer.
    try:
        with cb:
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert cb.state is CircuitState.OPEN
    # Right after the failure, the cooldown is brand-new — not elapsed.
    clock.advance(5.0)
    assert cb.state is CircuitState.OPEN
    clock.advance(6.0)
    assert cb.state is CircuitState.HALF_OPEN


def test_context_manager_records_success():
    cb = CircuitBreaker("x", failure_threshold=2)
    cb.record_failure()
    with cb:
        pass
    assert cb.failures == 0


def test_context_manager_records_failure_on_exception():
    cb = CircuitBreaker("x", failure_threshold=2)
    with pytest.raises(RuntimeError):
        with cb:
            raise RuntimeError("boom")
    assert cb.failures == 1


def test_circuit_open_error_does_not_double_count():
    cb = CircuitBreaker("x", failure_threshold=1)
    cb.record_failure()
    assert cb.failures == 1
    with pytest.raises(CircuitOpenError):
        with cb:
            pass
    # The breaker raised in __enter__ — failure already recorded.
    assert cb.failures == 1


def test_factory_returns_singleton():
    a = circuit_breaker("alias-a")
    b = circuit_breaker("alias-a", failure_threshold=99)
    assert a is b
    # Tuning kwargs ignored on second call: still the original threshold.
    assert a.failure_threshold == 5


def test_get_state_unknown_breaker():
    s = get_state("never-created")
    assert s["state"] == "unknown"


def test_get_state_known_breaker():
    cb = circuit_breaker("alias-b")
    cb.record_failure()
    s = get_state("alias-b")
    assert s["failures"] == 1
    assert s["state"] == "closed"


@pytest.mark.asyncio
async def test_aprotect_records_failure_on_exception():
    cb = CircuitBreaker("x", failure_threshold=2)
    with pytest.raises(RuntimeError):
        async with cb.aprotect():
            raise RuntimeError("boom")
    assert cb.failures == 1


@pytest.mark.asyncio
async def test_aprotect_records_success():
    cb = CircuitBreaker("x", failure_threshold=2)
    cb.record_failure()
    async with cb.aprotect():
        await asyncio.sleep(0)
    assert cb.failures == 0


@pytest.mark.asyncio
async def test_aprotect_open_raises_circuit_open_error():
    cb = CircuitBreaker("x", failure_threshold=1)
    cb.record_failure()
    with pytest.raises(CircuitOpenError):
        async with cb.aprotect():
            pass
