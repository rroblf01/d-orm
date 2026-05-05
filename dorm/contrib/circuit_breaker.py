"""Per-alias circuit breaker.

Layer above ``with_transient_retry`` for the case where the database is
unhealthy for *seconds-to-minutes* rather than a fraction of a second.
Without a breaker, every concurrent request burns the full retry budget
fighting a downed primary, exhausts pool connections, and cascades into
a request-storm against the server when it recovers — the classic
thundering-herd failure mode.

State machine::

       failure_threshold consecutive failures
    ┌─────────┐           ┌───────┐         ┌───────────┐
    │ CLOSED  ├──────────▶│ OPEN  ├────────▶│ HALF_OPEN │
    └────▲────┘           └───────┘ reset   └─────┬─────┘
         │                  open_window_s         │
         │            success on probe            │
         └────────────────────────────────────────┘

- **CLOSED**: requests flow through. Each failure increments a counter;
  ``failure_threshold`` failures in a row trip the breaker.
- **OPEN**: every call raises :class:`CircuitOpenError` immediately.
  After ``open_window_s`` seconds, the breaker switches to HALF_OPEN.
- **HALF_OPEN**: one probe call is allowed through. Success → CLOSED.
  Failure → OPEN with a fresh timer.

Usage::

    from dorm.contrib.circuit_breaker import circuit_breaker, CircuitOpenError

    breaker = circuit_breaker("default", failure_threshold=5, open_window_s=30.0)

    try:
        with breaker:
            Author.objects.count()
    except CircuitOpenError:
        # Skip the call, return cached/stub data.
        ...

The breaker is process-local — there is no shared state across workers.
For multi-process coordination, use a Redis-backed counter on top of
this primitive (out of scope here; the breaker just needs ``record_*``
methods, which a subclass can override).
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import threading
import time
from typing import Any

from ..exceptions import DatabaseError


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(DatabaseError):
    """Raised by an open breaker instead of executing the wrapped call.

    Subclasses :class:`dorm.exceptions.DatabaseError` so callers that
    already catch generic DB errors degrade gracefully — it is, after
    all, a database-availability signal."""


class CircuitBreaker:
    """Process-local circuit breaker keyed by an arbitrary name.

    Construct directly for advanced use cases; most callers prefer the
    :func:`circuit_breaker` factory which memoises one instance per
    name so different code paths sharing a database alias share state.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        open_window_s: float = 30.0,
        clock=time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if open_window_s <= 0:
            raise ValueError("open_window_s must be > 0")
        self.name = name
        self.failure_threshold = failure_threshold
        self.open_window_s = open_window_s
        self._clock = clock
        self._state: CircuitState = CircuitState.CLOSED
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_promote_to_half_open()
            return self._state

    @property
    def failures(self) -> int:
        with self._lock:
            return self._failures

    def _maybe_promote_to_half_open(self) -> None:
        # Caller holds the lock.
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            if (self._clock() - self._opened_at) >= self.open_window_s:
                self._state = CircuitState.HALF_OPEN

    def allow(self) -> bool:
        """Return ``True`` when a call may proceed; ``False`` otherwise.

        Half-open transitions are made here so the very next call to
        ``allow()`` after the open window elapses returns ``True``."""
        with self._lock:
            self._maybe_promote_to_half_open()
            return self._state is not CircuitState.OPEN

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state is CircuitState.HALF_OPEN:
                # Probe failed — re-open the circuit with a fresh
                # window timer so the next attempt waits the full
                # cooldown again.
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()
                return
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()

    def reset(self) -> None:
        """Forcibly return the breaker to CLOSED. Useful in tests."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failures = 0
            self._opened_at = None

    # ── Context-manager wrapper ────────────────────────────────────────────────
    def __enter__(self) -> "CircuitBreaker":
        if not self.allow():
            raise CircuitOpenError(
                f"Circuit '{self.name}' is OPEN — "
                f"{self._failures} consecutive failures, "
                f"cooldown ends in "
                f"{max(0.0, self.open_window_s - (self._clock() - (self._opened_at or 0))):.1f}s"
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.record_success()
        elif issubclass(exc_type, CircuitOpenError):
            # Don't double-count: the breaker raised this itself in
            # __enter__; we already know about the failure that caused it.
            return
        else:
            self.record_failure()

    @contextlib.asynccontextmanager
    async def aprotect(self):
        """Async context manager equivalent to ``with breaker:``.

        Awaits nothing inside the wrapper itself — exists so async code
        can use the same breaker without juggling sync/async boundary
        helpers."""
        if not self.allow():
            raise CircuitOpenError(
                f"Circuit '{self.name}' is OPEN — "
                f"{self._failures} consecutive failures."
            )
        try:
            yield self
        except CircuitOpenError:
            raise
        except (Exception, asyncio.CancelledError):
            self.record_failure()
            raise
        else:
            self.record_success()


_REGISTRY: dict[str, CircuitBreaker] = {}
_REGISTRY_LOCK = threading.Lock()


def circuit_breaker(
    name: str,
    *,
    failure_threshold: int = 5,
    open_window_s: float = 30.0,
) -> CircuitBreaker:
    """Return the process-shared :class:`CircuitBreaker` for *name*.

    Tuning parameters apply only the first time a breaker is created
    under a given name — later calls return the existing instance and
    ignore the kwargs (so different call sites can't fight over knobs).
    Use :func:`reset_circuit_breakers` in test setUp.
    """
    with _REGISTRY_LOCK:
        cb = _REGISTRY.get(name)
        if cb is None:
            cb = CircuitBreaker(
                name,
                failure_threshold=failure_threshold,
                open_window_s=open_window_s,
            )
            _REGISTRY[name] = cb
        return cb


def reset_circuit_breakers() -> None:
    """Drop every registered breaker. Safe to call between tests so a
    breaker tripped by one test doesn't bleed into the next."""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()


def get_state(name: str) -> dict[str, Any]:
    """Return a snapshot of one breaker's state for monitoring /
    Prometheus exposition. Keys: ``state``, ``failures``,
    ``opened_at`` (monotonic, ``None`` if not open)."""
    with _REGISTRY_LOCK:
        cb = _REGISTRY.get(name)
    if cb is None:
        return {"state": "unknown", "failures": 0, "opened_at": None}
    with cb._lock:
        return {
            "state": cb._state.value,
            "failures": cb._failures,
            "opened_at": cb._opened_at,
        }


__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "circuit_breaker",
    "reset_circuit_breakers",
    "get_state",
]
