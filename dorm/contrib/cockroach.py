"""CockroachDB helpers — serialization-retry primitives.

CockroachDB runs every transaction at SERIALIZABLE isolation. Under
concurrent writes the database surfaces SQLSTATE ``40001``
(``serialization_failure``) and expects the client to retry the entire
transaction. The helpers in this module wrap a callable in a bounded
retry loop with exponential backoff + jitter.

Functional form::

    from dorm.contrib.cockroach import retry_on_serialization

    def transfer():
        with dorm.transaction.atomic():
            ...

    retry_on_serialization(transfer)

Decorator form::

    from dorm.contrib.cockroach import with_retry

    @with_retry(max_attempts=5)
    def transfer(src_id, dst_id, amount):
        with dorm.transaction.atomic():
            ...

Async equivalents are :func:`aretry_on_serialization` and the
:func:`with_retry` decorator auto-detects coroutine functions.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import random
import time
from typing import Any, Awaitable, Callable, TypeVar

_log = logging.getLogger("dorm.contrib.cockroach")

# Cockroach uses 40001 (serialization_failure) and 40003
# (statement_completion_unknown) as retryable conditions per the
# official client guidance.
_RETRY_SQLSTATES = frozenset({"40001", "40003"})

_T = TypeVar("_T")


def _is_serialization_failure(exc: BaseException) -> bool:
    """Return True if *exc* is a Cockroach-style retryable error.

    Inspects psycopg's ``sqlstate`` attribute (and the ``pgcode``
    fallback used by some driver versions) and, as a last resort,
    the exception message — some wrappers strip sqlstate on
    ``InFailedSqlTransaction``-style follow-ups.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if sqlstate in _RETRY_SQLSTATES:
        return True
    msg = str(exc).lower()
    return "restart transaction" in msg or "serialization" in msg


def _sleep_for(attempt: int, base: float, cap: float, jitter: bool) -> float:
    sleep_for = min(cap, base * (2 ** max(0, attempt - 1)))
    if jitter:
        sleep_for *= 0.5 + random.random()
    return sleep_for


def retry_on_serialization(
    func: Callable[[], _T],
    *,
    max_attempts: int = 5,
    base_backoff: float = 0.05,
    max_backoff: float = 2.0,
    jitter: bool = True,
) -> _T:
    """Synchronous retry runner.

    Args:
        func: zero-arg callable that performs the transaction. Bind
            its arguments with :func:`functools.partial` if needed.
        max_attempts: hard ceiling on total attempts (including the
            initial one). Must be >= 1.
        base_backoff: starting sleep in seconds; doubled per retry up
            to *max_backoff*.
        max_backoff: cap for the exponential backoff.
        jitter: when True, sleep is multiplied by a uniform ``[0.5, 1.5]``
            factor to avoid retry storms.

    Non-retryable exceptions propagate immediately; retryable ones
    bump the attempt counter and sleep before the next iteration. The
    last failure is re-raised when the retry budget is exhausted.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            if not _is_serialization_failure(exc) or attempt >= max_attempts:
                raise
            sleep_for = _sleep_for(attempt, base_backoff, max_backoff, jitter)
            _log.warning(
                "Cockroach serialization retry %d/%d in %.3fs: %s",
                attempt,
                max_attempts,
                sleep_for,
                exc,
            )
            time.sleep(sleep_for)
    raise RuntimeError("retry_on_serialization: unreachable")


async def aretry_on_serialization(
    func: Callable[[], Awaitable[_T]],
    *,
    max_attempts: int = 5,
    base_backoff: float = 0.05,
    max_backoff: float = 2.0,
    jitter: bool = True,
) -> _T:
    """Async counterpart of :func:`retry_on_serialization`.

    *func* must be a zero-arg callable returning a fresh coroutine on
    each call — coroutines can only be awaited once.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    for attempt in range(1, max_attempts + 1):
        try:
            return await func()
        except Exception as exc:
            if not _is_serialization_failure(exc) or attempt >= max_attempts:
                raise
            sleep_for = _sleep_for(attempt, base_backoff, max_backoff, jitter)
            _log.warning(
                "Cockroach serialization retry %d/%d in %.3fs: %s",
                attempt,
                max_attempts,
                sleep_for,
                exc,
            )
            await asyncio.sleep(sleep_for)
    raise RuntimeError("aretry_on_serialization: unreachable")


def with_retry(
    func: Callable[..., Any] | None = None,
    *,
    max_attempts: int = 5,
    base_backoff: float = 0.05,
    max_backoff: float = 2.0,
    jitter: bool = True,
) -> Any:
    """Decorator wrapping a function in :func:`retry_on_serialization` /
    :func:`aretry_on_serialization`.

    Async functions are auto-detected via :func:`inspect.iscoroutinefunction`.
    Supports both bare (``@with_retry``) and parametrised
    (``@with_retry(max_attempts=8)``) syntax.
    """

    def _decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _awrapper(*args: Any, **kwargs: Any) -> Any:
                return await aretry_on_serialization(
                    lambda: fn(*args, **kwargs),
                    max_attempts=max_attempts,
                    base_backoff=base_backoff,
                    max_backoff=max_backoff,
                    jitter=jitter,
                )

            return _awrapper

        @functools.wraps(fn)
        def _wrapper(*args: Any, **kwargs: Any) -> Any:
            return retry_on_serialization(
                lambda: fn(*args, **kwargs),
                max_attempts=max_attempts,
                base_backoff=base_backoff,
                max_backoff=max_backoff,
                jitter=jitter,
            )

        return _wrapper

    if func is not None:
        return _decorate(func)
    return _decorate


__all__ = [
    "retry_on_serialization",
    "aretry_on_serialization",
    "with_retry",
]
