"""In-process async background-task runner.

Lighter weight than :mod:`dorm.contrib.tasks` (no persistence, no
retries, no cross-process):

- ``BackgroundTasks.add(coro)`` schedules a coroutine on the current
  event loop with a bounded semaphore so the runner can't be DoS'd
  by an unbounded fan-out.
- ``BackgroundTasks.run()`` awaits every scheduled task; useful at
  request boundaries to drain in-flight work before the response
  ships.

Use case: short-lived "fire and forget" follow-ups (email send, cache
warmup, audit log push) inside a single request that should NOT
block the response.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

_log = logging.getLogger("dorm.contrib.background")


class BackgroundTasks:
    """Bounded async task scheduler.

    Args:
        concurrency: max in-flight tasks at any moment. Acts as a
            back-pressure throttle. ``None`` disables the throttle
            (use only when the caller is sure the fan-out is small).
    """

    def __init__(self, *, concurrency: int | None = 16) -> None:
        if concurrency is not None and concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self._sem = asyncio.Semaphore(concurrency) if concurrency else None
        self._tasks: list[asyncio.Task[Any]] = []

    def add(self, coro_fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> None:
        """Schedule *coro_fn(args, kwargs)* on the running loop.

        The semaphore is acquired *inside* the wrapper so scheduling
        is cheap — only the actual execution is throttled."""

        async def _wrapped() -> Any:
            if self._sem is not None:
                async with self._sem:
                    return await coro_fn(*args, **kwargs)
            return await coro_fn(*args, **kwargs)

        self._tasks.append(asyncio.create_task(_wrapped()))

    async def run(self, *, swallow_exceptions: bool = True) -> list[Any]:
        """Await every scheduled task. With ``swallow_exceptions=True``
        (default) failures are logged and the run continues — fits
        "fire and forget" semantics. With False, the first failure
        re-raises after every task has had a chance to finish."""
        if not self._tasks:
            return []
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        out: list[Any] = []
        first_exc: BaseException | None = None
        for r in results:
            if isinstance(r, BaseException):
                if swallow_exceptions:
                    _log.warning("background task failed: %s", r, exc_info=r)
                else:
                    first_exc = first_exc or r
                continue
            out.append(r)
        if first_exc is not None:
            raise first_exc
        return out

    def cancel_all(self) -> None:
        """Cancel every still-pending task. Useful in shutdown hooks
        when the surrounding request was aborted mid-flight."""
        for t in self._tasks:
            if not t.done():
                t.cancel()


__all__ = ["BackgroundTasks"]
