"""Per-asyncio.Task connection pinning + concurrency safety guard.

Default behaviour: every async query checks out a fresh connection
from the pool, runs, and returns the connection. That maximises pool
utilisation but pays a checkout/return cost on each call.

For request-scoped workloads that issue dozens of queries in a row
(typical FastAPI handler) a *task-local* pin avoids that overhead:
one checkout amortised across every query the task issues. The pin
lives only on the calling task — sibling tasks spawned via
``asyncio.gather`` get their own pool checkouts as usual.

Usage::

    from dorm.contrib.task_pool import pinned_connection

    async def handler():
        async with pinned_connection():
            # All ORM queries below reuse one pool connection.
            authors = await Author.objects.acount()
            await Author.objects.acreate(name="x", age=1)

The helper composes with ``aatomic()`` — entering an ``aatomic()``
block inside a pinned context simply reuses the same connection.
Exiting the ``aatomic()`` does not release the pin; only leaving the
``async with`` block does.

Concurrency-safety guard
------------------------

A second helper, :func:`assert_no_concurrent_gather`, raises when
``asyncio.gather`` runs ORM operations against the same pinned
connection in two siblings simultaneously. psycopg async connections
serialise concurrent awaits, but the resulting interleaving silently
corrupts cursor state across "transactions" that the caller assumed
were independent. Detecting it loudly is a strict improvement over
the silent wedge.
"""

from __future__ import annotations

import contextlib
import contextvars
from typing import Any

from ..db.connection import get_async_connection
from ..db.utils import ASYNC_ATOMIC_STATE


# (wrapper, connection, in_use_count) per task. ``in_use_count`` is
# incremented at the start of each query and decremented at the end;
# values >1 mean two ``asyncio.gather`` siblings share the same
# pinned connection — the guard tripwire.
_PINNED_STATE: contextvars.ContextVar[
    tuple[Any, Any, list[int]] | None
] = contextvars.ContextVar("dorm_pinned_async_conn", default=None)


@contextlib.asynccontextmanager
async def pinned_connection(*, using: str = "default"):
    """Pin one async pool connection to the current task. See module
    docstring for usage and rationale."""
    conn = get_async_connection(using)
    if getattr(conn, "vendor", None) != "postgresql":
        # Only PG has a real async pool right now; on other backends
        # the helper is a no-op (yields without pinning anything) so
        # caller code stays portable.
        yield None
        return

    pool = await conn._get_pool()
    cm = pool.connection()
    raw = await cm.__aenter__()
    counter = [0]
    token = _PINNED_STATE.set((conn, raw, counter))
    try:
        yield raw
    finally:
        _PINNED_STATE.reset(token)
        await cm.__aexit__(None, None, None)


def assert_no_concurrent_gather() -> None:
    """Raise ``RuntimeError`` when called inside two ``asyncio.gather``
    siblings that share a pinned connection. Useful as a self-check
    inside backend code that knows it is about to issue a query.
    """
    pinned = _PINNED_STATE.get()
    if pinned is None:
        return
    _wrapper, _conn, counter = pinned
    if counter[0] > 0:
        raise RuntimeError(
            "Detected concurrent ORM queries against a pinned connection: "
            "two asyncio.gather() siblings are sharing the same pool "
            "checkout, which corrupts cursor state. Either move the "
            "pinned_connection() into each sibling, or stop using "
            "gather() inside the pin."
        )


@contextlib.asynccontextmanager
async def _track_inflight():
    """Increment the in-use counter on the active pin (if any) for the
    duration of the wrapped block. Called by backends that respect the
    pin to make :func:`assert_no_concurrent_gather` deterministic."""
    pinned = _PINNED_STATE.get()
    if pinned is None:
        yield
        return
    _wrapper, _conn, counter = pinned
    counter[0] += 1
    try:
        yield
    finally:
        counter[0] -= 1


def get_pinned_connection() -> Any | None:
    """Return the currently pinned async connection, or ``None``.

    Backends call this from their async ``_choose_conn`` so that an
    active ``aatomic()`` connection still wins (transactions take
    precedence over pinning) but otherwise the pinned connection is
    used."""
    state = ASYNC_ATOMIC_STATE.get()
    if state is not None:
        # Atomic always wins.
        return None
    pinned = _PINNED_STATE.get()
    if pinned is None:
        return None
    return pinned[1]


__all__ = [
    "pinned_connection",
    "assert_no_concurrent_gather",
    "get_pinned_connection",
]
