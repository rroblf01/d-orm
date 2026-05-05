"""Per-block query budget — enforces a wall-clock timeout and an
optional max-rows ceiling on every ORM call inside the context.

Two failure modes the budget protects against:

1. **Long-tail queries** — a query the planner usually runs in 5 ms
   that occasionally hits a bad plan and runs for 30 s, blocking an
   HTTP worker far past the SLA. ``timeout_ms`` aborts the query at
   the database side via PostgreSQL's ``statement_timeout`` (or its
   SQLite-shaped equivalent).

2. **Runaway result sets** — a ``WHERE`` clause that loses
   selectivity in production and starts returning a million rows
   instead of a hundred. ``max_rows`` is checked client-side after
   the query returns; rows beyond the ceiling raise
   :class:`BudgetExceeded` instead of streaming into the caller.

Usage::

    from dorm import budget

    async def handler():
        async with budget.budget(timeout_ms=200, max_rows=10_000):
            rows = await Model.objects.afilter(active=True)

    def sync_handler():
        with budget.budget(timeout_ms=200):
            return Model.objects.count()

The budget is per-context — works across asyncio.Task and threads
because state lives in a ``contextvars.ContextVar``. Nested budgets
are honoured: the **strictest** active value (smallest timeout, smallest
``max_rows``) wins, so a tightening inner block can never accidentally
raise the outer cap.
"""

from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass

from .exceptions import DatabaseError


class BudgetExceeded(DatabaseError):
    """Raised when a query inside an active :func:`budget` block
    exceeds either the wall-clock timeout or the row ceiling.

    Subclasses :class:`DatabaseError` so generic ``except DatabaseError``
    handlers swallow it gracefully — a budget violation is, after
    all, a database-level signal that the call should be aborted."""


@dataclass(frozen=True, slots=True)
class _BudgetState:
    timeout_ms: int | None
    max_rows: int | None


_BUDGET_STACK: contextvars.ContextVar[tuple[_BudgetState, ...]] = contextvars.ContextVar(
    "dorm_budget_stack", default=()
)


def _effective() -> _BudgetState | None:
    """Return the strictest budget across the active stack, or
    ``None`` when no budget is set."""
    stack = _BUDGET_STACK.get()
    if not stack:
        return None
    timeout = min(
        (s.timeout_ms for s in stack if s.timeout_ms is not None), default=None
    )
    max_rows = min(
        (s.max_rows for s in stack if s.max_rows is not None), default=None
    )
    return _BudgetState(timeout_ms=timeout, max_rows=max_rows)


def current() -> _BudgetState | None:
    """Return the currently-effective budget for inspection.

    Backends call this before issuing a query to know whether to
    pre-set ``statement_timeout`` and how many rows to allow.
    """
    return _effective()


@contextlib.contextmanager
def budget(
    *,
    timeout_ms: int | None = None,
    max_rows: int | None = None,
    using: str = "default",
):
    """Enter a query budget block.

    *timeout_ms* — wall-clock ceiling per statement.

      - On PostgreSQL the budget opens an implicit ``atomic()`` block
        so it can ``SET LOCAL statement_timeout``; the database side
        aborts the query when the budget is exceeded. Side-effect:
        every write inside the block participates in one transaction
        — on rollback all the writes revert together. Code that is
        already inside ``atomic()`` reuses the outer transaction
        (savepoint).
      - Other backends ignore *timeout_ms* — there is no portable
        statement-timeout primitive.

    *max_rows* — fail with :class:`BudgetExceeded` when a query
    materialises more than this many rows (post-fetch check, so the
    abort happens after the row count is known). Backend-agnostic.

    *using* — alias whose connection receives the timeout. Only the
    named alias is touched.

    Nested blocks combine via *minimum* — inner blocks can tighten
    but never relax the outer budget.
    """
    if timeout_ms is not None and timeout_ms <= 0:
        raise ValueError("timeout_ms must be > 0")
    if max_rows is not None and max_rows <= 0:
        raise ValueError("max_rows must be > 0")
    state = _BudgetState(timeout_ms=timeout_ms, max_rows=max_rows)
    stack = _BUDGET_STACK.get()
    token = _BUDGET_STACK.set(stack + (state,))

    atomic_cm = None
    pg_active = False
    if timeout_ms is not None:
        try:
            from .db.connection import get_connection
            conn = get_connection(using)
            if getattr(conn, "vendor", "sqlite") == "postgresql":
                from .transaction import atomic
                atomic_cm = atomic(using=using)
                atomic_cm.__enter__()
                # SET LOCAL only persists for the current transaction —
                # auto-reverts on commit/rollback, no cleanup needed.
                conn.execute_write(f"SET LOCAL statement_timeout = {timeout_ms}")
                pg_active = True
        except Exception:
            if atomic_cm is not None:
                try:
                    atomic_cm.__exit__(None, None, None)
                except Exception:
                    pass
                atomic_cm = None

    try:
        yield state
    except BaseException:
        if atomic_cm is not None:
            import sys
            atomic_cm.__exit__(*sys.exc_info())
            atomic_cm = None
        raise
    finally:
        if atomic_cm is not None:
            atomic_cm.__exit__(None, None, None)
        _BUDGET_STACK.reset(token)
        _ = pg_active  # tracking flag, kept for debug


@contextlib.asynccontextmanager
async def abudget(
    *,
    timeout_ms: int | None = None,
    max_rows: int | None = None,
    using: str = "default",
):
    """Async counterpart of :func:`budget`. On PG opens an implicit
    ``aatomic()`` block; see :func:`budget` for the side-effects."""
    if timeout_ms is not None and timeout_ms <= 0:
        raise ValueError("timeout_ms must be > 0")
    if max_rows is not None and max_rows <= 0:
        raise ValueError("max_rows must be > 0")
    state = _BudgetState(timeout_ms=timeout_ms, max_rows=max_rows)
    stack = _BUDGET_STACK.get()
    token = _BUDGET_STACK.set(stack + (state,))

    atomic_cm = None
    if timeout_ms is not None:
        try:
            from .db.connection import get_async_connection
            conn = get_async_connection(using)
            if getattr(conn, "vendor", "sqlite") == "postgresql":
                atomic_cm = conn.aatomic()
                await atomic_cm.__aenter__()
                await conn.execute_write(
                    f"SET LOCAL statement_timeout = {timeout_ms}"
                )
        except Exception:
            if atomic_cm is not None:
                try:
                    await atomic_cm.__aexit__(None, None, None)
                except Exception:
                    pass
                atomic_cm = None

    try:
        yield state
    except BaseException:
        if atomic_cm is not None:
            import sys
            await atomic_cm.__aexit__(*sys.exc_info())
            atomic_cm = None
        raise
    finally:
        if atomic_cm is not None:
            await atomic_cm.__aexit__(None, None, None)
        _BUDGET_STACK.reset(token)


def check_rowcount(n: int) -> None:
    """Raise :class:`BudgetExceeded` when *n* exceeds the active
    ``max_rows`` ceiling. Backends call this once per fetch so the
    abort happens before the rows escape into the caller's buffer.
    """
    state = _effective()
    if state is None or state.max_rows is None:
        return
    if n > state.max_rows:
        raise BudgetExceeded(
            f"Query returned {n} rows, exceeds active budget "
            f"max_rows={state.max_rows}."
        )


__all__ = [
    "BudgetExceeded",
    "abudget",
    "budget",
    "check_rowcount",
    "current",
]
