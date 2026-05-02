"""Test helpers — transactional fixtures that roll back instead of dropping
and recreating tables for every test.

Speeds up suites by avoiding per-test ``DROP TABLE`` / ``CREATE TABLE``
churn. Each test starts inside an :func:`atomic` block; on exit the block
is rolled back unconditionally, so any data written during the test
disappears without touching the schema.

Use it in a pytest project by adding to ``conftest.py``::

    from dorm.test import transactional_db, atransactional_db  # noqa: F401

…and then either rely on the autouse fixture (when imported by the
star-import variant) or list it in your test signatures::

    def test_something(transactional_db):
        Author.objects.create(name="Alice", age=30)
        # rolled back automatically when the test exits

For pytest-asyncio tests, use ``atransactional_db``.

There is also a :class:`DormTestCase` mixin for unittest-style suites.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import pytest

from . import signals
from .transaction import atomic, aatomic


@pytest.fixture
def transactional_db():
    """Open an :func:`atomic` block around the test, roll it back on exit.

    Pending :func:`on_commit` callbacks scheduled inside the test are
    discarded (rollback semantics) — the test never sees post-commit
    side effects, which is exactly what most unit tests want.
    """
    with atomic() as tx:
        yield
        tx.set_rollback(True)


@pytest.fixture
async def atransactional_db():
    """Async counterpart of :func:`transactional_db` for ``pytest-asyncio``
    tests. Wrap each test in an :func:`aatomic` block that always rolls
    back."""
    async with aatomic() as tx:
        yield
        tx.set_rollback(True)


class DormTestCase:
    """Drop-in mixin for ``unittest.TestCase``-style suites that want
    transactional isolation between tests without managing the fixture
    explicitly. Inherit from this *and* ``unittest.TestCase``::

        class AuthorTests(DormTestCase, unittest.TestCase):
            def test_create(self):
                Author.objects.create(name="Alice", age=30)
                # rolled back at tearDown

    Each ``setUp`` opens an :func:`atomic` block; each ``tearDown`` rolls
    it back. Subclasses overriding either method should call ``super()``.
    """

    _dorm_atomic_cm: Any = None

    def setUp(self):
        # ``super()`` reaches the ``unittest.TestCase`` further up the
        # MRO; ty can't see that without a fake parent, so we silence
        # the static check (the runtime contract is "use this with
        # TestCase or another mixin that calls setUp/tearDown").
        super().setUp()  # type: ignore
        self._dorm_atomic_cm = atomic()
        self._dorm_atomic_cm.__enter__()

    def tearDown(self):
        cm = self._dorm_atomic_cm
        if cm is not None:
            try:
                cm.set_rollback(True)
            finally:
                cm.__exit__(None, None, None)
                self._dorm_atomic_cm = None
        super().tearDown()  # type: ignore


# ── assertNumQueries ─────────────────────────────────────────────────────────
#
# Django parity helper: assert exactly N queries fire inside a block.
# Implementation mirrors ``dorm.contrib.querycount`` — a single
# ``pre_query`` listener is connected on first use and increments a
# per-task counter held in a ``ContextVar``, so async tests don't bleed
# counters across tasks.

_assert_count: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "dorm_test_assert_num_queries", default=None
)

_assert_listener_attached: bool = False
_assert_listener_lock = threading.Lock()


def _on_pre_query_assert(sender: Any, **kwargs: Any) -> None:
    n = _assert_count.get()
    if n is None:
        return
    _assert_count.set(n + 1)


def _ensure_assert_listener() -> None:
    global _assert_listener_attached
    if _assert_listener_attached:
        return
    with _assert_listener_lock:
        if _assert_listener_attached:
            return
        signals.pre_query.connect(_on_pre_query_assert, weak=False)
        _assert_listener_attached = True


class _NumQueriesContext:
    """Returned by :func:`assertNumQueries` so tests can inspect the
    actual count if they want a richer assertion than equality."""

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0


@contextmanager
def assertNumQueries(num: int) -> Iterator[_NumQueriesContext]:
    """Assert that exactly *num* SQL statements fire inside the block.

    Usage::

        def test_list_view(transactional_db):
            with assertNumQueries(3):
                list(Article.objects.select_related("author")[:10])

    The assertion runs on context exit; if the block raises, the
    original exception propagates and the count assertion is skipped
    (the failure is the more interesting signal). For a richer
    handle, the context manager yields a small object whose ``count``
    attribute holds the final count after exit::

        with assertNumQueries(2) as ctx:
            ...
        # ctx.count == 2

    Also usable as a decorator::

        @assertNumQueries(1)
        def test_loaded_with_one_query():
            ...
    """
    _ensure_assert_listener()
    state = _NumQueriesContext()
    token = _assert_count.set(0)
    try:
        yield state
    finally:
        state.count = _assert_count.get() or 0
        _assert_count.reset(token)
    assert state.count == num, (
        f"expected {num} query(ies), got {state.count}"
    )


# Allow ``@assertNumQueries(N)`` as a decorator on top of the
# context-manager form. Wrapping ``contextmanager`` output is fiddly
# because the generator-function it returns doesn't itself act as a
# decorator factory, so we expose a tiny shim.
#
# The decorator inspects the wrapped function: ``async def`` test
# functions get an async wrapper that ``await``s the coroutine inside
# the context manager — without this the sync wrapper would call the
# ``async def`` (returning a coroutine) and exit the context manager
# *before* the coroutine ran, so every query landed outside the count
# window and the assertion always failed with count 0.
def _decorate_with_num_queries(num: int):
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def awrapper(*a: Any, **kw: Any) -> Any:
                with assertNumQueries(num):
                    return await fn(*a, **kw)
            return awrapper

        @functools.wraps(fn)
        def wrapper(*a: Any, **kw: Any) -> Any:
            with assertNumQueries(num):
                return fn(*a, **kw)
        return wrapper
    return deco


def assertNumQueriesFactory(num: int):
    """Decorator factory equivalent of :func:`assertNumQueries` —
    use as ``@assertNumQueriesFactory(N)`` on a test function (sync
    or ``async def``). The context-manager form remains the primary
    API for inline use.
    """
    return _decorate_with_num_queries(num)


__all__ = [
    "transactional_db",
    "atransactional_db",
    "DormTestCase",
    "assertNumQueries",
    "assertNumQueriesFactory",
]
