"""Test helpers — transactional fixtures, query-count assertions and
the unittest mixin.

The pytest fixtures (``transactional_db`` / ``atransactional_db``) need
``pytest`` at import time. Everything else (``assertNumQueries``,
``assertMaxQueries``, ``DormTestCase``) works without pytest, so the
import is guarded — projects using stdlib ``unittest`` only can still
call ``from dorm.test import assertNumQueries`` without a pytest
install.

Use the fixtures in a pytest project by adding to ``conftest.py``::

    from dorm.test import transactional_db, atransactional_db  # noqa: F401

Then list them in your test signatures::

    def test_something(transactional_db):
        Author.objects.create(name="Alice", age=30)
        # rolled back automatically when the test exits

For pytest-asyncio tests, use ``atransactional_db``.
"""

from __future__ import annotations

import functools
import inspect
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from . import signals
from ._scoped import ScopedCollector
from .transaction import atomic, aatomic

try:
    import pytest as _pytest_mod
    pytest: Any = _pytest_mod
except ImportError:  # pragma: no cover - pytest is a soft dep
    pytest = None


if pytest is not None:

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

else:  # pragma: no cover - pytest absent

    def transactional_db(*args: Any, **kwargs: Any):
        raise RuntimeError(
            "dorm.test.transactional_db requires pytest. Install it via "
            "`pip install pytest`."
        )

    def atransactional_db(*args: Any, **kwargs: Any):
        raise RuntimeError(
            "dorm.test.atransactional_db requires pytest. Install it via "
            "`pip install pytest pytest-asyncio`."
        )


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


# ── Query-count assertions ───────────────────────────────────────────────────
#
# Per-task isolation via ``ScopedCollector`` over the ``pre_query``
# signal. State is a single-element list so the receiver mutates
# ``state[0]`` in place — one ``ContextVar.set`` per assertion block
# instead of per query.

def _bump(state: list[int], _kwargs: dict[str, Any]) -> None:
    state[0] += 1


_collector: ScopedCollector[list[int]] = ScopedCollector(
    signals.pre_query, "dorm_test_assert_num_queries", _bump
)


class _NumQueriesContext:
    """Yielded by :func:`assertNumQueries` / :func:`assertMaxQueries` so
    callers can read the actual count after the block exits — useful when
    the assertion is just one of several checks the test wants to make."""

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
    (the failure is the more interesting signal).
    """
    handle = _NumQueriesContext()
    state: list[int] = [0]
    token = _collector.open(state)
    try:
        yield handle
    finally:
        handle.count = state[0]
        _collector.close(token)
    assert handle.count == num, (
        f"expected {num} query(ies), got {handle.count}"
    )


@contextmanager
def assertMaxQueries(num: int) -> Iterator[_NumQueriesContext]:
    """Assert that *at most* ``num`` SQL statements fire inside the
    block. Fewer is fine — useful when the upper bound is what
    matters (defending against an N+1 regression) without pinning the
    exact count."""
    handle = _NumQueriesContext()
    state: list[int] = [0]
    token = _collector.open(state)
    try:
        yield handle
    finally:
        handle.count = state[0]
        _collector.close(token)
    assert handle.count <= num, (
        f"expected at most {num} query(ies), got {handle.count}"
    )


def _decorate(num: int, *, max_only: bool):
    """Build a decorator that wraps the function in the appropriate
    assertion context manager. ``async def`` functions get an async
    wrapper so the coroutine actually runs INSIDE the count window
    — the previous version returned a sync wrapper that exited the
    context manager before the coroutine awaited any query, so every
    async test failed with count 0.
    """
    cm = assertMaxQueries if max_only else assertNumQueries

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def awrapper(*a: Any, **kw: Any) -> Any:
                with cm(num):
                    return await fn(*a, **kw)
            return awrapper

        @functools.wraps(fn)
        def wrapper(*a: Any, **kw: Any) -> Any:
            with cm(num):
                return fn(*a, **kw)
        return wrapper
    return deco


def assertNumQueriesFactory(num: int):
    """Decorator factory equivalent of :func:`assertNumQueries` —
    use as ``@assertNumQueriesFactory(N)`` on a test function (sync or
    ``async def``)."""
    return _decorate(num, max_only=False)


def assertMaxQueriesFactory(num: int):
    """Decorator factory equivalent of :func:`assertMaxQueries`."""
    return _decorate(num, max_only=True)


class override_settings:
    """Context manager + decorator that temporarily mutates dorm
    settings for the duration of the wrapped block / function.

    Mirrors Django's ``django.test.utils.override_settings``::

        with override_settings(SLOW_QUERY_MS=10):
            run_some_queries()

        @override_settings(USE_TZ=True, TIME_ZONE="Europe/Madrid")
        def test_tz_aware(): ...

    Reverts every mutated key on exit, including deletion of keys
    that didn't exist before.
    """

    def __init__(self, **overrides: Any) -> None:
        self.overrides = overrides
        self._prev: dict[str, Any] = {}
        self._absent: set[str] = set()

    def _apply(self) -> None:
        from .conf import settings

        for k, v in self.overrides.items():
            if k in settings._explicit_settings:
                self._prev[k] = getattr(settings, k)
            else:
                self._absent.add(k)
            setattr(settings, k, v)
            settings._explicit_settings.add(k)

    def _revert(self) -> None:
        from .conf import settings

        for k in self.overrides:
            if k in self._prev:
                setattr(settings, k, self._prev[k])
            else:
                if k in settings._explicit_settings:
                    try:
                        delattr(settings, k)
                    except AttributeError:
                        pass
                    settings._explicit_settings.discard(k)
        self._prev.clear()
        self._absent.clear()

    def __enter__(self):
        self._apply()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._revert()

    async def __aenter__(self):
        self._apply()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._revert()

    def __call__(self, fn):
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def awrapper(*a: Any, **kw: Any):
                self._apply()
                try:
                    return await fn(*a, **kw)
                finally:
                    self._revert()

            return awrapper

        @functools.wraps(fn)
        def wrapper(*a: Any, **kw: Any):
            self._apply()
            try:
                return fn(*a, **kw)
            finally:
                self._revert()

        return wrapper


def setUpTestData(get_data: Callable[..., dict[str, Any]]):
    """Class decorator factory that runs *get_data* once at class
    construction and attaches every ``{attr: value}`` entry as a
    class attribute.

    Mirrors Django's :meth:`TestCase.setUpTestData` semantics —
    rows created once and reused across test methods — without the
    Django metaclass machinery (pytest test classes don't inherit
    from ``unittest.TestCase`` by default).

    Usage::

        def _data(cls):
            return {
                "alice": Author.objects.create(name="Alice", age=30),
                "bob":   Author.objects.create(name="Bob",   age=40),
            }

        @setUpTestData(_data)
        class TestAuthor:
            def test_alice_age(self):
                assert self.alice.age == 30  # type: ignore[attr-defined]

    Tests must roll back any mutation they make to the shared rows
    (or rebuild via a fixture) — same contract Django enforces.
    """
    if not callable(get_data):
        raise TypeError(
            "setUpTestData expects a callable ``cls -> dict``."
        )

    def decorate(target_cls):
        data = get_data(target_cls)
        if not isinstance(data, dict):
            raise TypeError(
                "setUpTestData callable must return a dict."
            )
        for k, v in data.items():
            setattr(target_cls, k, v)
        return target_cls

    return decorate


__all__ = [
    "transactional_db",
    "atransactional_db",
    "DormTestCase",
    "assertNumQueries",
    "assertNumQueriesFactory",
    "assertMaxQueries",
    "assertMaxQueriesFactory",
    "override_settings",
    "setUpTestData",
]
