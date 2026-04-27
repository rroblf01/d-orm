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

from typing import Any

import pytest

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


__all__ = ["transactional_db", "atransactional_db", "DormTestCase"]
