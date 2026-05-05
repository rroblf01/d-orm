"""Tests for ``dorm.budget``.

Wall-clock timeout is exercised end-to-end on PostgreSQL via
``statement_timeout``; on SQLite the test only checks the row-ceiling
path because there is no portable server-side timeout.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.budget import BudgetExceeded, abudget, budget, current
from tests.models import Author


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_budget_validation():
    with pytest.raises(ValueError):
        with budget(timeout_ms=0):
            pass
    with pytest.raises(ValueError):
        with budget(max_rows=0):
            pass


def test_budget_no_op_when_no_active_block():
    assert current() is None


def test_budget_max_rows_violation_raises():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(20)])
    with pytest.raises(BudgetExceeded, match="20 rows"):
        with budget(max_rows=5):
            list(Author.objects.all())


def test_budget_max_rows_within_ceiling_passes():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(3)])
    with budget(max_rows=10):
        rows = list(Author.objects.all())
    assert len(rows) == 3


def test_budget_nested_minimum_wins():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(20)])
    # Outer permits 100, inner tightens to 5 — inner must win.
    with budget(max_rows=100):
        with pytest.raises(BudgetExceeded, match="20 rows"):
            with budget(max_rows=5):
                list(Author.objects.all())


def test_budget_state_pops_on_exit():
    with budget(max_rows=5):
        active = current()
        assert active is not None
        assert active.max_rows == 5
    assert current() is None


def test_budget_pg_statement_timeout(db_config):
    """End-to-end: a slow query inside a tight ``timeout_ms`` block
    aborts at the database side rather than running to completion."""
    if not _is_postgres(db_config):
        pytest.skip("statement_timeout is PG-specific")
    from dorm.db.connection import get_connection
    from dorm.exceptions import OperationalError, DatabaseError

    conn = get_connection()
    with pytest.raises((OperationalError, DatabaseError)):
        with budget(timeout_ms=10):
            # ``pg_sleep(2)`` runs for 2 s; the 10 ms budget aborts it.
            conn.execute("SELECT pg_sleep(2)")


@pytest.mark.asyncio
async def test_abudget_max_rows():
    await Author.objects.abulk_create(
        [Author(name=f"x{i}", age=i) for i in range(7)]
    )
    with pytest.raises(BudgetExceeded):
        async with abudget(max_rows=3):
            await Author.objects.all()


@pytest.mark.asyncio
async def test_abudget_pg_statement_timeout(db_config):
    if not _is_postgres(db_config):
        pytest.skip("statement_timeout is PG-specific")
    from dorm.db.connection import get_async_connection
    from dorm.exceptions import OperationalError, DatabaseError

    conn = get_async_connection()
    with pytest.raises((OperationalError, DatabaseError)):
        async with abudget(timeout_ms=10):
            await conn.execute("SELECT pg_sleep(2)")


def test_budget_exposes_via_dorm_namespace():
    assert dorm.budget is not None
    assert dorm.BudgetExceeded is BudgetExceeded
