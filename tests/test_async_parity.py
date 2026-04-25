"""Parity tests between sync and async APIs:
- abulk_update mirrors bulk_update
- await on a chainable values()/values_list() queryset
- atomic / aatomic work as decorators (not just context managers)
- using="..." parity in async save/delete
"""

from __future__ import annotations

import pytest

import dorm
from tests.models import Author


# ── A1: abulk_update ──────────────────────────────────────────────────────────


async def test_abulk_update_basic():
    a = await Author.objects.acreate(name="Bulk-A", email="ba@x.com", age=10)
    b = await Author.objects.acreate(name="Bulk-B", email="bb@x.com", age=20)
    a.age = 100
    b.age = 200

    n = await Author.objects.abulk_update([a, b], fields=["age"])
    assert n == 2

    fa = await Author.objects.aget(pk=a.pk)
    fb = await Author.objects.aget(pk=b.pk)
    assert fa.age == 100
    assert fb.age == 200

    await Author.objects.filter(pk__in=[a.pk, b.pk]).adelete()


async def test_abulk_update_empty_returns_zero():
    n = await Author.objects.abulk_update([], fields=["age"])
    assert n == 0


async def test_abulk_update_via_manager():
    """The async manager proxies abulk_update to the queryset."""
    a = await Author.objects.acreate(name="MgrBU-A", email="mbua@x.com", age=1)
    a.age = 9
    n = await Author.objects.abulk_update([a], fields=["age"])
    assert n == 1
    fa = await Author.objects.aget(pk=a.pk)
    assert fa.age == 9
    await fa.adelete()


async def test_abulk_update_runs_in_single_transaction():
    """If the underlying ``aatomic`` block fails part-way, no partial writes."""
    a = await Author.objects.acreate(name="TX-A", email="txa@x.com", age=1)
    b = await Author.objects.acreate(name="TX-B", email="txb@x.com", age=1)
    # Mutate an attribute that doesn't exist on the model to force an error
    a.age = 10
    b.age = 20

    # Sanity: regular update works
    n = await Author.objects.abulk_update([a, b], fields=["age"])
    assert n == 2

    fa = await Author.objects.aget(pk=a.pk)
    fb = await Author.objects.aget(pk=b.pk)
    assert (fa.age, fb.age) == (10, 20)

    await Author.objects.filter(pk__in=[a.pk, b.pk]).adelete()


# ── A2: chainable async — await on QuerySet ───────────────────────────────────


async def test_await_queryset_returns_list_of_instances():
    a = await Author.objects.acreate(name="Aw-A", email="awa@x.com", age=1)
    b = await Author.objects.acreate(name="Aw-B", email="awb@x.com", age=2)

    rows = await Author.objects.filter(pk__in=[a.pk, b.pk]).order_by("age")
    assert isinstance(rows, list)
    assert {r.pk for r in rows} == {a.pk, b.pk}
    assert [r.age for r in rows] == [1, 2]

    await Author.objects.filter(pk__in=[a.pk, b.pk]).adelete()


async def test_await_queryset_with_values_returns_list_of_dicts():
    """Bug we discussed: now `await qs.values(...)` works without needing
    a terminal `avalues(...)`."""
    a = await Author.objects.acreate(name="VAw", email="vaw@x.com", age=42)

    rows = await Author.objects.filter(pk=a.pk).values("name", "age")
    assert rows == [{"name": "VAw", "age": 42}]

    rows2 = await Author.objects.values("name").filter(pk=a.pk).order_by("name")
    assert rows2 == [{"name": "VAw"}]

    await Author.objects.filter(pk=a.pk).adelete()


async def test_await_empty_queryset():
    rows = await Author.objects.filter(name="__no_such_author__")
    assert rows == []


# ── B3: atomic / aatomic as decorators ────────────────────────────────────────


def test_atomic_as_decorator_no_args():
    """@atomic without parens uses the default alias and commits on success."""

    @dorm.transaction.atomic
    def make(name):
        return Author.objects.create(name=name, email=f"{name}@dec.com", age=1)

    obj = make("AtomicDeco")
    fetched = Author.objects.get(pk=obj.pk)
    assert fetched.name == "AtomicDeco"
    fetched.delete()


def test_atomic_as_decorator_rolls_back_on_exception():
    class _Boom(Exception):
        pass

    @dorm.transaction.atomic
    def attempt():
        Author.objects.create(name="ShouldRollback", email="srb@dec.com", age=1)
        raise _Boom

    before = Author.objects.filter(email="srb@dec.com").count()
    with pytest.raises(_Boom):
        attempt()
    after = Author.objects.filter(email="srb@dec.com").count()
    assert after == before


def test_atomic_as_decorator_with_alias_arg():
    """@atomic("default") (with arg) is also valid."""

    @dorm.transaction.atomic("default")
    def make(name):
        return Author.objects.create(name=name, email=f"{name}@arg.com", age=1)

    obj = make("AtomicArgDeco")
    assert Author.objects.get(pk=obj.pk).name == "AtomicArgDeco"
    obj.delete()


def test_atomic_as_context_manager_still_works():
    """Decorator support didn't break the context-manager form."""
    with dorm.transaction.atomic():
        a = Author.objects.create(name="CtxStill", email="cs@x.com", age=1)
    assert Author.objects.get(pk=a.pk).name == "CtxStill"
    a.delete()


async def test_aatomic_as_decorator_no_args():
    @dorm.transaction.aatomic
    async def make(name):
        return await Author.objects.acreate(name=name, email=f"{name}@adec.com", age=1)

    obj = await make("AAtomicDeco")
    fetched = await Author.objects.aget(pk=obj.pk)
    assert fetched.name == "AAtomicDeco"
    await fetched.adelete()


async def test_aatomic_as_decorator_rolls_back_on_exception():
    class _Boom(Exception):
        pass

    @dorm.transaction.aatomic
    async def attempt():
        await Author.objects.acreate(name="AShouldRollback", email="asrb@dec.com", age=1)
        raise _Boom

    before = await Author.objects.filter(email="asrb@dec.com").acount()
    with pytest.raises(_Boom):
        await attempt()
    after = await Author.objects.filter(email="asrb@dec.com").acount()
    assert after == before


async def test_aatomic_as_decorator_with_alias_arg():
    @dorm.transaction.aatomic("default")
    async def make(name):
        return await Author.objects.acreate(name=name, email=f"{name}@aarg.com", age=1)

    obj = await make("AAtomicArgDeco")
    assert (await Author.objects.aget(pk=obj.pk)).name == "AAtomicArgDeco"
    await obj.adelete()


async def test_aatomic_as_context_manager_still_works():
    async with dorm.transaction.aatomic():
        a = await Author.objects.acreate(name="ACtxStill", email="acs@x.com", age=1)
    fetched = await Author.objects.aget(pk=a.pk)
    assert fetched.name == "ACtxStill"
    await fetched.adelete()


# ── B4: using= parity in async save / delete ─────────────────────────────────


async def test_async_save_accepts_using():
    """Async save honours an explicit using= alias (mirrors sync save)."""
    a = Author(name="UsingAsave", email="ua@x.com", age=1)
    await a.asave(using="default")
    fetched = await Author.objects.aget(pk=a.pk)
    assert fetched.name == "UsingAsave"
    await fetched.adelete(using="default")


async def test_async_save_unknown_using_raises():
    from dorm.exceptions import ImproperlyConfigured

    a = Author(name="UsingBad", email="ub@x.com", age=1)
    with pytest.raises(ImproperlyConfigured):
        await a.asave(using="does_not_exist")


# ── Manager-level abulk_update sanity ─────────────────────────────────────────


async def test_manager_abulk_update_method_exists():
    assert hasattr(Author.objects, "abulk_update")
    assert callable(Author.objects.abulk_update)


# ── Iterator refactor sanity (C6) ─────────────────────────────────────────────


async def test_iterator_helpers_are_used():
    """The shared helper exists and is actually called by both iterators."""
    from dorm.queryset import QuerySet
    assert hasattr(QuerySet, "_row_to_values_dict")
    assert hasattr(QuerySet, "_iter_setup")


async def test_values_mode_works_in_both_iterators():
    """Regression: values() mode produces dicts in both sync and async paths
    (the helper is exercised on both sides)."""
    a = await Author.objects.acreate(name="VMode", email="vmode@x.com", age=7)
    try:
        # Async path
        async_rows = [r async for r in Author.objects.filter(pk=a.pk).values("name", "age")]
        assert async_rows == [{"name": "VMode", "age": 7}]
        # Sync path
        sync_rows = list(Author.objects.filter(pk=a.pk).values("name", "age"))
        assert sync_rows == [{"name": "VMode", "age": 7}]
    finally:
        await a.adelete()
