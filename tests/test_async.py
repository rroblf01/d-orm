"""Asynchronous ORM tests (pytest-asyncio)."""
import pytest

import dorm
from dorm import Q
from tests.models import Author


pytestmark = pytest.mark.asyncio


# ── create / get ──────────────────────────────────────────────────────────────

async def test_acreate_and_aget():
    alice = await Author.objects.acreate(name="Alice", age=30)
    assert alice.pk is not None
    found = await Author.objects.aget(pk=alice.pk)
    assert found.name == "Alice"


async def test_aget_does_not_exist():
    with pytest.raises(Author.DoesNotExist):
        await Author.objects.aget(name="Nobody")


async def test_aget_multiple_objects_returned():
    await Author.objects.acreate(name="Twin", age=20)
    await Author.objects.acreate(name="Twin", age=21)
    with pytest.raises(Author.MultipleObjectsReturned):
        await Author.objects.aget(name="Twin")


# ── filter / count / exists ───────────────────────────────────────────────────

async def test_async_filter():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=25)
    results = [a async for a in Author.objects.filter(age__gte=28)]
    assert len(results) == 1
    assert results[0].name == "Alice"


async def test_acount():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=25)
    assert await Author.objects.acount() == 2


async def test_aexists_true():
    await Author.objects.acreate(name="Alice", age=30)
    assert await Author.objects.filter(name="Alice").aexists() is True


async def test_aexists_false():
    assert await Author.objects.filter(name="Nobody").aexists() is False


# ── first / last ──────────────────────────────────────────────────────────────

async def test_afirst():
    await Author.objects.acreate(name="Carol", age=35)
    await Author.objects.acreate(name="Bob", age=25)
    first = await Author.objects.order_by("age").afirst()
    assert first is not None
    assert first.name == "Bob"


async def test_alast():
    await Author.objects.acreate(name="Carol", age=35)
    await Author.objects.acreate(name="Bob", age=25)
    last = await Author.objects.order_by("age").alast()
    assert last is not None
    assert last.name == "Carol"


async def test_afirst_empty():
    result = await Author.objects.afirst()
    assert result is None


# ── update / delete ───────────────────────────────────────────────────────────

async def test_aupdate():
    await Author.objects.acreate(name="Alice", age=30)
    n = await Author.objects.filter(name="Alice").aupdate(age=31)
    assert n == 1
    alice = await Author.objects.aget(name="Alice")
    assert alice.age == 31


async def test_adelete_queryset():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=25)
    count, _ = await Author.objects.filter(name="Alice").adelete()
    assert count == 1
    assert await Author.objects.acount() == 1


async def test_adelete_instance():
    alice = await Author.objects.acreate(name="Alice", age=30)
    await alice.adelete()
    assert not await Author.objects.filter(name="Alice").aexists()


# ── get_or_create / update_or_create ─────────────────────────────────────────

async def test_aget_or_create_creates():
    obj, created = await Author.objects.aget_or_create(
        name="Alice", defaults={"age": 30}
    )
    assert created is True


async def test_aget_or_create_gets():
    await Author.objects.acreate(name="Alice", age=30)
    obj, created = await Author.objects.aget_or_create(
        name="Alice", defaults={"age": 99}
    )
    assert created is False
    assert obj.age == 30


async def test_aupdate_or_create_creates():
    obj, created = await Author.objects.aupdate_or_create(
        name="Alice", defaults={"age": 30}
    )
    assert created is True


async def test_aupdate_or_create_updates():
    await Author.objects.acreate(name="Alice", age=30)
    obj, created = await Author.objects.aupdate_or_create(
        name="Alice", defaults={"age": 31}
    )
    assert created is False
    assert obj.age == 31


# ── save / refresh ────────────────────────────────────────────────────────────

async def test_asave():
    alice = await Author.objects.acreate(name="Alice", age=30)
    alice.age = 31
    await alice.asave()
    fresh = await Author.objects.aget(pk=alice.pk)
    assert fresh.age == 31


async def test_arefresh_from_db():
    alice = await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.filter(pk=alice.pk).aupdate(age=99)
    await alice.arefresh_from_db()
    assert alice.age == 99


# ── bulk / in_bulk ────────────────────────────────────────────────────────────

async def test_abulk_create():
    objs = [Author(name=f"Bulk{i}", age=20 + i) for i in range(3)]
    await Author.objects.abulk_create(objs)
    assert await Author.objects.acount() == 3


async def test_ain_bulk():
    a1 = await Author.objects.acreate(name="Alice", age=30)
    a2 = await Author.objects.acreate(name="Bob", age=25)
    result = await Author.objects.ain_bulk([a1.pk, a2.pk])
    assert len(result) == 2
    assert result[a1.pk].name == "Alice"


# ── async iteration ───────────────────────────────────────────────────────────

async def test_async_iteration():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=25)
    names = []
    async for author in Author.objects.order_by("name"):
        names.append(author.name)
    assert names == ["Alice", "Bob"]


async def test_async_aggregate():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=20)
    result = await Author.objects.aaggregate(
        total=dorm.Count("id"), avg=dorm.Avg("age")
    )
    assert result["total"] == 2
    assert result["avg"] == 25.0


# ── Q objects async ───────────────────────────────────────────────────────────

async def test_async_q_or():
    await Author.objects.acreate(name="Alice", age=30)
    await Author.objects.acreate(name="Bob", age=25)
    await Author.objects.acreate(name="Carol", age=35)
    results = [a async for a in Author.objects.filter(Q(age__lt=28) | Q(age__gt=33))]
    names = sorted(a.name for a in results)
    assert names == ["Bob", "Carol"]
