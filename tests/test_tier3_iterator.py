"""Tests for Tier-3.2: iterator() streaming."""
from __future__ import annotations

import pytest

from tests.models import Author


def test_iterator_yields_instances():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    result = list(Author.objects.all().iterator())
    assert len(result) == 2
    assert all(isinstance(a, Author) for a in result)


def test_iterator_does_not_cache():
    Author.objects.create(name="Alice", age=30)

    qs = Author.objects.all()
    assert qs._result_cache is None

    # Consuming iterator should NOT populate _result_cache
    list(qs.iterator())
    assert qs._result_cache is None


def test_regular_iteration_does_cache():
    Author.objects.create(name="Alice", age=30)

    qs = Author.objects.all()
    list(qs)  # regular __iter__ calls _fetch_all
    assert qs._result_cache is not None


def test_iterator_with_filter():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)
    Author.objects.create(name="Carol", age=50)

    result = list(Author.objects.filter(age__gte=40).iterator())
    assert len(result) == 2
    names = {a.name for a in result}
    assert names == {"Bob", "Carol"}


def test_iterator_chunk_size_accepted():
    Author.objects.create(name="Alice", age=30)

    # chunk_size should be accepted without error (even if not used for batching)
    result = list(Author.objects.all().iterator(chunk_size=100))
    assert len(result) == 1


def test_iterator_empty_queryset():
    result = list(Author.objects.none().iterator())
    assert result == []


@pytest.mark.asyncio
async def test_aiterator_yields_instances():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    result = []
    async for a in Author.objects.all().aiterator():
        result.append(a)

    assert len(result) == 2
    assert all(isinstance(a, Author) for a in result)


@pytest.mark.asyncio
async def test_aiterator_does_not_cache():
    Author.objects.create(name="Alice", age=30)

    qs = Author.objects.all()
    assert qs._result_cache is None

    async for _ in qs.aiterator():
        pass

    assert qs._result_cache is None
