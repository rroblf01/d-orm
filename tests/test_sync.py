"""Synchronous ORM tests."""
import pytest

import dorm
from dorm import Q
from tests.models import Author, Book


# ── create / get ──────────────────────────────────────────────────────────────

def test_create_and_get():
    alice = Author.objects.create(name="Alice", age=30)
    assert alice.pk is not None
    found = Author.objects.get(pk=alice.pk)
    assert found.name == "Alice"
    assert found.age == 30


def test_get_does_not_exist():
    with pytest.raises(Author.DoesNotExist):
        Author.objects.get(name="Nobody")


def test_get_multiple_objects_returned():
    Author.objects.create(name="Twin", age=20)
    Author.objects.create(name="Twin", age=21)
    with pytest.raises(Author.MultipleObjectsReturned):
        Author.objects.get(name="Twin")


# ── filter / exclude ──────────────────────────────────────────────────────────

def test_filter_exact():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    results = list(Author.objects.filter(name="Alice"))
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_filter_gte():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    Author.objects.create(name="Carol", age=35)
    adults = list(Author.objects.filter(age__gte=30))
    names = sorted(a.name for a in adults)
    assert names == ["Alice", "Carol"]


def test_filter_icontains():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    results = list(Author.objects.filter(name__icontains="ali"))
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_filter_in():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    Author.objects.create(name="Carol", age=35)
    results = list(Author.objects.filter(name__in=["Alice", "Carol"]))
    assert len(results) == 2


def test_filter_isnull():
    Author.objects.create(name="Alice", age=30, email=None)
    Author.objects.create(name="Bob", age=25, email="bob@example.com")
    no_email = list(Author.objects.filter(email__isnull=True))
    assert len(no_email) == 1
    assert no_email[0].name == "Alice"


def test_exclude():
    Author.objects.create(name="Alice", age=30, is_active=True)
    Author.objects.create(name="Bob", age=25, is_active=False)
    active = list(Author.objects.exclude(is_active=False))
    assert len(active) == 1
    assert active[0].name == "Alice"


def test_filter_startswith():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Albert", age=28)
    Author.objects.create(name="Bob", age=25)
    results = list(Author.objects.filter(name__startswith="Al"))
    assert len(results) == 2


def test_filter_range():
    Author.objects.create(name="Alice", age=20)
    Author.objects.create(name="Bob", age=30)
    Author.objects.create(name="Carol", age=40)
    results = list(Author.objects.filter(age__range=(25, 35)))
    assert len(results) == 1
    assert results[0].name == "Bob"


# ── Q objects ────────────────────────────────────────────────────────────────

def test_q_or():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    Author.objects.create(name="Carol", age=35)
    results = list(Author.objects.filter(Q(age__lt=28) | Q(age__gt=33)))
    names = sorted(a.name for a in results)
    assert names == ["Bob", "Carol"]


def test_q_and():
    Author.objects.create(name="Alice", age=30, is_active=True)
    Author.objects.create(name="Bob", age=30, is_active=False)
    results = list(Author.objects.filter(Q(age=30) & Q(is_active=True)))
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_q_negate():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    results = list(Author.objects.filter(~Q(name="Alice")))
    assert len(results) == 1
    assert results[0].name == "Bob"


# ── count / exists ────────────────────────────────────────────────────────────

def test_count():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    assert Author.objects.count() == 2


def test_exists_true():
    Author.objects.create(name="Alice", age=30)
    assert Author.objects.filter(name="Alice").exists() is True


def test_exists_false():
    assert Author.objects.filter(name="Nobody").exists() is False


# ── order_by / first / last ───────────────────────────────────────────────────

def test_order_by_asc():
    Author.objects.create(name="Carol", age=35)
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    ordered = [a.name for a in Author.objects.order_by("age")]
    assert ordered == ["Bob", "Alice", "Carol"]


def test_order_by_desc():
    Author.objects.create(name="Carol", age=35)
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    ordered = [a.name for a in Author.objects.order_by("-age")]
    assert ordered == ["Carol", "Alice", "Bob"]


def test_first():
    Author.objects.create(name="Carol", age=35)
    Author.objects.create(name="Bob", age=25)
    first = Author.objects.order_by("age").first()
    assert first is not None
    assert first.name == "Bob"


def test_last():
    Author.objects.create(name="Carol", age=35)
    Author.objects.create(name="Bob", age=25)
    last = Author.objects.order_by("age").last()
    assert last is not None
    assert last.name == "Carol"


def test_first_empty():
    assert Author.objects.first() is None


# ── values / values_list ──────────────────────────────────────────────────────

def test_values():
    Author.objects.create(name="Alice", age=30)
    rows = list(Author.objects.values("name", "age"))
    assert rows == [{"name": "Alice", "age": 30}]


def test_values_list():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    pairs = sorted(Author.objects.values_list("name", "age"))
    assert pairs == [("Alice", 30), ("Bob", 25)]


def test_values_list_flat():
    Author.objects.create(name="Carol", age=35)
    Author.objects.create(name="Alice", age=30)
    names = sorted(Author.objects.values_list("name", flat=True))
    assert names == ["Alice", "Carol"]


# ── slicing ───────────────────────────────────────────────────────────────────

def test_slice_limit():
    for i in range(5):
        Author.objects.create(name=f"Author{i}", age=20 + i)
    first2 = list(Author.objects.order_by("age")[:2])
    assert len(first2) == 2
    assert first2[0].age == 20


def test_slice_offset():
    for i in range(4):
        Author.objects.create(name=f"Author{i}", age=20 + i)
    skipped = list(Author.objects.order_by("age")[2:4])
    assert len(skipped) == 2
    assert skipped[0].age == 22


# ── aggregate / annotate ──────────────────────────────────────────────────────

def test_aggregate_count():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    result = Author.objects.aggregate(total=dorm.Count("id"))
    assert result["total"] == 2


def test_aggregate_avg():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=20)
    result = Author.objects.aggregate(avg=dorm.Avg("age"))
    assert result["avg"] == 25.0


def test_aggregate_max_min():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=20)
    Author.objects.create(name="Carol", age=25)
    result = Author.objects.aggregate(mx=dorm.Max("age"), mn=dorm.Min("age"))
    assert result["mx"] == 30
    assert result["mn"] == 20


def test_aggregate_sum():
    Author.objects.create(name="Alice", age=10)
    Author.objects.create(name="Bob", age=20)
    result = Author.objects.aggregate(total=dorm.Sum("age"))
    assert result["total"] == 30


# ── update / delete ───────────────────────────────────────────────────────────

def test_update():
    Author.objects.create(name="Alice", age=30)
    n = Author.objects.filter(name="Alice").update(age=31)
    assert n == 1
    assert Author.objects.get(name="Alice").age == 31


def test_delete_queryset():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=25)
    count, _ = Author.objects.filter(name="Alice").delete()
    assert count == 1
    assert Author.objects.count() == 1


def test_delete_instance():
    alice = Author.objects.create(name="Alice", age=30)
    alice.delete()
    assert Author.objects.filter(name="Alice").exists() is False


# ── get_or_create / update_or_create ─────────────────────────────────────────

def test_get_or_create_creates():
    obj, created = Author.objects.get_or_create(name="Alice", defaults={"age": 30})
    assert created is True
    assert obj.name == "Alice"


def test_get_or_create_gets():
    Author.objects.create(name="Alice", age=30)
    obj, created = Author.objects.get_or_create(name="Alice", defaults={"age": 99})
    assert created is False
    assert obj.age == 30


def test_update_or_create_creates():
    obj, created = Author.objects.update_or_create(
        name="Alice", defaults={"age": 30}
    )
    assert created is True


def test_update_or_create_updates():
    Author.objects.create(name="Alice", age=30)
    obj, created = Author.objects.update_or_create(
        name="Alice", defaults={"age": 31}
    )
    assert created is False
    assert obj.age == 31


# ── save / refresh_from_db ────────────────────────────────────────────────────

def test_save_update():
    alice = Author.objects.create(name="Alice", age=30)
    alice.age = 31
    alice.save()
    fresh = Author.objects.get(pk=alice.pk)
    assert fresh.age == 31


def test_refresh_from_db():
    alice = Author.objects.create(name="Alice", age=30)
    Author.objects.filter(pk=alice.pk).update(age=99)
    alice.refresh_from_db()
    assert alice.age == 99


# ── bulk operations ───────────────────────────────────────────────────────────

def test_bulk_create():
    objs = [Author(name=f"Bulk{i}", age=20 + i) for i in range(5)]
    Author.objects.bulk_create(objs)
    assert Author.objects.count() == 5


def test_in_bulk():
    a1 = Author.objects.create(name="Alice", age=30)
    a2 = Author.objects.create(name="Bob", age=25)
    result = Author.objects.in_bulk([a1.pk, a2.pk])
    assert len(result) == 2
    assert result[a1.pk].name == "Alice"


# ── none / distinct ───────────────────────────────────────────────────────────

def test_none_queryset():
    Author.objects.create(name="Alice", age=30)
    results = list(Author.objects.none())
    assert results == []


def test_distinct():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Alice", age=30)
    results = list(Author.objects.values_list("name", flat=True).distinct())
    assert results.count("Alice") == 1 or len(results) == 1


# ── FK / related field ────────────────────────────────────────────────────────

def test_fk_create_and_filter():
    alice = Author.objects.create(name="Alice", age=30)
    book = Book.objects.create(title="Python 101", author_id=alice.pk, pages=300)
    assert book.author_id == alice.pk
    books = list(Book.objects.filter(author_id=alice.pk))
    assert len(books) == 1
    assert books[0].title == "Python 101"


def test_full_clean_valid():
    alice = Author(name="Alice", age=30)
    alice.full_clean()  # should not raise


def test_full_clean_invalid_email():
    import dorm as _dorm
    author = Author(name="Alice", age=30, email="not-an-email")
    with pytest.raises(_dorm.ValidationError):
        author.full_clean()
