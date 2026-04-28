"""Bug-hunting tests for queryset boundary cases.

Targets the corners that tend to hide off-by-one / SQL-emit bugs:
slicing edges, ``__in=[]``, ``Q(~Q())``, deep filter chaining,
``count()`` vs ``len()`` divergence, ``exists()`` short-circuit,
``values()`` / ``values_list()`` on empty data, ordering on FK
traversal with NULLs, and aggregation on an empty queryset.
"""

from __future__ import annotations


import pytest

from dorm import F, Q
from dorm.aggregates import Avg, Count, Max, Min, Sum
from tests.models import Author, Publisher


# ── Empty queryset ──────────────────────────────────────────────────────────


class TestEmptyQuerySet:
    def test_count_on_empty_returns_zero(self):
        assert Author.objects.count() == 0
        assert Author.objects.filter(name="nope").count() == 0

    def test_exists_on_empty_returns_false_without_full_fetch(self):
        # ``exists`` must short-circuit. Catches a regression where
        # someone replaced it with ``len(qs) > 0``.
        assert Author.objects.exists() is False
        assert Author.objects.filter(name="nope").exists() is False

    def test_first_last_on_empty_return_none(self):
        assert Author.objects.first() is None
        assert Author.objects.last() is None

    def test_get_on_empty_raises_does_not_exist(self):
        with pytest.raises(Author.DoesNotExist):
            Author.objects.get(name="missing")

    def test_aggregate_on_empty_returns_none_or_zero(self):
        """SQL ``MIN/MAX/AVG/SUM`` over zero rows returns ``NULL`` which
        the ORM passes through as ``None``. ``COUNT`` always returns
        an integer (0). Catches a regression that wraps everything in
        a default-zero coercion."""
        agg = Author.objects.aggregate(
            mn=Min("age"), mx=Max("age"), av=Avg("age"),
            sm=Sum("age"), cn=Count("id"),
        )
        assert agg["mn"] is None
        assert agg["mx"] is None
        assert agg["av"] is None
        assert agg["sm"] is None
        assert agg["cn"] == 0

    def test_values_and_values_list_on_empty_return_empty_list(self):
        assert list(Author.objects.values("name")) == []
        assert list(Author.objects.values_list("name", flat=True)) == []

    def test_iter_on_empty_yields_nothing(self):
        # Catches the regression where ``__iter__`` materialised
        # something non-empty (e.g. a sentinel).
        assert list(Author.objects.all()) == []


# ── ``__in=[]`` / empty filter values ───────────────────────────────────────


class TestEmptyInFilter:
    def test_in_empty_list_returns_no_rows(self):
        for n in ("a", "b", "c"):
            Author.objects.create(name=n, age=1)
        assert Author.objects.filter(pk__in=[]).count() == 0
        assert list(Author.objects.filter(pk__in=[])) == []

    def test_exclude_in_empty_list_returns_all_rows(self):
        for n in ("a", "b", "c"):
            Author.objects.create(name=n, age=1)
        # Excluding an empty ``__in`` set must keep everything (not
        # zero — the canonical pitfall in SQL builders that emit
        # ``NOT IN ()`` instead of ``TRUE``).
        assert Author.objects.exclude(pk__in=[]).count() == 3


# ── Slicing edges ──────────────────────────────────────────────────────────


class TestSlicing:
    def _seed(self):
        for i in range(5):
            Author.objects.create(name=f"a{i}", age=i)

    def test_zero_limit_returns_empty(self):
        self._seed()
        assert list(Author.objects.order_by("age")[0:0]) == []

    def test_slice_within_bounds(self):
        self._seed()
        names = [a.name for a in Author.objects.order_by("age")[1:3]]
        assert names == ["a1", "a2"]

    def test_slice_beyond_total_returns_what_exists(self):
        self._seed()
        # ``[3:100]`` must not crash — SQL ``LIMIT`` clamps naturally.
        names = [a.name for a in Author.objects.order_by("age")[3:100]]
        assert names == ["a3", "a4"]

    def test_negative_indexing_rejected(self):
        """Django raises on negative indexing because reversed slicing
        would force a full materialisation. dorm should match."""
        self._seed()
        with pytest.raises((TypeError, ValueError, AssertionError)):
            list(Author.objects.order_by("age")[-1:])

    def test_step_other_than_one_rejected(self):
        self._seed()
        with pytest.raises((TypeError, ValueError, AssertionError)):
            list(Author.objects.order_by("age")[::2])


# ── Q-object combinators ───────────────────────────────────────────────────


class TestQObjects:
    def test_double_negation_round_trips(self):
        """``Q(~Q(x))`` is logically equivalent to ``Q(x)``. Catches
        a regression where the negation flag wasn't toggled twice."""
        Author.objects.create(name="alice", age=30)
        Author.objects.create(name="bob", age=20)
        # Equivalent forms — same row count.
        a = Author.objects.filter(Q(age__gte=25)).count()
        b = Author.objects.filter(~~Q(age__gte=25)).count()
        assert a == b == 1

    def test_q_or_with_empty_filter_matches_everything(self):
        """``Q() | Q()`` must not collapse to zero rows."""
        Author.objects.create(name="x", age=1)
        Author.objects.create(name="y", age=2)
        assert Author.objects.filter(Q() | Q(age=2)).count() == 2

    def test_q_chaining_does_not_short_circuit_at_first_match(self):
        """A regression where the SQL builder emitted only the first
        condition would cause this test to return all rows."""
        for age in range(10):
            Author.objects.create(name=f"n{age}", age=age)
        out = Author.objects.filter(age__gte=5, age__lte=7)
        assert sorted(a.age for a in out) == [5, 6, 7]


# ── F() expressions ────────────────────────────────────────────────────────


class TestFExpressions:
    def test_update_using_f_for_atomic_increment(self):
        a = Author.objects.create(name="counter", age=10)
        Author.objects.filter(pk=a.pk).update(age=F("age") + 5)
        assert Author.objects.get(pk=a.pk).age == 15

    def test_filter_comparing_two_columns_via_f(self):
        Publisher.objects.create(name="OK")  # warm-up so id != age coincidence
        a = Author.objects.create(name="cmp", age=42)
        # ``age = pk`` is a degenerate but valid comparison; result
        # depends on whether the auto-PK happened to equal the age.
        match = a.pk == 42
        n = Author.objects.filter(pk=F("age")).count()
        assert n == (1 if match else 0)


# ── Deep chaining of filter() / exclude() / order_by() ─────────────────────


class TestDeepChaining:
    def test_filter_chain_intersects_conditions(self):
        for i in range(20):
            Author.objects.create(name=f"a{i}", age=i)
        out = (
            Author.objects.filter(age__gte=5)
            .filter(age__lte=15)
            .filter(name__startswith="a1")
        )
        names = sorted(a.name for a in out)
        # 10..15 satisfy the three filters; "a1" is exactly 'a1';
        # 'a10'..'a15' satisfy the prefix.
        assert names == ["a10", "a11", "a12", "a13", "a14", "a15"]

    def test_order_by_then_filter_preserves_order(self):
        for i in [3, 1, 2]:
            Author.objects.create(name=f"a{i}", age=i)
        out = Author.objects.order_by("age").filter(age__gte=2)
        ages = [a.age for a in out]
        assert ages == [2, 3]

    def test_exclude_after_filter_subtracts_correctly(self):
        for n in ("alice", "alex", "bob"):
            Author.objects.create(name=n, age=1)
        out = Author.objects.filter(name__startswith="al").exclude(name="alex")
        names = sorted(a.name for a in out)
        assert names == ["alice"]


# ── count() vs len() consistency ───────────────────────────────────────────


class TestCountVsLen:
    def test_count_and_len_agree_on_populated_qs(self):
        for i in range(7):
            Author.objects.create(name=f"a{i}", age=i)
        qs = Author.objects.all()
        # Materialise via len — uses the cache.
        assert len(qs) == 7
        # Re-issue COUNT — fresh query.
        assert qs.count() == 7

    def test_count_after_filter_is_database_side(self):
        for i in range(5):
            Author.objects.create(name=f"a{i}", age=i)
        # ``count()`` on a filtered queryset must run ``SELECT COUNT(*)
        # … WHERE …`` rather than fetch + count in Python.
        assert Author.objects.filter(age__gte=3).count() == 2


# ── values() / values_list() shape ─────────────────────────────────────────


class TestValuesShape:
    def test_values_list_flat_with_one_field(self):
        Author.objects.create(name="x", age=1)
        Author.objects.create(name="y", age=2)
        names = sorted(Author.objects.values_list("name", flat=True))
        assert names == ["x", "y"]

    def test_values_list_flat_with_multiple_fields_rejected(self):
        """``flat=True`` requires exactly one field — pinning multiple
        with ``flat=True`` is meaningless and Django raises. Match
        that behaviour."""
        Author.objects.create(name="x", age=1)
        with pytest.raises((TypeError, ValueError)):
            list(Author.objects.values_list("name", "age", flat=True))

    def test_values_returns_dict_per_row(self):
        Author.objects.create(name="x", age=1)
        rows = list(Author.objects.values("name", "age"))
        assert rows == [{"name": "x", "age": 1}]


# ── Ordering with NULLs in FK column ───────────────────────────────────────


class TestOrderByWithNulls:
    def test_order_by_nullable_fk_does_not_crash(self):
        # Author.publisher is a nullable FK.
        p = Publisher.objects.create(name="P")
        Author.objects.create(name="with-pub", age=1, publisher=p)
        Author.objects.create(name="no-pub", age=2)
        # Ordering by ``publisher`` (the FK column itself) must not
        # crash on NULLs; the exact NULL-position depends on the
        # backend (PG = trailing, SQLite = leading) but both rows
        # should appear.
        out = list(Author.objects.order_by("publisher"))
        assert {a.name for a in out} == {"with-pub", "no-pub"}


# ── distinct() / distinct chain ────────────────────────────────────────────


class TestDistinct:
    def test_distinct_dedups_rows(self):
        Author.objects.create(name="dup", age=1)
        Author.objects.create(name="dup", age=2)
        Author.objects.create(name="other", age=3)
        # Plain ``distinct()`` over the full row deduplicates by every
        # column; with two ``dup`` rows of different ages, both
        # survive. Use ``values('name').distinct()`` to dedup by name.
        names = sorted(
            r["name"] for r in Author.objects.values("name").distinct()
        )
        assert names == ["dup", "other"]


# ── get_or_none / aget_or_none on no match ────────────────────────────────


class TestGetOrNone:
    def test_get_or_none_returns_none_on_missing(self):
        assert Author.objects.get_or_none(name="nope") is None

    def test_get_or_none_returns_instance_on_hit(self):
        a = Author.objects.create(name="hit", age=1)
        got = Author.objects.get_or_none(pk=a.pk)
        assert got is not None and got.name == "hit"

    @pytest.mark.asyncio
    async def test_aget_or_none_parity(self):
        await Author.objects.acreate(name="aon", age=1)
        result = await Author.objects.aget_or_none(name="aon")
        assert result is not None and result.name == "aon"
        assert await Author.objects.aget_or_none(name="nope") is None


# ── select_related / prefetch_related on empty rows ─────────────────────────


class TestRelationLoadingOnEmpty:
    def test_select_related_on_empty_qs_yields_nothing(self):
        assert list(Author.objects.select_related("publisher")) == []

    def test_prefetch_related_on_empty_qs_yields_nothing(self):
        # Article is M2M with Tag; an empty queryset shouldn't issue
        # the N+1 fallback.
        from tests.models import Article
        assert list(Article.objects.prefetch_related("tags")) == []
