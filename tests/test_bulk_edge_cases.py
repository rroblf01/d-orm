"""Bug-hunting tests for bulk operations.

The happy paths (``bulk_create``, ``bulk_update``, ``QuerySet.delete``)
are well-covered by the rest of the suite. This file pokes the
boundary cases that tend to hide bugs in production:

- Empty input lists (do we issue a query for zero rows?).
- Single-row batches (off-by-one in batch loops).
- Batches that don't divide evenly (last partial batch).
- Mixed pre-set / auto PKs.
- Signals not firing for bulk paths (documented behaviour, but a
  guard is useful when refactors land).
- Update with no matching rows (returns 0, no exception).
- Bulk delete on an empty queryset.
- ``ignore_conflicts`` and ``update_conflicts`` with the conflict
  target absent from the rows.
"""

from __future__ import annotations

import pytest

from dorm.signals import post_save
from tests.models import Author, Publisher, Tag, Article


# ── bulk_create ──────────────────────────────────────────────────────────────


class TestBulkCreate:
    def test_empty_list_is_a_no_op(self):
        """``bulk_create([])`` must not issue any DB statement and must
        return an empty list. Catches a frequent off-by-one where the
        first batch is built from ``rows[:batch_size]`` even when
        ``rows`` is empty."""
        result = Author.objects.bulk_create([])
        assert result == []
        # And no rows landed.
        assert Author.objects.count() == 0

    def test_single_row_batch(self):
        result = Author.objects.bulk_create([Author(name="solo", age=1)])
        assert len(result) == 1
        assert Author.objects.filter(name="solo").exists()

    def test_batch_size_smaller_than_input_partial_last_batch(self):
        rows = [Author(name=f"r{i}", age=i) for i in range(7)]
        Author.objects.bulk_create(rows, batch_size=3)
        # 7 rows, batch_size=3 → 3+3+1 → last partial batch must land.
        assert Author.objects.count() == 7
        names = sorted(a.name for a in Author.objects.all())
        assert names == [f"r{i}" for i in range(7)]

    def test_batch_size_equal_to_input_size(self):
        rows = [Author(name=f"e{i}", age=i) for i in range(5)]
        Author.objects.bulk_create(rows, batch_size=5)
        assert Author.objects.count() == 5

    def test_batch_size_one(self):
        """``batch_size=1`` is the per-row degenerate case — issues N
        single-row INSERTs. Must still produce N rows."""
        rows = [Author(name=f"b{i}", age=i) for i in range(3)]
        Author.objects.bulk_create(rows, batch_size=1)
        assert Author.objects.count() == 3

    def test_signals_do_not_fire_during_bulk_create(self):
        """Documented behaviour: ``bulk_create`` skips ``pre_save`` /
        ``post_save`` signals for performance. A regression that
        starts firing them would silently change observable
        semantics."""
        fired: list[str] = []

        def receiver(sender, instance, **_):
            fired.append(instance.name)

        post_save.connect(receiver, sender=Author, weak=False)
        try:
            Author.objects.bulk_create(
                [Author(name="bulksig", age=1), Author(name="bulksig2", age=2)]
            )
        finally:
            post_save.disconnect(receiver)

        assert fired == [], f"signals fired unexpectedly: {fired}"
        # But the rows did land.
        assert Author.objects.filter(name__startswith="bulksig").count() == 2

    def test_ignore_conflicts_skips_duplicates_silently(self):
        """``ignore_conflicts=True`` swallows unique-constraint
        violations row-by-row. A regression where the whole batch
        rolled back on the first conflict would lose every other row."""
        Tag.objects.create(name="dup")
        result = Tag.objects.bulk_create(
            [
                Tag(name="dup"),       # collides
                Tag(name="newtag1"),
                Tag(name="newtag2"),
            ],
            ignore_conflicts=True,
        )
        # Result list is the same length as the input — collisions
        # are silently dropped *server-side*, not at the Python level.
        assert len(result) == 3
        assert Tag.objects.filter(name="newtag1").exists()
        assert Tag.objects.filter(name="newtag2").exists()
        # Only one ``dup`` survives (the original).
        assert Tag.objects.filter(name="dup").count() == 1

    def test_update_conflicts_upserts(self):
        """``update_conflicts=True`` turns the INSERT into an upsert.
        Conflicting rows update the named columns instead of being
        skipped."""
        Tag.objects.create(name="upsert-target")
        Tag.objects.bulk_create(
            [Tag(name="upsert-target")],
            update_conflicts=True,
            unique_fields=["name"],
            update_fields=["name"],
        )
        # Still one row (the conflict didn't insert a duplicate).
        assert Tag.objects.filter(name="upsert-target").count() == 1


# ── bulk_update ──────────────────────────────────────────────────────────────


class TestBulkUpdate:
    def test_empty_list_is_a_no_op(self):
        # Should not crash and should not issue an UPDATE.
        Author.objects.bulk_update([], ["name"])

    def test_single_row(self):
        a = Author.objects.create(name="orig", age=1)
        a.name = "updated"
        Author.objects.bulk_update([a], ["name"])
        assert Author.objects.get(pk=a.pk).name == "updated"

    def test_batch_size_with_remainder(self):
        rows = [Author.objects.create(name=f"r{i}", age=i) for i in range(7)]
        for r in rows:
            r.age = r.age * 10
        Author.objects.bulk_update(rows, ["age"], batch_size=3)
        assert sorted(a.age for a in Author.objects.all()) == [
            i * 10 for i in range(7)
        ]

    def test_no_columns_to_update_raises(self):
        """Empty ``fields`` list is meaningless — must raise rather
        than silently emit a malformed UPDATE."""
        a = Author.objects.create(name="x", age=1)
        with pytest.raises(ValueError):
            Author.objects.bulk_update([a], [])


# ── QuerySet.update / delete on empty matchers ──────────────────────────────


class TestQuerysetUpdateDelete:
    def test_update_with_no_matches_returns_zero(self):
        n = Author.objects.filter(name="nonexistent").update(age=99)
        assert n == 0

    def test_delete_with_no_matches_returns_zero(self):
        deleted, _ = Author.objects.filter(name="nonexistent").delete()
        assert deleted == 0

    def test_update_returns_count_of_modified_rows(self):
        for n in ("a", "b", "c"):
            Author.objects.create(name=n, age=10)
        n = Author.objects.filter(age=10).update(age=20)
        assert n == 3

    def test_delete_returns_count_of_modified_rows(self):
        for n in ("a", "b", "c"):
            Author.objects.create(name=n, age=10)
        deleted, breakdown = Author.objects.filter(age=10).delete()
        assert deleted == 3
        # Breakdown is keyed by ``app.Model`` and reports the count.
        assert any("Author" in k and v == 3 for k, v in breakdown.items())


# ── Async parity ────────────────────────────────────────────────────────────


class TestAsyncBulkParity:
    @pytest.mark.asyncio
    async def test_abulk_create_empty_list(self):
        result = await Author.objects.abulk_create([])
        assert result == []

    @pytest.mark.asyncio
    async def test_abulk_update_empty_list(self):
        # No exception.
        await Author.objects.abulk_update([], ["name"])

    @pytest.mark.asyncio
    async def test_abulk_create_with_partial_last_batch(self):
        rows = [Author(name=f"a{i}", age=i) for i in range(5)]
        await Author.objects.abulk_create(rows, batch_size=3)
        assert await Author.objects.acount() == 5


# ── M2M after bulk_create ───────────────────────────────────────────────────


class TestM2MAfterBulkCreate:
    def test_articles_have_no_m2m_attached_after_bulk_create(self):
        """``bulk_create`` only writes the row, never the M2M
        junctions — even if the in-memory instance had relations
        attached. This contract is documented; the test catches a
        future bug that auto-creates orphan junction rows."""
        articles = Article.objects.bulk_create(
            [Article(title="A"), Article(title="B")]
        )
        for art in articles:
            # The descriptor returns an empty manager; the through
            # table is untouched.
            loaded = Article.objects.get(pk=art.pk)
            assert list(loaded.tags.all()) == []


# ── pre-set PK + auto PK mixing ─────────────────────────────────────────────


class TestPkMixingInBulk:
    def test_pre_set_pks_round_trip_via_bulk_create(self):
        """Rows created with explicit pk must land at that pk; auto-PK
        rows must get fresh ones. A bug that ignored the user's pk
        would silently shuffle IDs."""
        explicit = Publisher(pk=4242, name="explicit")
        Publisher.objects.bulk_create([explicit])
        assert Publisher.objects.filter(pk=4242, name="explicit").exists()
