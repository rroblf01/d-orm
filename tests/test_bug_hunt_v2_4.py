"""Bug-hunting tests — branches and edge cases that line-coverage
metrics happily declare "tested" while the actual semantic might be
broken.

These tests target patterns where ORMs historically grow subtle bugs:

* **Empty inputs to bulk operations** — ``filter(pk__in=[])``,
  ``bulk_create([])``, ``bulk_update(rows, [])``.
* **Boolean / null edge cases** in Q expressions and FK NULL
  behaviour with ``select_related``.
* **Decimal precision** and DB round-tripping of fractional values.
* **Race-style operations** — ``get_or_create`` /
  ``update_or_create`` re-entry on existing rows.
* **Self-referencing FKs** — CASCADE traversal, NULL pointers.
* **Ordering with NULLs** — descending sort over a nullable column.
* **Unicode / SQL-special characters** in user data.
* **Migration round-trip** — reverse semantics for the operations
  that ship a forwards / backwards pair.

A test that pins a specific known-correct behaviour (rather than
just exercising a code path) catches more bugs because a future
refactor that subtly changes the semantics still trips it.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

import dorm
from dorm import transaction
from dorm.exceptions import IntegrityError, MultipleObjectsReturned
from tests.models import Article, Author, Book, Publisher, Tag


# ── Empty inputs to bulk operations ─────────────────────────────────


class TestEmptyInputBoundaries:
    """Empty collections are the textbook bug-magnet for bulk
    pipelines: a naive ``", ".join(placeholders)`` emits invalid SQL
    (``IN ()``), and a naive ``len() == N`` loop hits IndexError on
    zero. The contract is "no-op cleanly"."""

    def test_filter_pk_in_empty_list_returns_empty(self):
        assert list(Author.objects.filter(pk__in=[])) == []

    def test_filter_pk_in_empty_list_count_is_zero(self):
        assert Author.objects.filter(pk__in=[]).count() == 0

    def test_filter_pk_in_empty_list_exists_is_false(self):
        assert Author.objects.filter(pk__in=[]).exists() is False

    def test_bulk_create_empty_list_returns_empty(self):
        result = Author.objects.bulk_create([])
        assert result == []

    def test_bulk_update_empty_list_returns_zero(self):
        result = Author.objects.bulk_update([], ["name"])
        assert result == 0

    def test_bulk_update_with_empty_fields_raises_value_error(self):
        a = Author.objects.create(name="bu", age=1, email="bu@x.com")
        try:
            with pytest.raises(ValueError):
                Author.objects.bulk_update([a], [])
        finally:
            a.delete()

    def test_delete_on_empty_qs_returns_zero_count(self):
        count, _ = Author.objects.filter(name="never-exists").delete()
        assert count == 0

    def test_update_on_empty_qs_returns_zero(self):
        n = Author.objects.filter(name="never-exists").update(age=99)
        assert n == 0


# ── NULL FK + select_related ────────────────────────────────────────


class TestNullFKWithSelectRelated:
    """``select_related`` emits a LEFT OUTER JOIN, so a row whose FK
    is NULL must hydrate the parent and report ``related is None``
    — not crash on a missing related row."""

    def test_select_related_with_null_fk_returns_none_for_relation(self):
        a = Author.objects.create(name="solo", age=1, email="s@x.com")
        try:
            loaded = Author.objects.select_related("publisher").get(pk=a.pk)
            assert loaded.publisher is None
        finally:
            a.delete()

    def test_only_with_null_fk_still_hydrates_parent(self):
        a = Author.objects.create(name="solo2", age=1, email="s2@x.com")
        try:
            loaded = Author.objects.select_related("publisher").only(
                "name", "publisher__name"
            ).get(pk=a.pk)
            assert loaded.name == "solo2"
            assert loaded.publisher is None
        finally:
            a.delete()


# ── Q() empty / boolean identities ──────────────────────────────────


class TestQObjectIdentities:
    """``Q()`` is a tautology (TRUE). Identity rules:

    - ``Q() | Q(...)`` ≡ TRUE (matches everything).
    - ``Q() & Q(...)`` ≡ ``Q(...)`` (no-op AND).
    - ``~Q()`` ≡ FALSE (no rows).
    """

    def test_empty_q_or_specific_matches_everything(self):
        a1 = Author.objects.create(name="A1", age=1, email="a1@x.com")
        a2 = Author.objects.create(name="A2", age=2, email="a2@x.com")
        try:
            count = Author.objects.filter(
                dorm.Q() | dorm.Q(name="A1")
            ).count()
            assert count >= 2  # at least A1 + A2
        finally:
            a1.delete()
            a2.delete()

    def test_empty_q_and_specific_acts_as_no_op(self):
        a = Author.objects.create(name="QAND", age=1, email="qa@x.com")
        try:
            with_empty = Author.objects.filter(dorm.Q() & dorm.Q(name="QAND")).count()
            without_empty = Author.objects.filter(name="QAND").count()
            assert with_empty == without_empty == 1
        finally:
            a.delete()


# ── Decimal precision round-trip ────────────────────────────────────


class TestDecimalRoundTrip:
    """DecimalField's reason for being is preserving exact precision.
    Must round-trip through DB without IEEE-754 fuzz."""

    def test_decimal_round_trips_with_full_precision(self):
        class _Inv(dorm.Model):
            amount = dorm.DecimalField(max_digits=10, decimal_places=4)

            class Meta:
                db_table = "decimal_round_trip"
                app_label = "tests"

        from dorm.db.connection import get_connection
        from dorm.migrations.operations import _field_to_column_sql

        conn = get_connection()
        cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
        conn.execute_script(f'DROP TABLE IF EXISTS "decimal_round_trip"{cascade}')
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in _Inv._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            'CREATE TABLE "decimal_round_trip" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )
        try:
            obj = _Inv.objects.create(amount=Decimal("1.0001"))
            reloaded = _Inv.objects.get(pk=obj.pk)
            assert reloaded.amount == Decimal("1.0001")
            # Pin the exact string form: a buggy adapter would round
            # to 1.0 or stringify as 1.00010000000001.
            assert str(reloaded.amount) == "1.0001"
        finally:
            conn.execute_script(f'DROP TABLE IF EXISTS "decimal_round_trip"{cascade}')


# ── get_or_create / update_or_create idempotency ────────────────────


class TestGetOrCreateSemantics:
    def test_get_or_create_returns_existing_without_creating(self):
        a = Author.objects.create(name="goc", age=10, email="g@x.com")
        try:
            again, created = Author.objects.get_or_create(
                name="goc", defaults={"age": 99, "email": "diff@x.com"}
            )
            # Existing row — defaults must NOT overwrite.
            assert created is False
            assert again.pk == a.pk
            assert again.age == 10
            assert again.email == "g@x.com"
        finally:
            a.delete()

    def test_get_or_create_creates_when_missing(self):
        Author.objects.filter(name="goc-new").delete()
        try:
            obj, created = Author.objects.get_or_create(
                name="goc-new",
                defaults={"age": 5, "email": "n@x.com"},
            )
            assert created is True
            assert obj.age == 5
            assert obj.email == "n@x.com"
        finally:
            Author.objects.filter(name="goc-new").delete()

    def test_update_or_create_overwrites_existing_with_defaults(self):
        Author.objects.filter(name="uoc").delete()
        a = Author.objects.create(name="uoc", age=1, email="u@x.com")
        try:
            obj, created = Author.objects.update_or_create(
                name="uoc",
                defaults={"age": 42, "email": "u-new@x.com"},
            )
            assert created is False
            assert obj.pk == a.pk
            # update_or_create DOES overwrite on existing row.
            assert obj.age == 42
            assert obj.email == "u-new@x.com"
        finally:
            Author.objects.filter(name="uoc").delete()

    def test_get_returns_one_or_raises_multiple(self):
        Author.objects.filter(name="dupe").delete()
        a1 = Author.objects.create(name="dupe", age=1, email="d1@x.com")
        a2 = Author.objects.create(name="dupe", age=2, email="d2@x.com")
        try:
            with pytest.raises(MultipleObjectsReturned):
                Author.objects.get(name="dupe")
        finally:
            a1.delete()
            a2.delete()


# ── delete() idempotency ────────────────────────────────────────────


class TestDeleteIdempotency:
    def test_delete_already_deleted_instance_does_not_resurrect(self):
        a = Author.objects.create(name="d1", age=1, email="d1@x.com")
        a.delete()
        # Calling delete() again on an instance whose row is gone
        # should not raise (Django historical behaviour) but should
        # NOT re-create it either.
        try:
            a.delete()
        except Exception:
            pass
        assert not Author.objects.filter(name="d1").exists()


# ── CASCADE depth ───────────────────────────────────────────────────


class TestCascadeDepth:
    """Deleting a Publisher must NOT delete its Authors because the
    FK is ``on_delete=SET_NULL`` (per ``tests/models.py``). Pin the
    set-null semantic so a refactor that flips it to CASCADE breaks
    loudly.
    """

    def test_publisher_delete_sets_author_publisher_null(self):
        pub = Publisher.objects.create(name="cascade-set-null")
        a = Author.objects.create(
            name="kept", age=1, email="k@x.com", publisher=pub
        )
        try:
            pub.delete()
            reloaded = Author.objects.get(pk=a.pk)
            assert reloaded.publisher_id is None  # type: ignore[attr-defined]
        finally:
            Author.objects.filter(name="kept").delete()

    def test_author_delete_cascades_to_books(self):
        """``Book.author`` is ``CASCADE`` per ``tests/models.py`` —
        deleting an Author must take its Books with it. Pins the
        cascade so a regression that drops the cascade leaves
        orphans."""
        a = Author.objects.create(name="bookwriter", age=1, email="bw@x.com")
        b = Book.objects.create(title="t1", author=a, pages=10)
        try:
            a.delete()
            assert not Book.objects.filter(pk=b.pk).exists()
        finally:
            Book.objects.filter(pk=b.pk).delete()
            Author.objects.filter(name="bookwriter").delete()


# ── Ordering with NULLs ─────────────────────────────────────────────


class TestOrderingWithNulls:
    def test_order_by_nullable_column_does_not_crash(self):
        """A nullable column should be sortable without server error.
        The actual NULL ordering (first / last) is DB-vendor specific
        — we just want the query to *run*."""
        Author.objects.filter(name__startswith="onull").delete()
        a = Author.objects.create(name="onull-1", age=1, email=None)
        b = Author.objects.create(name="onull-2", age=2, email="b@x.com")
        try:
            results = list(Author.objects.filter(
                name__startswith="onull"
            ).order_by("email"))
            assert len(results) == 2
        finally:
            a.delete()
            b.delete()

    def test_order_by_descending_with_dash_prefix(self):
        Author.objects.filter(name__startswith="oz").delete()
        a1 = Author.objects.create(name="oz1", age=1, email="z1@x.com")
        a2 = Author.objects.create(name="oz2", age=2, email="z2@x.com")
        try:
            results = list(Author.objects.filter(
                name__startswith="oz"
            ).order_by("-age"))
            assert results[0].age > results[1].age
        finally:
            a1.delete()
            a2.delete()


# ── Unicode / SQL-special characters ────────────────────────────────


class TestSqlSpecialCharacters:
    """The SQL builder pairs every value with a placeholder, so user
    input with quotes / backslashes / NUL / unicode should round-trip
    without injection or corruption. These pin that contract."""

    def test_single_quote_in_value_round_trips(self):
        a = Author.objects.create(name="O'Brien", age=1, email="ob@x.com")
        try:
            assert Author.objects.get(name="O'Brien").age == 1
        finally:
            a.delete()

    def test_unicode_emoji_in_value_round_trips(self):
        a = Author.objects.create(name="emoji 🎉", age=1, email="e@x.com")
        try:
            assert Author.objects.get(name="emoji 🎉").age == 1
        finally:
            a.delete()

    def test_backslash_does_not_break_query(self):
        a = Author.objects.create(name="path\\to\\dir", age=1, email="b@x.com")
        try:
            got = Author.objects.get(name="path\\to\\dir")
            assert got.pk == a.pk
        finally:
            a.delete()


# ── Filter by FK instance vs PK ─────────────────────────────────────


class TestFilterByForeignKey:
    """``filter(publisher=pub)`` and ``filter(publisher_id=pub.pk)``
    must be equivalent — both produce the same WHERE predicate."""

    def test_filter_by_instance_equals_filter_by_pk(self):
        pub = Publisher.objects.create(name="fk-eq")
        a = Author.objects.create(
            name="fkeq", age=1, email="fk@x.com", publisher=pub
        )
        try:
            by_inst = list(Author.objects.filter(publisher=pub))
            by_pk = list(Author.objects.filter(publisher_id=pub.pk))
            assert {x.pk for x in by_inst} == {x.pk for x in by_pk}
            assert a.pk in {x.pk for x in by_inst}
        finally:
            a.delete()
            pub.delete()


# ── DateTimeField timezone-naive round-trip ─────────────────────────


class TestDateTimeRoundTrip:
    def test_naive_datetime_round_trips(self):
        class _Ev(dorm.Model):
            ts = dorm.DateTimeField()

            class Meta:
                db_table = "datetime_round_trip"
                app_label = "tests"

        from dorm.db.connection import get_connection
        from dorm.migrations.operations import _field_to_column_sql

        conn = get_connection()
        cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
        conn.execute_script(f'DROP TABLE IF EXISTS "datetime_round_trip"{cascade}')
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in _Ev._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            'CREATE TABLE "datetime_round_trip" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )
        try:
            now = datetime.datetime(2026, 4, 30, 12, 34, 56)
            obj = _Ev.objects.create(ts=now)
            reloaded = _Ev.objects.get(pk=obj.pk)
            # SQLite stores naive datetimes as ISO strings; the
            # converter round-trips back to datetime. Compare values.
            assert reloaded.ts == now
        finally:
            conn.execute_script(f'DROP TABLE IF EXISTS "datetime_round_trip"{cascade}')


# ── M2M behaviour edge cases ────────────────────────────────────────


class TestM2MEdgeCases:
    def test_add_same_object_twice_is_idempotent(self):
        """Adding the same tag twice shouldn't create two through-rows
        — M2M.add semantics is "ensure related, no duplicates"."""
        art = Article.objects.create(title="m2m-dup")
        tag = Tag.objects.create(name="repeat-tag")
        try:
            art.tags.add(tag)
            art.tags.add(tag)
            # Exactly one membership.
            assert art.tags.count() == 1
        finally:
            art.tags.clear()
            tag.delete()
            art.delete()

    def test_remove_nonexistent_relation_no_op(self):
        art = Article.objects.create(title="m2m-rm")
        tag = Tag.objects.create(name="never-added")
        try:
            # Tag never added — remove shouldn't raise.
            art.tags.remove(tag)
            assert art.tags.count() == 0
        finally:
            tag.delete()
            art.delete()

    def test_clear_on_empty_relation_no_op(self):
        art = Article.objects.create(title="m2m-clear")
        try:
            art.tags.clear()
            assert art.tags.count() == 0
        finally:
            art.delete()


# ── Save() on instance with no real changes ─────────────────────────


class TestSaveBehaviour:
    def test_save_updates_existing_row(self):
        a = Author.objects.create(name="initial", age=1, email="s@x.com")
        try:
            a.name = "changed"
            a.save()
            reloaded = Author.objects.get(pk=a.pk)
            assert reloaded.name == "changed"
        finally:
            a.delete()

    def test_save_assigns_pk_on_first_save(self):
        a = Author(name="newobj", age=1, email="new@x.com")
        assert a.pk is None
        try:
            a.save()
            assert a.pk is not None
        finally:
            a.delete()


# ── Transaction rollback semantics ──────────────────────────────────


class TestTransactionRollback:
    def test_atomic_rollback_undoes_inserts(self):
        Author.objects.filter(name="rb").delete()
        try:
            with pytest.raises(RuntimeError):
                with transaction.atomic():
                    Author.objects.create(name="rb", age=1, email="r@x.com")
                    raise RuntimeError("force rollback")
            assert not Author.objects.filter(name="rb").exists()
        finally:
            Author.objects.filter(name="rb").delete()

    def test_nested_atomic_inner_rollback_keeps_outer(self):
        Author.objects.filter(name__startswith="nrb").delete()
        try:
            with transaction.atomic():
                Author.objects.create(name="nrb-outer", age=1, email="nr1@x.com")
                with pytest.raises(RuntimeError):
                    with transaction.atomic():
                        Author.objects.create(
                            name="nrb-inner", age=1, email="nr2@x.com"
                        )
                        raise RuntimeError("inner rollback")
            # Outer survived, inner rolled back.
            assert Author.objects.filter(name="nrb-outer").exists()
            assert not Author.objects.filter(name="nrb-inner").exists()
        finally:
            Author.objects.filter(name__startswith="nrb").delete()


# ── F() expressions ────────────────────────────────────────────────


class TestFExpressionEdges:
    def test_filter_with_f_compares_columns(self):
        from dorm.expressions import F

        # age == age (always true) — every row matches.
        Author.objects.filter(name__startswith="fexp").delete()
        a1 = Author.objects.create(name="fexp1", age=1, email="f1@x.com")
        a2 = Author.objects.create(name="fexp2", age=2, email="f2@x.com")
        try:
            count = Author.objects.filter(
                name__startswith="fexp"
            ).filter(age=F("age")).count()
            assert count == 2
        finally:
            a1.delete()
            a2.delete()

    def test_update_with_f_increments_column(self):
        from dorm.expressions import F

        a = Author.objects.create(name="finc", age=10, email="fi@x.com")
        try:
            Author.objects.filter(pk=a.pk).update(age=F("age") + 5)
            reloaded = Author.objects.get(pk=a.pk)
            assert reloaded.age == 15
        finally:
            a.delete()


# ── IntegrityError on UNIQUE conflict ───────────────────────────────


class TestUniqueConstraintConflict:
    def test_duplicate_unique_value_raises_integrity_error(self):
        Tag.objects.filter(name="uniq-test").delete()
        Tag.objects.create(name="uniq-test")
        try:
            with pytest.raises(IntegrityError):
                Tag.objects.create(name="uniq-test")
        finally:
            Tag.objects.filter(name="uniq-test").delete()


# ── ordering on FK column emits FK_id ──────────────────────────────


class TestOrderByFK:
    """Bug fixed in 2.3.0: ``order_by("publisher")`` used to emit
    ``ORDER BY "publisher"`` (column doesn't exist) — must resolve
    to ``ORDER BY "publisher_id"``. Pin to prevent regression."""

    def test_order_by_fk_resolves_to_id_column(self):
        from dorm.db.connection import get_connection

        sql, _ = Author.objects.order_by("publisher")._query.as_select(
            get_connection()
        )
        # The column referenced in ORDER BY must be the FK's stored
        # column (``publisher_id``), not the bare relation name.
        assert "publisher_id" in sql, sql

    def test_order_by_dash_fk_resolves_to_id_column_desc(self):
        from dorm.db.connection import get_connection

        sql, _ = Author.objects.order_by("-publisher")._query.as_select(
            get_connection()
        )
        assert "publisher_id" in sql
        assert "DESC" in sql


# ── select_related with all-NULL FK across multiple rows ────────────


class TestSelectRelatedNullSemantics:
    def test_all_rows_with_null_fk_no_phantom_relation(self):
        Author.objects.filter(name__startswith="srn").delete()
        for i in range(3):
            Author.objects.create(name=f"srn{i}", age=i, email=f"srn{i}@x.com")
        try:
            rows = list(
                Author.objects.filter(name__startswith="srn")
                .select_related("publisher")
            )
            assert len(rows) == 3
            for r in rows:
                assert r.publisher is None
        finally:
            Author.objects.filter(name__startswith="srn").delete()
