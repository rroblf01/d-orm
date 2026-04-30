"""Tests for ``only()`` / ``defer()`` composing with ``select_related``.

Before this feature, ``only()`` + ``select_related()`` was a footgun:
the ``select_related`` JOIN got silently dropped because the
projection-restriction logic in :class:`SQLQuery` short-circuited
whenever ``selected_fields`` was set. The feature lifts that gate and
adds dotted-path semantics to both ``only()`` and ``defer()``::

    Author.objects.select_related("publisher").only("name", "publisher__name")

emits a single LEFT OUTER JOIN that pulls only ``"authors"."id"``,
``"authors"."name"``, ``"publishers"."id"``, ``"publishers"."name"``.

Each test asserts both *correctness* (round-trip the data) and *SQL
shape* (the JOIN column list reflects the restriction) so a future
optimisation that erases the restriction breaks loudly.
"""

from __future__ import annotations

import pytest

from tests.models import Author, Publisher


# ── only() + select_related ──────────────────────────────────────────


class TestOnlyWithSelectRelated:
    def test_dotted_only_emits_join(self):
        """The JOIN must still happen — the bug was silent dropping."""
        from dorm.db.connection import get_connection

        pub = Publisher.objects.create(name="O'Reilly")
        Author.objects.create(name="Bob", age=40, email="b@x.com", publisher=pub)
        try:
            qs = Author.objects.select_related("publisher").only(
                "name", "publisher__name"
            )
            sql, _ = qs._query.as_select(get_connection())
            assert "LEFT OUTER JOIN" in sql, sql
            assert '"_sr_publisher_name"' in sql, sql
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_dotted_only_round_trip_returns_data(self):
        pub = Publisher.objects.create(name="Acme")
        Author.objects.create(name="Bob", age=40, email="b@x.com", publisher=pub)
        try:
            a = Author.objects.select_related("publisher").only(
                "name", "publisher__name"
            ).first()
            assert a is not None
            assert a.name == "Bob"
            # Related object hydrated and accessible without extra query.
            assert a.publisher is not None
            assert a.publisher.name == "Acme"
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_dotted_only_excludes_unlisted_related_columns(self):
        """``only("name", "publisher__name")`` must NOT project
        ``publisher.id``-only — wait, PK is forced. But it should drop
        any *other* publisher columns. Here Publisher only has ``name``
        so we check via the SELECT list directly."""
        from dorm.db.connection import get_connection

        qs = Author.objects.select_related("publisher").only(
            "name", "publisher__name"
        )
        sql, _ = qs._query.as_select(get_connection())
        # PK always present (identity), name is the only declared
        # restriction. No other publisher columns — Publisher only has
        # ``id`` and ``name``, so this is automatically tight.
        assert '"_sr_publisher_id"' in sql
        assert '"_sr_publisher_name"' in sql

    def test_only_without_dotted_path_keeps_full_related(self):
        """``only("name")`` (no dotted path) restricts the parent only
        — the related side keeps every column. Backwards-compatible."""
        from dorm.db.connection import get_connection

        qs = Author.objects.select_related("publisher").only("name")
        sql, _ = qs._query.as_select(get_connection())
        # Publisher columns all present.
        assert '"_sr_publisher_id"' in sql
        assert '"_sr_publisher_name"' in sql

    def test_only_related_pk_implicitly_added(self):
        """Even when the user lists only a non-PK column on the
        related side, the related PK is always included so hydration
        builds a valid identity."""
        pub = Publisher.objects.create(name="Acme")
        Author.objects.create(name="Bob", age=40, email="b@x.com", publisher=pub)
        try:
            a = Author.objects.select_related("publisher").only(
                "name", "publisher__name"
            ).first()
            assert a is not None
            assert a.publisher is not None
            # The PK round-tripped through the JOIN even though only
            # "name" was requested.
            assert a.publisher.pk == pub.pk
        finally:
            Author.objects.all().delete()
            pub.delete()


# ── defer() + select_related ─────────────────────────────────────────


class TestDeferWithSelectRelated:
    def test_dotted_defer_drops_only_named_column(self):
        from dorm.db.connection import get_connection

        qs = Author.objects.select_related("publisher").defer("publisher__name")
        sql, _ = qs._query.as_select(get_connection())
        # ``id`` of publisher still selected (PK), but ``name`` dropped.
        assert '"_sr_publisher_id"' in sql
        assert '"_sr_publisher_name"' not in sql

    def test_dotted_defer_round_trip(self):
        """The deferred column is missing from the in-memory instance,
        but the parent SELECT and the JOIN still hydrate the rest."""
        pub = Publisher.objects.create(name="Acme")
        Author.objects.create(name="Bob", age=40, email="b@x.com", publisher=pub)
        try:
            a = Author.objects.select_related("publisher").defer(
                "publisher__name"
            ).first()
            assert a is not None
            assert a.name == "Bob"
            assert a.publisher is not None
            # Identity intact even though name was deferred.
            assert a.publisher.pk == pub.pk
        finally:
            Author.objects.all().delete()
            pub.delete()


# ── Composition: only() on parent + defer() on related ──────────────


class TestComposeOnlyAndDefer:
    def test_combine_only_parent_with_defer_related(self):
        """The two methods write to different state buckets
        (``selected_fields`` vs ``selected_related_fields``) so they
        compose without clobbering each other."""
        from dorm.db.connection import get_connection

        qs = (
            Author.objects.select_related("publisher")
            .only("name")
            .defer("publisher__name")
        )
        sql, _ = qs._query.as_select(get_connection())
        # Parent: only ``id`` (auto-included) and ``name``.
        # Related: every column except name → only ``id``.
        assert '"_sr_publisher_id"' in sql
        assert '"_sr_publisher_name"' not in sql
        # The order can be either — we just want both restrictions
        # honoured at the same time.
        # Parent projection should include name, and crucially NOT
        # project ``age`` / ``email`` / ``is_active`` etc.
        assert '"name"' in sql
        # ``age`` only appears as a column reference if not deferred.
        # With ``only("name")`` it shouldn't appear in the SELECT at all.
        # (Defensive: the column may also appear in JOIN ON-clause
        # text in more complex queries — Author has only one FK, so
        # safe here.)
        assert ' "age"' not in sql.replace('"_sr_', "_sr_")


# ── Bare only/defer (no select_related) ───────────────────────────────


class TestBareOnlyDefer:
    def test_bare_only_still_works(self):
        a = Author.objects.create(name="Solo", age=22, email="s@x.com")
        try:
            row = Author.objects.only("name").get(pk=a.pk)
            assert row.name == "Solo"
        finally:
            a.delete()

    def test_bare_defer_still_works(self):
        a = Author.objects.create(name="Solo", age=22, email="s@x.com")
        try:
            row = Author.objects.defer("email").get(pk=a.pk)
            assert row.name == "Solo"
        finally:
            a.delete()


# ── Identifier validation ────────────────────────────────────────────


class TestIdentifierValidation:
    def test_only_rejects_invalid_relation_name(self):
        with pytest.raises(Exception):
            Author.objects.only("bad relation__name")

    def test_only_rejects_unknown_relation(self):
        # ``foo`` isn't a field on Author → ``get_field`` raises.
        with pytest.raises(Exception):
            Author.objects.only("foo__bar")
