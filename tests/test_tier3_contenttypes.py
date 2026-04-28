"""Tests for ``dorm.contrib.contenttypes``: ``ContentType`` registry,
``GenericForeignKey``, and ``GenericRelation``.

Covers:

* :class:`ContentType` create / lookup / cache.
* :meth:`ContentTypeManager.get_for_model` returns a single row per
  model and memoises it.
* ``GenericForeignKey`` descriptor: read returns instance, write
  populates ``content_type_id`` + ``object_id``, ``None`` clears both.
* ``GenericRelation`` reverse manager: ``filter`` / ``count`` /
  ``create`` are scoped by ``(content_type, object_id)``.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.contrib.contenttypes import ContentType, GenericForeignKey, GenericRelation
from dorm.contrib.contenttypes.models import ContentTypeManager
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


# ── Polymorphic models ────────────────────────────────────────────────────────


class CTArticle(dorm.Model):
    title = dorm.CharField(max_length=200)
    # ``CTTag`` rather than the bare string ``"CTTag"`` so the lookup
    # doesn't go through the global registry — the conftest also
    # registers a model named ``Tag`` and we don't want any chance of
    # confusion between the two.
    tags = GenericRelation(
        "CTTag", content_type_field="content_type", object_id_field="object_id"
    )

    class Meta:
        db_table = "ct_articles"


class CTBook(dorm.Model):
    name = dorm.CharField(max_length=200)
    tags = GenericRelation(
        "CTTag", content_type_field="content_type", object_id_field="object_id"
    )

    class Meta:
        db_table = "ct_books"


class CTTag(dorm.Model):
    label = dorm.CharField(max_length=50)
    content_type = dorm.ForeignKey(ContentType, on_delete=dorm.CASCADE)
    # ``ForeignKey.contribute_to_class`` installs a typed descriptor for
    # the underlying ``<fk>_id`` slot at runtime. Annotate it explicitly
    # so ty sees ``tag.content_type_id`` as ``int | None`` (matches the
    # convention used in :mod:`tests.models`).
    content_type_id: int | None
    object_id = dorm.PositiveIntegerField()
    target = GenericForeignKey("content_type", "object_id")

    class Meta:
        db_table = "ct_tags"


@pytest.fixture
def _create_ct_tables(clean_db):
    """Create the ContentType, polymorphic-target, and Tag tables.

    ``clean_db`` (autouse) already wiped the per-test schema; here we
    add only the tables this module's tests need on top."""
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    # Tables we own — drop and recreate every test for isolation.
    for tbl in ["ct_tags", "ct_articles", "ct_books", "django_content_type"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    # Manager-level cache from a previous test would point at IDs that
    # no longer exist. Wipe before recreating tables.
    ContentType.objects.clear_cache()

    for model in (ContentType, CTArticle, CTBook, CTTag):
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{model._meta.db_table}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )
    yield
    ContentType.objects.clear_cache()


# ── ContentType model ─────────────────────────────────────────────────────────


class TestContentType:
    def test_get_for_model_creates_row(self, _create_ct_tables):
        ct = ContentType.objects.get_for_model(CTArticle)
        assert ct.app_label == CTArticle._meta.app_label
        assert ct.model == "ctarticle"
        assert ct.pk is not None

    def test_get_for_model_is_cached(self, _create_ct_tables):
        ct1 = ContentType.objects.get_for_model(CTArticle)
        ct2 = ContentType.objects.get_for_model(CTArticle)
        # Cache returns the same instance, not just same pk.
        assert ct1 is ct2

    def test_get_for_model_one_row_per_model(self, _create_ct_tables):
        ContentType.objects.get_for_model(CTArticle)
        ContentType.objects.get_for_model(CTArticle)
        ContentType.objects.get_for_model(CTBook)
        # Two distinct models → two rows.
        assert ContentType.objects.count() == 2

    def test_model_class_resolves(self, _create_ct_tables):
        ct = ContentType.objects.get_for_model(CTArticle)
        assert ct.model_class() is CTArticle

    def test_get_object_for_this_type(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        ct = ContentType.objects.get_for_model(CTArticle)
        loaded = ct.get_object_for_this_type(pk=article.pk)
        assert loaded.pk == article.pk
        assert isinstance(loaded, CTArticle)
        assert loaded.title == "Hello"

    def test_get_object_for_this_type_missing_model(self, _create_ct_tables):
        # Manually craft a ContentType pointing at a model we never
        # registered → model_class() returns None → LookupError.
        ct = ContentType.objects.create(app_label="ghost", model="ghost")
        with pytest.raises(LookupError):
            ct.get_object_for_this_type(pk=1)

    def test_clear_cache(self, _create_ct_tables):
        ct = ContentType.objects.get_for_model(CTArticle)
        ContentType.objects.clear_cache()
        ct2 = ContentType.objects.get_for_model(CTArticle)
        # New instance, but same pk.
        assert ct.pk == ct2.pk


# ── GenericForeignKey ─────────────────────────────────────────────────────────


class TestGenericForeignKey:
    def test_assignment_sets_both_columns(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        tag = CTTag(label="featured")
        tag.target = article
        # Underlying columns populated.
        ct = ContentType.objects.get_for_model(CTArticle)
        assert tag.content_type.pk == ct.pk
        assert tag.object_id == article.pk

    def test_round_trip_descriptor(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        tag = CTTag(label="featured")
        tag.target = article
        tag.save()

        # Reload to avoid descriptor cache-hit path.
        loaded = CTTag.objects.get(pk=tag.pk)
        target = loaded.target
        assert isinstance(target, CTArticle)
        assert target.pk == article.pk
        assert target.title == "Hello"

    def test_descriptor_cache_avoids_extra_query(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        tag = CTTag(label="x", content_type=ContentType.objects.get_for_model(CTArticle))
        tag.object_id = article.pk
        tag.save()

        loaded = CTTag.objects.get(pk=tag.pk)
        first = loaded.target
        second = loaded.target
        assert first is second  # cached

    def test_assignment_to_none_clears(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        tag = CTTag(label="x")
        tag.target = article
        tag.target = None
        assert tag.content_type_id is None
        assert tag.object_id is None

    def test_polymorphic_targets(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        book = CTBook.objects.create(name="Manual")

        CTTag.objects.create(
            label="for-article",
            content_type=ContentType.objects.get_for_model(CTArticle),
            object_id=article.pk,
        )
        CTTag.objects.create(
            label="for-book",
            content_type=ContentType.objects.get_for_model(CTBook),
            object_id=book.pk,
        )

        for_article = CTTag.objects.get(label="for-article")
        for_book = CTTag.objects.get(label="for-book")
        assert isinstance(for_article.target, CTArticle)
        assert isinstance(for_book.target, CTBook)

    def test_dangling_object_id_returns_none(self, _create_ct_tables):
        # Polymorphic FKs aren't enforced by the database; if the row
        # they point at is deleted we should return None instead of
        # raising. Mirrors Django's behaviour.
        article = CTArticle.objects.create(title="Hello")
        ct = ContentType.objects.get_for_model(CTArticle)
        tag = CTTag.objects.create(label="x", content_type=ct, object_id=article.pk)
        article.delete()

        loaded = CTTag.objects.get(pk=tag.pk)
        assert loaded.target is None


# ── GenericRelation ───────────────────────────────────────────────────────────


class TestGenericRelation:
    def test_manager_filters_by_ct_and_pk(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        book = CTBook.objects.create(name="Manual")

        ct_article = ContentType.objects.get_for_model(CTArticle)
        ct_book = ContentType.objects.get_for_model(CTBook)

        CTTag.objects.create(label="a", content_type=ct_article, object_id=article.pk)
        CTTag.objects.create(label="b", content_type=ct_article, object_id=article.pk)
        CTTag.objects.create(label="c", content_type=ct_book, object_id=book.pk)

        labels = sorted(t.label for t in article.tags.all())
        assert labels == ["a", "b"]

        assert article.tags.count() == 2
        assert book.tags.count() == 1

    def test_create_via_relation_sets_ct_and_pk(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        article.tags.create(label="auto")

        tag = CTTag.objects.get(label="auto")
        assert tag.content_type_id == ContentType.objects.get_for_model(CTArticle).pk
        assert tag.object_id == article.pk

    def test_filter_chains_extra_predicates(self, _create_ct_tables):
        article = CTArticle.objects.create(title="Hello")
        article.tags.create(label="urgent")
        article.tags.create(label="archive")

        urgent = list(article.tags.filter(label="urgent"))
        assert len(urgent) == 1
        assert urgent[0].label == "urgent"

    def test_relation_isolation_between_instances(self, _create_ct_tables):
        a1 = CTArticle.objects.create(title="A1")
        a2 = CTArticle.objects.create(title="A2")
        a1.tags.create(label="only-a1")

        assert a1.tags.count() == 1
        assert a2.tags.count() == 0


# ── Async paths ───────────────────────────────────────────────────────────────


class TestAsyncContentType:
    @pytest.mark.asyncio
    async def test_aget_for_model(self, _create_ct_tables):
        ct = await ContentType.objects.aget_for_model(CTArticle)
        assert ct.model == "ctarticle"

    @pytest.mark.asyncio
    async def test_aget_object_for_this_type(self, _create_ct_tables):
        article = await CTArticle.objects.acreate(title="Async")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        loaded = await ct.aget_object_for_this_type(pk=article.pk)
        assert isinstance(loaded, CTArticle)
        assert loaded.title == "Async"


# ── Manager isolation ─────────────────────────────────────────────────────────


def test_contenttype_manager_class():
    """Make sure ``ContentType.objects`` is the specialised manager —
    if a refactor accidentally re-installed a vanilla ``Manager`` the
    cache wouldn't be reachable and tests would silently lose the cache
    invariant."""
    assert isinstance(ContentType.objects, ContentTypeManager)
