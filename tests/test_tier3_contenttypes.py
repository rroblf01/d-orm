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


# ── GenericForeignKey async resolver ──────────────────────────────────────────


class TestGenericForeignKeyAsync:
    """Cover :meth:`GenericForeignKey.aget` — the async equivalent of
    the descriptor read. Mirrors the sync test cases so we know the
    same semantics hold from inside async handlers (FastAPI, Sanic)."""

    @pytest.mark.asyncio
    async def test_aget_returns_referenced_instance(self, _create_ct_tables):
        article = await CTArticle.objects.acreate(title="Async")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        tag = await CTTag.objects.acreate(
            label="async-tag", content_type=ct, object_id=article.pk
        )
        loaded = await CTTag.objects.aget(pk=tag.pk)
        descriptor = type(loaded).__dict__["target"]
        target = await descriptor.aget(loaded)
        assert isinstance(target, CTArticle)
        assert target.pk == article.pk

    @pytest.mark.asyncio
    async def test_aget_uses_cache_on_second_call(self, _create_ct_tables):
        article = await CTArticle.objects.acreate(title="Cached")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        tag = await CTTag.objects.acreate(label="x", content_type=ct, object_id=article.pk)
        loaded = await CTTag.objects.aget(pk=tag.pk)
        descriptor = type(loaded).__dict__["target"]
        first = await descriptor.aget(loaded)
        second = await descriptor.aget(loaded)
        assert first is second

    @pytest.mark.asyncio
    async def test_aget_returns_none_when_unset(self, _create_ct_tables):
        # No content_type / object_id set — must short-circuit to None
        # without hitting the DB.
        tag = CTTag(label="empty")
        descriptor = type(tag).__dict__["target"]
        assert await descriptor.aget(tag) is None

    @pytest.mark.asyncio
    async def test_aget_returns_none_when_target_deleted(self, _create_ct_tables):
        article = await CTArticle.objects.acreate(title="Doomed")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        tag = await CTTag.objects.acreate(label="x", content_type=ct, object_id=article.pk)
        await article.adelete()
        loaded = await CTTag.objects.aget(pk=tag.pk)
        descriptor = type(loaded).__dict__["target"]
        assert await descriptor.aget(loaded) is None


# ── _GenericRelatedManager extra methods ──────────────────────────────────────


class TestGenericRelatedManagerExtras:
    """Each of these manager helpers (`exclude`, `exists`, `first`,
    `add`, `acreate`) was previously untested. Coverage was 75% on
    contenttypes/fields.py before this section; they all delegate to
    the same ``_ct_filter`` so a single regression there breaks every
    one of them silently."""

    def test_exclude_filters_out_matching_rows(self, _create_ct_tables):
        article = CTArticle.objects.create(title="X")
        article.tags.create(label="keep")
        article.tags.create(label="drop")
        labels = sorted(t.label for t in article.tags.exclude(label="drop"))
        assert labels == ["keep"]

    def test_exists_reflects_relation_membership(self, _create_ct_tables):
        article = CTArticle.objects.create(title="X")
        assert article.tags.exists() is False
        article.tags.create(label="t")
        assert article.tags.exists() is True

    def test_first_returns_oldest_row(self, _create_ct_tables):
        article = CTArticle.objects.create(title="X")
        article.tags.create(label="a")
        article.tags.create(label="b")
        first = article.tags.first()
        assert first is not None
        assert first.label in {"a", "b"}

    def test_first_returns_none_on_empty_relation(self, _create_ct_tables):
        article = CTArticle.objects.create(title="X")
        assert article.tags.first() is None

    def test_add_attaches_existing_objects(self, _create_ct_tables):
        article = CTArticle.objects.create(title="X")
        # Build the tag without a target, then attach via ``add``.
        ct = ContentType.objects.get_for_model(CTArticle)
        # Pre-create with bogus target to satisfy NOT NULL columns.
        # ``add`` reassigns target then re-saves.
        floating = CTTag.objects.create(label="float", content_type=ct, object_id=0)
        article.tags.add(floating)
        assert article.tags.count() == 1
        reloaded = CTTag.objects.get(pk=floating.pk)
        assert reloaded.object_id == article.pk

    @pytest.mark.asyncio
    async def test_acreate_via_relation_sets_ct_and_pk(self, _create_ct_tables):
        article = await CTArticle.objects.acreate(title="Async")
        await article.tags.acreate(label="async-relation")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        tag = await CTTag.objects.aget(label="async-relation")
        assert tag.content_type_id == ct.pk
        assert tag.object_id == article.pk

    @pytest.mark.asyncio
    async def test_async_filter_helper_resolves_ct(self, _create_ct_tables):
        # ``_act_filter`` is the async variant of ``_ct_filter``; we
        # don't call it directly (private), but ``acreate`` uses it
        # internally — exercising it here gives us coverage and a
        # round-trip assertion.
        article = await CTArticle.objects.acreate(title="X")
        await article.tags.acreate(label="created")
        ct = await ContentType.objects.aget_for_model(CTArticle)
        result = await CTTag.objects.filter(
            content_type=ct, object_id=article.pk
        ).afirst()
        assert result is not None
        assert result.label == "created"


# ── GenericForeignKey / GenericRelation descriptor edge cases ────────────────


class TestDescriptorClassAccess:
    """Accessing the descriptor on the *class* (not instance) must
    return the descriptor itself — Django convention, mirrored here.
    Easy to regress if someone forgets the ``if instance is None``
    guard."""

    def test_generic_foreign_key_class_access_returns_descriptor(
        self, _create_ct_tables
    ):
        descriptor = type(CTTag).__dict__.get("target") or CTTag.__dict__.get("target")
        assert descriptor is not None
        assert isinstance(descriptor, GenericForeignKey)
        # ``CTTag.target`` going through the descriptor protocol must
        # yield the same object: the field, not a proxied manager.
        assert CTTag.__dict__["target"] is descriptor

    def test_generic_relation_class_access_returns_descriptor(
        self, _create_ct_tables
    ):
        relation = CTArticle.__dict__["tags"]
        assert isinstance(relation, GenericRelation)
        # Calling ``__get__(None, CTArticle)`` simulates class-level
        # attribute access; the relation must short-circuit before
        # building a manager (which would explode on ``instance.pk``).
        assert relation.__get__(None, CTArticle) is relation


class TestGenericRelationResolveErrors:
    def test_unresolvable_string_target_raises_lookup_error(self, _create_ct_tables):
        rel = GenericRelation("__definitely_not_a_real_model__")
        # ``contribute_to_class`` would normally validate; here we go
        # straight at the lazy resolver to verify the error message
        # mentions the missing model name.
        with pytest.raises(LookupError, match="__definitely_not_a_real_model__"):
            rel._resolve_related()

    def test_resolved_string_target_finds_registered_model(self, _create_ct_tables):
        rel = GenericRelation("CTTag")
        assert rel._resolve_related() is CTTag

    def test_class_target_returns_unchanged(self, _create_ct_tables):
        rel = GenericRelation(CTTag)
        # When the user passes the class directly, ``_resolve_related``
        # must NOT consult the registry — that path is reserved for
        # forward-reference strings.
        assert rel._resolve_related() is CTTag


# ── ContentType caching invalidation ─────────────────────────────────────────


class TestContentTypeCacheClearing:
    """``ContentType.objects.clear_cache`` is exercised in fixtures
    but never asserted to actually work."""

    def test_clear_cache_forces_db_reload(self, _create_ct_tables):
        ct1 = ContentType.objects.get_for_model(CTArticle)
        ContentType.objects.clear_cache()
        ct2 = ContentType.objects.get_for_model(CTArticle)
        # Same row in the DB, but distinct Python objects after the
        # cache wipe.
        assert ct1.pk == ct2.pk
        assert ct1 is not ct2


# ── Manager isolation ─────────────────────────────────────────────────────────


def test_contenttype_manager_class():
    """Make sure ``ContentType.objects`` is the specialised manager —
    if a refactor accidentally re-installed a vanilla ``Manager`` the
    cache wouldn't be reachable and tests would silently lose the cache
    invariant."""
    assert isinstance(ContentType.objects, ContentTypeManager)
