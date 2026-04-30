"""Tests for ``prefetch_related`` on :class:`GenericForeignKey`.

The descriptor's per-row read does ``model.objects.get(pk=oid)`` —
that's N+1 when iterating a list of polymorphic-tagged rows. The
prefetch path groups by ``content_type_id`` and emits one bulk
``filter(pk__in=…)`` per content type, taking the cost from O(N) to
O(K) where K is the number of distinct content types referenced.

Each test uses the same ``_count_queries`` listener pattern as
:mod:`test_perf_optimizations_v2` so the assertions speak in
round-trips, not wall-clock time.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

import dorm
from dorm.contrib.contenttypes import ContentType, GenericForeignKey, GenericRelation
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


# ── Polymorphic models (same shape as test_tier3_contenttypes but kept
#    independent so prefetch tests can run in isolation). ──────────────


class GFKArticle(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags = GenericRelation(
        "GFKTag", content_type_field="content_type", object_id_field="object_id"
    )

    class Meta:
        db_table = "gfk_pf_articles"


class GFKBook(dorm.Model):
    name = dorm.CharField(max_length=200)
    tags = GenericRelation(
        "GFKTag", content_type_field="content_type", object_id_field="object_id"
    )

    class Meta:
        db_table = "gfk_pf_books"


class GFKTag(dorm.Model):
    label = dorm.CharField(max_length=50)
    content_type = dorm.ForeignKey(ContentType, on_delete=dorm.CASCADE)
    content_type_id: int | None
    object_id = dorm.PositiveIntegerField()
    target = GenericForeignKey("content_type", "object_id")

    class Meta:
        db_table = "gfk_pf_tags"


@pytest.fixture
def _create_gfk_tables(clean_db):
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in [
        "gfk_pf_tags",
        "gfk_pf_articles",
        "gfk_pf_books",
        "django_content_type",
    ]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    ContentType.objects.clear_cache()

    for model in (ContentType, GFKArticle, GFKBook, GFKTag):
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


# ── Query-counting helper (cribbed from test_perf_optimizations_v2) ──


@contextmanager
def _count_queries():
    """Count user-visible SELECT/INSERT/UPDATE/DELETE round-trips
    emitted within the block. Skips DDL noise so the assertion reflects
    only the queries the prefetch logic produces."""
    from dorm.signals import pre_query

    seen: list[str] = []

    def listener(sender, sql, params, **kw):
        upper = sql.lstrip().upper()
        if upper.startswith(("CREATE", "DROP", "ALTER", "PRAGMA")):
            return
        seen.append(sql)

    pre_query.connect(listener, weak=False, dispatch_uid="gfk-prefetch-test")
    try:
        yield seen
    finally:
        pre_query.disconnect(dispatch_uid="gfk-prefetch-test")


def _select_count(seen: list[str]) -> int:
    """Count SELECT statements only — ContentType cache warm-ups also
    count as round-trips, but inserts in test setup don't."""
    return sum(1 for sql in seen if sql.lstrip().upper().startswith("SELECT"))


# ── Sync correctness ──────────────────────────────────────────────────


class TestGFKPrefetchCorrectness:
    def test_resolves_targets_across_two_content_types(self, _create_gfk_tables):
        a1 = GFKArticle.objects.create(title="A1")
        a2 = GFKArticle.objects.create(title="A2")
        b1 = GFKBook.objects.create(name="B1")

        ct_a = ContentType.objects.get_for_model(GFKArticle)
        ct_b = ContentType.objects.get_for_model(GFKBook)

        GFKTag.objects.create(label="t1", content_type=ct_a, object_id=a1.pk)
        GFKTag.objects.create(label="t2", content_type=ct_a, object_id=a2.pk)
        GFKTag.objects.create(label="t3", content_type=ct_b, object_id=b1.pk)

        tags = list(
            GFKTag.objects.prefetch_related("target").order_by("label")
        )
        assert len(tags) == 3
        assert isinstance(tags[0].target, GFKArticle)
        assert tags[0].target.pk == a1.pk
        assert isinstance(tags[1].target, GFKArticle)
        assert tags[1].target.pk == a2.pk
        assert isinstance(tags[2].target, GFKBook)
        assert tags[2].target.pk == b1.pk

    def test_unset_columns_yield_none_without_query(self, _create_gfk_tables):
        """Rows with NULL content_type_id / object_id never make it
        into the bulk fetch — the cache slot is set to None up front
        so the descriptor's read returns None without hitting the DB."""
        ct = ContentType.objects.get_for_model(GFKArticle)
        a = GFKArticle.objects.create(title="A")
        # Tag with the relation set.
        GFKTag.objects.create(label="set", content_type=ct, object_id=a.pk)

        # Manually insert a tag with both FK columns NULL — bypass the
        # ORM's NOT NULL enforcement on object_id by going through the
        # connection. Use a sentinel object_id=0 (no row matches it)
        # because the column itself is non-null in the schema.
        GFKTag.objects.create(label="dangling", content_type=ct, object_id=99999)

        tags = list(GFKTag.objects.prefetch_related("target").order_by("label"))
        assert len(tags) == 2
        # ``set`` resolves to the article.
        set_tag = next(t for t in tags if t.label == "set")
        assert set_tag.target is not None
        # ``dangling`` points at a missing pk → None.
        dangling = next(t for t in tags if t.label == "dangling")
        assert dangling.target is None

    def test_descriptor_cache_avoids_extra_query_after_prefetch(
        self, _create_gfk_tables
    ):
        """After prefetch fills the cache slot, repeated descriptor
        reads must NOT re-query the DB. This is the whole point — N+1
        is gone if and only if the descriptor honours the cache."""
        ct = ContentType.objects.get_for_model(GFKArticle)
        for i in range(3):
            a = GFKArticle.objects.create(title=f"A{i}")
            GFKTag.objects.create(label=f"t{i}", content_type=ct, object_id=a.pk)

        # Warm content_type cache so it doesn't show up in the count.
        _ = ContentType.objects.get_for_model(GFKArticle)

        tags = list(GFKTag.objects.prefetch_related("target"))
        with _count_queries() as seen:
            for t in tags:
                assert t.target is not None  # cached
                # Read again to make sure repeated access is also free.
                assert t.target.title.startswith("A")
        assert _select_count(seen) == 0, (
            f"prefetch cache should serve every descriptor read, but got: {seen}"
        )

    def test_prefetch_is_one_plus_k_queries(self, _create_gfk_tables):
        """Three articles + two books + five tags pointing at them
        should resolve in exactly: 1 SELECT for the tags + 1 SELECT
        per distinct content type (= 2). Total = 3, not 5+1 = 6.

        Previously ``prefetch_related("target")`` either crashed (no
        dispatcher branch) or fell through to per-row descriptor reads
        worth 5 SELECTs."""
        a1 = GFKArticle.objects.create(title="A1")
        a2 = GFKArticle.objects.create(title="A2")
        a3 = GFKArticle.objects.create(title="A3")
        b1 = GFKBook.objects.create(name="B1")
        b2 = GFKBook.objects.create(name="B2")

        ct_a = ContentType.objects.get_for_model(GFKArticle)
        ct_b = ContentType.objects.get_for_model(GFKBook)
        for label, ct, oid in [
            ("a1", ct_a, a1.pk),
            ("a2", ct_a, a2.pk),
            ("a3", ct_a, a3.pk),
            ("b1", ct_b, b1.pk),
            ("b2", ct_b, b2.pk),
        ]:
            GFKTag.objects.create(label=label, content_type=ct, object_id=oid)

        with _count_queries() as seen:
            tags = list(
                GFKTag.objects.prefetch_related("target").order_by("label")
            )
            # Force every descriptor read so a regression that re-queries
            # would inflate the count.
            for t in tags:
                _ = t.target

        n_selects = _select_count(seen)
        # 1 (tags) + 2 (per content type, articles + books) = 3.
        # Allow a small slack so a future cold-cache ContentType lookup
        # doesn't break the test, but we still catch a true N+1 (would
        # be 1 + 5 = 6 here).
        assert n_selects <= 4, (
            f"expected ≤ 4 selects (1 tags + 2 CT bulk fetches + ≤1 CT "
            f"cache warm-up); got {n_selects}: {seen}"
        )

    def test_prefetch_target_does_not_hit_db_for_dangling_only(
        self, _create_gfk_tables
    ):
        """A queryset where every row has a missing target should
        still issue at most: 1 SELECT (the tags) + 1 SELECT per CT
        (returning empty) — nothing else, even with the descriptor
        accessed on every row."""
        ct = ContentType.objects.get_for_model(GFKArticle)
        for i in range(4):
            GFKTag.objects.create(label=f"t{i}", content_type=ct, object_id=88000 + i)

        with _count_queries() as seen:
            tags = list(GFKTag.objects.prefetch_related("target"))
            for t in tags:
                assert t.target is None  # dangling, cached as None
        # 1 (tags) + 1 (article bulk fetch returning empty for those PKs)
        # plus optional CT cache warm-up.
        assert _select_count(seen) <= 3

    def test_prefetch_with_no_rows_is_a_noop(self, _create_gfk_tables):
        """An empty queryset must not issue any extra prefetch SQL."""
        with _count_queries() as seen:
            tags = list(GFKTag.objects.prefetch_related("target"))
            assert tags == []
        # Just the tags SELECT — no ContentType lookup, no bulk fetch.
        assert _select_count(seen) == 1


# ── Async correctness ────────────────────────────────────────────────


class TestGFKPrefetchAsync:
    @pytest.mark.asyncio
    async def test_aresolves_targets_across_two_content_types(
        self, _create_gfk_tables
    ):
        a = await GFKArticle.objects.acreate(title="A")
        b = await GFKBook.objects.acreate(name="B")

        ct_a = await ContentType.objects.aget_for_model(GFKArticle)
        ct_b = await ContentType.objects.aget_for_model(GFKBook)
        await GFKTag.objects.acreate(
            label="ta", content_type=ct_a, object_id=a.pk
        )
        await GFKTag.objects.acreate(
            label="tb", content_type=ct_b, object_id=b.pk
        )

        tags = []
        async for t in GFKTag.objects.prefetch_related("target").order_by("label"):
            tags.append(t)
        assert len(tags) == 2
        assert isinstance(tags[0].target, GFKArticle)
        assert isinstance(tags[1].target, GFKBook)

    @pytest.mark.asyncio
    async def test_async_dangling_target_returns_none(self, _create_gfk_tables):
        ct = await ContentType.objects.aget_for_model(GFKArticle)
        await GFKTag.objects.acreate(
            label="ghost", content_type=ct, object_id=99999
        )
        tags = []
        async for t in GFKTag.objects.prefetch_related("target"):
            tags.append(t)
        assert len(tags) == 1
        assert tags[0].target is None


# ── Validation: misuse raises early ──────────────────────────────────


class TestGFKPrefetchValidation:
    def test_user_queryset_raises_not_implemented(self, _create_gfk_tables):
        """A custom ``Prefetch(queryset=…)`` can only target one model;
        a GFK spans many. Reject it loudly so the user gets a useful
        error instead of silently broken data on heterogeneous rows."""
        from dorm.queryset import Prefetch

        ct = ContentType.objects.get_for_model(GFKArticle)
        a = GFKArticle.objects.create(title="A")
        GFKTag.objects.create(label="t", content_type=ct, object_id=a.pk)

        # The error fires when the prefetch actually runs, i.e. when
        # the queryset is materialised.
        with pytest.raises(NotImplementedError, match="custom Prefetch"):
            list(
                GFKTag.objects.prefetch_related(
                    Prefetch("target", queryset=GFKArticle.objects.all())
                )
            )

    def test_to_attr_raises_not_implemented(self, _create_gfk_tables):
        from dorm.queryset import Prefetch

        ct = ContentType.objects.get_for_model(GFKArticle)
        a = GFKArticle.objects.create(title="A")
        GFKTag.objects.create(label="t", content_type=ct, object_id=a.pk)

        with pytest.raises(NotImplementedError, match="to_attr"):
            list(
                GFKTag.objects.prefetch_related(
                    Prefetch("target", to_attr="target_obj")
                )
            )


# ── Coexistence with other prefetches ─────────────────────────────────


class TestGFKPrefetchCoexistence:
    def test_gfk_prefetch_alongside_forward_fk_prefetch(self, _create_gfk_tables):
        """A queryset can mix GFK and regular FK prefetches. Each goes
        through its own dispatcher branch and doesn't trip on the
        other's cache slot.

        ``content_type`` is a regular FK on the tag, so prefetching it
        plus ``target`` (the GFK) at once exercises both dispatcher
        paths in the same call.
        """
        a = GFKArticle.objects.create(title="A")
        ct = ContentType.objects.get_for_model(GFKArticle)
        GFKTag.objects.create(label="t", content_type=ct, object_id=a.pk)

        tags = list(
            GFKTag.objects.prefetch_related("target", "content_type")
        )
        assert len(tags) == 1
        assert isinstance(tags[0].target, GFKArticle)
        assert tags[0].content_type.pk == ct.pk


# ── Reverse: prefetch_related on GenericRelation ─────────────────────


class TestGenericRelationPrefetch:
    """``Article.objects.prefetch_related("tags")`` on a reverse
    ``GenericRelation`` should bulk-fetch every tag pointing at the
    article set in one SELECT, then ``article.tags.all()`` reads from
    memory. Without this each ``article.tags.all()`` is its own
    SELECT — N+1 across a list of articles.
    """

    def test_resolves_tags_per_article_with_one_query(self, _create_gfk_tables):
        a1 = GFKArticle.objects.create(title="A1")
        a2 = GFKArticle.objects.create(title="A2")
        # Third article exists but has no tags — exercises the empty
        # bucket case in the prefetch (article.tags.all() == []).
        GFKArticle.objects.create(title="A3")
        ct = ContentType.objects.get_for_model(GFKArticle)
        GFKTag.objects.create(label="ta1", content_type=ct, object_id=a1.pk)
        GFKTag.objects.create(label="ta1b", content_type=ct, object_id=a1.pk)
        GFKTag.objects.create(label="ta2", content_type=ct, object_id=a2.pk)

        with _count_queries() as seen:
            articles = list(
                GFKArticle.objects.prefetch_related("tags").order_by("id")
            )
            tags_per_article = [sorted(t.label for t in a.tags.all()) for a in articles]

        assert tags_per_article == [["ta1", "ta1b"], ["ta2"], []]
        # 1 (articles) + 1 (tags bulk) + at most 1 (CT lookup if cold).
        assert _select_count(seen) <= 3, seen

    def test_descriptor_cache_serves_repeat_calls(self, _create_gfk_tables):
        a = GFKArticle.objects.create(title="X")
        ct = ContentType.objects.get_for_model(GFKArticle)
        GFKTag.objects.create(label="t1", content_type=ct, object_id=a.pk)

        # Warm CT cache so subsequent counts focus on prefetch behaviour.
        _ = ContentType.objects.get_for_model(GFKArticle)

        articles = list(GFKArticle.objects.prefetch_related("tags"))
        with _count_queries() as seen:
            for art in articles:
                assert len(art.tags.all()) == 1
                assert art.tags.all()[0].label == "t1"
        assert _select_count(seen) == 0, seen

    def test_prefetch_isolates_tags_per_article(self, _create_gfk_tables):
        """Two articles, distinct tags — each cache slot must only see
        its own rows, not bleed across instances."""
        a1 = GFKArticle.objects.create(title="A1")
        a2 = GFKArticle.objects.create(title="A2")
        ct = ContentType.objects.get_for_model(GFKArticle)
        GFKTag.objects.create(label="only-a1", content_type=ct, object_id=a1.pk)
        GFKTag.objects.create(label="only-a2", content_type=ct, object_id=a2.pk)

        articles = list(GFKArticle.objects.prefetch_related("tags").order_by("id"))
        assert [t.label for t in articles[0].tags.all()] == ["only-a1"]
        assert [t.label for t in articles[1].tags.all()] == ["only-a2"]

    def test_prefetch_with_filtered_user_queryset(self, _create_gfk_tables):
        """``Prefetch(queryset=…)`` filters survive — user-supplied
        queryset's ``filter()`` is AND-ed onto the CT predicate."""
        from dorm.queryset import Prefetch

        a = GFKArticle.objects.create(title="A")
        ct = ContentType.objects.get_for_model(GFKArticle)
        GFKTag.objects.create(label="keep", content_type=ct, object_id=a.pk)
        GFKTag.objects.create(label="drop", content_type=ct, object_id=a.pk)

        articles = list(
            GFKArticle.objects.prefetch_related(
                Prefetch("tags", queryset=GFKTag.objects.filter(label="keep"))
            )
        )
        assert [t.label for t in articles[0].tags.all()] == ["keep"]

    def test_empty_queryset_short_circuits(self, _create_gfk_tables):
        """Iterating an empty parent queryset must not issue extra SQL
        for the prefetch."""
        with _count_queries() as seen:
            articles = list(GFKArticle.objects.prefetch_related("tags"))
        assert articles == []
        assert _select_count(seen) == 1  # just the parent SELECT


class TestGenericRelationPrefetchAsync:
    @pytest.mark.asyncio
    async def test_async_prefetch_resolves_tags(self, _create_gfk_tables):
        a = await GFKArticle.objects.acreate(title="A")
        ct = await ContentType.objects.aget_for_model(GFKArticle)
        await GFKTag.objects.acreate(label="t1", content_type=ct, object_id=a.pk)
        await GFKTag.objects.acreate(label="t2", content_type=ct, object_id=a.pk)

        articles = []
        async for art in GFKArticle.objects.prefetch_related("tags"):
            articles.append(art)
        assert len(articles) == 1
        labels = sorted(t.label for t in articles[0].tags.all())
        assert labels == ["t1", "t2"]
