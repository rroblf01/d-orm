"""End-to-end tests for the Django-parity features added in the
2.2-followup batch:

- Vendor-aware date-part lookups (``__year``, ``__month``,
  ``__day``, ``__hour``, ``__minute``, ``__second``, ``__week_day``,
  ``__date``) — the previous SQLite-only ``STRFTIME`` template
  crashed on PostgreSQL.
- ``Trunc*`` / ``Extract*`` concrete-unit helpers.
- ``StringAgg`` / ``ArrayAgg`` PG aggregates.
- ``Manager.from_queryset(QuerySetCls)``.
- ``Prefetch(lookup, queryset=…, to_attr=…)``.
- ``distinct(*fields)`` → PG ``DISTINCT ON``.
- ``CTE(raw_sql, recursive=True)`` for tree walks.
- ``ImageField`` content validation via Pillow.
"""

from __future__ import annotations

import datetime
import io

import pytest

import dorm
from dorm import Prefetch
from tests.models import Author, Book, Publisher, Tag


# ── Date-part lookups: PG no longer crashes ─────────────────────────────────


class _DatedPost(dorm.Model):
    title = dorm.CharField(max_length=50)
    published_at = dorm.DateTimeField()

    class Meta:
        db_table = "dpf_posts"


@pytest.fixture
def _dated_table(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "dpf_posts"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _DatedPost._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "dpf_posts" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "dpf_posts"{cascade}')


class TestDatePartLookups:
    """Bug regression: ``__year`` etc. used to emit ``STRFTIME(...)``
    unconditionally, which PostgreSQL doesn't have. The fix routes
    PG through ``EXTRACT(unit FROM col)`` while SQLite stays on
    ``STRFTIME``."""

    def _seed(self):
        for ts in [
            datetime.datetime(2025, 12, 1, 9, 0, 0),
            datetime.datetime(2026, 1, 15, 14, 30, 0),
            datetime.datetime(2026, 4, 28, 10, 45, 30),
            datetime.datetime(2026, 4, 28, 23, 59, 59),
        ]:
            _DatedPost.objects.create(title=f"p-{ts.isoformat()}", published_at=ts)

    def test_year_lookup_works_on_both_backends(self, _dated_table):
        self._seed()
        assert _DatedPost.objects.filter(published_at__year=2026).count() == 3
        assert _DatedPost.objects.filter(published_at__year=2025).count() == 1

    def test_month_lookup(self, _dated_table):
        self._seed()
        assert _DatedPost.objects.filter(published_at__month=4).count() == 2

    def test_day_lookup(self, _dated_table):
        self._seed()
        assert _DatedPost.objects.filter(published_at__day=28).count() == 2

    def test_hour_lookup(self, _dated_table):
        self._seed()
        # Hours: 9, 14, 10, 23 — only one row at 10.
        assert _DatedPost.objects.filter(published_at__hour=10).count() == 1

    def test_chained_year_month(self, _dated_table):
        self._seed()
        qs = _DatedPost.objects.filter(
            published_at__year=2026, published_at__month=4
        )
        assert qs.count() == 2

    def test_date_lookup(self, _dated_table):
        self._seed()
        # ``__date`` accepts a Python ``date`` object on both backends.
        assert _DatedPost.objects.filter(
            published_at__date=datetime.date(2026, 4, 28)
        ).count() == 2


# ── Trunc / Extract helper classes ──────────────────────────────────────────


class TestTruncExtractHelpers:
    def test_trunc_helpers_pin_unit_correctly(self):
        from dorm.functions import (
            TruncDate, TruncDay, TruncHour, TruncMinute, TruncMonth,
            TruncQuarter, TruncWeek, TruncYear,
        )

        for cls, expected in (
            (TruncDate, "day"),
            (TruncDay, "day"),
            (TruncHour, "hour"),
            (TruncMinute, "minute"),
            (TruncMonth, "month"),
            (TruncQuarter, "quarter"),
            (TruncWeek, "week"),
            (TruncYear, "year"),
        ):
            assert cls("col").unit == expected

    def test_extract_helpers_pin_unit_correctly(self):
        from dorm.functions import (
            ExtractDay, ExtractHour, ExtractMinute, ExtractMonth,
            ExtractSecond, ExtractWeek, ExtractWeekDay, ExtractYear,
        )

        for cls, expected in (
            (ExtractYear, "year"),
            (ExtractMonth, "month"),
            (ExtractDay, "day"),
            (ExtractHour, "hour"),
            (ExtractMinute, "minute"),
            (ExtractSecond, "second"),
            (ExtractWeekDay, "dow"),
            (ExtractWeek, "week"),
        ):
            assert cls("col").unit == expected


# ── Coalesce regression: must validate argc ─────────────────────────────────


def test_coalesce_rejects_no_args():
    """Bug-fix regression — was previously possible to construct
    ``Coalesce()`` and only learn at the cursor."""
    from dorm.functions import Coalesce

    with pytest.raises(ValueError, match="at least one expression"):
        Coalesce()


# ── PG aggregates (StringAgg / ArrayAgg) ───────────────────────────────────


@pytest.fixture
def _postgres_only(db_config):
    if db_config.get("ENGINE") != "postgresql":
        pytest.skip("PG-only feature.")


class TestPGAggregates:
    def test_string_agg_concatenates_with_separator(self, _postgres_only):
        for n in ("alpha", "beta", "gamma"):
            Tag.objects.create(name=n)
        result = Tag.objects.aggregate(
            joined=dorm.StringAgg("name", separator=", ")
        )
        # Order is unspecified for plain STRING_AGG (without ORDER BY),
        # so split-and-compare as a set.
        assert isinstance(result["joined"], str)
        assert set(result["joined"].split(", ")) == {"alpha", "beta", "gamma"}

    def test_array_agg_collects_into_pg_array(self, _postgres_only):
        Tag.objects.create(name="a")
        Tag.objects.create(name="b")
        result = Tag.objects.aggregate(names=dorm.ArrayAgg("name"))
        # psycopg returns PG arrays as Python lists.
        assert isinstance(result["names"], list)
        assert set(result["names"]) == {"a", "b"}


# ── Manager.from_queryset ───────────────────────────────────────────────────


class TestManagerFromQuerySet:
    """Custom queryset → custom manager. The generated subclass must
    proxy every public method of the queryset *and* return the
    custom queryset class from ``get_queryset`` so chained calls
    keep their custom methods."""

    def test_custom_queryset_methods_reflect_onto_manager(self):
        class _ActiveQuerySet(dorm.QuerySet):
            def active(self):
                return self.filter(is_active=True)

            def by_age(self, age):
                return self.filter(age=age)

        # Build the manager class on demand for this test.
        ActiveManager = dorm.Manager.from_queryset(_ActiveQuerySet)

        # Smoke: the generated class is a Manager subclass.
        assert issubclass(ActiveManager, dorm.Manager)

        # Build a Model that uses it.
        class _UModel(dorm.Model):
            name = dorm.CharField(max_length=20)
            age = dorm.IntegerField()
            is_active = dorm.BooleanField(default=True)

            objects = ActiveManager()

            class Meta:
                db_table = "authors"  # piggyback the existing test table
                managed = False

        # Manager-level proxy method exists. ``active`` / ``by_age``
        # are added dynamically by ``from_queryset`` so a static type
        # checker can't see them — go through ``getattr`` to keep ty
        # happy without weakening the runtime check.
        assert callable(getattr(_UModel.objects, "active"))
        assert callable(getattr(_UModel.objects, "by_age"))

        # Calling it returns a queryset of the *custom* class.
        qs = getattr(_UModel.objects, "active")()
        assert isinstance(qs, _ActiveQuerySet)

        # And the chained custom methods compose as expected.
        Author.objects.create(name="alive", age=30, is_active=True)
        Author.objects.create(name="dormant", age=30, is_active=False)
        # We can't query against ``_UModel`` (managed=False, table
        # mapped to authors) but we can drive ``ActiveQuerySet``
        # directly with the canonical Author model.
        AuthorManager = dorm.Manager.from_queryset(_ActiveQuerySet)
        ActiveAuthorMgr = AuthorManager()
        ActiveAuthorMgr.contribute_to_class(Author, "_test_active_objects")
        try:
            mgr = getattr(Author, "_test_active_objects")
            results = list(mgr.active().by_age(30))
            assert len(results) == 1
            assert results[0].name == "alive"
        finally:
            # Clean up so the descriptor doesn't leak across tests.
            delattr(Author, "_test_active_objects")

    def test_from_queryset_rejects_non_queryset(self):
        with pytest.raises(TypeError):
            dorm.Manager.from_queryset(int)  # type: ignore[arg-type]


# ── Prefetch class ──────────────────────────────────────────────────────────


class TestPrefetchObject:
    def test_reverse_fk_with_filtered_queryset(self):
        pub = Publisher.objects.create(name="P")
        a = Author.objects.create(name="a", age=30, publisher=pub)
        Book.objects.create(title="Pub-X", author=a, pages=100, published=True)
        Book.objects.create(title="Draft-Y", author=a, pages=50, published=False)

        # Default prefetch fetches every book.
        only_published = Book.objects.filter(published=True)
        out = list(
            Author.objects.filter(pk=a.pk)
            .prefetch_related(Prefetch("book_set", queryset=only_published))
        )
        author = out[0]
        # Default cache slot is reused; the descriptor's manager pulls it.
        cached = author.__dict__.get("_prefetch_book_set")
        assert cached is not None
        assert {b.title for b in cached} == {"Pub-X"}

    def test_reverse_fk_with_to_attr_writes_to_attribute(self):
        pub = Publisher.objects.create(name="P")
        a = Author.objects.create(name="a", age=30, publisher=pub)
        Book.objects.create(title="Pub", author=a, pages=10, published=True)
        Book.objects.create(title="Draft", author=a, pages=5, published=False)

        out = list(
            Author.objects.filter(pk=a.pk).prefetch_related(
                Prefetch(
                    "book_set",
                    queryset=Book.objects.filter(published=True),
                    to_attr="published_books",
                )
            )
        )
        author = out[0]
        # ``to_attr`` writes the list directly to the named attribute.
        # The attribute is dynamic (set in ``__dict__``); go through
        # ``getattr`` so ty doesn't complain about a missing slot.
        published_books = getattr(author, "published_books")
        assert isinstance(published_books, list)
        assert {b.title for b in published_books} == {"Pub"}

    def test_forward_fk_with_filtered_queryset(self):
        active = Publisher.objects.create(name="active")
        archived = Publisher.objects.create(name="archived")
        Author.objects.create(name="a1", age=1, publisher=active)
        Author.objects.create(name="a2", age=2, publisher=archived)

        # Limit the prefetched publishers to ``name="active"``. Both
        # authors have a publisher pk, but only one matches the filter.
        only_active = Publisher.objects.filter(name="active")
        out = list(
            Author.objects.order_by("name").prefetch_related(
                Prefetch("publisher", queryset=only_active)
            )
        )
        # ``a1.publisher`` cached → resolves; ``a2.publisher`` cached
        # to None (its pk wasn't in the filtered set).
        assert out[0].__dict__["_cache_publisher"] is not None
        assert out[0].__dict__["_cache_publisher"].pk == active.pk
        assert out[1].__dict__["_cache_publisher"] is None
        # Sanity: the underlying FK ids didn't change.
        assert out[0].publisher_id == active.pk
        assert out[1].publisher_id == archived.pk


# ── distinct(*fields) — PG DISTINCT ON ────────────────────────────────────


class TestDistinctOn:
    def test_distinct_on_first_row_per_group(self, _postgres_only):
        # Build two authors and a couple of books per author with
        # different ``pages`` values.
        a = Author.objects.create(name="a", age=1)
        b = Author.objects.create(name="b", age=1)
        Book.objects.create(title="A1", author=a, pages=100, published=True)
        Book.objects.create(title="A2", author=a, pages=300, published=True)
        Book.objects.create(title="B1", author=b, pages=50, published=True)
        Book.objects.create(title="B2", author=b, pages=200, published=True)

        # Pick the *highest-page* book per author. ``DISTINCT ON
        # (author_id)`` keeps the first row per group as the ORDER BY
        # presents them — order by author_id, then -pages, so the
        # highest is first.
        out = list(
            Book.objects
                .order_by("author", "-pages")
                .distinct("author")
        )
        # One row per author; each is the max-pages title.
        assert {b.title for b in out} == {"A2", "B2"}

    def test_distinct_on_sqlite_raises(self, db_config):
        if db_config.get("ENGINE") == "postgresql":
            pytest.skip("SQLite-only assertion.")
        with pytest.raises(NotImplementedError, match="PostgreSQL"):
            list(Book.objects.distinct("author"))

    def test_plain_distinct_still_works(self):
        # No-arg form unchanged on both backends.
        for n in ("dup", "dup", "other"):
            Tag.objects.create(name=n) if n != "dup" or not Tag.objects.filter(
                name="dup"
            ).exists() else None
        names = sorted(
            r["name"] for r in Tag.objects.values("name").distinct()
        )
        # "dup" was inserted at most once due to UNIQUE constraint;
        # we just verify distinct() returned each name once.
        assert len(set(names)) == len(names)


# ── Recursive CTE via raw-SQL CTE class ────────────────────────────────────


class TestRecursiveCTE:
    """``CTE(sql, recursive=True)`` lets users plug in tree-walk
    queries that the queryset builder doesn't express natively.
    SQLite and PG both speak ``WITH RECURSIVE``; the test asserts on
    the resulting row set, not the SQL shape, so it works on both."""

    def test_recursive_cte_compiles_with_recursive_prefix(self):
        """The compiler emits ``WITH RECURSIVE`` when any attached
        CTE is marked recursive. We assert on the rendered SQL —
        executing a real recursive walk requires a self-referential
        table that ``tests/models.py`` doesn't ship, and going through
        ``raw()`` would bypass the WITH-clause emit anyway."""
        from dorm.db.connection import get_connection

        conn = get_connection()
        cte_obj = dorm.CTE(
            "SELECT 1 AS x UNION ALL SELECT x+1 FROM walk WHERE x < 3",
            recursive=True,
        )
        qs = Tag.objects.with_cte(walk=cte_obj)
        sql, params = qs._query.as_select(conn)
        assert sql.startswith("WITH RECURSIVE "), sql
        assert "walk" in sql

    def test_non_recursive_cte_uses_plain_with(self):
        """Sanity guard: only ``recursive=True`` triggers the
        ``WITH RECURSIVE`` prefix."""
        from dorm.db.connection import get_connection

        conn = get_connection()
        cte_obj = dorm.CTE("SELECT 1 AS x", recursive=False)
        qs = Tag.objects.with_cte(plain=cte_obj)
        sql, params = qs._query.as_select(conn)
        assert sql.startswith("WITH "), sql
        assert not sql.startswith("WITH RECURSIVE"), sql

    def test_cte_class_carries_params(self):
        from dorm.db.connection import get_connection

        conn = get_connection()
        cte_obj = dorm.CTE(
            "SELECT * FROM x WHERE id = %s",
            params=[42],
            recursive=False,
        )
        qs = Tag.objects.with_cte(narrowed=cte_obj)
        _sql, params = qs._query.as_select(conn)
        # The CTE's params land at the start of the bound list, before
        # any params from the outer query.
        assert 42 in params

    def test_with_cte_rejects_non_queryset_non_cte(self):
        with pytest.raises(TypeError, match="QuerySet or a CTE"):
            Tag.objects.with_cte(bad=42)  # type: ignore[arg-type]


# ── ImageField (Pillow) ─────────────────────────────────────────────────────


def _make_png_bytes() -> bytes:
    """Build a minimal valid PNG (1×1 black pixel) in-memory."""
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (1, 1), color=(0, 0, 0)).save(buf, format="PNG")
    return buf.getvalue()


class TestImageField:
    def test_assigning_valid_png_succeeds(self):
        # Bare-Field exercise — no model, just the validator.
        from dorm import ImageField

        field = ImageField(upload_to="x/")
        # Build a "model-like" object with a __dict__ for assignment.
        class _Stub:
            attname = "img"

        stub = _Stub()
        stub.__dict__ = {}
        field.attname = "img"
        # Also need ``contribute_to_class`` to install descriptor
        # state — fake it minimally.
        field.column = "img"
        field.name = "img"
        # Drive __set__ directly (the descriptor protocol expects
        # the class to have the field, but we're testing the
        # validation hook).
        png = _make_png_bytes()
        cf = dorm.ContentFile(png, name="x.png")
        # __set__ recognises ``File`` and routes through the
        # validator; pending file lands in __dict__.
        field.__set__(stub, cf)
        assert "_pending_file_img" in stub.__dict__

    def test_assigning_garbage_bytes_raises_validation_error(self):
        from dorm import ImageField

        field = ImageField(upload_to="x/")
        field.attname = "img"
        field.column = "img"
        field.name = "img"

        class _Stub:
            attname = "img"

        stub = _Stub()
        stub.__dict__ = {}
        cf = dorm.ContentFile(b"this is not an image", name="bogus.png")
        with pytest.raises(dorm.ValidationError, match="not a recognisable image"):
            field.__set__(stub, cf)

    def test_assigning_string_path_skips_validation(self):
        """Pre-existing storage names aren't re-validated — we don't
        want to fetch the bytes from S3 just to verify."""
        from dorm import ImageField

        field = ImageField(upload_to="x/")
        field.attname = "img"
        field.column = "img"
        field.name = "img"

        class _Stub:
            attname = "img"

        stub = _Stub()
        stub.__dict__ = {}
        # No exception — strings are passed through.
        field.__set__(stub, "x/already-on-disk.png")
        assert stub.__dict__["img"] == "x/already-on-disk.png"

    def test_validator_resets_stream_position(self):
        """``ImageField.__set__`` calls ``Image.verify`` which advances
        the underlying stream. The validator must rewind so the storage
        backend reads the full payload, not the bytes after verify."""
        from dorm import ImageField

        field = ImageField(upload_to="x/")
        field.attname = "img"
        field.column = "img"
        field.name = "img"

        class _Stub:
            attname = "img"

        stub = _Stub()
        stub.__dict__ = {}
        png = _make_png_bytes()
        cf = dorm.ContentFile(png, name="x.png")
        field.__set__(stub, cf)
        # The pending file's stream should be rewound to 0 so a
        # subsequent ``read()`` returns all the bytes.
        pending = stub.__dict__["_pending_file_img"]
        assert pending.read() == png
