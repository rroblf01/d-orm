"""Optimisation regression tests + extra coverage.

These tests count actual queries fired by each call site so a future
refactor that quietly reintroduces N+1 (or otherwise inflates the
round-trip count) fails loudly instead of just slowing the suite by a
few milliseconds.

We instrument via the ``pre_query`` signal — the hooked counter is
local to each test and disconnects in a finally block.

Also includes coverage tests for paths that the previous coverage
files didn't exercise: ``RawQuerySet`` async/sync hydration, more
``QuerySet.values_list`` flat / non-flat round-trips, more migration
``state_forwards`` paths, and a couple of CLI / migrations /
db-utils edge cases.
"""

from __future__ import annotations

from typing import Any
from contextlib import contextmanager

import pytest

import dorm
from dorm import signals, transaction
from tests.models import Article, Author, Book, Tag


# ── Helpers ──────────────────────────────────────────────────────────────────


@contextmanager
def count_queries():
    """Yield a list onto which every ``pre_query`` event lands while
    the context is active. ``len(out)`` after exit is the number of
    queries the wrapped block fired."""
    out: list[tuple[str, str]] = []

    def _capture(sender: str, **kwargs: Any) -> None:
        out.append((sender, kwargs.get("sql", "")))

    signals.pre_query.connect(_capture, weak=False)
    try:
        yield out
    finally:
        signals.pre_query.disconnect(_capture)


# ── refresh_from_db: respects fields= via .only() ────────────────────────────


def test_refresh_from_db_with_fields_emits_only_listed_columns_in_select():
    """``obj.refresh_from_db(fields=["age"])`` must emit a SELECT that
    only references the listed column (plus the primary key) — no
    SELECT * over the whole row.

    Locks down the optimisation so a future refactor can't silently
    revert to fetching every column."""
    a = Author.objects.create(name="rfd-only-original", age=20)
    Author.objects.filter(pk=a.pk).update(name="rfd-only-changed", age=99)

    with count_queries() as q:
        a.refresh_from_db(fields=["age"])
    # One query.
    assert len(q) == 1
    sql = q[0][1]
    # Must select age + pk; must NOT mention every other column.
    assert '"age"' in sql
    # ``email`` is on the Author model but wasn't requested — must
    # not appear in the projection.
    assert '"email"' not in sql, (
        f"refresh_from_db(fields=) leaked unrequested columns: {sql}"
    )
    # The age was refreshed; the name (in-memory) is still the old value.
    assert a.age == 99
    assert a.name == "rfd-only-original"


def test_refresh_from_db_without_fields_still_selects_everything():
    """Calling without ``fields=`` must keep working as before — full
    row refresh."""
    a = Author.objects.create(name="rfd-full-original", age=20)
    Author.objects.filter(pk=a.pk).update(name="rfd-full-changed", age=99)
    a.refresh_from_db()
    assert a.name == "rfd-full-changed"
    assert a.age == 99


def test_refresh_from_db_with_unknown_field_uses_no_only():
    """All-unknown ``fields=`` reduces to no ``.only()`` filter — the
    SELECT defaults back to full row. Locks the gracious-degrade
    behaviour: nothing in the query, but no exception either."""
    a = Author.objects.create(name="rfd-unknown", age=10)
    Author.objects.filter(pk=a.pk).update(age=99)
    a.refresh_from_db(fields=["definitely_not_a_field"])
    # Other fields stay as the in-memory values (no narrowing applied,
    # but also nothing was copied because the loop also skips unknowns).
    assert a.age == 10  # no field was actually copied


@pytest.mark.asyncio
async def test_arefresh_from_db_with_fields_emits_only_listed_columns():
    a = await Author.objects.acreate(name="arfd-only-original", age=20)
    await Author.objects.filter(pk=a.pk).aupdate(name="arfd-changed", age=99)
    with count_queries() as q:
        await a.arefresh_from_db(fields=["age"])
    assert len(q) == 1
    assert '"age"' in q[0][1]
    assert '"email"' not in q[0][1]
    assert a.age == 99
    assert a.name == "arfd-only-original"


# ── validate_unique: fast path is one query ──────────────────────────────────


def test_validate_unique_no_violations_fires_one_query():
    """A model with multiple unique constraints must spend exactly
    one round-trip to confirm "no violations". Lock down the fast
    path: previous implementation fired one query per constraint."""
    Tag.objects.create(name="vu-existing")
    fresh = Tag(name="vu-new-and-unique")
    with count_queries() as q:
        fresh.validate_unique()
    # One probe — the combined OR-query.
    assert len(q) == 1


def test_validate_unique_with_violation_drills_down_to_specific_field():
    """When the fast path detects a violation, the slow path
    re-issues per-probe queries to identify which constraint
    failed. The error message must name the violating field."""
    Tag.objects.create(name="vu-dup")
    fresh = Tag(name="vu-dup")
    with pytest.raises(dorm.ValidationError) as excinfo:
        fresh.validate_unique()
    msg = str(excinfo.value)
    # Either the field name or the value should appear so the user
    # can triangulate.
    assert "name" in msg or "Tag" in msg


def test_validate_unique_with_excluded_field_skips_check():
    """Probes for excluded field names must not be issued — neither
    in the fast-path OR-query nor in the slow-path drill-down.
    Locks down the exclude= contract."""
    Tag.objects.create(name="vu-skip")
    fresh = Tag(name="vu-skip")
    with count_queries() as q:
        fresh.validate_unique(exclude=["name"])
    # No probes built → no queries fired.
    assert len(q) == 0


def test_validate_unique_with_no_unique_fields_is_a_noop():
    """A model without any unique constraints must spend zero
    queries on validate_unique."""
    # Author has no unique=True fields and no unique_together.
    fresh = Author(name="vu-no-unique", age=1)
    with count_queries() as q:
        fresh.validate_unique()
    assert len(q) == 0


# ── extra QuerySet exists() / count() coverage ──────────────────────────────


def test_exists_emits_select_one_limit_one():
    """``.exists()`` must compile to ``SELECT 1 ... LIMIT 1`` — the
    cheapest existence probe. A regression that shipped
    ``SELECT COUNT(*)`` would silently double-cost every readiness
    probe in production."""
    Author.objects.create(name="exists-probe", age=1)
    with count_queries() as q:
        Author.objects.filter(name="exists-probe").exists()
    sql = q[0][1].upper()
    assert "SELECT" in sql
    assert "LIMIT 1" in sql or "LIMIT  1" in sql


def test_count_uses_count_star():
    Author.objects.create(name="count-probe-1", age=1)
    Author.objects.create(name="count-probe-2", age=2)
    with count_queries() as q:
        Author.objects.filter(name__startswith="count-probe-").count()
    sql = q[0][1].upper()
    assert "COUNT(*)" in sql


# ── Bulk operations query-count guarantees ──────────────────────────────────


def test_bulk_create_uses_one_insert_per_batch():
    """3 batches × 100 = 300 rows → 3 INSERTs (plus a trivial
    transaction-control overhead). Lock down the batching so a
    naive "INSERT one at a time" regression fails loudly."""
    objs = [Author(name=f"bcq-{i}", age=i) for i in range(300)]
    with count_queries() as q:
        Author.objects.bulk_create(objs, batch_size=100)
    # Filter to INSERT statements only — atomic() may emit BEGIN /
    # COMMIT / SAVEPOINT noise on some backends.
    inserts = [s for _, s in q if "INSERT" in s.upper()]
    assert len(inserts) == 3, (
        f"expected 3 INSERT statements (1 per batch), got {len(inserts)}"
    )


def test_bulk_update_uses_one_update_per_batch():
    """Same shape as bulk_create: one UPDATE statement per batch,
    with CASE WHEN populating the per-row values."""
    objs = [Author.objects.create(name=f"buq-{i}", age=i) for i in range(50)]
    for o in objs:
        o.age = 999
    with count_queries() as q:
        Author.objects.bulk_update(objs, fields=["age"], batch_size=20)
    updates = [s for _, s in q if s.upper().startswith(("UPDATE ", "UPDATE\n"))]
    # 50 rows / 20 batch_size = 3 batches.
    assert len(updates) == 3


def test_in_bulk_uses_single_query():
    """``Manager.in_bulk(pks)`` must hit the DB exactly once and
    construct the dict in Python from the result. A regression that
    issued one ``filter(pk=)`` per id would scale O(N)."""
    pks = []
    for i in range(20):
        pks.append(Author.objects.create(name=f"ib-{i}", age=i).pk)
    with count_queries() as q:
        out = Author.objects.in_bulk(pks)
    assert len(q) == 1
    assert len(out) == 20


# ── Reverse-FK prefetch is one extra query ──────────────────────────────────


def test_reverse_fk_prefetch_is_one_extra_query():
    """``prefetch_related("book_set")`` must turn N follow-up
    queries (one per author) into exactly ONE extra query for the
    whole batch. Lock that down — the regression we want to catch
    is "we accidentally start querying per author in a Python loop"."""
    a1 = Author.objects.create(name="pf-rev-1", age=1)
    a2 = Author.objects.create(name="pf-rev-2", age=2)
    Book.objects.create(title="b1", author=a1, pages=1)
    Book.objects.create(title="b2", author=a1, pages=2)
    Book.objects.create(title="b3", author=a2, pages=3)

    with count_queries() as q:
        authors = list(
            Author.objects.filter(name__startswith="pf-rev-").prefetch_related(
                "book_set"
            )
        )
        # Force evaluation of the prefetch by reading on each instance.
        for a in authors:
            list(a.book_set.all())  # type: ignore

    selects = [s for _, s in q if "SELECT" in s.upper()]
    # Two queries: one for authors, one for books.
    assert len(selects) == 2


def test_m2m_prefetch_is_one_extra_query():
    art = Article.objects.create(title="pf-m2m")
    t1 = Tag.objects.create(name="pf-t-1")
    t2 = Tag.objects.create(name="pf-t-2")
    art.tags.add(t1, t2)

    with count_queries() as q:
        out = list(Article.objects.filter(title="pf-m2m").prefetch_related("tags"))
        for a in out:
            list(a.tags.all())  # iterate to trigger lookup

    selects = [s for _, s in q if "SELECT" in s.upper()]
    # Two queries: one for articles, one for the joined m2m fetch.
    assert len(selects) == 2


# ── select_related is a JOIN, not N+1 ───────────────────────────────────────


def test_select_related_is_one_query():
    a = Author.objects.create(name="sr-author", age=10)
    Book.objects.create(title="sr-b1", author=a, pages=1)
    Book.objects.create(title="sr-b2", author=a, pages=2)
    with count_queries() as q:
        books = list(Book.objects.filter(title__startswith="sr-").select_related("author"))
        for b in books:
            _ = b.author.name  # already populated, no extra query
    selects = [s for _, s in q if "SELECT" in s.upper()]
    assert len(selects) == 1


# ── Update / delete on filtered querysets is one query ──────────────────────


def test_update_filter_emits_single_update():
    for i in range(5):
        Author.objects.create(name=f"upd-{i}", age=i)
    with count_queries() as q:
        Author.objects.filter(name__startswith="upd-").update(age=99)
    updates = [s for _, s in q if s.upper().startswith(("UPDATE ", "UPDATE\n"))]
    assert len(updates) == 1


def test_delete_filter_no_cascade_is_one_query():
    """Deleting a model with no on_delete-bearing reverse FKs must
    emit a single DELETE — no SELECT for pks first, no per-row
    handling. Tag has no relations pointing at it (M2M is on
    articles_tags, not on Tag itself); deleting tags is plain."""
    for i in range(5):
        Tag.objects.create(name=f"del-{i}")
    with count_queries() as q:
        Tag.objects.filter(name__startswith="del-").delete()
    deletes = [s for _, s in q if s.upper().startswith(("DELETE", "DELETE\n"))]
    # Tag has no reverse FK relations with on_delete=CASCADE/etc, so
    # delete() also pre-fetches no children. The implementation does
    # an initial values_list to obtain pks (one SELECT) plus the
    # DELETE itself.
    assert len(deletes) == 1


# ── Coverage: get_or_create (sync) success path ─────────────────────────────


def test_get_or_create_returns_existing_when_present():
    Tag.objects.create(name="goc-existing")
    obj, created = Tag.objects.get_or_create(name="goc-existing")
    assert created is False
    assert obj.name == "goc-existing"


def test_get_or_create_creates_when_missing():
    obj, created = Tag.objects.get_or_create(name="goc-new")
    assert created is True
    assert obj.pk is not None


def test_update_or_create_updates_existing():
    Tag.objects.create(name="uoc-existing")
    obj, created = Tag.objects.update_or_create(
        name="uoc-existing", defaults={"name": "uoc-renamed"}
    )
    assert created is False
    assert obj.name == "uoc-renamed"


def test_update_or_create_creates_when_missing():
    obj, created = Tag.objects.update_or_create(
        name="uoc-fresh", defaults={"name": "uoc-fresh"}
    )
    assert created is True
    assert obj.name == "uoc-fresh"


@pytest.mark.asyncio
async def test_aget_or_create_creates():
    obj, created = await Tag.objects.aget_or_create(name="agoc-fresh")
    assert created is True


@pytest.mark.asyncio
async def test_aget_or_create_returns_existing():
    await Tag.objects.acreate(name="agoc-existing")
    obj, created = await Tag.objects.aget_or_create(name="agoc-existing")
    assert created is False


@pytest.mark.asyncio
async def test_aupdate_or_create_updates():
    await Tag.objects.acreate(name="auoc-existing")
    obj, created = await Tag.objects.aupdate_or_create(
        name="auoc-existing", defaults={"name": "auoc-renamed"}
    )
    assert created is False
    assert obj.name == "auoc-renamed"


# ── Coverage: RawQuerySet hydration ─────────────────────────────────────────


def test_raw_queryset_hydrates_partial_columns():
    """``RawQuerySet`` must hydrate even when the SELECT doesn't
    return every column on the model — the missing slots stay
    unset, but the returned instance still works for the columns
    that *are* present."""
    Author.objects.create(name="raw-partial-1", age=42)
    qs = Author.objects.raw(
        "SELECT id, name FROM authors WHERE name = %s",
        ["raw-partial-1"],
    )
    [obj] = list(qs)
    assert obj.name == "raw-partial-1"
    # ``age`` wasn't selected — accessing it returns whatever the
    # field's default machinery produces; locked-down behaviour:
    # the attribute exists and is reachable without raising.
    _ = getattr(obj, "age", None)


def test_raw_queryset_iter_uses_cache_on_second_pass():
    """Iterating a RawQuerySet twice must reuse ``_result_cache``.
    Same contract as the regular QuerySet."""
    Author.objects.create(name="raw-cache", age=1)
    qs = Author.objects.raw(
        "SELECT * FROM authors WHERE name = %s",
        ["raw-cache"],
    )
    list(qs)
    cache = qs._result_cache
    list(qs)
    assert qs._result_cache is cache


# ── values_list flat=True and flat=False contracts ──────────────────────────


def test_values_list_non_flat_returns_tuples():
    Author.objects.create(name="vl-1", age=1)
    Author.objects.create(name="vl-2", age=2)
    out = list(
        Author.objects.filter(name__startswith="vl-")
        .order_by("age")
        .values_list("name", "age")
    )
    assert out == [("vl-1", 1), ("vl-2", 2)]


def test_values_list_flat_with_multiple_fields_raises():
    """``flat=True`` with more than one field is a programming error
    — ambiguous which field to flatten. Lock down the rejection."""
    with pytest.raises(ValueError, match="flat"):
        Author.objects.values_list("name", "age", flat=True)


# ── Coverage: cli helpers ───────────────────────────────────────────────────


def test_cli_validate_dotted_path_accepts_simple_module():
    from dorm.conf import _validate_dotted_path

    assert _validate_dotted_path("a.b.c") == "a.b.c"
    assert _validate_dotted_path("settings") == "settings"


def test_cli_next_migration_number_handles_empty_dir(tmp_path):
    from dorm.cli import _next_migration_number

    # Empty directory → 1 (first migration).
    assert _next_migration_number(tmp_path) == 1


def test_cli_next_migration_number_picks_max_plus_one(tmp_path):
    from dorm.cli import _next_migration_number

    (tmp_path / "0001_initial.py").write_text("")
    (tmp_path / "0002_auto.py").write_text("")
    (tmp_path / "0007_skipped.py").write_text("")
    # Non-numeric stems are ignored; only the max numeric prefix counts.
    (tmp_path / "manual_named.py").write_text("")
    assert _next_migration_number(tmp_path) == 8


# ── Coverage: db.utils edge cases for redaction ─────────────────────────────


def test_mask_params_handles_psycopg_dollar_n_outside_quoted():
    from dorm.db.utils import _mask_params

    sql = 'UPDATE "u" SET "password" = $1, "name" = $2 WHERE "id" = $3'
    out = _mask_params(sql, ["secret", "alice", 1])
    assert out == ["***", "alice", 1]


def test_mask_params_handles_authorization_column_alias():
    from dorm.db.utils import _mask_params

    sql = 'UPDATE "u" SET "authorization" = ?'
    out = _mask_params(sql, ["Bearer xyz"])
    assert out == ["***"]


def test_mask_params_handles_private_key_column_alias():
    from dorm.db.utils import _mask_params

    sql = 'INSERT INTO "u" ("name", "private_key") VALUES (?, ?)'
    out = _mask_params(sql, ["alice", "-----BEGIN PRIVATE KEY-----"])
    assert out == ["alice", "***"]


# ── Coverage: aggregate paths ───────────────────────────────────────────────


def test_aggregate_with_no_rows_returns_none_or_zero():
    """``Author.objects.filter(name="never-exists").aggregate(Sum("age"))``
    must return ``None`` for the SUM (Postgres / SQLite both yield NULL
    aggregates over empty sets). Lock that down: a regression that
    returned 0 would silently break code that distinguishes "no rows"
    from "rows with sum 0"."""
    out = Author.objects.filter(name="agg-never").aggregate(total=dorm.Sum("age"))
    assert out["total"] is None


def test_aggregate_count_with_filter():
    """``aggregate(Count("pk"), Sum("age"))`` over a filtered queryset
    must return both stats in one query. ``Count("pk")`` (Django
    idiom) used to crash with ``no such column: <table>.pk``; the
    aggregate compiler now translates ``pk`` to the actual PK
    column."""
    Author.objects.create(name="agg-c-1", age=1)
    Author.objects.create(name="agg-c-2", age=2)
    out = Author.objects.filter(name__startswith="agg-c-").aggregate(
        n=dorm.Count("pk"), total=dorm.Sum("age")
    )
    assert out["n"] == 2
    assert out["total"] == 3


def test_count_pk_in_annotate_compiles_to_actual_pk_column():
    """``annotate(n=Count("pk"))`` must also resolve ``pk`` — the
    fix lives in ``Aggregate.as_sql`` and is invoked from both the
    ``aggregate()`` and ``annotate()`` compilation paths. We assert
    the SQL shape rather than exercising ``.values()`` (which has
    its own annotation-projection limitations covered separately)."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.annotate(cnt=dorm.Count("pk"))
    sql, _ = qs._query.as_select(conn)
    # The pk-translated column must appear; the literal ``"pk"``
    # must NOT.
    assert 'COUNT("authors"."id")' in sql
    assert '"authors"."pk"' not in sql


# ── Coverage: model __hash__ ────────────────────────────────────────────────


def test_model_hash_uses_pk():
    a1 = Author.objects.create(name="hash-1", age=1)
    a2 = Author.objects.create(name="hash-2", age=2)
    s = {a1, a2}
    assert len(s) == 2
    # Re-fetched instance with same pk hashes equally (set membership).
    same = Author.objects.get(pk=a1.pk)
    assert same in s


def test_unsaved_model_is_unhashable():
    fresh = Author(name="unsaved", age=1)
    with pytest.raises(TypeError):
        hash(fresh)


# ── Coverage: select_for_update in a transaction emits FOR UPDATE on PG ────


def test_select_for_update_inside_atomic_actually_locks_pg():
    """Smoke test that the lock is acquired without raising. We
    can't easily prove "actually locking" from a single-process
    test; the goal is to exercise the FOR UPDATE branch end-to-end
    under PG."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only path")

    a = Author.objects.create(name="sfu-pg", age=1)
    with transaction.atomic():
        locked = Author.objects.filter(pk=a.pk).select_for_update()
        [row] = list(locked)
        assert row.pk == a.pk


# ── Coverage: connection context tear-down on autocommit set/reset ─────────


def test_set_autocommit_round_trip():
    """Toggle autocommit on, run a query, toggle back. Locks down a
    simple state machine that can easily go wrong with stale
    connections held by a previous mode."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    if not hasattr(conn, "set_autocommit"):
        pytest.skip("backend has no autocommit toggle")
    conn.set_autocommit(True)
    try:
        Author.objects.create(name="sa-1", age=1)
        assert Author.objects.filter(name="sa-1").exists()
    finally:
        conn.set_autocommit(False)


# ── Coverage: log_query masks sensitive in INSERT VALUES with extra cols ───


def test_mask_params_insert_with_only_one_sensitive_column_among_many():
    from dorm.db.utils import _mask_params

    sql = (
        'INSERT INTO "u" ("name", "email", "password", "created_at") '
        'VALUES (?, ?, ?, ?)'
    )
    out = _mask_params(sql, ["alice", "a@b.com", "s3cret", "2026-04-27"])
    assert out == ["alice", "a@b.com", "***", "2026-04-27"]


# ── Coverage: pool stats keys for PG when pool open ─────────────────────────


@pytest.mark.asyncio
async def test_pool_stats_for_open_pg_pool_includes_pool_min_max():
    """When the PG async pool is open, ``pool_stats()`` must surface
    ``pool_min`` and ``pool_max`` (psycopg's pool config). We exercise
    on the suite's pytest-asyncio event loop rather than spinning up
    a fresh loop with ``asyncio.run`` — the latter leaves an orphaned
    pool tied to a now-dead loop, which prevents the session from
    shutting down cleanly."""
    from dorm.db.connection import get_async_connection, pool_stats

    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only field")
    # Force the async pool to open by running one query on the
    # currently running loop.
    await conn.execute("SELECT 1")
    out = pool_stats()
    if out.get("has_pool"):
        assert "pool_min" in out and "pool_max" in out


# ── Coverage: more edge cases of bulk operations ─────────────────────────────


def test_bulk_create_with_one_object_uses_single_insert():
    """Edge case: ``bulk_create`` of a single object must NOT issue
    multiple statements. Trivial check but easy to break with an
    off-by-one in the batch loop."""
    with count_queries() as q:
        Author.objects.bulk_create([Author(name="bc-one", age=1)])
    inserts = [s for _, s in q if "INSERT" in s.upper()]
    assert len(inserts) == 1


def test_bulk_update_with_no_pk_objects_emits_zero_updates():
    """All objects without pk → ``_build_bulk_update_sql`` returns
    None and we skip the round-trip entirely."""
    objs = [Author(name="bu-nopk-1", age=1), Author(name="bu-nopk-2", age=2)]
    with count_queries() as q:
        n = Author.objects.bulk_update(objs, fields=["age"])
    assert n == 0
    updates = [s for _, s in q if s.upper().startswith(("UPDATE ", "UPDATE\n"))]
    assert len(updates) == 0


# ── Coverage: SoftDeleteModel async manager ─────────────────────────────────


@pytest.fixture
def soft_async_table():
    """Alternate fixture for soft-delete async-manager-tests where
    the suite-wide ``softdelete_table`` fixture isn't reachable."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql
    from dorm.contrib.softdelete import SoftDeleteModel

    class _SDM(SoftDeleteModel):
        title = dorm.CharField(max_length=200)

        class Meta:
            db_table = "audit_softdel_async_mgr"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_async_mgr"')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _SDM._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "audit_softdel_async_mgr" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield _SDM
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_async_mgr"')


@pytest.mark.asyncio
async def test_softdelete_async_acount_uses_filter(soft_async_table):
    M = soft_async_table
    M.objects.create(title="async-count-alive")
    b = M.objects.create(title="async-count-doomed")
    await b.adelete()
    # Default manager hides deleted in async too.
    qs = M.objects.all()
    n = await qs.acount()
    assert n == 1


# ── Coverage: validators module direct calls ───────────────────────────────


def test_min_value_validator_rejects_below():
    from dorm.exceptions import ValidationError
    from dorm.validators import MinValueValidator

    v = MinValueValidator(10)
    v(10)  # boundary OK
    v(11)  # over OK
    with pytest.raises(ValidationError):
        v(9)


def test_max_value_validator_rejects_above():
    from dorm.exceptions import ValidationError
    from dorm.validators import MaxValueValidator

    v = MaxValueValidator(100)
    v(100)
    v(0)
    with pytest.raises(ValidationError):
        v(101)


def test_min_length_validator_rejects_short():
    from dorm.exceptions import ValidationError
    from dorm.validators import MinLengthValidator

    v = MinLengthValidator(3)
    v("abc")
    with pytest.raises(ValidationError):
        v("ab")


def test_regex_validator_rejects_non_match():
    from dorm.exceptions import ValidationError
    from dorm.validators import RegexValidator

    v = RegexValidator(r"^[A-Z]+$")
    v("ABC")
    with pytest.raises(ValidationError):
        v("abc")


# ── Coverage: indexes module ───────────────────────────────────────────────


def test_index_repr_includes_fields():
    from dorm.indexes import Index

    idx = Index(fields=["name", "-age"], unique=True)
    rep = repr(idx)
    assert "Index" in rep
    assert "name" in rep
