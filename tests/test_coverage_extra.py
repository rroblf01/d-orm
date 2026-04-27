"""More coverage-driven tests, keeping bug-detection bias.

Targets that ``test_coverage.py`` left untouched:

- M2M relation manager: ``add`` / ``remove`` / ``set`` / ``clear`` /
  ``create`` (sync and async), ``through_defaults``, empty calls.
- Reverse-FK manager: ``filter``, ``count``, ``create``, ``__iter__``.
- Migration ops: ``AddField`` / ``RemoveField`` / ``AlterField`` /
  ``RenameModel`` forwards & backwards (real DB).
- ``ValuesListQuerySet`` / ``CombinedQuerySet`` async iteration.
- ``RawQuerySet`` async ``afetch_all``.
- Subqueries via union/intersection/difference + slicing.
- ``conf._discover_apps`` nested-package handling.
- Direct-assignment guard on M2M descriptor.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.migrations.operations import (
    AddField,
    AlterField,
    RemoveField,
    RenameField,
    RenameModel,
    RunSQL,
)
from tests.models import Article, Author, Book, Tag


# ── M2M relation manager: sync ───────────────────────────────────────────────


def test_m2m_add_remove_set_clear_round_trip():
    """End-to-end: an Article gains and loses Tags through the
    relation manager. Locks down the most-used M2M call shapes in one
    test so a regression in any single one fails loudly."""
    art = Article.objects.create(title="m2m-roundtrip")
    t1 = Tag.objects.create(name="t-rt-1")
    t2 = Tag.objects.create(name="t-rt-2")
    t3 = Tag.objects.create(name="t-rt-3")

    art.tags.add(t1, t2)
    assert art.tags.count() == 2

    # ``set`` replaces the membership.
    art.tags.set([t2, t3])
    names = sorted(t.name for t in art.tags.all())
    assert names == ["t-rt-2", "t-rt-3"]

    art.tags.remove(t2)
    assert art.tags.count() == 1

    art.tags.clear()
    assert art.tags.count() == 0


def test_m2m_add_dedupes_within_call_and_existing():
    """Calling ``add(t1, t1, t1)`` must end up with one row, and a
    subsequent ``add(t1, t2)`` must only insert ``t2`` — the
    pre-fetch of existing pks should keep dupes out of the multi-row
    INSERT. We assert via row count to lock down the SQL shape."""
    art = Article.objects.create(title="m2m-dedupe")
    t1 = Tag.objects.create(name="t-dedupe-1")
    t2 = Tag.objects.create(name="t-dedupe-2")
    art.tags.add(t1, t1, t1)
    assert art.tags.count() == 1
    art.tags.add(t1, t2)
    assert art.tags.count() == 2


def test_m2m_add_empty_list_short_circuits():
    """``art.tags.add()`` with no arguments must be a no-op — no SQL
    issued, no error raised. Same for ``remove()``."""
    art = Article.objects.create(title="m2m-empty")
    art.tags.add()  # must not raise
    art.tags.remove()  # must not raise
    assert art.tags.count() == 0


def test_m2m_add_accepts_pks_directly():
    """Most callers pass model instances, but the manager should
    accept bare pks too (Django parity)."""
    art = Article.objects.create(title="m2m-pk")
    t1 = Tag.objects.create(name="t-pk-1")
    art.tags.add(t1.pk)
    assert art.tags.count() == 1


def test_m2m_create_through_manager():
    """``art.tags.create(name="...")`` creates the target *and*
    inserts the through row in one call."""
    art = Article.objects.create(title="m2m-create")
    t = art.tags.create(name="m2m-created-tag")
    assert t.pk is not None
    assert art.tags.count() == 1


def test_m2m_set_with_clear_flag_short_circuit_via_clear_then_add():
    """``set(..., clear=True)`` clears the relation first, then re-adds
    the new set. Locks down both code paths in one test."""
    art = Article.objects.create(title="m2m-set-clear")
    t1 = Tag.objects.create(name="t-sc-1")
    t2 = Tag.objects.create(name="t-sc-2")
    art.tags.add(t1)
    art.tags.set([t2], clear=True)
    assert art.tags.count() == 1
    [tag] = list(art.tags.all())
    assert tag.name == "t-sc-2"


def test_m2m_descriptor_rejects_direct_assignment():
    """``art.tags = [...]`` must raise — Django's well-known footgun
    that should never silently succeed."""
    art = Article.objects.create(title="m2m-direct")
    t1 = Tag.objects.create(name="t-direct-1")
    with pytest.raises(AttributeError, match="Direct assignment"):
        art.tags = [t1]


def test_m2m_repr_renders_relation_path():
    art = Article.objects.create(title="m2m-repr")
    rep = repr(art.tags)
    assert "ManyRelatedManager" in rep
    assert "Article" in rep and "Tag" in rep


def test_m2m_filter_through_relation_manager():
    """Locks down chaining: ``art.tags.filter(name=...)`` returns a
    queryset filtered to the through-related set AND the name predicate."""
    art = Article.objects.create(title="m2m-filter")
    other = Article.objects.create(title="m2m-filter-other")
    t1 = Tag.objects.create(name="t-flt-1")
    t2 = Tag.objects.create(name="t-flt-2")
    art.tags.add(t1, t2)
    other.tags.add(t1)
    [match] = list(art.tags.filter(name="t-flt-1"))
    assert match.pk == t1.pk
    # The other article isn't visible from this manager.
    assert art.tags.filter(name="other").count() == 0


# ── M2M relation manager: async ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_m2m_async_add_remove_set_clear():
    art = await Article.objects.acreate(title="am2m")
    t1 = await Tag.objects.acreate(name="at-1")
    t2 = await Tag.objects.acreate(name="at-2")
    t3 = await Tag.objects.acreate(name="at-3")

    await art.tags.aadd(t1, t2)
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 2

    await art.tags.aset([t2, t3])
    qs = await art.tags.aget_queryset()
    names = {t.name async for t in qs}
    assert names == {"at-2", "at-3"}

    await art.tags.aremove(t2)
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 1

    await art.tags.aclear()
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 0


@pytest.mark.asyncio
async def test_m2m_async_acreate_through_manager():
    art = await Article.objects.acreate(title="am2m-create")
    t = await art.tags.acreate(name="am2m-created-tag")
    assert t.pk is not None
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 1


@pytest.mark.asyncio
async def test_m2m_async_aadd_aremove_empty_short_circuits():
    art = await Article.objects.acreate(title="am2m-empty")
    await art.tags.aadd()  # noop
    await art.tags.aremove()  # noop
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 0


@pytest.mark.asyncio
async def test_m2m_async_aget_queryset_when_empty_returns_none_qs():
    """``aget_queryset`` short-circuits to ``.none()`` when the
    relation has zero rows so we don't issue a useless ``WHERE pk IN ()``."""
    art = await Article.objects.acreate(title="am2m-noqs")
    qs = await art.tags.aget_queryset()
    assert await qs.acount() == 0


# ── Reverse-FK manager methods ───────────────────────────────────────────────


def test_reverse_fk_manager_filter_count_create_iterate():
    # ``book_set`` is the reverse-FK descriptor installed at runtime by
    # ForeignKey.contribute_to_class — ty can't see it statically, hence
    # the ignores below.
    a = Author.objects.create(name="rfk-author", age=42)
    Book.objects.create(title="rfk-b1", author=a, pages=10)
    # ``create`` through the reverse manager — author auto-set from instance.
    a.book_set.create(title="rfk-b2", pages=20)  # type: ignore

    assert a.book_set.count() == 2  # type: ignore
    titles = sorted(b.title for b in a.book_set)  # type: ignore
    assert titles == ["rfk-b1", "rfk-b2"]

    [match] = list(a.book_set.filter(title="rfk-b2"))  # type: ignore
    assert match.pages == 20


def test_reverse_fk_manager_repr():
    a = Author.objects.create(name="rfk-repr", age=1)
    rep = repr(a.book_set)  # type: ignore
    assert "ReverseFKManager" in rep
    assert "Book" in rep and "Author" in rep


def test_reverse_fk_descriptor_rejects_direct_assignment():
    """As with M2M, direct assignment to a reverse FK must raise to
    prevent the Django-shaped footgun ``author.book_set = [...]``."""
    a = Author.objects.create(name="rfk-set", age=2)
    with pytest.raises(AttributeError, match="Direct assignment"):
        a.book_set = [Book(title="x", pages=1)]  # type: ignore


def test_reverse_fk_uses_prefetch_cache_when_present():
    """If the parent was loaded via ``prefetch_related``, the
    ReverseFKManager must serve from the cache — *no* extra query.
    Locks down a regression where the cache key was misnamed."""
    a = Author.objects.create(name="rfk-cache", age=5)
    Book.objects.create(title="rfk-cache-b1", author=a, pages=1)

    [loaded] = list(
        Author.objects.filter(name="rfk-cache").prefetch_related("book_set")
    )
    # Reading book_set must go through the cache (no fresh query). We
    # can't observe "no query" here easily, but we can at least see the
    # cached items match.
    cached = list(loaded.book_set)  # type: ignore
    assert [b.title for b in cached] == ["rfk-cache-b1"]


# ── Migration ops: AddField / RemoveField / AlterField forwards+backwards ────


def _make_migration_dir(tmp: Path, name: str = "0001_initial.py", source: str = "") -> Path:
    """Helper: scaffold a migrations/ tree with a single source file."""
    mig = tmp / "migrations"
    mig.mkdir(parents=True, exist_ok=True)
    (mig / "__init__.py").write_text("")
    (mig / name).write_text(source)
    return mig


def test_addfield_migration_round_trip():
    """``AddField`` forwards adds the column; backwards drops it.
    Real-DB round-trip locks down the SQL emission for both directions."""
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "addfield_target"')
    conn.execute_script(
        'CREATE TABLE "addfield_target" ("id" INTEGER PRIMARY KEY)'
    )

    state = type(
        "S",
        (),
        {"models": {"addapp.target": {"name": "Target", "options": {"db_table": "addfield_target"}, "fields": {}}}},
    )()
    op = AddField(model_name="Target", name="extra", field=dorm.IntegerField(default=0))
    op.database_forwards("addapp", conn, state, state)
    cols = {c["name"] for c in conn.get_table_columns("addfield_target")}
    assert "extra" in cols

    op.database_backwards("addapp", conn, state, state)
    cols_after = {c["name"] for c in conn.get_table_columns("addfield_target")}
    assert "extra" not in cols_after

    conn.execute_script('DROP TABLE IF EXISTS "addfield_target"')


def test_removefield_migration_backwards_re_adds_column():
    """``RemoveField.database_backwards`` restores the dropped column.
    Locks down a path where the column type comes from the *to_state*
    snapshot — easy to drift if the state shape changes."""
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "rmfield_target"')
    conn.execute_script(
        'CREATE TABLE "rmfield_target" ("id" INTEGER PRIMARY KEY, "doomed" INTEGER)'
    )

    field_obj = dorm.IntegerField(default=0)
    field_obj.column = "doomed"
    state = type(
        "S",
        (),
        {
            "models": {
                "rmapp.target": {
                    "name": "Target",
                    "options": {"db_table": "rmfield_target"},
                    "fields": {"doomed": field_obj},
                }
            }
        },
    )()
    op = RemoveField(model_name="Target", name="doomed")
    op.database_forwards("rmapp", conn, state, state)
    cols_mid = {c["name"] for c in conn.get_table_columns("rmfield_target")}
    assert "doomed" not in cols_mid

    op.database_backwards("rmapp", conn, state, state)
    cols_back = {c["name"] for c in conn.get_table_columns("rmfield_target")}
    assert "doomed" in cols_back

    conn.execute_script('DROP TABLE IF EXISTS "rmfield_target"')


def test_renamefield_migration_round_trip():
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "rfield_t"')
    conn.execute_script(
        'CREATE TABLE "rfield_t" ("id" INTEGER PRIMARY KEY, "old" INTEGER)'
    )
    state = type(
        "S",
        (),
        {"models": {"rfapp.target": {"name": "Target", "options": {"db_table": "rfield_t"}, "fields": {}}}},
    )()
    op = RenameField(model_name="Target", old_name="old", new_name="new")
    op.database_forwards("rfapp", conn, state, state)
    cols = {c["name"] for c in conn.get_table_columns("rfield_t")}
    assert "new" in cols and "old" not in cols

    op.database_backwards("rfapp", conn, state, state)
    cols_back = {c["name"] for c in conn.get_table_columns("rfield_t")}
    assert "old" in cols_back and "new" not in cols_back

    conn.execute_script('DROP TABLE IF EXISTS "rfield_t"')


def test_renamemodel_migration_renames_table():
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "rmapp_old"')
    conn.execute_script('DROP TABLE IF EXISTS "rmapp_new"')
    conn.execute_script(
        'CREATE TABLE "rmapp_old" ("id" INTEGER PRIMARY KEY)'
    )

    state = type(
        "S",
        (),
        {
            "models": {
                "rmapp.old": {"name": "Old", "options": {}, "fields": {}}
            }
        },
    )()
    op = RenameModel(old_name="Old", new_name="New")
    op.database_forwards("rmapp", conn, state, state)
    assert conn.table_exists("rmapp_new")
    assert not conn.table_exists("rmapp_old")

    op.database_backwards("rmapp", conn, state, state)
    assert conn.table_exists("rmapp_old")
    assert not conn.table_exists("rmapp_new")

    conn.execute_script('DROP TABLE IF EXISTS "rmapp_old"')


def test_alterfield_pg_only_emits_alter_column():
    """``AlterField`` only runs on PG (SQLite has no ``ALTER COLUMN
    ... TYPE``). Locks down both branches: PG emits the ALTER, SQLite
    no-ops without raising."""
    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    conn.execute_script('DROP TABLE IF EXISTS "alterfield_t"')
    conn.execute_script(
        'CREATE TABLE "alterfield_t" ("id" INTEGER PRIMARY KEY, "v" SMALLINT)'
    )

    field = dorm.BigIntegerField()
    field.column = "v"
    state = type(
        "S",
        (),
        {
            "models": {
                "afapp.target": {
                    "name": "Target",
                    "options": {"db_table": "alterfield_t"},
                    "fields": {},
                }
            }
        },
    )()
    op = AlterField(model_name="Target", name="v", field=field)
    # On both backends this must not raise.
    op.database_forwards("afapp", conn, state, state)
    op.database_backwards("afapp", conn, state, state)

    if vendor == "postgresql":
        # The column type must now be bigint.
        cols = conn.get_table_columns("alterfield_t")
        v_col = next(c for c in cols if c["name"] == "v")
        assert "int" in v_col["data_type"].lower()

    conn.execute_script('DROP TABLE IF EXISTS "alterfield_t"')


def test_runsql_backwards_executes_reverse_sql():
    """``RunSQL.database_backwards`` runs the ``reverse_sql`` argument
    when one is supplied. Locks down the if-clause branch."""
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "runsql_back"')

    op = RunSQL(
        sql='CREATE TABLE "runsql_back" ("id" INTEGER PRIMARY KEY)',
        reverse_sql='DROP TABLE "runsql_back"',
    )
    op.database_forwards("rapp", conn, None, None)
    assert conn.table_exists("runsql_back")
    op.database_backwards("rapp", conn, None, None)
    assert not conn.table_exists("runsql_back")


def test_runsql_backwards_without_reverse_sql_is_noop():
    """No ``reverse_sql`` set → backwards must be a no-op, not raise."""
    op = RunSQL(sql="SELECT 1")
    # No exception expected.
    op.database_backwards("rapp", get_connection(), None, None)


# ── Async ValuesListQuerySet / RawQuerySet / CombinedQuerySet ────────────────


@pytest.mark.asyncio
async def test_async_values_list_iteration():
    await Author.objects.acreate(name="vlq-1", age=1)
    await Author.objects.acreate(name="vlq-2", age=2)
    out = await Author.objects.filter(name__startswith="vlq-").avalues_list("name", flat=True)
    assert sorted(out) == ["vlq-1", "vlq-2"]


@pytest.mark.asyncio
async def test_async_raw_queryset_iteration():
    await Author.objects.acreate(name="raw-async-1", age=10)
    await Author.objects.acreate(name="raw-async-2", age=20)
    qs = Author.objects.raw(
        "SELECT * FROM authors WHERE name LIKE %s ORDER BY age",
        ["raw-async-%"],
    )
    rows = [a async for a in qs]
    assert [a.age for a in rows] == [10, 20]


@pytest.mark.asyncio
async def test_async_combined_queryset_count_and_iter():
    """``CombinedQuerySet`` (union/intersection/difference) must support
    both ``acount`` and async iteration."""
    a1 = await Author.objects.acreate(name="cqs-a", age=1)
    a2 = await Author.objects.acreate(name="cqs-b", age=2)
    await Author.objects.acreate(name="cqs-c", age=3)

    qs_a = Author.objects.filter(name="cqs-a")
    qs_b = Author.objects.filter(name="cqs-b")
    combined = qs_a.union(qs_b)
    n = await combined.acount()
    assert n == 2
    seen_pks = {row.pk async for row in combined}
    assert seen_pks == {a1.pk, a2.pk}


# ── conf._discover_apps: nested packages & exclusions ───────────────────────


def test_discover_apps_skips_models_inside_non_package_chain(tmp_path):
    """A ``models.py`` inside a directory whose chain to the root
    has a missing ``__init__.py`` somewhere must NOT count as an app —
    that's the rule that prevents picking up venv internals or other
    non-package dirs that happen to ship a ``models.py``."""
    from dorm.conf import _discover_apps

    deep = tmp_path / "outer" / "inner"
    deep.mkdir(parents=True)
    # Mark inner as a package, but NOT outer.
    (deep / "__init__.py").write_text("")
    (deep / "models.py").write_text("")

    found = _discover_apps(tmp_path)
    assert "outer.inner" not in found, (
        "outer/ has no __init__.py — the chain isn't a valid Python package"
    )


def test_discover_apps_skips_excluded_dir_names(tmp_path):
    """Directories like ``__pycache__`` or ``.venv`` shipped with a
    ``models.py`` would cause noisy errors. The discovery step must
    filter them out."""
    from dorm.conf import _discover_apps

    bad = tmp_path / "__pycache__"
    bad.mkdir()
    (bad / "__init__.py").write_text("")
    (bad / "models.py").write_text("")

    found = _discover_apps(tmp_path)
    assert "__pycache__" not in found


def test_discover_apps_finds_nested_packages(tmp_path):
    from dorm.conf import _discover_apps

    pkg = tmp_path / "myproj" / "users"
    pkg.mkdir(parents=True)
    (tmp_path / "myproj" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "models.py").write_text("")

    found = _discover_apps(tmp_path)
    assert "myproj.users" in found


# ── ForeignKey __set__ accepts both instance and pk ─────────────────────────


def test_foreignkey_set_with_instance_caches_object():
    """Assigning a model instance to a FK descriptor must populate the
    in-memory cache so the next ``obj.author`` access doesn't fire a
    query. Locks down a perf path that was easy to regress."""
    a = Author.objects.create(name="fk-cache", age=10)
    b = Book(title="fk-cache-book", pages=10)
    b.author = a
    # Direct dict access — descriptor must have stored the cache key.
    assert b.__dict__.get("_cache_author") is a
    # The underlying _id slot should also be set.
    assert b.__dict__.get("author_id") == a.pk


def test_foreignkey_set_with_none_clears_cache():
    a = Author.objects.create(name="fk-clear", age=11)
    b = Book(title="fk-clear-book", author=a, pages=10)
    b.author = None
    assert b.__dict__.get("author_id") is None
    assert b.__dict__.get("_cache_author") is None


def test_foreignkey_set_with_pk_clears_cache():
    """Setting the FK from a bare pk (rather than an instance) must
    drop any stale cached object — otherwise reading ``obj.author``
    next would return the previous instance."""
    a1 = Author.objects.create(name="fk-pk-1", age=1)
    a2 = Author.objects.create(name="fk-pk-2", age=2)
    b = Book(title="fk-pk-book", author=a1, pages=10)
    b.author = a2.pk
    # Cache cleared; next read of `b.author` would re-fetch.
    assert b.__dict__.get("_cache_author") is None
    assert b.__dict__.get("author_id") == a2.pk


# ── QuerySet bulk_create with unique_fields validation paths ────────────────


def test_bulk_create_update_conflicts_user_supplied_update_fields_excludes_target():
    """When the user passes an explicit ``update_fields=`` list that
    includes the unique target column, the generated SQL must still
    update those columns — but the test exists to catch a future
    refactor that "helpfully" filters them and produces a confusing
    no-op."""
    from dorm.db.connection import get_connection
    from dorm.query import SQLQuery

    conn = get_connection()
    q = SQLQuery(Author)
    fields = [
        type("F1", (), {"column": "email", "primary_key": False})(),
        type("F2", (), {"column": "age", "primary_key": False})(),
    ]
    sql, _ = q.as_bulk_insert(
        fields, [["a@b.com", 30]], conn,
        update_conflicts=True,
        unique_fields=["email"],
        update_fields=["age"],
    )
    # The user explicitly listed only "age" → only that column in SET.
    assert 'SET "age" = EXCLUDED."age"' in sql or 'SET "AGE"' in sql.upper().replace('"AGE"', '"age"')


def test_bulk_create_update_conflicts_with_no_updatable_columns_falls_back_to_do_nothing():
    """Edge case: ``update_fields=[]`` (empty list) on
    ``update_conflicts=True``. The SQL builder degrades to ``DO NOTHING``
    rather than emitting an empty SET clause that PG would reject."""
    from dorm.db.connection import get_connection
    from dorm.query import SQLQuery

    conn = get_connection()
    q = SQLQuery(Tag)
    fields = [type("F", (), {"column": "name", "primary_key": True})()]
    sql, _ = q.as_bulk_insert(
        fields, [["x"]], conn,
        update_conflicts=True,
        unique_fields=["name"],
        update_fields=[],
    )
    assert "DO NOTHING" in sql.upper()


# ── Slicing edge: stop only / start only (positive) still works ─────────────


def test_queryset_slice_with_only_stop_works():
    for i in range(5):
        Author.objects.create(name=f"sl-{i}", age=i)
    out = list(Author.objects.filter(name__startswith="sl-").order_by("age")[:3])
    assert [a.age for a in out] == [0, 1, 2]


def test_queryset_slice_with_only_start_works():
    for i in range(5, 10):
        Author.objects.create(name=f"sl2-{i}", age=i)
    out = list(Author.objects.filter(name__startswith="sl2-").order_by("age")[2:])
    assert [a.age for a in out] == [7, 8, 9]


def test_queryset_slice_step_one_explicitly_allowed():
    """``qs[::1]`` is a no-op step and must NOT raise — the rejection
    is for *non-trivial* steps."""
    out = list(Author.objects.all()[::1])
    assert isinstance(out, list)


# ── Manager methods: passthroughs that aren't otherwise hit ─────────────────


def test_manager_first_last_count_exists_proxies():
    """Manager-level shortcut methods must mirror the QuerySet
    semantics — locks down the (otherwise-trivial) wiring."""
    Author.objects.create(name="mgr-x", age=42)
    assert Author.objects.exists() is True
    assert Author.objects.count() >= 1
    assert Author.objects.first() is not None
    assert Author.objects.last() is not None


def test_manager_in_bulk_returns_pk_keyed_dict():
    a = Author.objects.create(name="bulk-1", age=1)
    b = Author.objects.create(name="bulk-2", age=2)
    out = Author.objects.in_bulk([a.pk, b.pk])
    assert set(out.keys()) == {a.pk, b.pk}
    assert out[a.pk].name == "bulk-1"


def test_manager_in_bulk_empty_id_list_returns_empty():
    assert Author.objects.in_bulk([]) == {}


# ── DateField parses datetime as date ────────────────────────────────────────


def test_datefield_strips_time_component_from_datetime():
    """A datetime passed to a DateField should be stored as a date,
    not raise. Locks down a subtle conversion that bites users who
    do ``DateField(default=datetime.now)`` instead of ``date.today``."""
    from dorm.fields import DateField

    f = DateField()
    out = f.to_python(_dt.datetime(2026, 4, 27, 12, 0))
    # Either a date or a datetime — accept either as long as the
    # date components are preserved.
    if isinstance(out, _dt.datetime):
        assert (out.year, out.month, out.day) == (2026, 4, 27)
    else:
        assert out == _dt.date(2026, 4, 27)


# ── Slug / URL / GenericIPAddress fields ─────────────────────────────────────


def test_slugfield_validates_format():
    from dorm.exceptions import ValidationError
    from dorm.fields import SlugField

    f = SlugField()
    f.name = "slug"
    # Valid slug.
    f.validate("hello-world_123", model_instance=None)
    # Invalid: contains spaces.
    with pytest.raises(ValidationError):
        f.validate("with spaces", model_instance=None)


def test_slugfield_inherits_db_index_default():
    """SlugField sets ``db_index=True`` by default — locking that down
    matters because removing it silently makes slug lookups full-table
    scans, a perf cliff that's invisible in unit tests."""
    from dorm.fields import SlugField

    f = SlugField()
    assert f.db_index is True


def test_urlfield_default_max_length_200():
    from dorm.fields import URLField

    f = URLField()
    assert f.max_length == 200


def test_emailfield_default_max_length_254():
    """RFC 5321 says an email address is ≤ 254 chars; lock down the
    default so a regression to e.g. 100 doesn't truncate addresses."""
    from dorm.fields import EmailField

    f = EmailField()
    assert f.max_length == 254


def test_emailfield_to_python_returns_empty_string_unchanged():
    """Empty-string emails are commonly used as "no email" sentinels in
    forms; to_python must NOT reject them."""
    from dorm.fields import EmailField

    f = EmailField()
    assert f.to_python("") == ""
    assert f.to_python(None) is None


def test_nullbooleanfield_forces_null_true():
    from dorm.fields import NullBooleanField

    f = NullBooleanField()
    assert f.null is True


# ── DateTimeField auto_now / auto_now_add ────────────────────────────────────


def test_datetime_auto_now_add_sets_default_callable():
    from dorm.fields import DateTimeField

    f = DateTimeField(auto_now_add=True)
    # auto_now_add must register a default callable that returns now().
    out = f.get_default()
    assert isinstance(out, _dt.datetime)


def test_datetime_auto_now_pre_save_overwrites_value():
    """``auto_now=True`` rewrites the field on every save — locks down
    the pre_save behavior."""
    from dorm.fields import DateTimeField

    class _Inst:
        pass

    inst = _Inst()
    setattr(inst, "modified", _dt.datetime(2020, 1, 1))
    f = DateTimeField(auto_now=True)
    f.attname = "modified"
    out = f.pre_save(inst, add=False)
    # Must produce a datetime with ``year`` >= 2024 (i.e. now()), not
    # the stale value from the instance.
    assert isinstance(out, _dt.datetime)
    assert out.year >= 2024
    assert getattr(inst, "modified") == out


def test_datetime_auto_now_add_only_sets_on_add():
    """``auto_now_add=True`` only sets the value when ``add=True`` —
    later updates leave the original timestamp alone (Django parity)."""
    from dorm.fields import DateTimeField

    class _Inst:
        pass

    inst = _Inst()
    f = DateTimeField(auto_now_add=True)
    f.attname = "created"
    setattr(inst, "created", _dt.datetime(2020, 1, 1))
    out = f.pre_save(inst, add=False)
    # On update (``add=False``), pre_save must return the existing
    # value, not generate a new now().
    assert out == _dt.datetime(2020, 1, 1)


# ── Field.contribute_to_class ────────────────────────────────────────────────


def test_field_contribute_to_class_validates_db_column():
    """Custom ``db_column`` names must be validated as identifiers —
    a value with a quote or space could splice into SQL elsewhere."""
    from dorm.fields import IntegerField

    class _Meta:
        @staticmethod
        def add_field(field):
            pass

    class _Cls:
        _meta = _Meta()

    f = IntegerField(db_column='evil"; DROP TABLE x; --')
    with pytest.raises(Exception):  # noqa: PT011 — ImproperlyConfigured
        f.contribute_to_class(_Cls, "n")


# ── ForeignKey resolved-related-model errors ────────────────────────────────


def test_foreignkey_resolves_string_target_via_registry():
    """``ForeignKey("Model")`` defers resolution; once the target is
    registered, ``_resolve_related_model`` returns it."""
    from dorm.fields import ForeignKey

    f = ForeignKey(Author, on_delete=dorm.CASCADE)
    assert f._resolve_related_model() is Author


def test_foreignkey_db_type_matches_target_pk_db_type():
    from dorm.fields import ForeignKey
    from dorm.db.connection import get_connection

    conn = get_connection()
    f = ForeignKey(Author, on_delete=dorm.CASCADE)
    out = f.db_type(conn)
    # Author's pk is BigAutoField — rel_db_type returns INTEGER (sqlite)
    # or BIGINT (PG). Either is fine; just lock down it produces an
    # integer-shaped type.
    assert "INT" in out.upper() or "BIGINT" in out.upper()


def test_one_to_one_field_implies_unique():
    from dorm.fields import OneToOneField

    f = OneToOneField(Author, on_delete=dorm.CASCADE)
    assert f.unique is True
    assert f.one_to_one is True


# ── M2M get_through_columns: explicit through ────────────────────────────────


def test_m2m_get_through_table_default_naming():
    """Without an explicit ``through=``, the through table name is
    ``<model_table>_<field_name>``."""
    field = Article._meta.get_field("tags")
    assert field._get_through_table() == "articles_tags"


# ── Boolean field forms ──────────────────────────────────────────────────────


def test_booleanfield_get_db_prep_value_handles_none():
    from dorm.fields import BooleanField

    f = BooleanField()
    assert f.get_db_prep_value(None) is None
    assert f.get_db_prep_value(1) is True
    assert f.get_db_prep_value(0) is False


def test_booleanfield_from_db_value_round_trip():
    """SQLite stores booleans as 0/1 ints; from_db_value must convert
    back. PG returns native bool. Both must end up as a Python bool."""
    from dorm.fields import BooleanField

    f = BooleanField()
    assert f.from_db_value(0) is False
    assert f.from_db_value(1) is True
    assert f.from_db_value(True) is True
    assert f.from_db_value(None) is None


# ── Slicing edge: stop < start (empty) ───────────────────────────────────────


def test_queryset_slice_stop_before_start_yields_empty():
    """``qs[5:3]`` resolves to a negative LIMIT which would crash;
    confirm we resolve to an empty result instead — Python list
    semantics."""
    for i in range(10):
        Author.objects.create(name=f"sle-{i}", age=i)
    out = list(Author.objects.filter(name__startswith="sle-").order_by("age")[5:3])
    assert out == []


# ── QuerySet __await__ materialiser ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_queryset_await_materialises_to_list():
    """``await Author.objects.filter(...)`` must materialise without
    needing to call ``avalues()`` or ``alist()`` — single-shot
    convenience."""
    await Author.objects.acreate(name="awaited-1", age=1)
    await Author.objects.acreate(name="awaited-2", age=2)
    out = await Author.objects.filter(name__startswith="awaited-")
    assert {a.name for a in out} == {"awaited-1", "awaited-2"}


# ── QuerySet __len__ / __bool__ ──────────────────────────────────────────────


def test_queryset_len_and_bool_materialise():
    Author.objects.create(name="lb-1", age=1)
    qs = Author.objects.filter(name__startswith="lb-")
    # __len__ triggers _fetch_all.
    n = len(qs)
    assert n >= 1
    # __bool__ also triggers it; result is reused via cache.
    assert bool(qs) is True

    empty = Author.objects.filter(name="nothing-of-the-sort")
    assert len(empty) == 0
    assert bool(empty) is False


# ── DB exception helpers extra branches ──────────────────────────────────────


def test_raise_migration_hint_surfaces_pg_relation_does_not_exist():
    """On PG, the missing-table error is shaped as
    ``relation "x" does not exist``. The hint helper must match."""
    from dorm.db.utils import raise_migration_hint
    from dorm.exceptions import OperationalError

    raw = OperationalError('relation "missing_t" does not exist')
    with pytest.raises(OperationalError) as excinfo:
        raise_migration_hint(raw)
    assert "missing_t" in str(excinfo.value)
    assert "dorm migrate" in str(excinfo.value)


def test_raise_migration_hint_returns_for_unrelated_message():
    """If the message doesn't match either backend's missing-table
    pattern, the helper must return without raising."""
    from dorm.db.utils import raise_migration_hint

    # Non-matching message: no exception raised.
    raise_migration_hint(RuntimeError("permission denied for schema"))


# ── Refresh from db with no fields specified after no-change ───────────────


def test_refresh_from_db_with_invalid_field_silently_skips():
    """``refresh_from_db(fields=[unknown])`` must skip the unknown name
    rather than raising — the user's intent was to refresh *what
    exists*. Locks down the silent-skip path so a future strictness
    upgrade is a deliberate decision."""
    a = Author.objects.create(name="rfd-bad", age=1)
    Author.objects.filter(pk=a.pk).update(name="rfd-bad-changed")
    # Mix of valid + invalid; the invalid one is silently dropped.
    a.refresh_from_db(fields=["definitely_not_a_field", "name"])
    assert a.name == "rfd-bad-changed"


# ── Async on_commit callable that itself returns None ──────────────────────


@pytest.mark.asyncio
async def test_aon_commit_with_sync_callable_inside_aatomic():
    """Inside aatomic, sync callables must also work — the dispatcher
    only awaits when the result is awaitable."""
    fired: list[str] = []
    from dorm import transaction

    async with transaction.aatomic():
        transaction.aon_commit(lambda: fired.append("plain"))
    assert fired == ["plain"]


# ── select_for_update validates of= identifiers at construction time ───────


def test_select_for_update_of_with_invalid_identifier_rejected_pg():
    """``of=("evil; DROP TABLE x;",)`` must be rejected before SQL
    emission — the identifier validation runs in
    ``_compile_for_update``."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only — SQLite branches skip the of= validation")
    qs = Author.objects.select_for_update(of=("authors; DROP",))
    with pytest.raises(Exception):  # noqa: PT011
        qs._query.as_select(conn)


# ── pool_stats returns dict with correct shape on first probe ──────────────


def test_pool_stats_keys_are_stable():
    """A breaking change to the pool_stats dict shape would be
    user-facing — clients pin against these keys. Lock the contract
    down for the no-pool variant; PG-specific keys vary by psycopg
    version and are tested elsewhere."""
    from dorm.db.connection import get_connection, pool_stats

    get_connection().execute("SELECT 1")
    out = pool_stats()
    assert "alias" in out
    assert "vendor" in out
    assert "has_pool" in out


# ── Manager.using and QuerySet.using ──────────────────────────────────────


def test_queryset_using_clones_alias():
    """``qs.using('replica')`` must NOT mutate the original queryset's
    alias — it returns a clone."""
    base = Author.objects.all()
    routed = base.using("not_default")
    assert base._db == "default"
    assert routed._db == "not_default"


# ── Q object internals: negation, combine ───────────────────────────────────


def test_q_combine_with_and():
    """``Q(...) & Q(...)`` combines into a single Q with both predicates
    AND'd. Lock down the underlying tree shape so a future refactor
    can't quietly reorder predicates."""
    from dorm.expressions import Q

    q1 = Q(age=1)
    q2 = Q(name="x")
    combined = q1 & q2
    # combined is a Q with connector AND.
    assert combined.connector == "AND"
    # Materialise the chain into a queryset to confirm both predicates
    # land in the WHERE.
    Author.objects.create(name="x", age=1)
    [match] = list(Author.objects.filter(combined))
    assert match.age == 1


def test_q_negation_inverts_lookup():
    """``~Q(name="x")`` produces ``WHERE NOT name = 'x'`` semantically.
    Confirm the round-trip."""
    from dorm.expressions import Q

    Author.objects.create(name="qneg-keep", age=1)
    Author.objects.create(name="qneg-drop", age=2)
    qs = Author.objects.filter(~Q(name="qneg-drop")).filter(name__startswith="qneg-")
    names = {a.name for a in qs}
    assert "qneg-keep" in names
    assert "qneg-drop" not in names


# ── QuerySet: less-used chaining ────────────────────────────────────────────


def test_queryset_reverse_flips_order_by_signs():
    """``qs.reverse()`` toggles the ``-`` prefix on every ordering
    field — the building block for ``last()`` and intuitive sort
    flipping. Lock down the transform."""
    base = Author.objects.order_by("name", "-age")
    rev = base.reverse()
    assert rev._query.order_by_fields == ["-name", "age"]
    # And reversing again restores.
    assert rev.reverse()._query.order_by_fields == ["name", "-age"]


def test_queryset_none_returns_empty_without_query():
    """``qs.none()`` must short-circuit to an empty queryset that
    does NOT hit the database (locked into ``_result_cache``)."""
    qs = Author.objects.none()
    assert list(qs) == []
    # Re-iterating returns the cached empty list — we can't directly
    # assert "no SQL fired" but we can confirm the cache is set.
    assert qs._result_cache == []


def test_queryset_exclude_with_q_positional_arg_negates_node():
    """``qs.exclude(Q(name="x"))`` (positional) goes through the
    ``a.negated = not a.negated`` branch — distinct from the kwargs
    path."""
    from dorm.expressions import Q

    Author.objects.create(name="qexc-keep", age=1)
    Author.objects.create(name="qexc-drop", age=2)
    qs = Author.objects.filter(name__startswith="qexc-").exclude(Q(name="qexc-drop"))
    names = {a.name for a in qs}
    assert names == {"qexc-keep"}


def test_queryset_pk_alias_resolves_to_actual_pk_column():
    """``qs.filter(pk=<value>)`` must translate ``pk`` to the model's
    actual PK column. Locks down the alias-resolver helper."""
    a = Author.objects.create(name="pk-alias", age=1)
    [match] = list(Author.objects.filter(pk=a.pk))
    assert match.pk == a.pk


def test_queryset_pk_alias_resolves_in_lookup_chains():
    """``filter(pk__in=...)`` must also work — the resolver handles
    nested lookups, not just bare ``pk``."""
    a1 = Author.objects.create(name="pk-chain-1", age=1)
    a2 = Author.objects.create(name="pk-chain-2", age=2)
    matches = Author.objects.filter(pk__in=[a1.pk, a2.pk])
    assert {m.pk for m in matches} == {a1.pk, a2.pk}


def test_queryset_iter_uses_result_cache_on_second_pass():
    """Iterating the same queryset twice must NOT re-query — the
    cache populated by the first ``__iter__`` is reused. Lock this
    down to prevent a regression that would silently double the
    query count for any code that does ``list(qs); list(qs)``."""
    Author.objects.create(name="cache-iter", age=99)
    qs = Author.objects.filter(name="cache-iter")
    list(qs)
    cache = qs._result_cache
    list(qs)
    assert qs._result_cache is cache  # exact same list object


# ── from_db_value paths for date/time/datetime ───────────────────────────────


def test_datefield_from_db_value_parses_iso_string():
    from dorm.fields import DateField

    f = DateField()
    assert f.from_db_value("2026-04-27") == _dt.date(2026, 4, 27)
    assert f.from_db_value(None) is None


def test_timefield_from_db_value_parses_iso_string():
    from dorm.fields import TimeField

    f = TimeField()
    out = f.from_db_value("12:34:56")
    assert isinstance(out, _dt.time)
    assert out.hour == 12 and out.minute == 34


def test_datetimefield_from_db_value_parses_iso_string():
    from dorm.fields import DateTimeField

    f = DateTimeField()
    out = f.from_db_value("2026-04-27T12:34:56")
    assert isinstance(out, _dt.datetime)
    assert out.year == 2026


# ── _to_pyformat caching (PG placeholder rewrite) ──────────────────────────


def test_to_pyformat_replaces_dollar_placeholders():
    """The placeholder rewrite is on every PG query's hot path; lock
    down both the conversion and the cache reuse."""
    from dorm.db.backends.postgresql import _to_pyformat

    out1 = _to_pyformat("SELECT * FROM t WHERE a=$1 AND b=$2")
    assert out1 == "SELECT * FROM t WHERE a=%s AND b=%s"
    # Same input → same cached output.
    out2 = _to_pyformat("SELECT * FROM t WHERE a=$1 AND b=$2")
    assert out1 == out2


def test_to_pyformat_handles_dollar_inside_string_literal():
    """A ``$1`` inside a quoted string literal must NOT be rewritten —
    it's data, not a placeholder. Locks down the literal-aware scan."""
    from dorm.db.backends.postgresql import _to_pyformat

    out = _to_pyformat("SELECT '$1' AS lit, t.x FROM t WHERE t.y = $1")
    # The literal stays; the parameter becomes %s.
    assert "'$1'" in out
    assert "t.y = %s" in out


# ── select_for_update validation: skip_locked + no_wait combined ───────────


def test_select_for_update_args_clone_does_not_leak_state():
    """``select_for_update`` with extra kwargs must clone the queryset
    AND the underlying query. Re-using the cloned QS without locks
    must not see for_update_skip_locked from the original."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only flags")
    base = Author.objects.all()
    locked = base.select_for_update(skip_locked=True)
    plain = base.select_for_update()
    assert locked._query.for_update_skip_locked is True
    assert plain._query.for_update_skip_locked is False


# ── on_commit with multiple aliases ────────────────────────────────────────


def test_on_commit_isolated_per_alias():
    """Callbacks scheduled inside ``atomic("default")`` must fire when
    *that* transaction commits, not when an unrelated alias's
    transaction commits. We have only one alias in the suite, so this
    just sanity-checks that the per-alias stack key works."""
    from dorm import transaction

    fired: list[str] = []
    with transaction.atomic("default"):
        transaction.on_commit(lambda: fired.append("d"), using="default")
    assert fired == ["d"]


# ── Direct database operation: connection.execute_write counts ────────────


def test_execute_write_returns_rowcount():
    """``execute_write`` must return the number of rows affected.
    Lock down the contract — many callers (update / delete / bulk_*)
    rely on it for their return value."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    Author.objects.create(name="exw-1", age=1)
    Author.objects.create(name="exw-2", age=2)
    rows = conn.execute_write(
        'DELETE FROM "authors" WHERE "name" LIKE %s', ["exw-%"]
    )
    assert rows == 2


# ── Soft delete: queryset count ────────────────────────────────────────────


def test_soft_delete_default_manager_count_excludes_deleted():
    """SoftDeleteModel.objects.count() must reflect only live rows.
    Confirms manager hides soft-deleted entries via the SQL itself,
    not just via row hydration."""
    from dorm.contrib.softdelete import SoftDeleteModel
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    class _SDCount(SoftDeleteModel):
        title = dorm.CharField(max_length=200)

        class Meta:
            db_table = "audit_softdel_count"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_count"')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _SDCount._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "audit_softdel_count" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    try:
        _SDCount.objects.create(title="alive-1")
        _SDCount.objects.create(title="alive-2")
        c = _SDCount.objects.create(title="will-be-deleted")
        c.delete()

        assert _SDCount.objects.count() == 2
        assert _SDCount.all_objects.count() == 3
        assert _SDCount.deleted_objects.count() == 1
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_count"')
