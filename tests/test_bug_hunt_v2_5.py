"""Regression tests for the v2.5 bug-hunt fixes.

Each section pins down one specific bug found during the audit so a
future refactor can't reintroduce the original problem. Bug numbers
match the audit report.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

import dorm
from dorm import Prefetch, Q
from tests.models import Article, Author, Book, Publisher, Tag


# ────────────────────────────────────────────────────────────────────────────
# Bug 1 — count / exists / update / delete must emit FK-traversal JOINs
# ────────────────────────────────────────────────────────────────────────────


def test_count_with_fk_traversal() -> None:
    pub = Publisher.objects.create(name="P1")
    a1 = Author.objects.create(name="A1", age=30, publisher=pub)
    Book.objects.create(title="B1", author=a1, pages=100)
    Book.objects.create(title="B2", author=a1, pages=120)

    a2 = Author.objects.create(name="A2", age=40)
    Book.objects.create(title="B3", author=a2, pages=80)

    n = Book.objects.filter(author__name="A1").count()
    assert n == 2


def test_exists_with_fk_traversal() -> None:
    a1 = Author.objects.create(name="ExA", age=30)
    Book.objects.create(title="ExB", author=a1, pages=10)

    assert Book.objects.filter(author__name="ExA").exists() is True
    assert Book.objects.filter(author__name="Nope").exists() is False


def test_update_with_fk_traversal() -> None:
    a1 = Author.objects.create(name="UpA", age=30)
    a2 = Author.objects.create(name="UpB", age=40)
    Book.objects.create(title="K1", author=a1, pages=10)
    Book.objects.create(title="K2", author=a2, pages=10)

    n = Book.objects.filter(author__name="UpA").update(pages=999)
    assert n == 1
    assert Book.objects.get(title="K1").pages == 999
    assert Book.objects.get(title="K2").pages == 10


def test_delete_with_fk_traversal() -> None:
    a1 = Author.objects.create(name="DelA", age=30)
    a2 = Author.objects.create(name="DelB", age=40)
    Book.objects.create(title="D1", author=a1, pages=1)
    Book.objects.create(title="D2", author=a2, pages=1)

    n, _ = Book.objects.filter(author__name="DelA").delete()
    assert n == 1
    assert Book.objects.filter(title="D1").exists() is False
    assert Book.objects.filter(title="D2").exists() is True


# ────────────────────────────────────────────────────────────────────────────
# Bug 2 — sliced delete() must honour LIMIT (no dataloss)
# ────────────────────────────────────────────────────────────────────────────


def test_sliced_delete_only_removes_slice() -> None:
    """``qs[:N].delete()`` previously dropped the LIMIT and wiped the
    full WHERE-matching set. Verify only N rows go."""
    a = Author.objects.create(name="Slicer", age=33)
    for i in range(10):
        Book.objects.create(title=f"slice-{i}", author=a, pages=i)

    qs = Book.objects.filter(author=a).order_by("pages")
    n, _ = qs[:3].delete()
    assert n == 3
    remaining = Book.objects.filter(author=a).count()
    assert remaining == 7


@pytest.mark.asyncio
async def test_async_sliced_delete_only_removes_slice() -> None:
    a = await Author.objects.acreate(name="ASlicer", age=33)
    for i in range(6):
        await Book.objects.acreate(title=f"aslice-{i}", author=a, pages=i)

    qs = Book.objects.filter(author=a).order_by("pages")
    n, _ = await qs[:2].adelete()
    assert n == 2
    remaining = await Book.objects.filter(author=a).acount()
    assert remaining == 4


# ────────────────────────────────────────────────────────────────────────────
# Bug 3 — nullable FK joins must be LEFT OUTER for __isnull semantics
# ────────────────────────────────────────────────────────────────────────────


def test_nullable_fk_traversal_isnull_returns_null_rows() -> None:
    """``Author.publisher`` is null=True. INNER JOIN excluded the null
    rows; LEFT OUTER includes them so the user sees what they asked for."""
    pub = Publisher.objects.create(name="HasPub")
    Author.objects.create(name="WithPub", age=30, publisher=pub)
    Author.objects.create(name="NullPub1", age=31)  # publisher None
    Author.objects.create(name="NullPub2", age=32)

    rows = list(Author.objects.filter(publisher__name__isnull=True))
    names = sorted(a.name for a in rows)
    assert names == ["NullPub1", "NullPub2"]


def test_nullable_fk_join_type_is_left_outer() -> None:
    qs = Author.objects.filter(publisher__name="X")
    qs._query._compile_nodes(qs._query.where_nodes, qs._get_connection())
    assert any(j[0] == "LEFT OUTER" for j in qs._query.joins)


def test_non_null_fk_join_type_is_inner() -> None:
    qs = Book.objects.filter(author__name="X")
    qs._query._compile_nodes(qs._query.where_nodes, qs._get_connection())
    assert any(j[0] == "INNER" for j in qs._query.joins)


# ────────────────────────────────────────────────────────────────────────────
# Bug 4 — AlterField SQLite rebuild keeps Meta.constraints + indexes
# ────────────────────────────────────────────────────────────────────────────


def test_alterfield_sqlite_rebuild_preserves_constraints() -> None:
    """When AlterField rebuilds a SQLite table, Meta.constraints must
    be re-emitted on the new table; previously only indexes were."""
    from dorm import CheckConstraint, IntegerField
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import AlterField, CreateModel
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("Constraint preservation test targets SQLite rebuild path.")

    conn.execute_script('DROP TABLE IF EXISTS "buggy_t4"')

    state = ProjectState()
    create = CreateModel(
        name="BuggyT4",
        fields=[
            ("id", dorm.BigAutoField(primary_key=True)),
            ("qty", IntegerField()),
        ],
        options={
            "db_table": "buggy_t4",
            "constraints": [
                CheckConstraint(check=Q(qty__gte=0), name="qty_nonneg"),
            ],
        },
    )
    create.state_forwards("bug4", state)
    create.database_forwards("bug4", conn, ProjectState(), state)

    # Sanity — original constraint enforces qty>=0
    with pytest.raises(Exception):
        conn.execute_write(
            'INSERT INTO "buggy_t4" ("qty") VALUES (?)', [-5]
        )
    conn.execute_write('INSERT INTO "buggy_t4" ("qty") VALUES (?)', [10])

    # Now AlterField the qty column → triggers rebuild path
    new_state = state.clone()
    new_field = IntegerField(null=True)
    new_field.name = 'qty'
    new_field.column = 'qty'
    alter = AlterField(model_name="BuggyT4", name="qty", field=new_field)
    alter.state_forwards("bug4", new_state)
    alter.database_forwards("bug4", conn, state, new_state)

    # Constraint must STILL forbid negative values after the rebuild —
    # if it was lost the next INSERT would silently succeed.
    with pytest.raises(Exception):
        conn.execute_write(
            'INSERT INTO "buggy_t4" ("qty") VALUES (?)', [-1]
        )

    conn.execute_script('DROP TABLE IF EXISTS "buggy_t4"')


# ────────────────────────────────────────────────────────────────────────────
# Bug 5 — DDL DEFAULT must escape single quotes
# ────────────────────────────────────────────────────────────────────────────


def test_ddl_default_escapes_single_quote() -> None:
    """``default="O'Brien"`` previously emitted ``DEFAULT 'O'Brien'``
    which is broken SQL and an injection vector."""
    from dorm.db.connection import get_connection
    from dorm.fields import CharField
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    f = CharField(max_length=20, default="O'Brien")
    f.name = "name"
    f.column = "name"
    sql = _field_to_column_sql("name", f, conn)
    assert sql is not None
    # Standard SQL escape is ``'`` → ``''``. The literal 'O'Brien'
    # must NOT appear naked in the output.
    assert "'O''Brien'" in sql
    assert "'O'Brien'" not in sql.replace("'O''Brien'", "")


def test_ddl_default_escapes_single_quote_executes() -> None:
    """The escaped DDL must actually execute and round-trip the
    default through SQLite (no syntax error)."""
    from dorm.db.connection import get_connection
    from dorm.fields import CharField, IntegerField
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("SQLite-specific DDL execution check.")

    conn.execute_script('DROP TABLE IF EXISTS "ddl_default_test"')

    f_name = CharField(max_length=20, default="O'Brien")
    f_name.name = 'name'
    f_name.column = 'name'
    f_qty = IntegerField(default=1)
    f_qty.name = 'qty'
    f_qty.column = 'qty'

    cols = [
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT',
        _field_to_column_sql("name", f_name, conn),
        _field_to_column_sql("qty", f_qty, conn),
    ]
    conn.execute_script(
        f'CREATE TABLE "ddl_default_test" ({", ".join(c for c in cols if c)})'
    )
    # Insert with NO explicit ``name`` → default should kick in.
    conn.execute_write(
        'INSERT INTO "ddl_default_test" ("qty") VALUES (?)', [42]
    )
    rows = conn.execute('SELECT "name" FROM "ddl_default_test"', [])
    assert rows[0][0] == "O'Brien"

    conn.execute_script('DROP TABLE IF EXISTS "ddl_default_test"')


# ────────────────────────────────────────────────────────────────────────────
# Bug 6 — async iterator must hydrate annotations
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_iterator_hydrates_annotations() -> None:
    """Async iterator must populate ``instance.<alias>`` for every
    queryset annotation, the same way the sync iterator does."""
    await Author.objects.acreate(name="AnnA", age=30, email=None)
    await Author.objects.acreate(name="AnnB", age=20, email="b@x.com")

    qs = Author.objects.filter(name__in=["AnnA", "AnnB"]).annotate(
        contact=dorm.Coalesce(dorm.F("email"), dorm.Value("no-email"))
    )
    pairs = {a.name: getattr(a, "contact") async for a in qs}
    assert pairs == {"AnnA": "no-email", "AnnB": "b@x.com"}


# ────────────────────────────────────────────────────────────────────────────
# Bug 7 — async Prefetch(queryset=…) must work
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_prefetch_with_queryset_object() -> None:
    a = await Author.objects.acreate(name="PA", age=30)
    await Book.objects.acreate(title="keep-1", author=a, pages=10)
    await Book.objects.acreate(title="drop-1", author=a, pages=10)
    await Book.objects.acreate(title="keep-2", author=a, pages=10)

    custom = Book.objects.filter(title__startswith="keep")
    authors = [
        au async for au in Author.objects.filter(pk=a.pk).prefetch_related(
            Prefetch("book_set", queryset=custom, to_attr="kept")
        )
    ]
    assert len(authors) == 1
    titles = sorted(b.title for b in getattr(authors[0], "kept"))
    assert titles == ["keep-1", "keep-2"]


# ────────────────────────────────────────────────────────────────────────────
# Bug 8 — aget_queryset must read prefetch cache
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_aget_queryset_uses_prefetch_cache() -> None:
    art = await Article.objects.acreate(title="Pf")
    t1 = await Tag.objects.acreate(name="pf-t1")
    t2 = await Tag.objects.acreate(name="pf-t2")
    await art.tags.aadd(t1, t2)

    arts = [
        a async for a in Article.objects.filter(pk=art.pk).prefetch_related("tags")
    ]
    art2 = arts[0]

    # After prefetch, ``aget_queryset()`` must return a queryset
    # whose ``_result_cache`` is populated from the prefetch slot
    # (no DB round-trip needed to discover the relation). Patch
    # the async connection's ``execute`` to detect any DB call.
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()

    call_count = {"n": 0}
    real = conn.execute

    async def _wrapped(*args: Any, **kwargs: Any) -> Any:
        call_count["n"] += 1
        return await real(*args, **kwargs)

    with patch.object(conn, "execute", _wrapped):
        qs = await art2.tags.aget_queryset()

    assert call_count["n"] == 0, "aget_queryset must hit the prefetch cache"
    assert qs._result_cache is not None
    assert {t.name for t in qs._result_cache} == {"pf-t1", "pf-t2"}


# ────────────────────────────────────────────────────────────────────────────
# Bug 9 — adelete must run cascades through the async path
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_adelete_uses_async_cascade_handler() -> None:
    """The presence of :meth:`_ahandle_on_delete` and the wiring in
    :meth:`adelete` is the contract. Verify it's invoked instead of
    the sync version, and the cascade still works end-to-end."""
    from tests.models import Author as A

    a = await A.objects.acreate(name="ACasc", age=33)
    await Book.objects.acreate(title="bc1", author=a, pages=1)
    await Book.objects.acreate(title="bc2", author=a, pages=1)

    sync_called = {"n": 0}
    async_called = {"n": 0}

    real_sync = A._handle_on_delete
    real_async = A._ahandle_on_delete

    def _spy_sync(self: Any, **kw: Any) -> Any:
        sync_called["n"] += 1
        return real_sync(self, **kw)

    async def _spy_async(self: Any, **kw: Any) -> Any:
        async_called["n"] += 1
        return await real_async(self, **kw)

    with patch.object(A, "_handle_on_delete", _spy_sync), \
         patch.object(A, "_ahandle_on_delete", _spy_async):
        await a.adelete()

    assert async_called["n"] == 1
    assert sync_called["n"] == 0
    assert await Book.objects.filter(title__in=["bc1", "bc2"]).acount() == 0


# ────────────────────────────────────────────────────────────────────────────
# Bug 10 — set/add/remove invalidate the prefetch cache
# ────────────────────────────────────────────────────────────────────────────


def test_m2m_add_invalidates_prefetch_cache() -> None:
    art = Article.objects.create(title="CacheArt")
    t1 = Tag.objects.create(name="c-t1")
    t2 = Tag.objects.create(name="c-t2")
    art.tags.add(t1)

    art2 = Article.objects.filter(pk=art.pk).prefetch_related("tags").first()
    assert art2 is not None
    cache_key = "_prefetch_tags"
    assert cache_key in art2.__dict__  # cache populated by prefetch

    art2.tags.add(t2)  # mutation must invalidate the cache slot
    assert cache_key not in art2.__dict__

    # ``all()`` after the mutation now reflects the live state.
    names = sorted(t.name for t in art2.tags.all())
    assert names == ["c-t1", "c-t2"]


def test_m2m_set_uses_live_state_after_add() -> None:
    """``set()`` previously diffed against the stale prefetch cache,
    causing wrong INSERT/DELETE. Verify the diff is correct after a
    mid-flight ``add()``."""
    art = Article.objects.create(title="SetArt")
    t1 = Tag.objects.create(name="s-t1")
    t2 = Tag.objects.create(name="s-t2")
    t3 = Tag.objects.create(name="s-t3")
    art.tags.add(t1)

    a2 = Article.objects.filter(pk=art.pk).prefetch_related("tags").first()
    assert a2 is not None
    a2.tags.add(t2)  # cache busted, now [t1, t2] live
    a2.tags.set([t1, t3])  # diff must be: remove t2, add t3

    names = sorted(t.name for t in Article.objects.get(pk=art.pk).tags.all())
    assert names == ["s-t1", "s-t3"]


# ────────────────────────────────────────────────────────────────────────────
# Bug 11 + 12 + 13 — autodetector precedence / rename heuristic / serialise
# ────────────────────────────────────────────────────────────────────────────


def test_autodetector_precedence_excludes_renamed_field() -> None:
    """``set_a & set_b - set_c`` must NOT iterate over names in
    ``set_c``. Previously Python's precedence quietly evaluated
    ``set_b - set_c`` first, leaving renamed fields in the
    AlterField loop."""
    from dorm.fields import CharField, IntegerField
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    old = ProjectState()
    new = ProjectState()

    f1 = IntegerField()
    f1.name = 'old_qty'
    f1.column = 'old_qty'
    f2 = CharField(max_length=10)
    f2.name = 'name'
    f2.column = 'name'
    old.add_model("autoapp", "Thing", {"old_qty": f1, "name": f2})

    f1n = IntegerField()
    f1n.name = 'new_qty'
    f1n.column = 'new_qty'
    f2n = CharField(max_length=10)
    f2n.name = 'name'
    f2n.column = 'name'
    new.add_model("autoapp", "Thing", {"new_qty": f1n, "name": f2n})

    det = MigrationAutodetector(
        old,
        new,
        rename_hints={"fields": {"autoapp.Thing": {"old_qty": "new_qty"}}},
    )
    ops = det.changes("autoapp")
    op_types = [type(op).__name__ for op in ops.get("autoapp", [])]
    # We expect exactly one RenameField and zero AlterField. The bug
    # would emit a spurious AlterField.
    assert "RenameField" in op_types
    assert "AlterField" not in op_types


def test_autodetector_rename_heuristic_with_dim_change() -> None:
    """A rename that ALSO changes a non-db_type attribute (e.g.
    max_length) must NOT be collapsed to a pure rename."""
    from dorm.fields import CharField
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    old = ProjectState()
    new = ProjectState()

    f_old = CharField(max_length=20)
    f_old.name = 'title_old'
    f_old.column = 'title_old'
    old.add_model("autoapp2", "Doc", {"title_old": f_old})

    f_new = CharField(max_length=200)
    f_new.name = 'title_new'
    f_new.column = 'title_new'
    new.add_model("autoapp2", "Doc", {"title_new": f_new})

    det = MigrationAutodetector(old, new, detect_renames=True)
    ops = det.changes("autoapp2")
    op_types = [type(op).__name__ for op in ops.get("autoapp2", [])]
    # Either we get RemoveField + AddField, OR RenameField +
    # AlterField — but never a bare RenameField alone. The bug
    # would collapse to a single RenameField and silently drop
    # the max_length change.
    assert not (op_types == ["RenameField"]), (
        "rename heuristic must not absorb a max_length change"
    )


def test_autodetector_serialise_failure_does_not_silence_change() -> None:
    """If ``_serialize_field`` raises for one side but succeeds for
    the other, that asymmetry must still emit AlterField."""
    from dorm.fields import IntegerField
    from dorm.migrations import autodetector as ad
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    old = ProjectState()
    new = ProjectState()
    f_old = IntegerField()
    f_old.name = 'qty'
    f_old.column = 'qty'
    f_new = IntegerField(null=True)
    f_new.name = 'qty'
    f_new.column = 'qty'
    old.add_model("autoapp3", "X", {"qty": f_old})
    new.add_model("autoapp3", "X", {"qty": f_new})

    real = ad.MigrationAutodetector  # noqa: F841 (kept for readability)

    def _flaky_serialise(field: Any) -> str:
        # Raise only for the OLD field; succeed for the new one.
        if not field.null:
            raise RuntimeError("flaky writer")
        return "IntegerField(null=True)"

    with patch("dorm.migrations.writer._serialize_field", _flaky_serialise):
        det = MigrationAutodetector(old, new)
        ops = det.changes("autoapp3")
    op_types = [type(op).__name__ for op in ops.get("autoapp3", [])]
    assert "AlterField" in op_types


# ────────────────────────────────────────────────────────────────────────────
# Bug 14 — M2M add() runs inside an atomic block
# ────────────────────────────────────────────────────────────────────────────


def test_m2m_add_runs_inside_atomic() -> None:
    art = Article.objects.create(title="AtomicArt")
    t = Tag.objects.create(name="atomic-t")

    from dorm import transaction as txn

    real_atomic = txn.atomic
    seen = {"n": 0}

    def _spy_atomic(*a: Any, **kw: Any) -> Any:
        seen["n"] += 1
        return real_atomic(*a, **kw)

    # ``related_managers.add`` does ``from .transaction import
    # atomic`` at call time, so patch the source attribute.
    with patch.object(txn, "atomic", _spy_atomic):
        art.tags.add(t)

    assert seen["n"] >= 1
    assert list(art.tags.all())[0].pk == t.pk


@pytest.mark.asyncio
async def test_async_m2m_aadd_runs_inside_aatomic() -> None:
    art = await Article.objects.acreate(title="AAtomicArt")
    t = await Tag.objects.acreate(name="aatomic-t")

    from dorm import transaction as txn

    real_aatomic = txn.aatomic
    seen = {"n": 0}

    def _spy_aatomic(*a: Any, **kw: Any) -> Any:
        seen["n"] += 1
        return real_aatomic(*a, **kw)

    with patch.object(txn, "aatomic", _spy_aatomic):
        await art.tags.aadd(t)

    assert seen["n"] >= 1


# ────────────────────────────────────────────────────────────────────────────
# Bug 15 — CombinedQuerySet.order_by must validate identifiers
# ────────────────────────────────────────────────────────────────────────────


def test_combined_queryset_order_by_rejects_injection() -> None:
    """A user-controlled ``order_by`` value forwarded to a UNION
    queryset previously interpolated raw — verify it now raises
    ``ValueError`` from ``_validate_identifier``."""
    qs1 = Author.objects.filter(age=10)
    qs2 = Author.objects.filter(age=20)
    union = qs1.union(qs2)
    union._query.order_by_fields = ['name"; DROP TABLE authors; --']
    with pytest.raises(ValueError):
        union._build_sql(union._get_connection())


def test_combined_queryset_order_by_accepts_safe_identifier() -> None:
    Author.objects.create(name="UA", age=10)
    Author.objects.create(name="UB", age=20)
    qs1 = Author.objects.filter(age=10)
    qs2 = Author.objects.filter(age=20)
    union = qs1.union(qs2).order_by("-name")
    names = [a.name for a in union]
    assert "UA" in names and "UB" in names
