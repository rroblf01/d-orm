"""End-to-end smoke coverage promoted from ``example/smoke.py``.

Each function here exercises a public-API contract that didn't
previously have a runtime test: feature surfaces that *did* have unit
coverage but only at the SQL-string level (e.g. annotation pipeline
with ``F("x") + 1``), or features whose silent-wrong-result behaviour
masked real bugs (e.g. JSON path traversal returning empty rows on
SQLite).

These run against the same ``db_config`` parametrisation as the rest
of the suite (sqlite + postgres when available); the auth-dependent
tests reuse the ``_auth_tables`` fixture pattern from
``test_v3_0_integration``.
"""

from __future__ import annotations

import asyncio
import datetime
import time

import pytest

import dorm
from dorm.exceptions import (
    FieldDoesNotExist,
    IntegrityError,
    ValidationError,
)


# ──────────────────────────────────────────────────────────────────────────────
# Shared auth-table fixture — duplicated from test_v3_0_integration so this
# file can be deleted/replaced without churning that one.
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _auth_tables():
    from dorm.contrib.auth.models import Group, Permission, User
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in (
        "auth_user_user_permissions",
        "auth_user_groups",
        "auth_group_permissions",
        "auth_user",
        "auth_group",
        "auth_permission",
    ):
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, table in [
        (Permission, "auth_permission"),
        (Group, "auth_group"),
        (User, "auth_user"),
    ]:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )

    vendor = getattr(conn, "vendor", "sqlite")
    pk_type = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if vendor == "sqlite"
        else "SERIAL PRIMARY KEY"
    )
    for junction, left, right in (
        ("auth_group_permissions", "group_id", "permission_id"),
        ("auth_user_groups", "user_id", "group_id"),
        ("auth_user_user_permissions", "user_id", "permission_id"),
    ):
        left_table = "auth_group" if left == "group_id" else "auth_user"
        right_table = "auth_permission" if right == "permission_id" else "auth_group"
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{junction}" (\n'
            f'  "id" {pk_type},\n'
            f'  "{left}" BIGINT NOT NULL REFERENCES "{left_table}"("id"),\n'
            f'  "{right}" BIGINT NOT NULL REFERENCES "{right_table}"("id")\n'
            f")"
        )
    yield


# ──────────────────────────────────────────────────────────────────────────────
# CRUD basics
# ──────────────────────────────────────────────────────────────────────────────


def test_crud_roundtrip():
    from tests.models import Author

    a = Author.objects.create(name="Alice", age=40, email="a@e.com")
    assert a.id is not None  # ty:ignore[unresolved-attribute]
    a2 = Author.objects.get(id=a.id)  # ty:ignore[unresolved-attribute]
    assert a2.name == "Alice"
    a2.name = "Alicia"
    a2.save()
    assert Author.objects.get(id=a.id).name == "Alicia"  # ty:ignore[unresolved-attribute]
    a2.delete()
    assert not Author.objects.filter(id=a.id).exists()  # ty:ignore[unresolved-attribute]


def test_get_or_create_and_update_or_create():
    from tests.models import Author

    a, created = Author.objects.get_or_create(
        email="goc@e.com", defaults={"name": "GOC", "age": 30}
    )
    assert created
    a2, created2 = Author.objects.get_or_create(
        email="goc@e.com", defaults={"name": "Other", "age": 99}
    )
    assert not created2
    assert a2.id == a.id  # ty:ignore[unresolved-attribute]

    a3, created3 = Author.objects.update_or_create(
        email="goc@e.com",
        defaults={"name": "Updated", "age": 31},
    )
    assert not created3
    assert a3.name == "Updated"
    assert a3.age == 31


# ──────────────────────────────────────────────────────────────────────────────
# Querying
# ──────────────────────────────────────────────────────────────────────────────


def test_filter_q_f_chained():
    from dorm import F, Q
    from tests.models import Author

    Author.objects.bulk_create(
        [Author(name=f"X{i}", age=20 + i, email=f"x{i}@e.com") for i in range(10)]
    )
    qs = Author.objects.filter(age__gte=25).order_by("-age")
    rows = list(qs)
    assert len(rows) == 5
    assert rows[0].age == 29

    qs2 = Author.objects.filter(Q(age__lt=22) | Q(name="X9"))
    assert qs2.count() == 3

    Author.objects.filter(email__startswith="x").update(age=F("age") + 100)
    assert Author.objects.filter(age__gte=120).count() == 10


def test_q_negation_chain():
    from dorm import Q
    from tests.models import Author

    Author.objects.create(name="A", age=10, email="a@e.com")
    Author.objects.create(name="B", age=20, email="b@e.com")
    Author.objects.create(name="C", age=30, email="c@e.com")
    qs = Author.objects.filter(~Q(name="A") & Q(age__lt=30))
    rows = sorted(a.name for a in qs)
    assert rows == ["B"]


def test_values_and_first_last_exists():
    from tests.models import Author

    Author.objects.create(name="V", age=99, email="v@e.com")
    rows = list(Author.objects.values("name", "age"))
    assert any(r["name"] == "V" for r in rows)
    flat = list(Author.objects.values_list("name", flat=True).order_by("name"))
    assert "V" in flat
    assert Author.objects.first() is not None
    assert Author.objects.last() is not None
    assert Author.objects.exists()


def test_aggregate_and_simple_annotate():
    from dorm import Avg, Count, Sum
    from tests.models import Author, Book

    a1 = Author.objects.create(name="A1", age=30, email="aa1@e.com")
    a2 = Author.objects.create(name="A2", age=40, email="aa2@e.com")
    Book.objects.create(title="B1", author=a1, pages=100)
    Book.objects.create(title="B2", author=a1, pages=200)
    Book.objects.create(title="B3", author=a2, pages=150)

    agg = Book.objects.aggregate(total=Count("id"), avg=Avg("pages"))
    assert agg["total"] == 3
    assert agg["avg"] == 150.0

    a1_count = Book.objects.filter(author=a1).count()
    a2_count = Book.objects.filter(author=a2).count()
    assert a1_count == 2
    assert a2_count == 1

    assert Book.objects.aggregate(s=Sum("pages"))["s"] == 450


def test_aggregate_on_empty_returns_zero_or_null():
    from dorm import Count, Sum
    from tests.models import Author

    agg = Author.objects.aggregate(n=Count("id"), s=Sum("age"))
    assert agg["n"] == 0
    assert agg["s"] in (None, 0)


# ──────────────────────────────────────────────────────────────────────────────
# F + Subquery + Exists end-to-end through annotation pipeline
# ──────────────────────────────────────────────────────────────────────────────


def test_subquery_outerref_exists():
    from dorm import Exists, OuterRef
    from tests.models import Author, Book

    a = Author.objects.create(name="SubA", age=30, email="sa@e.com")
    Author.objects.create(name="SubB", age=31, email="sb@e.com")
    Book.objects.create(title="SA1", author=a, pages=100)

    rows = list(
        Author.objects.annotate(
            has=Exists(Book.objects.filter(author_id=OuterRef("pk")))
        ).order_by("name")
    )
    by_name = {r.name: bool(r.has) for r in rows}  # ty:ignore[unresolved-attribute]
    assert by_name["SubA"] is True
    assert by_name["SubB"] is False


def test_annotate_f_arithmetic_executes():
    """Regression for: ``CombinedExpression.as_sql`` had the wrong
    signature, so ``annotate(x=F("a") + 1)`` crashed with
    ``unexpected keyword argument 'model'``. Plus ``F`` had no
    ``as_sql`` so the F instance was bound as a parameter."""
    from dorm import F
    from tests.models import Author

    Author.objects.create(name="AF", age=30, email="af@e.com")
    rows = list(Author.objects.annotate(plus=F("age") + 5))
    assert rows[0].plus == 35  # ty:ignore[unresolved-attribute]


def test_update_with_f_expression():
    from dorm import F
    from tests.models import Author

    Author.objects.create(name="UF", age=20, email="uf@e.com")
    Author.objects.filter(email="uf@e.com").update(age=F("age") + 25)
    assert Author.objects.get(email="uf@e.com").age == 45


# ──────────────────────────────────────────────────────────────────────────────
# Relations
# ──────────────────────────────────────────────────────────────────────────────


def test_fk_select_related():
    from tests.models import Author, Book

    a = Author.objects.create(name="FK", age=50, email="fk@e.com")
    Book.objects.create(title="FKB", author=a, pages=100)
    b = Book.objects.select_related("author").get(title="FKB")
    assert b.author.name == "FK"


def test_prefetch_related():
    from tests.models import Author, Book

    a = Author.objects.create(name="P", age=60, email="p@e.com")
    for i in range(3):
        Book.objects.create(title=f"P{i}", author=a, pages=100)
    rows = list(Author.objects.prefetch_related("book_set"))
    target = [r for r in rows if r.name == "P"][0]
    assert len(list(target.book_set.all())) == 3  # ty:ignore[unresolved-attribute]


def test_cascade_delete():
    from tests.models import Author, Book

    a = Author.objects.create(name="C1", age=22, email="c1@e.com")
    Book.objects.create(title="cb", author=a, pages=1)
    assert Book.objects.filter(author=a).count() == 1
    a.delete()
    assert Book.objects.filter(author_id=a.id).count() == 0  # ty:ignore[unresolved-attribute]


# ──────────────────────────────────────────────────────────────────────────────
# Transactions
# ──────────────────────────────────────────────────────────────────────────────


def test_atomic_rollback_on_raise():
    from dorm.transaction import atomic
    from tests.models import Author

    try:
        with atomic():
            Author.objects.create(name="TX", age=1, email="tx@e.com")
            raise RuntimeError("rollback")
    except RuntimeError:
        pass
    assert not Author.objects.filter(email="tx@e.com").exists()


def test_atomic_nested_savepoint():
    from dorm.transaction import atomic
    from tests.models import Author

    with atomic():
        Author.objects.create(name="N1", age=1, email="np1@e.com")
        try:
            with atomic():
                Author.objects.create(name="N2", age=2, email="np2@e.com")
                raise RuntimeError("inner")
        except RuntimeError:
            pass
    assert Author.objects.filter(email="np1@e.com").exists()
    assert not Author.objects.filter(email="np2@e.com").exists()


# ──────────────────────────────────────────────────────────────────────────────
# Signals
# ──────────────────────────────────────────────────────────────────────────────


def test_pre_post_save_delete_signals():
    from dorm.signals import post_delete, post_save, pre_delete, pre_save
    from tests.models import Author

    captured: list[tuple] = []

    def on_pre_save(sender, instance, **kw):
        captured.append(("pre_save", instance.name))

    def on_post_save(sender, instance, created, **kw):
        captured.append(("post_save", instance.name, created))

    def on_pre_delete(sender, instance, **kw):
        captured.append(("pre_delete", instance.name))

    def on_post_delete(sender, instance, **kw):
        captured.append(("post_delete", instance.name))

    pre_save.connect(on_pre_save, sender=Author)
    post_save.connect(on_post_save, sender=Author)
    pre_delete.connect(on_pre_delete, sender=Author)
    post_delete.connect(on_post_delete, sender=Author)
    try:
        a = Author.objects.create(name="SIG", age=1, email="sig@e.com")
        a.delete()
    finally:
        pre_save.disconnect(on_pre_save, sender=Author)
        post_save.disconnect(on_post_save, sender=Author)
        pre_delete.disconnect(on_pre_delete, sender=Author)
        post_delete.disconnect(on_post_delete, sender=Author)
    kinds = [c[0] for c in captured]
    assert "pre_save" in kinds and "post_save" in kinds
    assert "pre_delete" in kinds and "post_delete" in kinds


# ──────────────────────────────────────────────────────────────────────────────
# Async
# ──────────────────────────────────────────────────────────────────────────────


def test_async_crud_roundtrip():
    from tests.models import Author

    async def go():
        a = await Author.objects.acreate(name="ASY", age=2, email="async@e.com")
        a.name = "ASY2"
        await a.asave()
        got = await Author.objects.aget(id=a.id)  # ty:ignore[unresolved-attribute]
        assert got.name == "ASY2"
        rows: list[Author] = []
        async for r in Author.objects.filter(email="async@e.com").aiterator():
            rows.append(r)
        assert len(rows) == 1
        await a.adelete()

    asyncio.run(go())


# ──────────────────────────────────────────────────────────────────────────────
# Observability / cache
# ──────────────────────────────────────────────────────────────────────────────


def test_querylog_collects_records():
    from dorm.contrib.querylog import query_log
    from tests.models import Author

    with query_log() as ql:
        list(Author.objects.all()[:5])
        Author.objects.create(name="QL", age=3, email="ql@e.com")
    assert len(ql.records) >= 2


def test_assert_num_queries():
    from dorm.test import assertNumQueries
    from tests.models import Author

    Author.objects.create(name="QC", age=4, email="qc@e.com")
    with assertNumQueries(1):
        list(Author.objects.filter(email="qc@e.com"))


# ──────────────────────────────────────────────────────────────────────────────
# Cache
# ──────────────────────────────────────────────────────────────────────────────


def test_locmem_cache_get_set_delete():
    from dorm.cache import get_cache, reset_caches
    from dorm.cache.locmem import LocMemCache
    from dorm.conf import settings

    prev_caches = getattr(settings, "CACHES", None)
    settings.CACHES = {"default": {"BACKEND": "dorm.cache.locmem.LocMemCache"}}
    reset_caches()
    try:
        cache = get_cache()
        assert isinstance(cache, LocMemCache)
        cache.set("k", b"v", 60)
        assert cache.get("k") == b"v"
        cache.delete("k")
        assert cache.get("k") is None
    finally:
        if prev_caches is None:
            del settings.CACHES
        else:
            settings.CACHES = prev_caches
        reset_caches()


# ──────────────────────────────────────────────────────────────────────────────
# Auth: password + token
# ──────────────────────────────────────────────────────────────────────────────


def test_auth_password_and_token_roundtrip(_auth_tables):
    from dorm.conf import settings
    from dorm.contrib.auth.models import User
    from dorm.contrib.auth.password import check_password, make_password
    from dorm.contrib.auth.tokens import default_token_generator as gen

    h = make_password("secret")
    assert check_password("secret", h)
    assert not check_password("wrong", h)

    settings.SECRET_KEY = "smoke-test-only-not-for-prod"
    u = User.objects.create_user(email="t@e.com", password="orig")
    tok = gen.make_token(u)
    assert gen.check_token(u, tok)
    u.set_password("new")
    u.save()
    assert not gen.check_token(u, tok)


def test_m2m_add_idempotent_remove_clear(_auth_tables):
    from dorm.contrib.auth.models import Group, User

    g1 = Group.objects.create(name="g1")
    g2 = Group.objects.create(name="g2")
    u = User.objects.create_user(email="m2mid@e.com", password="pw")
    u.groups.add(g1)
    u.groups.add(g1)  # idempotent
    assert u.groups.count() == 1
    u.groups.add(g2)
    assert u.groups.count() == 2
    u.groups.remove(g1)
    assert u.groups.count() == 1
    u.groups.clear()
    assert u.groups.count() == 0


# ──────────────────────────────────────────────────────────────────────────────
# Soft delete
# ──────────────────────────────────────────────────────────────────────────────


def test_softdelete_mixin_filters_default_manager():
    from dorm.contrib.softdelete import SoftDeleteModel
    from dorm.db.connection import get_connection

    class Note(SoftDeleteModel):
        body = dorm.CharField(max_length=20)

        class Meta:
            db_table = "smoke_notes"
            app_label = "smoke_softdelete"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_notes"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_notes" ({pk_decl}, '
        f'"body" VARCHAR(20) NOT NULL, "deleted_at" TIMESTAMP)'
    )
    n = Note.objects.create(body="hi")
    assert Note.objects.count() == 1
    n.delete()
    assert Note.objects.count() == 0
    assert Note.all_objects.count() == 1


# ──────────────────────────────────────────────────────────────────────────────
# Pydantic interop
# ──────────────────────────────────────────────────────────────────────────────


def test_pydantic_schema_for_constructs_validator():
    pytest.importorskip("pydantic")
    from dorm.contrib.pydantic import schema_for
    from tests.models import Author

    Schema = schema_for(Author)
    sample = Schema(id=1, name="P", age=30, email="p@e.com", is_active=True, publisher_id=None)
    assert sample.email == "p@e.com"  # ty:ignore[unresolved-attribute]


# ──────────────────────────────────────────────────────────────────────────────
# Validators / EmailField
# ──────────────────────────────────────────────────────────────────────────────


def test_emailfield_rejects_invalid_at_assignment():
    from tests.models import Author

    with pytest.raises(ValidationError):
        Author(name="V", age=10, email="not-an-email")


# ──────────────────────────────────────────────────────────────────────────────
# Bulk extras / refresh_from_db / raw / only / defer
# ──────────────────────────────────────────────────────────────────────────────


def test_bulk_update_in_bulk_iterator_only_defer():
    from tests.models import Author

    rows = [Author(name=f"BU{i}", age=20 + i, email=f"bu{i}@e.com") for i in range(5)]
    Author.objects.bulk_create(rows)
    fetched = list(Author.objects.order_by("age"))
    for a in fetched:
        a.age += 100
    Author.objects.bulk_update(fetched, fields=["age"])
    assert Author.objects.filter(age__gte=100).count() == 5

    by_id = Author.objects.in_bulk([fetched[0].id, fetched[1].id])  # ty:ignore[unresolved-attribute]
    assert len(by_id) == 2

    only_qs = list(Author.objects.only("name").order_by("name")[:3])
    for a in only_qs:
        assert a.name is not None
    defer_qs = list(Author.objects.defer("age").order_by("name")[:3])
    assert len(defer_qs) == 3

    iter_n = sum(1 for _ in Author.objects.iterator(chunk_size=2))
    assert iter_n == 5


def test_refresh_from_db_picks_up_external_update():
    from tests.models import Author

    a = Author.objects.create(name="R", age=10, email="r@e.com")
    Author.objects.filter(id=a.id).update(name="Renamed")  # ty:ignore[unresolved-attribute]
    a.refresh_from_db()
    assert a.name == "Renamed"


def test_raw_sql_with_percent_s_placeholder():
    from tests.models import Author

    Author.objects.create(name="RW", age=20, email="rw@e.com")
    rows = list(Author.objects.raw("SELECT * FROM authors WHERE name = %s", ["RW"]))
    assert len(rows) == 1
    assert rows[0].name == "RW"


def test_save_update_fields_restricts_columns():
    """save(update_fields=[…]) must NOT push other in-memory attrs."""
    from tests.models import Author

    a = Author.objects.create(name="UF1", age=10, email="uf1@e.com", is_active=True)
    Author.objects.filter(id=a.id).update(age=99)  # ty:ignore[unresolved-attribute]
    a.name = "UF1-renamed"
    a.age = 7  # in-memory only
    a.save(update_fields=["name"])
    fresh = Author.objects.get(id=a.id)  # ty:ignore[unresolved-attribute]
    assert fresh.name == "UF1-renamed"
    # ``age`` should still be the DB-side 99 — not the in-memory 7.
    assert fresh.age == 99


# ──────────────────────────────────────────────────────────────────────────────
# UniqueConstraint at DB level
# ──────────────────────────────────────────────────────────────────────────────


def test_unique_email_violation_raises_integrity_error():
    from tests.models import Tag

    Tag.objects.create(name="dup-tag")
    with pytest.raises(IntegrityError):
        Tag.objects.create(name="dup-tag")


# ──────────────────────────────────────────────────────────────────────────────
# JSONField path traversal must raise
# ──────────────────────────────────────────────────────────────────────────────


def test_jsonfield_path_traversal_raises_field_does_not_exist():
    """Regression: ``filter(jsonfield__sub_key=…)`` used to emit
    ``WHERE "sub_key" = ?`` against a non-existent column, returning
    0 rows on SQLite without erroring. Now raises ``FieldDoesNotExist``
    so the unsupported-feature signal is loud."""
    from dorm.db.connection import get_connection

    class Doc(dorm.Model):
        data = dorm.JSONField(default=dict)

        class Meta:
            db_table = "smoke_jsondoc"
            app_label = "smoke_json"

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_jsondoc"{cascade}')
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_jsondoc" ({pk_decl}, "data" TEXT NOT NULL)'
    )
    Doc.objects.create(data={"name": "alice"})
    with pytest.raises(FieldDoesNotExist):
        list(Doc.objects.filter(data__name="alice"))


def test_jsonfield_value_roundtrip():
    from dorm.db.connection import get_connection

    class JSDoc(dorm.Model):
        payload = dorm.JSONField(default=dict)

        class Meta:
            db_table = "smoke_jsdoc_rt"
            app_label = "smoke_json_rt"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_jsdoc_rt"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_jsdoc_rt" ({pk_decl}, "payload" TEXT NOT NULL)'
    )
    JSDoc.objects.create(payload={"a": 1, "nested": {"b": [1, 2, 3]}})
    fetched = JSDoc.objects.first()
    assert fetched.payload == {"a": 1, "nested": {"b": [1, 2, 3]}}  # ty:ignore[unresolved-attribute]


# ──────────────────────────────────────────────────────────────────────────────
# auto_now / auto_now_add behaviour
# ──────────────────────────────────────────────────────────────────────────────


def test_auto_now_add_freezes_created_bumps_updated():
    from dorm.db.connection import get_connection

    class Stamped(dorm.Model):
        body = dorm.CharField(max_length=20)
        created_at = dorm.DateTimeField(auto_now_add=True)
        updated_at = dorm.DateTimeField(auto_now=True)

        class Meta:
            db_table = "smoke_stamped"
            app_label = "smoke_stamped"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_stamped"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    ts_decl = "DATETIME" if vendor == "sqlite" else "TIMESTAMP WITH TIME ZONE"
    conn.execute_script(
        f'CREATE TABLE "smoke_stamped" ({pk_decl}, '
        f'"body" VARCHAR(20) NOT NULL, '
        f'"created_at" {ts_decl} NOT NULL, '
        f'"updated_at" {ts_decl} NOT NULL)'
    )
    s = Stamped.objects.create(body="hi")
    first_created = s.created_at
    first_updated = s.updated_at
    time.sleep(0.01)
    s.body = "bye"
    s.save()
    s.refresh_from_db()
    assert s.created_at == first_created  # frozen
    assert s.updated_at >= first_updated  # bumped


# ──────────────────────────────────────────────────────────────────────────────
# OneToOneField
# ──────────────────────────────────────────────────────────────────────────────


def test_one_to_one_forward_and_reverse():
    from dorm.db.connection import get_connection

    class Profile(dorm.Model):
        nickname = dorm.CharField(max_length=20)

        class Meta:
            db_table = "smoke_profile"
            app_label = "smoke_o2o"

    class Acct(dorm.Model):
        profile = dorm.OneToOneField(
            Profile, on_delete=dorm.CASCADE, related_name="acct"
        )
        email = dorm.EmailField()

        class Meta:
            db_table = "smoke_acct"
            app_label = "smoke_o2o"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_acct"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_profile"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_profile" ({pk_decl}, "nickname" VARCHAR(20) NOT NULL)'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_acct" ({pk_decl}, '
        '"profile_id" BIGINT NOT NULL UNIQUE REFERENCES "smoke_profile"("id"), '
        '"email" VARCHAR(254) NOT NULL)'
    )
    p = Profile.objects.create(nickname="ace")
    a = Acct.objects.create(profile=p, email="ace@e.com")
    assert a.profile.nickname == "ace"
    p_fresh = Profile.objects.get(id=p.id)  # ty:ignore[unresolved-attribute]
    assert p_fresh.acct.email == "ace@e.com"  # ty:ignore[unresolved-attribute]


# ──────────────────────────────────────────────────────────────────────────────
# DateField / TimeField / DurationField
# ──────────────────────────────────────────────────────────────────────────────


def test_date_time_duration_roundtrip():
    from dorm.db.connection import get_connection

    class Tracker(dorm.Model):
        d = dorm.DateField()
        t = dorm.TimeField()
        dur = dorm.DurationField()

        class Meta:
            db_table = "smoke_tracker"
            app_label = "smoke_tracker"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_tracker"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    dur_type = "BIGINT" if vendor == "sqlite" else "INTERVAL"
    conn.execute_script(
        f'CREATE TABLE "smoke_tracker" ({pk_decl}, '
        f'"d" DATE NOT NULL, "t" TIME NOT NULL, "dur" {dur_type} NOT NULL)'
    )
    today = datetime.date(2026, 5, 3)
    now = datetime.time(14, 30, 5)
    span = datetime.timedelta(hours=2, minutes=30)
    Tracker.objects.create(d=today, t=now, dur=span)
    fresh = Tracker.objects.first()
    assert fresh.d == today  # ty:ignore[unresolved-attribute]
    assert fresh.t == now  # ty:ignore[unresolved-attribute]
    assert fresh.dur == span  # ty:ignore[unresolved-attribute]


# ──────────────────────────────────────────────────────────────────────────────
# Migration autodetector edge cases
# ──────────────────────────────────────────────────────────────────────────────


def test_addfield_via_autodetect():
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    s_old = ProjectState()
    s_old.add_model(
        "smoke_addf", "Foo",
        fields={"id": dorm.BigAutoField(primary_key=True), "x": dorm.IntegerField()},
        options={},
    )
    s_new = ProjectState()
    s_new.add_model(
        "smoke_addf", "Foo",
        fields={
            "id": dorm.BigAutoField(primary_key=True),
            "x": dorm.IntegerField(),
            "y": dorm.IntegerField(default=0),
        },
        options={},
    )
    det = MigrationAutodetector(s_old, s_new)
    changes = det.changes(app_label="smoke_addf")
    op_names = [type(o).__name__ for o in changes.get("smoke_addf", [])]
    assert "AddField" in op_names


def test_alterfield_via_autodetect():
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    s_old = ProjectState()
    s_old.add_model(
        "smoke_altf", "Foo",
        fields={
            "id": dorm.BigAutoField(primary_key=True),
            "x": dorm.CharField(max_length=10),
        },
        options={},
    )
    s_new = ProjectState()
    s_new.add_model(
        "smoke_altf", "Foo",
        fields={
            "id": dorm.BigAutoField(primary_key=True),
            "x": dorm.CharField(max_length=50),
        },
        options={},
    )
    det = MigrationAutodetector(s_old, s_new)
    changes = det.changes(app_label="smoke_altf")
    op_names = [type(o).__name__ for o in changes.get("smoke_altf", [])]
    assert "AlterField" in op_names


def test_addindex_via_autodetect():
    from dorm.indexes import Index
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState

    s_old = ProjectState()
    s_old.add_model(
        "smoke_addidx", "Foo",
        fields={"id": dorm.BigAutoField(primary_key=True), "name": dorm.CharField(max_length=20)},
        options={"indexes": []},
    )
    s_new = ProjectState()
    s_new.add_model(
        "smoke_addidx", "Foo",
        fields={"id": dorm.BigAutoField(primary_key=True), "name": dorm.CharField(max_length=20)},
        options={"indexes": [Index(fields=["name"], name="ix_foo_name")]},
    )
    det = MigrationAutodetector(s_old, s_new)
    changes = det.changes(app_label="smoke_addidx")
    op_names = [type(o).__name__ for o in changes.get("smoke_addidx", [])]
    assert "AddIndex" in op_names


# ──────────────────────────────────────────────────────────────────────────────
# Encrypted fields — security regression test
# ──────────────────────────────────────────────────────────────────────────────


def test_encrypted_field_writes_ciphertext_not_plaintext():
    """Regression: ``EncryptedFieldMixin`` previously only overrode
    ``get_prep_value``, but dorm's INSERT/UPDATE pipeline calls
    ``get_db_prep_value`` — encryption was silently bypassed and
    plaintext landed on disk. Verify the on-disk bytes do NOT
    contain the plaintext."""
    pytest.importorskip("cryptography")
    from dorm.conf import settings
    from dorm.contrib.encrypted import EncryptedCharField
    from dorm.db.connection import get_connection

    settings.FIELD_ENCRYPTION_KEY = b"0" * 32  # ty:ignore[invalid-assignment]

    class Secret(dorm.Model):
        body = EncryptedCharField(max_length=200)

        class Meta:
            db_table = "smoke_secret"
            app_label = "smoke_enc"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_secret"{cascade}')
    vendor = getattr(conn, "vendor", "sqlite")
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "smoke_secret" ({pk_decl}, "body" VARCHAR(200) NOT NULL)'
    )
    s = Secret.objects.create(body="top-secret-data")
    fresh = Secret.objects.get(id=s.id)  # ty:ignore[unresolved-attribute]
    assert fresh.body == "top-secret-data"

    # Inspect on-disk representation directly. Round-tripping via the
    # ORM would re-decrypt; we want the raw column bytes.
    placeholder = "%s" if vendor == "postgresql" else "?"
    rows = conn.execute(
        f'SELECT "body" FROM "smoke_secret" WHERE "id" = {placeholder}', [s.id]  # ty:ignore[unresolved-attribute]
    )
    ct = rows[0]["body"]
    raw = ct if isinstance(ct, bytes) else ct.encode("utf-8")
    assert b"top-secret-data" not in raw
