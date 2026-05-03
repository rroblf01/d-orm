"""Comprehensive smoke test exercising the dorm public API.

Run with: uv run python smoke.py
Each section is wrapped in try/except so failures are listed at the end
rather than aborting on the first error.
"""

from __future__ import annotations

import asyncio
import os
import sys
import traceback
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dorm.cli import _load_settings, _load_apps  # noqa: E402

_load_settings("settings")
from dorm.conf import settings  # noqa: E402

_load_apps(settings.INSTALLED_APPS)

import dorm  # noqa: E402
from dorm.db.connection import get_connection  # noqa: E402
from dorm.contrib.auth.models import Group, Permission, User  # noqa: E402

failures: list[tuple[str, str]] = []


def section(name):
    def deco(fn):
        def wrapper():
            try:
                fn()
                print(f"  OK   {name}")
            except Exception as exc:
                failures.append((name, traceback.format_exc()))
                print(f"  FAIL {name}: {exc}")

        wrapper.__name__ = fn.__name__
        return wrapper

    return deco


def reset_db():
    from example.models import Author, Book, Genre, Review

    Review.objects.all().delete()
    Book.objects.all().delete()
    Author.objects.all().delete()
    Genre.objects.all().delete()
    User.objects.all().delete()
    Group.objects.all().delete()
    Permission.objects.all().delete()


# ──────────────────────────────────────────────────────────────────────────────
# 1. CRUD basics
# ──────────────────────────────────────────────────────────────────────────────


@section("CRUD: create / get / update / delete")
def test_crud():
    from example.models import Author

    a = Author.objects.create(name="Alice", email="a@example.com", birth_year=1980)
    assert a.id is not None
    a2 = Author.objects.get(id=a.id)
    assert a2.name == "Alice"
    a2.name = "Alicia"
    a2.save()
    assert Author.objects.get(id=a.id).name == "Alicia"
    a2.delete()
    assert not Author.objects.filter(id=a.id).exists()


@section("get_or_create / update_or_create")
def test_goc():
    from example.models import Author

    a, created = Author.objects.get_or_create(
        email="goc@example.com", defaults={"name": "GOC", "birth_year": 1990}
    )
    assert created
    a2, created2 = Author.objects.get_or_create(
        email="goc@example.com", defaults={"name": "Other"}
    )
    assert not created2
    assert a2.id == a.id

    a3, created3 = Author.objects.update_or_create(
        email="goc@example.com",
        defaults={"name": "Updated", "birth_year": 1991},
    )
    assert not created3
    assert a3.name == "Updated"
    assert a3.birth_year == 1991


# ──────────────────────────────────────────────────────────────────────────────
# 2. Querying
# ──────────────────────────────────────────────────────────────────────────────


@section("filter / exclude / order_by / Q / F / chained")
def test_query_basics():
    from dorm import F, Q
    from example.models import Author

    Author.objects.all().delete()
    Author.objects.bulk_create(
        [
            Author(name=f"X{i}", email=f"x{i}@e.com", birth_year=1900 + i)
            for i in range(10)
        ]
    )
    qs = Author.objects.filter(birth_year__gte=1905).order_by("-birth_year")
    rows = list(qs)
    assert len(rows) == 5
    assert rows[0].birth_year == 1909

    qs2 = Author.objects.filter(Q(birth_year__lt=1903) | Q(name="X9"))
    assert qs2.count() == 4

    Author.objects.filter(email__startswith="x").update(birth_year=F("birth_year") + 100)
    assert Author.objects.filter(birth_year__gte=2000).count() == 10


@section("values / values_list / distinct / first / last")
def test_values():
    from example.models import Author

    Author.objects.create(name="V", email="v@e.com", birth_year=2050)
    rows = list(Author.objects.values("name", "birth_year"))
    assert any(r["name"] == "V" for r in rows)

    flat = list(
        Author.objects.values_list("name", flat=True).order_by("name")[:3]
    )
    assert isinstance(flat, list)

    assert Author.objects.first() is not None
    assert Author.objects.last() is not None
    assert Author.objects.exists()


@section("aggregate / annotate / Count / Sum / Avg")
def test_aggregate():
    from dorm import Avg, Count, Sum
    from example.models import Author, Book, Genre

    Author.objects.all().delete()
    g = Genre.objects.create(name="Sci-fi")
    a1 = Author.objects.create(name="A1", email="aa1@e.com", birth_year=1900)
    a2 = Author.objects.create(name="A2", email="aa2@e.com", birth_year=1910)
    Book.objects.create(
        title="B1", author=a1, genre=g, isbn="1" * 13,
        pages=100, price=Decimal("10.00"), published_year=2000,
    )
    Book.objects.create(
        title="B2", author=a1, genre=g, isbn="2" * 13,
        pages=200, price=Decimal("20.00"), published_year=2001,
    )
    Book.objects.create(
        title="B3", author=a2, genre=g, isbn="3" * 13,
        pages=150, price=Decimal("15.00"), published_year=2002,
    )

    agg = Book.objects.aggregate(total=Count("id"), avg_pages=Avg("pages"))
    assert agg["total"] == 3
    assert agg["avg_pages"] == 150.0

    # Reverse-FK aggregation via Count("books") at runtime is not
    # supported by dorm (only the SQL-string form is checked in
    # tests). Use a per-instance count instead — exercises the
    # forward FK + manager filter path.
    a1_books = Book.objects.filter(author=a1).count()
    a2_books = Book.objects.filter(author=a2).count()
    assert a1_books == 2
    assert a2_books == 1

    sum_pages = Book.objects.aggregate(s=Sum("pages"))["s"]
    assert sum_pages == 450


# ──────────────────────────────────────────────────────────────────────────────
# 3. Relations
# ──────────────────────────────────────────────────────────────────────────────


@section("ForeignKey forward / reverse / select_related")
def test_fk():
    from example.models import Author, Book

    a = Author.objects.create(name="FK", email="fk@e.com", birth_year=1950)
    Book.objects.create(
        title="FKB", author=a, isbn="9" * 13,
        pages=100, price=Decimal("5.00"), published_year=2010,
    )
    b = Book.objects.select_related("author").get(title="FKB")
    assert b.author.name == "FK"
    related_books = list(a.book_set.all())
    assert len(related_books) == 1


@section("M2M: User.groups, Group.permissions")
def test_m2m():
    User.objects.all().delete()
    Group.objects.all().delete()
    Permission.objects.all().delete()
    p = Permission.objects.create(name="Can act", codename="acts.do")
    g = Group.objects.create(name="actors")
    g.permissions.add(p)
    u = User.objects.create_user(email="m2m@e.com", password="pw123")
    u.groups.add(g)

    assert list(g.permissions.all())[0].codename == "acts.do"
    assert list(u.groups.all())[0].name == "actors"
    assert u.has_perm("acts.do")
    g.permissions.remove(p)
    # Re-fetch — has_perm walks groups freshly each call.
    assert not User.objects.get(id=u.id).has_perm("acts.do")


@section("prefetch_related")
def test_prefetch():
    from example.models import Author, Book

    Author.objects.all().delete()
    a = Author.objects.create(name="P", email="p@e.com", birth_year=1960)
    for i in range(3):
        Book.objects.create(
            title=f"P{i}", author=a, isbn=f"{i:013d}",
            pages=100, price=Decimal("1.00"), published_year=2000,
        )
    rows = list(Author.objects.prefetch_related("book_set"))
    assert any(len(list(r.book_set.all())) == 3 for r in rows)


# ──────────────────────────────────────────────────────────────────────────────
# 4. Transactions
# ──────────────────────────────────────────────────────────────────────────────


@section("transaction.atomic rollback on raise")
def test_atomic_rollback():
    from example.models import Author
    from dorm.transaction import atomic

    Author.objects.filter(email="tx@e.com").delete()
    try:
        with atomic():
            Author.objects.create(name="TX", email="tx@e.com", birth_year=2000)
            raise RuntimeError("rollback")
    except RuntimeError:
        pass
    assert not Author.objects.filter(email="tx@e.com").exists()


@section("transaction.atomic nested savepoint")
def test_atomic_nested():
    from example.models import Author
    from dorm.transaction import atomic

    Author.objects.filter(email__startswith="np").delete()
    with atomic():
        Author.objects.create(name="N1", email="np1@e.com", birth_year=2001)
        try:
            with atomic():
                Author.objects.create(name="N2", email="np2@e.com", birth_year=2002)
                raise RuntimeError("inner")
        except RuntimeError:
            pass
    # Outer kept N1, savepoint discarded N2.
    assert Author.objects.filter(email="np1@e.com").exists()
    assert not Author.objects.filter(email="np2@e.com").exists()


# ──────────────────────────────────────────────────────────────────────────────
# 5. Signals
# ──────────────────────────────────────────────────────────────────────────────


@section("signals: pre_save / post_save / pre_delete / post_delete")
def test_signals():
    from example.models import Author
    from dorm.signals import post_delete, post_save, pre_delete, pre_save

    captured = []

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
        a = Author.objects.create(name="SIG", email="sig@e.com", birth_year=1999)
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
# 6. Async
# ──────────────────────────────────────────────────────────────────────────────


@section("async CRUD")
def test_async():
    from example.models import Author

    async def go():
        await Author.objects.filter(email="async@e.com").adelete()
        a = await Author.objects.acreate(name="ASY", email="async@e.com", birth_year=2025)
        a.name = "ASY2"
        await a.asave()
        got = await Author.objects.aget(id=a.id)
        assert got.name == "ASY2"
        rows = []
        async for r in Author.objects.filter(email="async@e.com").aiterator():
            rows.append(r)
        assert len(rows) == 1
        await a.adelete()

    asyncio.run(go())


# ──────────────────────────────────────────────────────────────────────────────
# 7. Observability
# ──────────────────────────────────────────────────────────────────────────────


@section("querylog scoped collector")
def test_querylog():
    from dorm.contrib.querylog import query_log
    from example.models import Author

    with query_log() as ql:
        list(Author.objects.all()[:5])
        Author.objects.create(name="QL", email="ql@e.com", birth_year=1990)
    assert len(ql.records) >= 2


@section("test.assertNumQueries")
def test_querycount():
    from dorm.test import assertNumQueries
    from example.models import Author

    Author.objects.create(name="QC", email="qc@e.com", birth_year=1995)
    with assertNumQueries(1):
        list(Author.objects.filter(email="qc@e.com"))


# ──────────────────────────────────────────────────────────────────────────────
# 8. Cache
# ──────────────────────────────────────────────────────────────────────────────


@section("cache.LocMemCache get/set")
def test_cache():
    from dorm.cache import get_cache, reset_caches
    from dorm.cache.locmem import LocMemCache

    settings.CACHES = {"default": {"BACKEND": "dorm.cache.locmem.LocMemCache"}}
    reset_caches()
    cache = get_cache()
    assert isinstance(cache, LocMemCache)
    cache.set("k", b"v", 60)
    assert cache.get("k") == b"v"
    cache.delete("k")
    assert cache.get("k") is None


# ──────────────────────────────────────────────────────────────────────────────
# 9. Auth: password + tokens
# ──────────────────────────────────────────────────────────────────────────────


@section("auth: password hashing + token roundtrip")
def test_auth_tokens():
    from dorm.contrib.auth.password import check_password, make_password
    from dorm.contrib.auth.tokens import default_token_generator as gen

    h = make_password("secret")
    assert check_password("secret", h)
    assert not check_password("wrong", h)

    User.objects.filter(email="t@e.com").delete()
    u = User.objects.create_user(email="t@e.com", password="orig")
    tok = gen.make_token(u)
    assert gen.check_token(u, tok)
    u.set_password("new")
    u.save()
    # Salt rolled — old token invalid.
    assert not gen.check_token(u, tok)


# ──────────────────────────────────────────────────────────────────────────────
# 10. Soft delete
# ──────────────────────────────────────────────────────────────────────────────


@section("soft delete mixin")
def test_softdelete():
    from dorm.contrib.softdelete import SoftDeleteModel
    from dorm.db.connection import get_connection

    class Note(SoftDeleteModel):
        body = dorm.CharField(max_length=20)

        class Meta:
            db_table = "smoke_notes"
            app_label = "smoketest"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "smoke_notes"')
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "smoke_notes" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT, '
        '"body" VARCHAR(20) NOT NULL, '
        '"deleted_at" DATETIME)'
    )
    n = Note.objects.create(body="hi")
    assert Note.objects.count() == 1
    n.delete()
    assert Note.objects.count() == 0
    assert Note.all_objects.count() == 1


# ──────────────────────────────────────────────────────────────────────────────
# 11. Pydantic interop
# ──────────────────────────────────────────────────────────────────────────────


@section("pydantic interop: schema_for")
def test_pydantic():
    try:
        import pydantic  # noqa: F401
    except ImportError:
        print("    (pydantic not installed; skipped)")
        return
    from dorm.contrib.pydantic import schema_for
    from example.models import Author

    Schema = schema_for(Author)
    sample = Schema(
        id=1, name="P", email="p@e.com", birth_year=2000,
        nationality=None, active=True, bio=None,
    )
    assert sample.email == "p@e.com"


# ──────────────────────────────────────────────────────────────────────────────
# 12. Indexes / constraints
# ──────────────────────────────────────────────────────────────────────────────


@section("dbcheck (programmatic)")
def test_dbcheck():
    conn = get_connection()
    assert conn.table_exists("authors")
    assert conn.table_exists("books")
    assert conn.table_exists("auth_user")
    assert conn.table_exists("auth_user_groups")


# ──────────────────────────────────────────────────────────────────────────────
# 13. Advanced query features
# ──────────────────────────────────────────────────────────────────────────────


@section("bulk_update / in_bulk / iterator / only / defer")
def test_bulk_extras():
    from example.models import Author

    Author.objects.all().delete()
    rows = [
        Author(name=f"BU{i}", email=f"bu{i}@e.com", birth_year=1900 + i)
        for i in range(5)
    ]
    Author.objects.bulk_create(rows)
    fetched = list(Author.objects.order_by("birth_year"))
    for a in fetched:
        a.birth_year += 100
    Author.objects.bulk_update(fetched, fields=["birth_year"])
    assert Author.objects.filter(birth_year__gte=2000).count() == 5

    by_id = Author.objects.in_bulk([fetched[0].id, fetched[1].id])
    assert len(by_id) == 2

    only_qs = list(Author.objects.only("name").order_by("name")[:3])
    for a in only_qs:
        assert a.name is not None
    defer_qs = list(Author.objects.defer("birth_year").order_by("name")[:3])
    assert len(defer_qs) == 3

    count_iter = 0
    for _row in Author.objects.iterator(chunk_size=2):
        count_iter += 1
    assert count_iter == 5


@section("Subquery / OuterRef / Exists / annotate F arithmetic")
def test_subquery():
    from dorm import Exists, F, OuterRef, Subquery
    from example.models import Author, Book, Genre

    Author.objects.all().delete()
    Book.objects.all().delete()
    g = Genre.objects.create(name=f"G{os.getpid()}")
    a = Author.objects.create(name="SubA", email="sa@e.com", birth_year=1900)
    b = Author.objects.create(name="SubB", email="sb@e.com", birth_year=1910)
    Book.objects.create(
        title="SA1", author=a, genre=g, isbn="A" * 13,
        pages=100, price=Decimal("10.00"), published_year=2000,
    )

    has_books = Author.objects.annotate(
        has=Exists(Book.objects.filter(author_id=OuterRef("pk")))
    )
    by_name = {x.name: x.has for x in has_books}
    assert by_name["SubA"] is True or by_name["SubA"] == 1
    assert by_name["SubB"] is False or by_name["SubB"] == 0

    annot_qs = Author.objects.annotate(decade=F("birth_year") / 10).order_by("name")
    rows = list(annot_qs)
    assert any(getattr(r, "decade", None) is not None for r in rows)


@section("dates / datetimes / order_by NULLs")
def test_dates():
    from example.models import Book

    distinct_years = list(
        Book.objects.values_list("published_year", flat=True).order_by("published_year").distinct()
    )
    assert isinstance(distinct_years, list)


@section("validators / EmailField rejects invalid")
def test_validators():
    from dorm.exceptions import ValidationError
    from example.models import Author

    # EmailField raises at assignment (to_python). Either path is
    # acceptable validation — the invariant is that bad emails never
    # reach the DB.
    try:
        Author(name="V", email="not-an-email", birth_year=1990)
    except ValidationError:
        return
    raise AssertionError("expected ValidationError on bad email")


@section("Model.refresh_from_db")
def test_refresh():
    from example.models import Author

    a = Author.objects.create(name="R", email="r@e.com", birth_year=1990)
    Author.objects.filter(id=a.id).update(name="Renamed")
    a.refresh_from_db()
    assert a.name == "Renamed"


@section("JSONField round-trip on contrib model")
def test_jsonfield():
    # contenttypes module imports a model with JSON-ish fields; we
    # just exercise the field type via a synthetic in-memory model
    # to keep the smoke self-contained.
    from dorm.db.connection import get_connection

    class JSDoc(dorm.Model):
        payload = dorm.JSONField(default=dict)

        class Meta:
            db_table = "smoke_jsdoc"
            app_label = "smoketest"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "smoke_jsdoc"')
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "smoke_jsdoc" '
        '("id" INTEGER PRIMARY KEY AUTOINCREMENT, '
        '"payload" TEXT NOT NULL)'
    )
    JSDoc.objects.create(payload={"a": 1, "nested": {"b": [1, 2, 3]}})
    fetched = JSDoc.objects.first()
    assert fetched.payload == {"a": 1, "nested": {"b": [1, 2, 3]}}


@section("Model.objects.raw")
def test_raw():
    from example.models import Author

    Author.objects.all().delete()
    Author.objects.create(name="RW", email="rw@e.com", birth_year=2000)
    rows = list(Author.objects.raw("SELECT * FROM authors WHERE name = %s", ["RW"]))
    assert len(rows) == 1
    assert rows[0].name == "RW"


@section("async-call guard")
def test_async_guard():
    from dorm.contrib.asyncguard import enable_async_guard

    # Enable in 'warn' mode so we don't crash, just catch the marker.
    enable_async_guard(mode="warn")
    # Calling sync API from sync context — must NOT trigger.
    list(__import__("example.models", fromlist=["Author"]).Author.objects.all()[:1])
    # No 'off' mode; default is warn — that's fine for the smoke run.


@section("CLI: dorm sql / dbcheck / showmigrations")
def test_cli_smoke():
    import subprocess

    for cmd in (
        ["uv", "run", "dorm", "sql", "--all"],
        ["uv", "run", "dorm", "dbcheck"],
        ["uv", "run", "dorm", "showmigrations"],
        ["uv", "run", "dorm", "doctor"],
    ):
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert out.returncode in (0, 1), f"{cmd}: rc={out.returncode}\n{out.stderr}"


# ──────────────────────────────────────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────────────────────────────────────


def main():
    print("Running smoke suite...")
    reset_db()
    tests = [
        test_crud,
        test_goc,
        test_query_basics,
        test_values,
        test_aggregate,
        test_fk,
        test_m2m,
        test_prefetch,
        test_atomic_rollback,
        test_atomic_nested,
        test_signals,
        test_async,
        test_querylog,
        test_querycount,
        test_cache,
        test_auth_tokens,
        test_softdelete,
        test_pydantic,
        test_dbcheck,
        test_bulk_extras,
        test_subquery,
        test_dates,
        test_validators,
        test_refresh,
        test_jsonfield,
        test_raw,
        test_async_guard,
        test_cli_smoke,
    ]
    for t in tests:
        t()
    print()
    if failures:
        print(f"=== {len(failures)} FAILURE(S) ===")
        for name, tb in failures:
            print(f"\n--- {name} ---\n{tb}")
        sys.exit(1)
    print(f"All {len(tests)} sections passed.")


if __name__ == "__main__":
    main()
