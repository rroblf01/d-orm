"""End-to-end test of d-orm."""
import asyncio
import os
import tempfile

import dorm

# Use a file-based SQLite DB so that both sync and async share the same data
_db_file = os.path.join(tempfile.mkdtemp(), "test_dorm.db")

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "sqlite",
            "NAME": _db_file,
        }
    },
    INSTALLED_APPS=["test_app"],
)


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()
    email = dorm.EmailField(null=True, blank=True)
    is_active = dorm.BooleanField(default=True)

    class Meta:
        db_table = "authors"
        ordering = ["name"]


class Book(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    pages = dorm.IntegerField(default=0)
    published = dorm.BooleanField(default=False)

    class Meta:
        db_table = "books"


def setup_tables():
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()

    # Create tables manually for this test (no migration files)
    author_cols = []
    for field in Author._meta.fields:
        if field.db_type(conn):
            author_cols.append(_field_to_column_sql(field.name, field, conn))
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "authors" (\n  '
        + ",\n  ".join(filter(None, author_cols))
        + "\n)"
    )

    book_cols = []
    for field in Book._meta.fields:
        if field.db_type(conn):
            book_cols.append(_field_to_column_sql(field.name, field, conn))
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "books" (\n  '
        + ",\n  ".join(filter(None, book_cols))
        + "\n)"
    )
    print("Tables created OK")


# ── Sync tests ─────────────────────────────────────────────────────────────────

def test_sync():
    print("\n=== SYNC TESTS ===")

    # create
    alice = Author.objects.create(name="Alice", age=30, email="alice@example.com")
    bob = Author.objects.create(name="Bob", age=25)
    carol = Author.objects.create(name="Carol", age=35, is_active=False)
    print(f"Created: {alice}, {bob}, {carol}")

    # get
    found = Author.objects.get(name="Alice")
    assert found.name == "Alice", found.name
    print(f"get(): {found}")

    # filter
    adults = Author.objects.filter(age__gte=30)
    names = sorted(a.name for a in adults)
    assert names == ["Alice", "Carol"], names
    print(f"filter(age__gte=30): {names}")

    # exclude
    active = Author.objects.exclude(is_active=False)
    names = sorted(a.name for a in active)
    assert names == ["Alice", "Bob"], names
    print(f"exclude(is_active=False): {names}")

    # Q objects
    from dorm import Q
    q_result = Author.objects.filter(Q(age__lt=30) | Q(name="Carol"))
    names = sorted(a.name for a in q_result)
    assert names == ["Bob", "Carol"], names
    print(f"Q(age<30 | name=Carol): {names}")

    # count
    cnt = Author.objects.count()
    assert cnt == 3, cnt
    print(f"count(): {cnt}")

    # exists
    assert Author.objects.filter(name="Alice").exists()
    assert not Author.objects.filter(name="Nobody").exists()
    print("exists(): OK")

    # order_by
    ordered = list(Author.objects.order_by("age"))
    assert [a.name for a in ordered] == ["Bob", "Alice", "Carol"]
    print(f"order_by('age'): {[a.name for a in ordered]}")

    # first / last
    first = Author.objects.order_by("age").first()
    assert first is not None
    assert first.name == "Bob", first.name
    last = Author.objects.order_by("age").last()
    assert last is not None
    assert last.name == "Carol", last.name
    print(f"first/last by age: {first.name} / {last.name}")

    # values
    vals = list(Author.objects.values("name", "age").filter(age__gte=30))
    assert len(vals) == 2
    print(f"values('name','age'): {vals}")

    # values_list
    ids = list(Author.objects.values_list("name", flat=True).order_by("name"))
    assert ids == ["Alice", "Bob", "Carol"], ids
    print(f"values_list(flat=True): {ids}")

    # aggregate
    result = Author.objects.aggregate(total=dorm.Count("id"), avg_age=dorm.Avg("age"))
    assert result["total"] == 3
    print(f"aggregate(count, avg): {result}")

    # update
    updated = Author.objects.filter(name="Bob").update(age=26)
    assert updated == 1
    bob_updated = Author.objects.get(name="Bob")
    assert bob_updated.age == 26, bob_updated.age
    print(f"update(): Bob age now {bob_updated.age}")

    # get_or_create
    dana, created = Author.objects.get_or_create(name="Dana", defaults={"age": 28})
    assert created
    dana2, created2 = Author.objects.get_or_create(name="Dana", defaults={"age": 28})
    assert not created2
    print(f"get_or_create(): created={created}, 2nd={not created2}")

    # delete
    cnt_before = Author.objects.count()
    n, _ = Author.objects.filter(name="Dana").delete()
    assert n == 1
    assert Author.objects.count() == cnt_before - 1
    print(f"delete(): removed Dana, now {Author.objects.count()} authors")

    # save / refresh
    alice.age = 31
    alice.save()
    alice.refresh_from_db()
    assert alice.age == 31
    print(f"save/refresh_from_db(): alice.age={alice.age}")

    # slicing
    first2 = list(Author.objects.order_by("name")[:2])
    assert len(first2) == 2
    print(f"slicing [:2]: {[a.name for a in first2]}")

    # icontains lookup
    found_c = list(Author.objects.filter(name__icontains="ali"))
    assert len(found_c) == 1 and found_c[0].name == "Alice"
    print(f"icontains 'ali': {found_c[0].name}")

    # in lookup
    found_in = list(Author.objects.filter(name__in=["Alice", "Bob"]))
    assert len(found_in) == 2
    print(f"__in lookup: {len(found_in)} results")

    # Books with FK
    book1 = Book.objects.create(title="Python Deep Dive", author_id=alice.pk, pages=400, published=True)
    book2 = Book.objects.create(title="Async Patterns", author_id=bob.pk, pages=250)
    print(f"Books created: {book1}, {book2}")

    print("\n✓ All sync tests passed!")


# ── Async tests ────────────────────────────────────────────────────────────────

async def test_async():
    print("\n=== ASYNC TESTS ===")


    # acreate
    eve = await Author.objects.acreate(name="Eve", age=22)
    print(f"acreate(): {eve}")

    # aget
    found = await Author.objects.aget(name="Eve")
    assert found.name == "Eve"
    print(f"aget(): {found}")

    # acount
    cnt = await Author.objects.acount()
    print(f"acount(): {cnt}")

    # aexists
    exists = await Author.objects.filter(name="Eve").aexists()
    assert exists
    print(f"aexists(): {exists}")

    # afirst / alast
    first = await Author.objects.order_by("age").afirst()
    assert first is not None
    print(f"afirst() by age: {first.name}")

    # aupdate
    n = await Author.objects.filter(name="Eve").aupdate(age=23)
    assert n == 1
    eve_updated = await Author.objects.aget(name="Eve")
    assert eve_updated.age == 23
    print(f"aupdate(): Eve age now {eve_updated.age}")

    # aget_or_create
    frank, created = await Author.objects.aget_or_create(name="Frank", defaults={"age": 40})
    assert created
    print(f"aget_or_create(): created={created}")

    # adelete
    n, _ = await Author.objects.filter(name="Frank").adelete()
    assert n == 1
    print("adelete(): removed Frank")

    # async iteration
    names = []
    async for author in Author.objects.filter(age__gte=20).order_by("name"):
        names.append(author.name)
    print(f"async for: {names}")

    # asave
    eve_updated.age = 24
    await eve_updated.asave()
    refreshed = await Author.objects.aget(name="Eve")
    assert refreshed.age == 24
    print(f"asave(): Eve age {refreshed.age}")

    # adelete instance
    await eve_updated.adelete()
    assert not await Author.objects.filter(name="Eve").aexists()
    print("adelete() instance: OK")

    print("\n✓ All async tests passed!")


# ── Migration system test ──────────────────────────────────────────────────────

def test_migrations():
    print("\n=== MIGRATION SYSTEM TEST ===")
    import tempfile
    from pathlib import Path
    from dorm.migrations.autodetector import MigrationAutodetector
    from dorm.migrations.state import ProjectState
    from dorm.migrations.writer import write_migration

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"

        # Detect changes (empty from_state → create models)
        from_state = ProjectState()
        to_state = ProjectState()
        to_state.add_model("myapp", "Author", {
            f.name: f for f in Author._meta.fields
        }, {"db_table": "authors_v2"})

        detector = MigrationAutodetector(from_state, to_state)
        changes = detector.changes("myapp")
        assert "myapp" in changes
        print(f"Detected changes: {[op.describe() for op in changes['myapp']]}")

        path = write_migration("myapp", mig_dir, 1, changes["myapp"])
        print(f"Written migration: {path.name}")
        print("Content:\n" + path.read_text()[:500])

        print("\n✓ Migration system tests passed!")


if __name__ == "__main__":
    setup_tables()
    test_sync()
    asyncio.run(test_async())
    test_migrations()
    print("\n\n✅ ALL TESTS PASSED!")
