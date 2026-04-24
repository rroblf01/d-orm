# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

## Features

- **Same API as Django ORM** — `filter`, `exclude`, `get`, `create`, `update`, `delete`, `Q`, `F`, aggregations, slicing...
- **Native async** — every method has an `a*` variant: `acreate`, `aget`, `aupdate`, `adelete`...
- **Atomic transactions** — `dorm.transaction.atomic()` / `aatomic()` with automatic savepoint nesting
- **SQLite** (sync via `sqlite3`, async via `aiosqlite`)
- **PostgreSQL** (sync/async via `psycopg`, connection pool via `psycopg-pool`)
- **Migration system** — `makemigrations` / `migrate` / rollback, `RunSQL` / `RunPython` with reverse hooks
- **CLI** — `dorm` command to manage migrations and open a shell (IPython auto-detected)
- **Thread-safe** — connections are safe to share across threads; async connections are coroutine-safe
- **Relationship loading** — `select_related()` (SQL JOIN) and `prefetch_related()` (batch query) to avoid N+1 queries
- **Partial loading** — `only()` / `defer()` to fetch a subset of columns
- **Convenience** — `get_or_none()` / `aget_or_none()` returns `None` instead of raising `DoesNotExist`
- **Efficient bulk inserts** — `bulk_create()` uses a single multi-row INSERT per batch

---

## Installation

```bash
# SQLite support
pip install "djanorm[sqlite]"

# PostgreSQL support
pip install "djanorm[postgresql]"

# Both
pip install "djanorm[sqlite,postgresql]"

# With uv
uv add "djanorm[sqlite]"
uv add "djanorm[postgresql]"
```

---

## Setup

There are two ways to configure djanorm depending on how you use it.

### Project with migrations (recommended)

Create a `settings.py` next to your app packages. The `dorm` CLI reads it automatically — you never call `dorm.configure()` yourself.

```python
# settings.py
DATABASES = {
    "default": {
        "ENGINE": "sqlite",   # or "postgresql"
        "NAME": "db.sqlite3",
    }
}
```

That's it. dorm scans the directory of `settings.py` recursively and registers every Python package that contains a `models.py` — no `INSTALLED_APPS` needed.

If you prefer explicit control (e.g. to exclude certain packages, or when app packages live outside the settings directory), you can declare them manually:

```python
# settings.py — explicit override, optional
INSTALLED_APPS = ["blog", "shop", "shop.payments"]
```

For PostgreSQL, pool size and driver options can be tuned:

```python
# DATABASES = {
#     "default": {
#         "ENGINE": "postgresql",
#         "NAME": "my_database",
#         "USER": "postgres",
#         "PASSWORD": "secret",
#         "HOST": "localhost",
#         "PORT": 5432,
#         "MIN_POOL_SIZE": 1,   # default
#         "MAX_POOL_SIZE": 10,  # default
#         "OPTIONS": {
#             "sslmode": "require",    # passed directly to psycopg
#             "connect_timeout": 10,
#         },
#     }
# }
```

Then run migrations and open a shell:

```bash
dorm makemigrations
dorm migrate
dorm shell
```

See the [Migrations](#migrations) section for the full CLI reference.

### Programmatic use (scripts and libraries)

If you are using djanorm in a standalone script or embedding it inside another framework — without the `dorm` CLI — call `dorm.configure()` at startup instead of using a `settings.py` file:

```python
import dorm

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "sqlite",   # or "postgresql"
            "NAME": "db.sqlite3",
        }
    }
)
```

For PostgreSQL:

```python
dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "postgresql",
            "NAME": "my_database",
            "USER": "postgres",
            "PASSWORD": "secret",
            "HOST": "localhost",
            "PORT": 5432,
            # Connection pool size (optional, defaults shown)
            "MIN_POOL_SIZE": 1,
            "MAX_POOL_SIZE": 10,
            # Any extra key under OPTIONS is forwarded verbatim to psycopg
            "OPTIONS": {
                "sslmode": "require",
                "connect_timeout": 10,
            },
        }
    }
)
```

---

## Defining models

```python
import dorm

class Author(dorm.Model):
    name     = dorm.CharField(max_length=100)
    email    = dorm.EmailField(unique=True)
    age      = dorm.IntegerField()
    bio      = dorm.TextField(null=True, blank=True)
    active   = dorm.BooleanField(default=True)
    joined   = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]


class Book(dorm.Model):
    title     = dorm.CharField(max_length=200)
    author    = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    pages     = dorm.IntegerField(default=0)
    published = dorm.BooleanField(default=False)


```

### Available fields

| Field | Description |
|---|---|
| `AutoField` / `BigAutoField` | Auto-increment integer (default PK) |
| `CharField(max_length=)` | Variable-length string |
| `TextField` | Unlimited text |
| `IntegerField` / `BigIntegerField` / `SmallIntegerField` | Integers |
| `FloatField` | Floating point number |
| `DecimalField(max_digits=, decimal_places=)` | Precise decimal |
| `BooleanField` | Boolean |
| `DateField` / `TimeField` / `DateTimeField` | Date and time |
| `EmailField` | String with email validation |
| `URLField` / `SlugField` | Specialised strings |
| `UUIDField` | UUID |
| `JSONField` | JSON (JSONB on PostgreSQL) |
| `ForeignKey(to, on_delete=)` | Foreign key |
| `OneToOneField(to, on_delete=)` | One-to-one relation |
| `ManyToManyField(to)` | Many-to-many relation |

---

## Synchronous operations

### Create

```python
# Create and save in one call
author = Author.objects.create(name="Alice", email="alice@example.com", age=30)


# Instantiate then save separately
author = Author(name="Bob", email="bob@example.com", age=25)
author.save()

# get_or_create — returns (instance, created)
author, created = Author.objects.get_or_create(
    email="carol@example.com",
    defaults={"name": "Carol", "age": 28},
)

# update_or_create
author, created = Author.objects.update_or_create(
    email="carol@example.com",
    defaults={"age": 29},
)

# Create many at once
authors = Author.objects.bulk_create([
    Author(name="Dave", email="dave@example.com", age=22),
    Author(name="Eve",  email="eve@example.com",  age=31),
])
```

### Query

```python
# All records
authors = Author.objects.all()

# Filter
adults   = Author.objects.filter(age__gte=18)
alices   = Author.objects.filter(name="Alice")
no_email = Author.objects.filter(email__isnull=True)

# Chain filters
result = (
    Author.objects
    .filter(active=True)
    .filter(age__gte=20)
    .order_by("-age")
)

# Exclude
inactive = Author.objects.exclude(active=True)

# Get a single record
author = Author.objects.get(email="alice@example.com")  # raises DoesNotExist or MultipleObjectsReturned

# Get or None — returns None instead of raising DoesNotExist
author = Author.objects.get_or_none(email="alice@example.com")
author = Author.objects.filter(active=True).get_or_none(name="Alice")

# First / last
first = Author.objects.order_by("age").first()
last  = Author.objects.order_by("age").last()

# Slicing (like Python lists)
top3  = Author.objects.order_by("-age")[:3]
page2 = Author.objects.order_by("name")[10:20]
```

### Lookups

```python
# Comparison
Author.objects.filter(age__exact=30)        # equal (default)
Author.objects.filter(age__gt=30)           # greater than
Author.objects.filter(age__gte=30)          # greater than or equal
Author.objects.filter(age__lt=30)           # less than
Author.objects.filter(age__lte=30)          # less than or equal
Author.objects.filter(age__range=(20, 30))  # between two values

# Strings
Author.objects.filter(name__contains="li")      # contains
Author.objects.filter(name__icontains="li")     # contains (case-insensitive)
Author.objects.filter(name__startswith="Al")    # starts with
Author.objects.filter(name__endswith="ce")      # ends with
Author.objects.filter(name__iexact="alice")     # equal (case-insensitive)

# Null
Author.objects.filter(bio__isnull=True)
Author.objects.filter(bio__isnull=False)

# Set membership
Author.objects.filter(name__in=["Alice", "Bob"])

# Dates
Author.objects.filter(joined__year=2024)
Author.objects.filter(joined__month=6)
```

### Q objects — complex queries

```python
from dorm import Q

# OR
Author.objects.filter(Q(age__lt=18) | Q(age__gt=65))

# Explicit AND
Author.objects.filter(Q(active=True) & Q(age__gte=18))

# NOT
Author.objects.filter(~Q(name="Admin"))

# Combined
Author.objects.filter(
    Q(active=True) & (Q(age__lt=18) | Q(age__gt=65))
)
```

### Count and existence

```python
total    = Author.objects.count()
filtered = Author.objects.filter(active=True).count()

exists = Author.objects.filter(email="alice@example.com").exists()  # True / False
```

### Values and value lists

```python
# Returns dicts — specific fields
rows = Author.objects.values("name", "age")
# [{"name": "Alice", "age": 30}, ...]

# No arguments → all fields as dicts
rows = Author.objects.values()
# [{"id": 1, "name": "Alice", "age": 30, ...}, ...]

# Returns tuples
pairs = Author.objects.values_list("name", "age")
# [("Alice", 30), ...]

# flat=True — single field only (raises ValueError with more than one field)
names = Author.objects.values_list("name", flat=True)
# ["Alice", "Bob", ...]

# Async equivalents — return a list directly
rows  = await Author.objects.avalues("name", "age")
names = await Author.objects.avalues_list("name", flat=True)

# Also chainable with filter, order_by, etc.
rows = await Author.objects.filter(active=True).avalues("name")
```

### Partial loading — `only()` and `defer()`

Use these to fetch only the columns you need. The returned objects are full model instances; unloaded fields are `None`.

```python
# only() — fetch just the listed columns (pk is always included)
authors = Author.objects.only("name", "email")
for a in authors:
    print(a.name)    # loaded
    print(a.age)     # None — not fetched

# defer() — fetch all columns except the listed ones
authors = Author.objects.defer("bio")
for a in authors:
    print(a.name)    # loaded
    print(a.bio)     # None — not fetched

# Both are chainable with filter, order_by, etc.
result = Author.objects.filter(active=True).only("name").order_by("name")
```

### Relationship loading — `select_related()` and `prefetch_related()`

Both methods avoid the N+1 query problem when accessing FK fields on a queryset.

`select_related()` resolves the relation in a single SQL query using a LEFT OUTER JOIN:

```python
# One query: SELECT books.*, author.* FROM books LEFT OUTER JOIN authors ...
books = Book.objects.select_related("author")
for book in books:
    print(book.author.name)  # no extra DB hit
```

`prefetch_related()` runs a second batch query and stitches the results in Python:

```python
# Two queries: one for books, one bulk fetch for all related authors
books = Book.objects.filter(published=True).prefetch_related("author")
for book in books:
    print(book.author.name)  # no extra DB hit
```

Choose `select_related` for forward FK/OneToOne fields when you always need the related object. Use `prefetch_related` when doing large bulk loads or when the JOIN would produce too many duplicated columns.

### Aggregations

```python
from dorm import Count, Sum, Avg, Max, Min

result = Author.objects.aggregate(
    total    = Count("id"),
    avg_age  = Avg("age"),
    max_age  = Max("age"),
    min_age  = Min("age"),
    age_sum  = Sum("age"),
)
# {"total": 42, "avg_age": 29.5, ...}

# On a filtered subset
result = Author.objects.filter(active=True).aggregate(total=Count("id"))
```

### Update and delete

```python
# Update multiple records
n = Author.objects.filter(active=False).update(active=True)  # returns row count

# Delete multiple records
count, detail = Author.objects.filter(age__lt=18).delete()

# Update an instance
author.age = 31
author.save()

# Delete an instance
author.delete()

# Reload from the database
author.refresh_from_db()

# Update only specific fields
author.save(update_fields=["age", "bio"])
```

### F expressions — reference columns

```python
from dorm import F

# Increment age by 1 without reading the value into Python
Author.objects.filter(active=True).update(age=F("age") + 1)
```

---

## Asynchronous operations

Every sync method has an async counterpart prefixed with `a`:

```python
import asyncio
import dorm

async def main():
    # Create
    author = await Author.objects.acreate(name="Alice", email="alice@example.com", age=30)

    # Get
    author = await Author.objects.aget(email="alice@example.com")

    # Get or None
    author = await Author.objects.aget_or_none(email="missing@example.com")  # None

    # get_or_create / update_or_create
    author, created = await Author.objects.aget_or_create(
        email="bob@example.com",
        defaults={"name": "Bob", "age": 25},
    )

    # Count / existence
    total  = await Author.objects.acount()
    exists = await Author.objects.filter(active=True).aexists()

    # First / last
    first = await Author.objects.order_by("age").afirst()
    last  = await Author.objects.order_by("age").alast()

    # Update
    n = await Author.objects.filter(active=False).aupdate(active=True)

    # Delete
    count, _ = await Author.objects.filter(age__lt=18).adelete()

    # Save / delete instance
    author.age = 31
    await author.asave()
    await author.adelete()

    # Reload from DB
    await author.arefresh_from_db()

    # Async iteration
    async for author in Author.objects.filter(active=True).order_by("name"):
        print(author.name)

    # Bulk async
    objs = [Author(name=f"User{i}", email=f"u{i}@x.com", age=20) for i in range(100)]
    await Author.objects.abulk_create(objs)

    # Async aggregation
    result = await Author.objects.aaggregate(total=dorm.Count("id"), avg=dorm.Avg("age"))

asyncio.run(main())
```

---

## Transactions

Use `dorm.transaction.atomic()` (sync) or `dorm.transaction.aatomic()` (async) to wrap one or more operations in a database transaction. On success the transaction is committed; on exception it is rolled back.

```python
import dorm

# Sync
with dorm.transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="My Book", author_id=author.pk)

# Async
async with dorm.transaction.aatomic():
    author = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="My Book", author_id=author.pk)
```

### Savepoint nesting

Nested calls automatically use savepoints. An inner failure rolls back only the inner block; the outer transaction can still commit.

```python
with dorm.transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)

    try:
        with dorm.transaction.atomic():      # creates SAVEPOINT
            Book.objects.create(title="Bad Book", author_id=author.pk)
            raise ValueError("something went wrong")
    except ValueError:
        pass  # inner block rolled back to savepoint; author still present

# only Alice is committed, no Book
```

The same nesting behaviour works with `aatomic()`:

```python
async with dorm.transaction.aatomic():
    author = await Author.objects.acreate(name="Alice", age=30)
    try:
        async with dorm.transaction.aatomic():
            await Book.objects.acreate(title="Bad Book", author_id=author.pk)
            raise ValueError("something went wrong")
    except ValueError:
        pass
```

### `get_or_create` / `update_or_create` are atomic

Both methods run inside an implicit transaction and handle concurrent inserts safely: if another thread or coroutine creates the same row first, they catch the `IntegrityError` and return the existing object instead of raising.

```python
# Safe to call concurrently — will never raise IntegrityError
author, created = Author.objects.get_or_create(
    email="alice@example.com",
    defaults={"name": "Alice", "age": 30},
)
```

---

## Migrations

### What is an app?

An **app** is a Python package (a directory with `__init__.py`) that groups related models together. Each app has its own `migrations/` folder so its schema changes are tracked independently.

```
myproject/
├── settings.py
├── blog/                  ← one app
│   ├── __init__.py
│   ├── models.py
│   └── migrations/
│       └── __init__.py
└── shop/                  ← another app
    ├── __init__.py
    ├── models.py
    └── migrations/
        └── __init__.py
```

### `settings.py`

```python
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "db.sqlite3",
    }
}
```

dorm automatically discovers every Python package under the `settings.py` directory that contains a `models.py`. You only need `INSTALLED_APPS` when you want to be explicit or need packages that live elsewhere:

```python
# Optional — override auto-discovery
INSTALLED_APPS = [
    "blog",
    "shop",
    "shop.payments",   # sub-package of shop
]
```

### CLI commands

`--settings` is **optional**. dorm resolves the settings module in this order:

1. `--settings=<module>` flag
2. `DORM_SETTINGS` environment variable
3. `settings` (default — looks for `settings.py` in the current directory)

```bash
# Detect model changes and generate migration files
dorm makemigrations

# Apply all pending migrations
dorm migrate

# Apply migrations for a specific app only
dorm migrate blog

# Show migration status ([ ] pending, [X] applied)
dorm showmigrations

# Interactive shell with all models pre-loaded
# Uses IPython automatically if installed, otherwise falls back to the
# standard Python shell. IPython enables top-level await, so async ORM
# methods work directly without wrapping them in asyncio.run().
dorm shell

# Override settings explicitly when needed
dorm makemigrations --settings=myproject.settings
dorm migrate --settings=myproject.settings

# Or export once and forget about it
export DORM_SETTINGS=myproject.settings
dorm makemigrations
dorm migrate
```

### Undoing migrations

`dorm migrate` detects direction automatically: if the target is before the current state it rolls back, otherwise it applies forward.

```bash
# Roll back blog to migration 0002 (undoes 0003, 0004, etc.)
dorm migrate blog 0002

# Roll back a specific migration by full name
dorm migrate blog 0002_add_email

# Undo all migrations for an app
dorm migrate blog zero
```

After a rollback the affected migrations are marked as unapplied, so `dorm migrate blog` will re-apply them later if needed.

### Empty migrations

Use `--empty` to create a blank migration file ready to be filled with `RunPython` or `RunSQL` operations:

```bash
# Creates myapp/migrations/0002_custom.py
dorm makemigrations myapp --empty

# Use --name to give it a descriptive suffix
dorm makemigrations myapp --empty --name seed_authors
# → myapp/migrations/0002_seed_authors.py
```

The generated file contains commented-out examples so you can start writing immediately:

```python
"""
Empty migration — add your RunPython / RunSQL operations below.
Generated: 2024-01-01T00:00:00+00:00
"""
from dorm.migrations.operations import RunPython, RunSQL

dependencies = []

operations = [
    # RunPython(code=forward, reverse_code=backward),
    # RunSQL(sql="UPDATE ...", reverse_sql="UPDATE ..."),
]
```

### Custom migrations with `RunSQL` / `RunPython`

Both operations accept an optional reverse that is called when the migration is rolled back.

#### `RunSQL`

```python
# myapp/migrations/0003_add_score.py
from dorm.migrations.operations import RunSQL

dependencies = []

operations = [
    RunSQL(
        sql="ALTER TABLE authors ADD COLUMN score INTEGER DEFAULT 0",
        reverse_sql="ALTER TABLE authors DROP COLUMN score",
    ),
]
```

`reverse_sql` is optional. If omitted, rolling back this migration is a no-op for that operation.

#### `RunPython`

`code` and `reverse_code` are plain Python functions that receive `(app_label, registry)`:

- `app_label` — the app being migrated (string)
- `registry` — dict mapping model name → model class, e.g. `registry["Author"]`

```python
# myapp/migrations/0004_seed_data.py
from dorm.migrations.operations import RunPython

def seed(app_label, registry):
    Author = registry["Author"]
    Author.objects.get_or_create(
        email="admin@example.com",
        defaults={"name": "Admin", "age": 0},
    )

def unseed(app_label, registry):
    Author = registry["Author"]
    Author.objects.filter(email="admin@example.com").delete()

dependencies = []

operations = [
    RunPython(code=seed, reverse_code=unseed),
]
```

`reverse_code` is optional. If omitted, rolling back this migration is a no-op for that operation.

---

## Full example

```python
import asyncio
import dorm

dorm.configure(
    DATABASES={"default": {"ENGINE": "sqlite", "NAME": "blog.db"}},
)

# — Models ─────────────────────────────────────────────────────────────────────

class Author(dorm.Model):
    name  = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    age   = dorm.IntegerField()

    class Meta:
        db_table = "authors"


class Post(dorm.Model):
    title     = dorm.CharField(max_length=200)
    body      = dorm.TextField()
    author    = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    published = dorm.BooleanField(default=False)
    views     = dorm.IntegerField(default=0)



# — Sync ───────────────────────────────────────────────────────────────────────

def sync_demo():
    alice = Author.objects.create(name="Alice", email="alice@example.com", age=30)
    bob   = Author.objects.create(name="Bob",   email="bob@example.com",   age=25)

    Post.objects.create(title="Hello World", body="...", author_id=alice.pk, published=True)
    Post.objects.create(title="Draft Post",  body="...", author_id=alice.pk)
    Post.objects.create(title="Bob's Post",  body="...", author_id=bob.pk,   published=True)

    # Query
    for post in Post.objects.filter(published=True).order_by("title"):
        print(post.title)

    # Complex filter with Q
    from dorm import Q
    result = Author.objects.filter(Q(age__gte=28) | Q(name="Bob"))

    # Aggregation
    stats = Post.objects.aggregate(
        total     = dorm.Count("id"),
        published = dorm.Count("published"),
    )

    # F expression
    Post.objects.filter(published=True).update(views=dorm.F("views") + 1)


# — Async ──────────────────────────────────────────────────────────────────────

async def async_demo():
    author = await Author.objects.aget(email="alice@example.com")

    post = await Post.objects.acreate(
        title="Async Post", body="...", author_id=author.pk, published=True
    )

    async for p in Post.objects.filter(author_id=author.pk).order_by("title"):
        print(p.title)

    stats = await Post.objects.aaggregate(total=dorm.Count("id"))

    await Post.objects.filter(published=False).adelete()


sync_demo()
asyncio.run(async_demo())
```

---

## Quick reference

| Operation | Sync | Async |
|---|---|---|
| Create | `objects.create(**kw)` | `await objects.acreate(**kw)` |
| Get one | `objects.get(**kw)` | `await objects.aget(**kw)` |
| Get or None | `objects.get_or_none(**kw)` | `await objects.aget_or_none(**kw)` |
| Filter | `objects.filter(**kw)` | `objects.filter(**kw)` + `async for` |
| First | `objects.first()` | `await objects.afirst()` |
| Last | `objects.last()` | `await objects.alast()` |
| Count | `objects.count()` | `await objects.acount()` |
| Exists | `objects.exists()` | `await objects.aexists()` |
| Update | `objects.update(**kw)` | `await objects.aupdate(**kw)` |
| Delete | `objects.delete()` | `await objects.adelete()` |
| Save instance | `instance.save()` | `await instance.asave()` |
| Delete instance | `instance.delete()` | `await instance.adelete()` |
| Reload | `instance.refresh_from_db()` | `await instance.arefresh_from_db()` |
| Get or create | `objects.get_or_create(...)` | `await objects.aget_or_create(...)` |
| Update or create | `objects.update_or_create(...)` | `await objects.aupdate_or_create(...)` |
| Bulk create | `objects.bulk_create([...])` | `await objects.abulk_create([...])` |
| Aggregate | `objects.aggregate(...)` | `await objects.aaggregate(...)` |
| Values (dicts) | `objects.values(...)` + `for` | `await objects.avalues(...)` |
| Values list | `objects.values_list(...)` + `for` | `await objects.avalues_list(...)` |
| Partial load | `objects.only("f1", "f2")` | — |
| Partial load | `objects.defer("f1", "f2")` | — |
| Eager FK load | `objects.select_related("fk")` | — |
| Batch FK load | `objects.prefetch_related("fk")` | — |

---

## Dependencies

| Extra | Package | Purpose |
|---|---|---|
| `sqlite` | `aiosqlite` | Async SQLite |
| `postgresql` | `psycopg[binary,pool]` | Sync/Async PostgreSQL with connection pool |

## License

MIT
