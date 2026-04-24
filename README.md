# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

## Features

- **Same API as Django ORM** — `filter`, `exclude`, `get`, `create`, `update`, `delete`, `Q`, `F`, aggregations, slicing...
- **Native async** — every method has an `a*` variant: `acreate`, `aget`, `aupdate`, `adelete`...
- **SQLite** (sync via `sqlite3`, async via `aiosqlite`)
- **PostgreSQL** (sync/async via `psycopg`)
- **Migration system** — `makemigrations` / `migrate` just like Django
- **CLI** — `dorm` command to manage migrations and open a shell (IPython auto-detected)

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

# For PostgreSQL, connection pool size can be tuned (optional):
# DATABASES = {
#     "default": {
#         "ENGINE": "postgresql",
#         "NAME": "my_database",
#         "USER": "postgres",
#         "PASSWORD": "secret",
#         "HOST": "localhost",
#         "PORT": 5432,
#         "MIN_POOL_SIZE": 1,
#         "MAX_POOL_SIZE": 10,
#     }
# }

INSTALLED_APPS = ["myapp"]
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
# Returns dicts
rows = Author.objects.values("name", "age")
# [{"name": "Alice", "age": 30}, ...]

# Returns tuples
pairs = Author.objects.values_list("name", "age")
# [("Alice", 30), ...]

# flat=True with a single field
names = Author.objects.values_list("name", flat=True)
# ["Alice", "Bob", ...]
```

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

# List every app whose models should be tracked.
# Use dotted paths for nested packages.
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

# Apply pending migrations
dorm migrate

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

### Custom migrations with `RunSQL` / `RunPython`

```python
# myapp/migrations/0003_custom.py
from dorm.migrations.operations import RunSQL, RunPython

dependencies = []

operations = [
    RunSQL(
        sql="ALTER TABLE authors ADD COLUMN score INTEGER DEFAULT 0",
        reverse_sql="ALTER TABLE authors DROP COLUMN score",
    ),
    RunPython(
        code=lambda app_label, registry: print("Migration executed"),
    ),
]
```

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

---

## Dependencies

| Extra | Package | Purpose |
|---|---|---|
| `sqlite` | `aiosqlite` | Async SQLite |
| `postgresql` | `psycopg[binary,pool]` | Sync/Async PostgreSQL with connection pool |

## License

MIT
