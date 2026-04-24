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
- **Relationship loading** — `select_related()` with nested paths and `prefetch_related()` for FK, reverse FK, and M2M
- **Partial loading** — `only()` / `defer()` to fetch a subset of columns
- **Abstract model inheritance** — share fields and Meta options across models with `abstract = True`
- **Signals** — `pre_save`, `post_save`, `pre_delete`, `post_delete` hooks
- **Validation** — `full_clean()` / `clean()` / `validate_unique()` with custom rules
- **DB functions** — `Case`/`When`, `Coalesce`, `Upper`, `Lower`, `Length`, `Concat`, `Now`, `Cast`, `Abs`
- **Set operations** — `union()`, `intersection()`, `difference()` across querysets
- **Default ordering** — `Meta.ordering` applied automatically to all queries
- **on_delete** — `CASCADE`, `PROTECT`, `SET_NULL`, `SET_DEFAULT` enforced at Python level
- **Streaming** — `iterator()` / `aiterator()` for memory-efficient row-by-row processing
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
DATABASES = {
    "default": {
        "ENGINE": "postgresql",
        "NAME": "my_database",
        "USER": "postgres",
        "PASSWORD": "secret",
        "HOST": "localhost",
        "PORT": 5432,
        "MIN_POOL_SIZE": 1,   # default
        "MAX_POOL_SIZE": 10,  # default
        "OPTIONS": {
            "sslmode": "require",    # passed directly to psycopg
            "connect_timeout": 10,
        },
    }
}
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

---

## Defining models

```python
import dorm

class Author(dorm.Model):
    name   = dorm.CharField(max_length=100)
    email  = dorm.EmailField(unique=True)
    age    = dorm.IntegerField()
    bio    = dorm.TextField(null=True, blank=True)
    active = dorm.BooleanField(default=True)
    joined = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "authors"
        ordering = ["name"]     # default sort for all queries


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

### `on_delete` options

When a referenced object is deleted, djanorm applies the `on_delete` policy **at the Python level** (not just via DB constraints):

| Constant | Behaviour |
|---|---|
| `dorm.CASCADE` | Delete related objects automatically |
| `dorm.PROTECT` | Raise `dorm.ProtectedError` if related objects exist |
| `dorm.SET_NULL` | Set the FK column to `NULL` (requires `null=True`) |
| `dorm.SET_DEFAULT` | Set the FK column to its default value |
| `dorm.DO_NOTHING` | Do nothing; rely on DB-level constraints |
| `dorm.RESTRICT` | Same as `PROTECT` but enforced only at the DB level |

```python
class Article(dorm.Model):
    author = dorm.ForeignKey(Author, on_delete=dorm.PROTECT)
    # Trying to delete an Author that has Articles raises ProtectedError

class Comment(dorm.Model):
    article = dorm.ForeignKey(Article, on_delete=dorm.CASCADE)
    # Deleting an Article also deletes all its Comments

class Profile(dorm.Model):
    author = dorm.ForeignKey(Author, on_delete=dorm.SET_NULL, null=True, blank=True)
    # Deleting an Author sets Profile.author to NULL
```

```python
from dorm import ProtectedError

try:
    author.delete()
except ProtectedError as e:
    print("Blocked:", e.protected_objects)
```

### Abstract models

Mark a model as `abstract = True` in its `Meta` to use it as a mixin. It defines no database table of its own; its fields and Meta options are inherited by concrete subclasses.

```python
class TimestampedModel(dorm.Model):
    created_at = dorm.DateTimeField(auto_now_add=True, null=True)
    updated_at = dorm.DateTimeField(auto_now=True, null=True)

    class Meta:
        abstract = True


class Post(TimestampedModel):
    title = dorm.CharField(max_length=200)
    body  = dorm.TextField()

    class Meta:
        db_table  = "posts"
        ordering  = ["-created_at"]  # overrides abstract ordering if any


class Comment(TimestampedModel):
    text = dorm.CharField(max_length=500)
    # inherits created_at and updated_at columns automatically
```

`Post._meta.fields` will include `id`, `created_at`, `updated_at`, `title`, and `body`. Meta options (`ordering`, etc.) defined on the abstract parent are inherited unless the concrete class defines its own.

### `Meta.ordering`

Set `ordering` on a model to apply a default `ORDER BY` clause to every query that doesn't call `.order_by()` explicitly:

```python
class Product(dorm.Model):
    name  = dorm.CharField(max_length=100)
    price = dorm.IntegerField()

    class Meta:
        ordering = ["price"]       # ascending
        # ordering = ["-price"]    # descending
        # ordering = ["name", "-price"]  # multiple fields
```

```python
# Automatically ordered by price ASC
products = list(Product.objects.all())

# Explicit order_by() overrides Meta.ordering
expensive_first = list(Product.objects.order_by("-price"))

# .order_by() with no arguments clears the default ordering
unordered = list(Product.objects.order_by())
```

---

## Signals

Signals let you run code automatically before or after a model is saved or deleted, without modifying the model itself.

```python
from dorm.signals import pre_save, post_save, pre_delete, post_delete

# Connect a receiver function
@post_save.connect
def on_author_saved(sender, instance, created, **kwargs):
    if created:
        print(f"New author created: {instance.name}")
    else:
        print(f"Author updated: {instance.name}")

@pre_delete.connect
def on_before_delete(sender, instance, **kwargs):
    print(f"About to delete: {instance}")
```

You can also connect to signals from a specific sender only:

```python
@post_save.connect_for(Author)
def welcome_new_author(sender, instance, created, **kwargs):
    if created:
        send_welcome_email(instance.email)
```

### Available signals

| Signal | When fired | Extra kwargs |
|---|---|---|
| `pre_save` | Before `save()` / `asave()` | `created` (bool) |
| `post_save` | After `save()` / `asave()` | `created` (bool) |
| `pre_delete` | Before `delete()` / `adelete()` | — |
| `post_delete` | After `delete()` / `adelete()` | — |

---

## Validation

### `full_clean()`

Runs all validation checks on a model instance: field-level type validation, custom business rules, and uniqueness constraints. Raises `dorm.ValidationError` on failure.

```python
author = Author(name="", age=-5, email="not-an-email")
try:
    author.full_clean()
except dorm.ValidationError as e:
    print(e.message)
```

### `clean()`

Override `clean()` on the model to add cross-field validation:

```python
class Event(dorm.Model):
    start = dorm.DateTimeField()
    end   = dorm.DateTimeField()

    def clean(self):
        if self.end <= self.start:
            raise dorm.ValidationError("end must be after start")
```

`full_clean()` calls `clean()` automatically.

### `validate_unique()`

Checks that no other row in the database has the same value for any `unique=True` field.

```python
author = Author(email="alice@example.com")  # already exists
try:
    author.validate_unique()
except dorm.ValidationError as e:
    print("Duplicate:", e.message)
```

`full_clean()` calls `validate_unique()` automatically. It is also checked when you call `save()` on a new instance.

---

## ManyToManyField

Declare a `ManyToManyField` to create a junction table automatically. The related manager exposes `.add()`, `.remove()`, `.set()`, `.clear()`, and `.all()`.

```python
class Tag(dorm.Model):
    name = dorm.CharField(max_length=50, unique=True)


class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags  = dorm.ManyToManyField(Tag, related_name="articles")
```

```python
article = Article.objects.create(title="Hello World")
python  = Tag.objects.create(name="python")
django  = Tag.objects.create(name="django")

# Add one or more tags
article.tags.add(python)
article.tags.add(python, django)

# Check current tags
tags = list(article.tags.all())   # [Tag(python), Tag(django)]

# Replace all tags at once
article.tags.set([django])        # removes python, keeps django

# Remove a specific tag
article.tags.remove(django)

# Remove all tags
article.tags.clear()

# Reverse relation: all articles for a tag
python_articles = list(python.articles.all())
```

### Prefetching M2M relations

```python
# Two queries: one for articles, one for all related tags
articles = list(Article.objects.prefetch_related("tags"))
for article in articles:
    print([t.name for t in article.tags.all()])  # no extra DB hit
```

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
# All records (default ordering from Meta.ordering is applied)
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

# First / last
first = Author.objects.order_by("age").first()
last  = Author.objects.order_by("age").last()

# Slicing
top3  = Author.objects.order_by("-age")[:3]
page2 = Author.objects.order_by("name")[10:20]
```

### Lookups

```python
Author.objects.filter(age__exact=30)
Author.objects.filter(age__gt=30)
Author.objects.filter(age__gte=30)
Author.objects.filter(age__lt=30)
Author.objects.filter(age__lte=30)
Author.objects.filter(age__range=(20, 30))

Author.objects.filter(name__contains="li")
Author.objects.filter(name__icontains="li")
Author.objects.filter(name__startswith="Al")
Author.objects.filter(name__endswith="ce")
Author.objects.filter(name__iexact="alice")

Author.objects.filter(bio__isnull=True)
Author.objects.filter(name__in=["Alice", "Bob"])

Author.objects.filter(joined__year=2024)
Author.objects.filter(joined__month=6)
```

### Q objects — complex queries

```python
from dorm import Q

Author.objects.filter(Q(age__lt=18) | Q(age__gt=65))
Author.objects.filter(Q(active=True) & Q(age__gte=18))
Author.objects.filter(~Q(name="Admin"))
Author.objects.filter(Q(active=True) & (Q(age__lt=18) | Q(age__gt=65)))
```

### Count and existence

```python
total    = Author.objects.count()
filtered = Author.objects.filter(active=True).count()
exists   = Author.objects.filter(email="alice@example.com").exists()
```

### Values and value lists

```python
# Returns dicts
rows = Author.objects.values("name", "age")

# Returns tuples
pairs = Author.objects.values_list("name", "age")

# Flat list (single field only)
names = Author.objects.values_list("name", flat=True)
```

### Partial loading — `only()` and `defer()`

```python
# Fetch only the listed columns (pk always included)
authors = Author.objects.only("name", "email")

# Fetch all columns except the listed ones
authors = Author.objects.defer("bio")
```

### Update and delete

```python
# Bulk update
n = Author.objects.filter(active=False).update(active=True)

# Bulk delete
count, detail = Author.objects.filter(age__lt=18).delete()

# Instance update
author.age = 31
author.save()
author.save(update_fields=["age"])   # only update specific fields

# Instance delete
author.delete()

# Reload from DB
author.refresh_from_db()
```

### F expressions — reference columns

```python
from dorm import F

# Increment without reading into Python
Author.objects.filter(active=True).update(age=F("age") + 1)
```

---

## Relationship loading

### `select_related()` — SQL JOIN

Resolves FK and OneToOne relations in a **single query** using LEFT OUTER JOINs. Supports nested paths with `__`.

```python
# Single level: Book → Author
books = list(Book.objects.select_related("author"))
for book in books:
    print(book.author.name)  # no extra DB hit

# Nested: Book → Author → Publisher
books = list(Book.objects.select_related("author__publisher"))
for book in books:
    print(book.author.publisher.name)  # no extra DB hit

# Multiple paths at once — duplicate JOINs are deduplicated automatically
books = list(Book.objects.select_related("author", "author__publisher"))
```

### `prefetch_related()` — batch queries

Runs a **separate bulk query** per relation and stitches results in Python. Works for forward FK, reverse FK (one-to-many), and ManyToMany.

```python
# Forward FK (same as select_related but via separate query)
books = list(Book.objects.prefetch_related("author"))

# Reverse FK: load all books for each author
authors = list(Author.objects.prefetch_related("book_set"))
for author in authors:
    print([b.title for b in author.book_set.all()])  # no extra DB hit

# ManyToMany
articles = list(Article.objects.prefetch_related("tags"))
for article in articles:
    print([t.name for t in article.tags.all()])      # no extra DB hit
```

---

## Aggregations and annotations

```python
from dorm import Count, Sum, Avg, Max, Min

# Aggregate across the full queryset
result = Author.objects.aggregate(
    total   = Count("id"),
    avg_age = Avg("age"),
    max_age = Max("age"),
)
# {"total": 42, "avg_age": 29.5, "max_age": 65}

# Annotate each row with a computed value
from dorm import Count
authors = list(
    Author.objects
    .annotate(book_count=Count("id"))   # per-row annotation
    .filter(book_count__gte=2)
)
```

---

## DB Functions

Use database-level functions inside `annotate()`, `filter()`, or `update()` calls.

### `Case` / `When` — conditional expressions

```python
from dorm import Case, When, Value

# Classify authors by age
authors = list(
    Author.objects.annotate(
        category=Case(
            When(age__lt=18,  then=Value("minor")),
            When(age__lt=65,  then=Value("adult")),
            default=Value("senior"),
        )
    )
)
for a in authors:
    print(a.name, a.category)
```

### `Coalesce` — first non-null value

```python
from dorm import Coalesce, Value, F

# Return bio if present, otherwise a fallback string
authors = Author.objects.annotate(
    display_bio=Coalesce(F("bio"), Value("No bio provided"))
)
```

### String functions

```python
from dorm import Upper, Lower, Length, Concat, F, Value

authors = Author.objects.annotate(
    name_upper  = Upper(F("name")),
    name_lower  = Lower(F("name")),
    name_length = Length(F("name")),
    display     = Concat(F("name"), Value(" ("), F("email"), Value(")")),
)
```

### `Now` — current timestamp

```python
from dorm import Now

# Set updated_at to the current DB timestamp
Author.objects.filter(active=True).update(updated_at=Now())
```

### `Cast` — type conversion

```python
from dorm import Cast, F

# Cast age to text for a concatenation
authors = Author.objects.annotate(
    age_str=Cast(F("age"), output_field="text")
)
```

### `Abs` — absolute value

```python
from dorm import Abs, F

Author.objects.annotate(balance_abs=Abs(F("balance")))
```

---

## Set operations

Combine two querysets from the **same model** using SQL set operators. The result is a `CombinedQuerySet` that supports `order_by()`, `count()`, slicing, and async iteration.

### `union()`

Returns rows present in **either** queryset. Deduplicates by default; pass `all=True` to keep duplicates.

```python
young   = Author.objects.filter(age__lt=30)
seniors = Author.objects.filter(age__gte=65)

# All young or senior authors (no duplicates)
result = list(young.union(seniors))

# Keep duplicates
result = list(young.union(seniors, all=True))

# Multiple querysets at once
result = list(
    Author.objects.filter(age=20).union(
        Author.objects.filter(age=30),
        Author.objects.filter(age=40),
    )
)

# Chain order_by and count on the combined result
ordered = young.union(seniors).order_by("name")
total   = young.union(seniors).count()
```

### `intersection()`

Returns rows present in **both** querysets.

```python
active  = Author.objects.filter(active=True)
seniors = Author.objects.filter(age__gte=65)

# Active seniors only
active_seniors = list(active.intersection(seniors))
```

### `difference()`

Returns rows in the first queryset that are **not** in the second.

```python
all_authors  = Author.objects.all()
inactive     = Author.objects.filter(active=False)

# All active authors (equivalent to .filter(active=True))
active_only = list(all_authors.difference(inactive))
```

---

## Streaming with `iterator()`

By default `list(qs)` fetches all rows and stores them in an internal cache (`_result_cache`). Use `iterator()` to stream rows one by one without buffering — useful for large result sets.

```python
# Iterate without caching
for author in Author.objects.filter(active=True).iterator():
    process(author)

# Optional chunk_size hint (accepted but currently advisory)
for author in Author.objects.all().iterator(chunk_size=500):
    process(author)

# Async variant
async for author in Author.objects.all().aiterator():
    await process_async(author)
```

`iterator()` is incompatible with `prefetch_related()` (which requires buffering all instances first).

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

    # Update / delete
    n = await Author.objects.filter(active=False).aupdate(active=True)
    count, _ = await Author.objects.filter(age__lt=18).adelete()

    # Instance save / delete / reload
    author.age = 31
    await author.asave()
    await author.adelete()
    await author.arefresh_from_db()

    # Async iteration
    async for author in Author.objects.filter(active=True).order_by("name"):
        print(author.name)

    # Streaming without cache
    async for author in Author.objects.all().aiterator():
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

Nested `atomic()` calls automatically use savepoints. An inner failure rolls back only the inner block; the outer transaction can still commit.

```python
with dorm.transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)
    try:
        with dorm.transaction.atomic():        # creates SAVEPOINT
            Book.objects.create(title="Bad Book", author_id=author.pk)
            raise ValueError("something went wrong")
    except ValueError:
        pass   # inner block rolled back; author still in transaction

# Only Alice is committed; no Book
```

### `get_or_create` / `update_or_create` are atomic

Both methods wrap their INSERT in an implicit transaction and handle concurrent inserts safely — if another thread creates the same row first, they return the existing object instead of raising `IntegrityError`.

---

## Migrations

### What is an app?

An **app** is a Python package (a directory with `__init__.py`) that groups related models. Each app has its own `migrations/` folder.

```
myproject/
├── settings.py
├── blog/
│   ├── __init__.py
│   ├── models.py
│   └── migrations/
│       └── __init__.py
└── shop/
    ├── __init__.py
    ├── models.py
    └── migrations/
        └── __init__.py
```

### CLI commands

```bash
# Detect model changes and generate migration files
dorm makemigrations

# Apply all pending migrations
dorm migrate

# Apply migrations for a specific app only
dorm migrate blog

# Show migration status ([ ] pending, [X] applied)
dorm showmigrations

# Interactive shell (IPython if available — top-level await works out of the box)
dorm shell

# Override settings explicitly
dorm makemigrations --settings=myproject.settings
```

`--settings` is optional. dorm resolves settings in order: `--settings` flag → `DORM_SETTINGS` env var → `settings` module in current directory.

### Undoing migrations

```bash
# Roll back to a specific migration (undoes everything after it)
dorm migrate blog 0002

# Undo all migrations for an app
dorm migrate blog zero
```

### Custom migrations with `RunSQL` / `RunPython`

```python
# myapp/migrations/0003_add_score.py
from dorm.migrations.operations import RunSQL, RunPython

def seed(app_label, registry):
    Author = registry["Author"]
    Author.objects.get_or_create(
        email="admin@example.com",
        defaults={"name": "Admin", "age": 0},
    )

def unseed(app_label, registry):
    Author = registry["Author"]
    Author.objects.filter(email="admin@example.com").delete()

operations = [
    RunSQL(
        sql="ALTER TABLE authors ADD COLUMN score INTEGER DEFAULT 0",
        reverse_sql="ALTER TABLE authors DROP COLUMN score",
    ),
    RunPython(code=seed, reverse_code=unseed),
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


# ── Models ─────────────────────────────────────────────────────────────────────

class TimestampedModel(dorm.Model):
    created_at = dorm.DateTimeField(auto_now_add=True, null=True)
    updated_at = dorm.DateTimeField(auto_now=True, null=True)

    class Meta:
        abstract = True


class Tag(dorm.Model):
    name = dorm.CharField(max_length=50, unique=True)

    class Meta:
        db_table = "tags"
        ordering = ["name"]


class Author(dorm.Model):
    name  = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    age   = dorm.IntegerField()

    class Meta:
        db_table = "authors"
        ordering = ["name"]


class Post(TimestampedModel):
    title     = dorm.CharField(max_length=200)
    body      = dorm.TextField()
    author    = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    published = dorm.BooleanField(default=False)
    views     = dorm.IntegerField(default=0)
    tags      = dorm.ManyToManyField(Tag, related_name="posts")

    class Meta:
        db_table = "posts"
        ordering = ["-created_at"]


# ── Signal ─────────────────────────────────────────────────────────────────────

from dorm.signals import post_save

@post_save.connect_for(Author)
def welcome_author(sender, instance, created, **kwargs):
    if created:
        print(f"Welcome, {instance.name}!")


# ── Sync demo ──────────────────────────────────────────────────────────────────

def sync_demo():
    alice = Author.objects.create(name="Alice", email="alice@example.com", age=30)
    bob   = Author.objects.create(name="Bob",   email="bob@example.com",   age=25)

    p1 = Post.objects.create(title="Hello World", body="...", author=alice, published=True)
    Post.objects.create(title="Draft",  body="...", author=alice)
    Post.objects.create(title="Bob's Post", body="...", author=bob, published=True)

    python_tag = Tag.objects.create(name="python")
    p1.tags.add(python_tag)

    # Nested select_related
    posts = list(Post.objects.select_related("author").filter(published=True))
    print([p.author.name for p in posts])

    # Prefetch M2M
    posts = list(Post.objects.prefetch_related("tags"))
    print([[t.name for t in p.tags.all()] for p in posts])

    # DB functions
    from dorm import Upper, Case, When, Value
    annotated = list(
        Author.objects.annotate(
            name_up  = Upper(dorm.F("name")),
            category = Case(
                When(age__lt=30, then=Value("young")),
                default=Value("experienced"),
            ),
        )
    )
    print(annotated[0].category)

    # Set operations
    young_or_senior = list(
        Author.objects.filter(age__lt=30).union(
            Author.objects.filter(age__gte=65)
        )
    )

    # Stream large results without caching
    for post in Post.objects.iterator():
        _ = post.title

    # Aggregations
    stats = Post.objects.aggregate(
        total     = dorm.Count("id"),
        avg_views = dorm.Avg("views"),
    )
    print(stats)

    # F expression
    Post.objects.filter(published=True).update(views=dorm.F("views") + 1)


# ── Async demo ─────────────────────────────────────────────────────────────────

async def async_demo():
    author = await Author.objects.aget(email="alice@example.com")

    await Post.objects.acreate(
        title="Async Post", body="...", author=author, published=True
    )

    async for post in Post.objects.filter(author=author).order_by("title"):
        print(post.title)

    async for post in Post.objects.all().aiterator():
        print(post.title)

    stats = await Post.objects.aaggregate(total=dorm.Count("id"))
    print(stats)


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
| Nested FK load | `objects.select_related("fk__nested")` | — |
| Batch FK/M2M load | `objects.prefetch_related("fk")` | — |
| Stream rows | `objects.iterator()` | `objects.aiterator()` |
| Union | `qs1.union(qs2)` | — |
| Intersection | `qs1.intersection(qs2)` | — |
| Difference | `qs1.difference(qs2)` | — |
| Validate | `instance.full_clean()` | — |
| Full clean | `instance.clean()` | — |

---

## Dependencies

| Extra | Package | Purpose |
|---|---|---|
| `sqlite` | `aiosqlite` | Async SQLite |
| `postgresql` | `psycopg[binary,pool]` | Sync/Async PostgreSQL with connection pool |

## License

MIT
