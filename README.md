# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

> **New here?** The 5-minute walkthrough lives in [docs/tutorial.md](docs/tutorial.md). The rest of this README is reference material — use the TOC to jump to what you need.

## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Setup](#setup)
- [Defining models](#defining-models)
- [Synchronous operations](#synchronous-operations)
- [Asynchronous operations](#asynchronous-operations)
- [Relationship loading](#relationship-loading)
- [Aggregations and annotations](#aggregations-and-annotations)
- [DB Functions](#db-functions)
- [Set operations](#set-operations)
- [Streaming with `iterator()`](#streaming-with-iterator)
- [Transactions](#transactions)
- [Signals](#signals)
- [Validation](#validation)
- [ManyToManyField](#manytomanyfield)
- [Migrations](#migrations)
- [Production deployment](#production-deployment)
- [Versioning and deprecation policy](#versioning-and-deprecation-policy)
- [Dependencies](#dependencies)
- [License](#license)

---

## Features

- **Type-safe** — every field is `Field[T]` (e.g. `CharField(Field[str])`), so `user.name` is statically `str` (not `Any`), and `Author.objects.filter(...).first()` is `Author | None`. Same `Mapped[T]` ergonomic SQLAlchemy 2.0 made famous, no extra annotation per field.
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
- **Efficient bulk operations** — `bulk_create()` uses a single multi-row INSERT per batch; `bulk_update()` rewrites N rows in one `UPDATE ... SET col = CASE pk WHEN ...` per batch (1 query, not N)
- **File storage** — `FileField` with a pluggable `Storage` abstraction. Local-disk default plus an opt-in S3 backend (`dorm.contrib.storage.s3.S3Storage`) that talks to AWS S3, MinIO, Cloudflare R2 or Backblaze B2 via the `djanorm[s3]` extra. Switching backends is a `STORAGES` setting change — application code is identical.
- **Async signals** — every signal accepts both regular and `async def` receivers. `Model.asave` / `Model.adelete` await coroutine receivers sequentially via `Signal.asend`; the sync `send` path skips them with a `WARNING` so missed work is never silent.
- **Rich field catalogue** — beyond the basics: `JSONField`, `BinaryField`, `ArrayField` (PG), `GeneratedField`, `DurationField`, `EnumField` (any `enum.Enum`), `CITextField`, and the full PostgreSQL range family (`IntegerRangeField`, `DecimalRangeField`, `DateRangeField`, `DateTimeRangeField`, `BigIntegerRangeField`).
- **JSON fixtures** — `dorm dumpdata` and `dorm loaddata` produce / consume a Django-compatible JSON shape. M2M relations restore in a second phase, the whole load runs inside `atomic()`, and signals are bypassed for deterministic seeding.

---

## Installation

```bash
# SQLite support
pip install "djanorm[sqlite]"

# PostgreSQL support
pip install "djanorm[postgresql]"

# Both
pip install "djanorm[sqlite,postgresql]"

# Add file uploads on S3 / MinIO / Cloudflare R2 / Backblaze B2
pip install "djanorm[postgresql,s3]"

# FastAPI + Pydantic schemas
pip install "djanorm[postgresql,pydantic]"

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
        "MIN_POOL_SIZE": 1,    # default
        "MAX_POOL_SIZE": 10,   # default
        "POOL_TIMEOUT": 30.0,  # seconds to wait for a free pool connection
        "POOL_CHECK": True,    # default: SELECT 1 on each checkout for stale-conn detection
                               # set to False on hot paths to skip the per-checkout probe
        "OPTIONS": {
            # Keys here are passed straight to psycopg.connect() — use
            # psycopg names (lowercase), not Django-style uppercase ones.
            "sslmode": "require",
            "application_name": "myapp",
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

`prefetch_related()` issues exactly **one extra query per relation**: forward
FKs do `SELECT ... WHERE pk IN (...)`, reverse FKs do `SELECT ... WHERE fk IN (...)`,
and M2M relations do a single `JOIN` between the through table and the target
table — never a "fetch through, then fetch targets" two-step.

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

    # Bulk update of multiple objects
    for a in fetched_authors:
        a.age += 1
    await Author.objects.abulk_update(fetched_authors, fields=["age"])

    # Async aggregation
    result = await Author.objects.aaggregate(total=dorm.Count("id"), avg=dorm.Avg("age"))

asyncio.run(main())
```

### Awaiting a queryset

A QuerySet is also directly awaitable. Use `values()` / `values_list()` (they
return a chainable QuerySet) and then `await` to materialize the whole result
in one expression:

```python
# Same chainable surface as the sync API, but consumed with a single await.
rows = await Author.objects.values("name", "age").filter(age__gte=18).order_by("name")
# rows is a list[dict[str, Any]]

# Equivalent async-iterator form (use this for streaming):
async for row in Author.objects.values("name", "age").filter(age__gte=18):
    print(row)
```

`avalues()` / `avalues_list()` still exist as terminal materializers if you
prefer a single call over the chained form.

### Cancellation

If an `await` inside a queryset call is cancelled (e.g. `asyncio.wait_for`
expires), connection cleanup depends on the backend:

- **PostgreSQL:** the pool's connection context manager runs on cancellation
  and returns the connection to the pool, rolling back any in-flight
  transaction.
- **SQLite (aiosqlite):** the SQL statement may still complete in the worker
  thread after cancellation; the result is discarded. The next operation
  reuses the same connection. If you need a guaranteed rollback, wrap the
  cancellable section in `async with dorm.transaction.aatomic(): ...`.

### Mixing sync and async on the same SQLite database

`get_connection()` and `get_async_connection()` return separate wrappers, so
sync and async code on the same SQLite file open distinct underlying
connections. This is fine in most cases, but be aware that the default
`DELETE` journal mode acquires file-level locks, so heavy interleaving can
serialize. If you need both sync and async with concurrency, set
`OPTIONS={"journal_mode": "WAL"}` in `DATABASES["default"]`.

---

## Transactions

`atomic()` and `aatomic()` work as **context managers** or as **decorators**.

```python
import dorm

# ── Context manager form ──────────────────────────────────────────────────────

with dorm.transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="My Book", author_id=author.pk)

async with dorm.transaction.aatomic():
    author = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="My Book", author_id=author.pk)

# ── Decorator form ────────────────────────────────────────────────────────────

@dorm.transaction.atomic
def transfer_funds(src_id, dst_id, amount):
    Account.objects.filter(pk=src_id).update(balance=F("balance") - amount)
    Account.objects.filter(pk=dst_id).update(balance=F("balance") + amount)

@dorm.transaction.aatomic("replica")
async def report():
    return await Author.objects.acount()
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
# Scaffold settings.py in the current directory (both SQLite and PostgreSQL
# blocks are generated commented out — uncomment whichever you need).
dorm init

# Same, plus an app folder with __init__.py and a starter models.py
dorm init --app blog

# Detect model changes and generate migration files
dorm makemigrations

# Apply all pending migrations
dorm migrate

# Apply migrations for a specific app only
dorm migrate blog

# Show migration status ([ ] pending, [X] applied)
dorm showmigrations

# Compare each model's columns with what's actually in the DB and exit
# non-zero on drift — handy as a pre-deploy gate.
dorm dbcheck

# Interactive shell (IPython if available — top-level await works out of the box)
dorm shell

# Override settings explicitly
dorm makemigrations --settings=myproject.settings

# Print this list any time
dorm help
```

You can also invoke the CLI as a module: `python -m dorm <command>`.

`--settings` is optional. dorm resolves settings in order: `--settings` flag → `DORM_SETTINGS` env var → `settings` module in current directory. Both the directory containing `settings.py` and its parent are added to `sys.path`, so apps work in flat layouts (`settings.py` next to `app/`) and dotted-package layouts (`myproject/settings.py` with `INSTALLED_APPS=["myproject.app"]`).

### Undoing migrations

```bash
# Roll back to a specific migration (undoes everything after it)
dorm migrate blog 0002

# Undo all migrations for an app
dorm migrate blog zero
```

### Detecting schema drift

`dorm dbcheck` compares each model's column set with what's currently in
the database and prints any drift — missing tables, columns missing in
the DB (model added a field that wasn't migrated) or in the model
(column edited by hand on the server). Exits non-zero when drift is
found, so it's a useful pre-deploy gate:

```bash
dorm dbcheck                  # check every app
dorm dbcheck blog shop        # check specific apps
```

```
App 'blog':
  ✓ Author (authors)
  ✗ Post (posts):
      missing in DB: published_at
Drift detected. Run 'dorm makemigrations' / 'dorm migrate' to reconcile.
```

### Concurrent migrations and rolling deploys

`dorm migrate` is safe to run from multiple processes simultaneously. On
PostgreSQL it acquires `pg_advisory_lock` so the second invocation blocks
until the first finishes; on SQLite the database's natural single-writer
file lock serializes them. This means it's safe to put `dorm migrate` in
every replica's startup script — only one will actually apply pending
migrations; the rest exit as no-ops.

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
| Raw SQL | `objects.raw(sql, params)` | `await objects.araw(sql, params)` |
| Bulk delete w/ on_delete | `objects.filter(...).delete()` | `await objects.filter(...).adelete()` |
| FK traversal order | `objects.order_by("fk__field")` | — |

---

## Tier 4 features

### Bulk delete respects `on_delete`

`QuerySet.delete()` and `QuerySet.adelete()` now enforce Python-level `on_delete`
policies (CASCADE, PROTECT, SET_NULL, SET_DEFAULT) before executing the bulk SQL DELETE,
identical to single-instance `instance.delete()`:

```python
# Deleting a queryset cascades to related objects
deleted, counts = Category.objects.filter(name="old").delete()
# counts: {"myapp.Category": 1, "myapp.Article": 5}

# PROTECT prevents deletion if related objects exist
from dorm.exceptions import ProtectedError
try:
    Publisher.objects.all().delete()
except ProtectedError:
    print("Cannot delete: protected references exist")
```

### `order_by()` with FK traversal

Sort by fields on related models using double-underscore traversal — dorm automatically
adds the required JOIN:

```python
# Sort books by their author's name
books = Book.objects.order_by("author__name")

# Descending
books = Book.objects.order_by("-author__name")

# Multi-level traversal
books = Book.objects.order_by("author__publisher__name", "title")

# Combined with filters
books = Book.objects.filter(published=True).order_by("author__name", "-year")
```

### Field validators

Attach validators to any field. They run when `full_clean()` is called:

```python
from dorm.validators import MinValueValidator, MaxValueValidator, RegexValidator, validate_email

class Product(dorm.Model):
    name = dorm.CharField(
        max_length=200,
        validators=[MinLengthValidator(3), MaxLengthValidator(50)],
    )
    price = dorm.FloatField(
        validators=[MinValueValidator(0), MaxValueValidator(9999)],
    )
    sku = dorm.CharField(
        max_length=20,
        validators=[RegexValidator(r"^[A-Z]{2}-\d{4}$", "Invalid SKU format")],
    )
    email = dorm.CharField(max_length=100, validators=[validate_email])
```

Built-in validators:

| Validator | Purpose |
|---|---|
| `MinValueValidator(n)` | Value ≥ n |
| `MaxValueValidator(n)` | Value ≤ n |
| `MinLengthValidator(n)` | `len(value) >= n` |
| `MaxLengthValidator(n)` | `len(value) <= n` |
| `RegexValidator(pattern, msg)` | Must match regex |
| `EmailValidator()` / `validate_email` | Valid email format |

Custom validators are plain callables that raise `ValidationError`:

```python
def no_spaces(value):
    if " " in value:
        raise dorm.ValidationError("No spaces allowed.")

class Tag(dorm.Model):
    slug = dorm.CharField(max_length=50, validators=[no_spaces])
```

### Raw SQL queries

Execute arbitrary SQL and get back hydrated model instances:

```python
# Sync
results = list(Author.objects.raw('SELECT * FROM "authors" WHERE age > %s', [25]))
for author in results:
    print(author.name, author.age)  # full model instances

# Async
authors = await Author.objects.araw('SELECT * FROM "authors" ORDER BY "name"')

# Async iteration
async for author in Author.objects.raw('SELECT * FROM "authors"'):
    print(author.name)
```

The query can return any columns — unknown columns are stored as plain attributes.
JOINs and computed columns work seamlessly:

```python
results = Author.objects.raw(
    'SELECT a.*, COUNT(b.id) AS book_count '
    'FROM authors a LEFT JOIN books b ON b.author_id = a.id '
    'GROUP BY a.id'
)
for a in results:
    print(a.name, a.book_count)
```

### Connection auto-reconnect

Both SQLite and PostgreSQL backends now perform a health-check before executing each
query. If the connection has been dropped (e.g. server restart, idle timeout), dorm
transparently reconnects:

- **SQLite sync/async**: executes `SELECT 1` on the cached connection; recreates it on
  failure.
- **PostgreSQL pool**: passes `check=ConnectionPool.check_connection` to the psycopg
  pool, which verifies each connection when it is borrowed.

No application code changes are needed. Long-running processes (workers, daemons) will
automatically recover from transient connection failures.

### Rename detection in migrations

The `MigrationAutodetector` can now produce `RenameModel` and `RenameField` operations
instead of delete+create pairs.

**Explicit hints (recommended)** — safe, deterministic:

```python
from dorm.migrations.autodetector import MigrationAutodetector

detector = MigrationAutodetector(
    from_state,
    to_state,
    rename_hints={
        "models": {"myapp": {"OldModelName": "NewModelName"}},
        "fields": {"myapp.MyModel": {"old_field": "new_field"}},
    },
)
changes = detector.changes("myapp")
```

**Heuristic auto-detection** (opt-in via `detect_renames=True`):

- **Model rename**: detects when a deleted model and a new model share identical field
  names and types.
- **Field rename**: detects when exactly one field is removed and one added within the
  same model, and both share the same `db_type`.

```python
detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
changes = detector.changes("myapp")
```

### Database indexes (`Meta.indexes`)

Define indexes on models using `dorm.Index`. They are stored in model metadata and
can be applied via migration operations:

```python
import dorm
from dorm.indexes import Index

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    slug  = dorm.CharField(max_length=200)
    published_at = dorm.DateTimeField()

    class Meta:
        indexes = [
            Index(fields=["slug"], unique=True, name="idx_article_slug"),
            Index(fields=["published_at"], name="idx_article_date"),
            Index(fields=["title", "slug"]),  # auto-named: idx_article_title_slug
        ]
```

**Migration operations:**

```python
from dorm.migrations.operations import AddIndex, RemoveIndex
from dorm.indexes import Index

class Migration:
    operations = [
        AddIndex(
            model_name="Article",
            index=Index(fields=["slug"], unique=True, name="idx_article_slug"),
        ),
        RemoveIndex(
            model_name="Article",
            index=Index(fields=["old_col"], name="idx_article_old"),
        ),
    ]
```

The `MigrationAutodetector` automatically detects added and removed indexes when
comparing two `ProjectState` objects.

### Subquery support (`__in=queryset`)

Pass a `QuerySet` directly as the value of an `__in` lookup. dorm compiles it as
an `IN (SELECT …)` subquery — no intermediate Python list is materialised:

```python
# All books whose author is in the "active authors" set
active_authors = Author.objects.filter(active=True)
books = Book.objects.filter(author__in=active_authors)

# Works with slicing too
top3 = Author.objects.order_by("-rating")[:3]
books = Book.objects.filter(author__in=top3)

# Same-model self-referential subquery
promoted = Author.objects.filter(rank__gte=5)
result = Author.objects.filter(pk__in=promoted)

# Async
books = await Book.objects.filter(author__in=active_authors).all()
```

Plain list values for `__in` continue to work unchanged.

### `squashmigrations` command

Combine a range of migrations into a single squashed file to keep your migration
history manageable:

```bash
# Squash migrations 0001 through 0005 for the "myapp" app
dorm squashmigrations myapp 0001 0005

# With a custom name
dorm squashmigrations myapp 0001 0005 --squashed-name initial_squashed

# Start from the very beginning (omit start_migration)
dorm squashmigrations myapp 0005
```

The generated file has a `replaces` list that the executor honours — once all
replaced migrations are recorded as applied, the squashed migration is
automatically marked applied too. Operations are automatically optimised
(e.g. consecutive `AddField`/`RemoveField` pairs cancel out, `AddField` followed
by `AlterField` is merged into a single `AddField`).

### `connection.set_autocommit()` for long-running processes

Control transaction behaviour on the active connection. Useful for batch jobs and
long-running workers where you want each statement to commit individually or where
you manage commits manually:

```python
from dorm.db.connection import get_connection, get_async_connection

# ── Sync ──────────────────────────────────────────────────────────────────────
conn = get_connection()

# Enable autocommit — each statement commits immediately
conn.set_autocommit(True)
for row in big_data:
    MyModel.objects.create(**row)

# Disable autocommit and manage transactions manually
conn.set_autocommit(False)
try:
    MyModel.objects.create(name="A")
    MyModel.objects.create(name="B")
    conn.commit()
except Exception:
    conn.rollback()
    raise

# ── Async ─────────────────────────────────────────────────────────────────────
conn = get_async_connection()
await conn.set_autocommit(True)
await MyModel.objects.acreate(name="immediate")
await conn.set_autocommit(False)

await conn.commit()    # manual commit
await conn.rollback()  # manual rollback
```

`set_autocommit()` is supported on all backends: SQLite sync/async and
PostgreSQL sync/async. On PostgreSQL a dedicated persistent connection
(separate from the pool) is used when autocommit is enabled.

---

## Production deployment

A short checklist for running djanorm in production.

### Logging

Every SQL statement is logged at `DEBUG` on a per-vendor logger. Slow queries
(default ≥ 500 ms) are emitted at `WARNING`. Pool open/close events go to
`INFO` on `dorm.db.lifecycle.<vendor>` so ops can trace boot/shutdown
without per-query noise.

```python
import logging

logging.getLogger("dorm.db.backends.postgresql").setLevel(logging.DEBUG)
# Or just enable warnings (slow queries) without per-statement noise:
logging.getLogger("dorm.db").setLevel(logging.WARNING)
# Boot / shutdown only:
logging.getLogger("dorm.db.lifecycle").setLevel(logging.INFO)
```

Tune the slow-query threshold with the `DORM_SLOW_QUERY_MS` environment
variable (default `500`).

### Query observability hooks

`dorm.pre_query` and `dorm.post_query` are dispatch-style signals fired
around every SQL statement, so you can wire metrics / tracing without
touching dorm internals:

```python
import dorm

def trace(sender, sql, params, elapsed_ms, error):
    # sender is the vendor string ("postgresql", "sqlite").
    if error is not None:
        my_metrics.incr(f"dorm.query.error.{sender}")
    else:
        my_metrics.timing(f"dorm.query.elapsed.{sender}", elapsed_ms)

dorm.post_query.connect(trace, weak=False)
```

Receivers must be cheap — they run inline on every query.

#### OpenTelemetry integration

Distributed tracing for every dorm query in ~15 lines — no monkey-patching:

```python
from opentelemetry import trace
import dorm

tracer = trace.get_tracer("dorm.db")

def _span_for_query(sender, sql, params):
    span = tracer.start_span(f"db.{sender}.query")
    span.set_attribute("db.system", sender)
    span.set_attribute("db.statement", sql)
    # Stash the span on a contextvar so post_query can close it.
    _active_spans.set(span)

def _close_span(sender, sql, params, elapsed_ms, error):
    span = _active_spans.get(None)
    if span is None:
        return
    span.set_attribute("db.duration_ms", elapsed_ms)
    if error is not None:
        span.record_exception(error)
        span.set_status(trace.Status(trace.StatusCode.ERROR))
    span.end()

import contextvars
_active_spans: contextvars.ContextVar = contextvars.ContextVar("dorm_otel_span")

dorm.pre_query.connect(_span_for_query, weak=False)
dorm.post_query.connect(_close_span, weak=False)
```

Every query becomes a `db.postgresql.query` (or `db.sqlite.query`)
span attached to the current trace context — visible in Jaeger,
Honeycomb, Datadog, anywhere with an OTel exporter.

### Transient-error retry

PostgreSQL execute paths automatically retry on `OperationalError` /
`InterfaceError` (network blip, server restart, RDS failover) up to
`DORM_RETRY_ATTEMPTS` times (default 3) with exponential backoff
starting at `DORM_RETRY_BACKOFF` seconds (default 0.1). Retries are
disabled while inside a transaction so committed state is never
re-applied.

```bash
export DORM_RETRY_ATTEMPTS=5    # more aggressive recovery
export DORM_RETRY_BACKOFF=0.25
```

For arbitrary user code that touches the DB, use the helpers directly:

```python
from dorm.db.utils import with_transient_retry, awith_transient_retry

result = with_transient_retry(lambda: my_complex_query())
result = await awith_transient_retry(lambda: my_async_query())
```

### Migration safety under rolling deploys

`dorm migrate` acquires a cross-process advisory lock so two pods that boot
simultaneously don't apply the same migration twice:

- **PostgreSQL:** `pg_advisory_lock` (blocking). Released on connection drop.
- **SQLite:** SQLite's natural single-writer file lock serializes concurrent
  migrators at the first write transaction; no separate lock is taken.

This means it's safe to run `dorm migrate` as part of every replica's
startup script — the second through Nth invocations will block until the
first one finishes, then exit as no-ops.

### Pool sizing and retries

`MIN_POOL_SIZE` / `MAX_POOL_SIZE` / `POOL_TIMEOUT` are documented in
[Setup](#setup). For Kubernetes, set `MIN_POOL_SIZE=0` so a temporarily
unreachable database doesn't fail the pod's liveness probe — the pool will
open lazily on first use. For long-lived workers, set `MIN_POOL_SIZE` ≥ 1
to avoid cold-start latency on the first request.

If you've eliminated stale-connection bugs and want to shave per-query
latency, set `POOL_CHECK=False` to skip the per-checkout `SELECT 1` probe.

### Secrets management

Don't put database passwords in source control. The `DATABASES` dict
accepts plain values, so any secrets backend that materializes them
into Python at startup works. Three patterns from least to most
operational:

**1. Environment variables** (good for local dev, basic deploys):

```python
import os
import dorm

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "postgresql",
            "NAME": os.environ["DB_NAME"],
            "USER": os.environ["DB_USER"],
            "PASSWORD": os.environ["DB_PASSWORD"],
            "HOST": os.environ.get("DB_HOST", "localhost"),
            "PORT": int(os.environ.get("DB_PORT", "5432")),
        }
    },
)
```

**2. `pydantic-settings`** (typed, validated, supports `.env` files):

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
import dorm

class DBConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env")
    name: str
    user: str
    password: str
    host: str = "localhost"
    port: int = 5432

cfg = DBConfig()  # fails fast if a required var is missing
dorm.configure(
    DATABASES={"default": {
        "ENGINE": "postgresql",
        **cfg.model_dump(),
        "NAME": cfg.name,
    }},
)
```

**3. AWS Secrets Manager / HashiCorp Vault** (production):

```python
import boto3, json
import dorm

secret = json.loads(
    boto3.client("secretsmanager")
    .get_secret_value(SecretId="prod/dorm/db")["SecretString"]
)

dorm.configure(
    DATABASES={"default": {
        "ENGINE": "postgresql",
        "NAME": secret["dbname"],
        "USER": secret["username"],
        "PASSWORD": secret["password"],
        "HOST": secret["host"],
        "PORT": secret["port"],
        "OPTIONS": {"sslmode": "require"},  # always TLS in prod
    }},
)
```

The SSL line is the easy one to forget — without it the connection
travels in cleartext within the VPC.

### Health check endpoint

For Kubernetes readiness / liveness probes, AWS ALB target groups, or
any orchestrator that polls a URL, dorm ships a tiny helper that runs
`SELECT 1` and returns a JSON-friendly status dict. It **never raises**
— health checks must answer the orchestrator even when the DB is down.

```python
import dorm

# Sync (Flask / Django views):
status = dorm.health_check()
# → {"status": "ok", "alias": "default", "elapsed_ms": 1.4}
# or  {"status": "error", "alias": "default", "elapsed_ms": 5000.0,
#      "error": "OperationalError: connection refused"}

# Async (FastAPI / Starlette):
@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

A failed check returns `{"status": "error"}` with HTTP 200; map that to
a non-2xx response in your route handler if your orchestrator demands
it. Multi-DB? Pass `alias="replica"` to check that one instead.

### Pool monitoring

Both wrappers expose `pool_stats()` for ad-hoc inspection or for
Prometheus exporters:

```python
from dorm.db.connection import get_async_connection

stats = get_async_connection().pool_stats()
# PG → {"open": True, "vendor": "postgresql", "min_size": 1, "max_size": 10,
#       "pool_size": 4, "pool_available": 3, "requests_waiting": 0, ...}
# SQLite → {"open": True, "vendor": "sqlite"}
```

Combined with `dorm.post_query` (see below), it's all you need to
wire end-to-end observability.

### Connection lifecycle (PG)

In addition to `MIN_POOL_SIZE` / `MAX_POOL_SIZE` / `POOL_TIMEOUT`,
the PG pool exposes:

| setting | default | meaning |
|---|---|---|
| `MAX_IDLE` | `600.0` (10 min) | recycle connections idle longer than this |
| `MAX_LIFETIME` | `3600.0` (1 hr) | recycle every connection after this regardless of activity |

The defaults are sensible for most apps. Long-lived workers behind a
proxy (PgBouncer, RDS Proxy) or with strict ALB idle timeouts may want
`MAX_IDLE` lower than the proxy's drop timeout.

### Multi-DB / read replicas (DATABASE_ROUTERS)

Pass `DATABASE_ROUTERS=[router]` to `dorm.configure()` to send reads
and writes to different aliases. Each router is an object with
optional `db_for_read(model, **hints)` / `db_for_write(model, **hints)`
methods; the first router that returns a non-`None` alias wins.

```python
class ReplicaRouter:
    def db_for_read(self, model, **hints):
        return "replica"
    def db_for_write(self, model, **hints):
        return "default"

dorm.configure(
    DATABASES={
        "default": {"ENGINE": "postgresql", "HOST": "primary.local", ...},
        "replica": {"ENGINE": "postgresql", "HOST": "replica.local", ...},
    },
    DATABASE_ROUTERS=[ReplicaRouter()],
)

# Now Author.objects.filter(...) reads from "replica";
# Author.objects.using("default").create(...) (or via db_for_write) writes
# to the primary.
```

Routers are consulted only when no explicit `using=` is set, so calls
that need to bypass routing (admin scripts, test setup) just pass
`using="default"`.

### Web frameworks

dorm doesn't impose a request lifecycle, so framework integration is just
about wiring `configure()` once at startup and (for async) cleaning up
connections on shutdown.

#### FastAPI: Pydantic schemas from dorm models

The recommended entry point is `DormSchema` — a `BaseModel` subclass
with a Django-REST-style `class Meta` that auto-fills fields from a
dorm Model. Anything you declare in the class body (overrides, extra
fields, `@field_validator` decorators) wins over the Meta-derived
defaults.

```python
from fastapi import FastAPI
from pydantic import field_validator
from dorm.contrib.pydantic import DormSchema
from .models import User

class UserOut(DormSchema):
    class Meta:
        model = User
        fields = "__all__"           # default; or e.g. ("id", "name", "email")

class UserCreate(DormSchema):
    confirm_password: str             # extra field not on the dorm model

    @field_validator("email")
    @classmethod
    def lower(cls, v: str) -> str:    # works on auto-generated fields too
        return v.lower()

    class Meta:
        model = User
        exclude = ("id",)             # mutually exclusive with `fields`
        # optional = ("phone",)       # mark required cols as optional in this schema

class UserPatch(DormSchema):
    class Meta:
        model = User
        exclude = ("id", "created_at")
        optional = ("name", "email")  # all-optional for PATCH bodies

app = FastAPI()

@app.post("/users", response_model=UserOut)
async def create_user(payload: UserCreate) -> User:
    return await User.objects.acreate(**payload.model_dump(exclude={"confirm_password"}))

@app.get("/users/{pk}", response_model=UserOut)
async def get_user(pk: int) -> User:
    return await User.objects.aget(pk=pk)
```

`Meta` accepts:

| key | meaning |
|---|---|
| `model` | dorm Model class (required) |
| `fields` | tuple of field names to include, or `"__all__"` (default) |
| `exclude` | tuple of field names to drop; mutually exclusive with `fields` |
| `optional` | tuple of field names to mark optional with default `None`, even if non-null on the model — useful for PATCH bodies |
| `nested` | dict mapping FK / O2O / M2M field names to a sub-`DormSchema`; serializes the embedded object instead of the bare PK |

##### Embedded relations

Use `Meta.nested` to serialize related rows inline (a typical FastAPI
response pattern):

```python
class PublisherOut(DormSchema):
    class Meta:
        model = Publisher

class TagOut(DormSchema):
    class Meta:
        model = Tag

class AuthorOut(DormSchema):
    class Meta:
        model = Author
        nested = {"publisher": PublisherOut}     # ForeignKey → PublisherOut | None

class ArticleOut(DormSchema):
    class Meta:
        model = Article
        nested = {"tags": TagOut}                 # ManyToManyField → list[TagOut]
```

Forward FK / OneToOne fields that are nullable yield ``Type | None``;
M2M always yields ``list[Type]`` (even if empty).

Notes:

- `from_attributes=True` is set automatically, so FastAPI's `response_model`
  populates the schema directly from a dorm instance. No glue.
- M2M fields are skipped (they don't live on the row). Add them as an
  explicit list field if you need them.
- FK / O2O serialize as the underlying PK column type (typically `int`).
- Validators (`@field_validator`, `@model_validator`) work as in any
  Pydantic model — they apply to auto-generated fields too.
- Type checkers see every field you declare in the class body. The
  Meta-derived ones are added at runtime (same fundamental limit as
  Django REST's `ModelSerializer`); for full type safety, declare
  the fields you care about explicitly.

##### Quick prototype: `schema_for()` (auto-generated, no class)

For one-off scripts, `schema_for()` produces a matching `BaseModel` in
one line — no class block at all:

```python
from dorm.contrib.pydantic import schema_for

UserOut    = schema_for(User)
UserCreate = schema_for(User, exclude=("id", "created_at"))
UserPatch  = schema_for(User, exclude=("id",), optional=("name", "email"))
```

Trade-off vs `DormSchema`: there's no class for the type checker to look
at, so the result is typed as `type[BaseModel]` and IDE autocompletion
on validated instances doesn't work. Prefer `DormSchema` whenever you
care about typing.

`schema_for(...)` knobs:

| arg | use |
|---|---|
| `exclude=("id",)` | drop fields |
| `only=("name", "email")` | keep only these fields |
| `optional=("name",)` | mark as optional (PATCH bodies) |
| `name="UserOut"` | override the generated class name |
| `base=MyBase` | use a custom Pydantic `BaseModel` parent |

`pydantic` is an optional extra: install with `pip install 'djanorm[pydantic]'`
or include in your project's `dependencies`.

#### FastAPI / Starlette (lifespan):

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
import dorm

@asynccontextmanager
async def lifespan(app: FastAPI):
    dorm.configure(
        DATABASES={"default": {"ENGINE": "postgresql", "NAME": "myapp", ...}},
    )
    yield
    # Optional but recommended: close async pools cleanly so the worker
    # doesn't have to rely on the daemon-thread fallback.
    from dorm.db.connection import close_all_async
    await close_all_async()

app = FastAPI(lifespan=lifespan)
```

**Flask (sync):**

```python
import dorm
from flask import Flask

dorm.configure(
    DATABASES={"default": {"ENGINE": "postgresql", "NAME": "myapp", ...}},
)

app = Flask(__name__)

@app.teardown_appcontext
def close_db(exc=None):
    # Optional — pool connections return on context exit anyway.
    pass
```

The async connection wrapper detects when the running event loop changes
between `asyncio.run()` calls and drops its references safely; you don't
need to call `close_all_async()` between requests, only at shutdown.

### Batch sizing for `bulk_create` / `bulk_update`

Both default to `batch_size=1000`. Each batch becomes one round-trip:

- `bulk_create`: a multi-row INSERT.
- `bulk_update`: an `UPDATE ... SET col = CASE pk WHEN ... END` whose SQL
  size is ≈ `batch_size × len(fields) × 2` parameter slots.

PostgreSQL's wire protocol caps a single statement's parameter count at
65535. With many fields, lower `batch_size` to stay safe — for example,
updating 10 columns means 1000 × 10 × 2 = 20 000 parameters per batch
(fine), but 5000 × 10 × 2 = 100 000 (will fail). When in doubt, batch by
500 for wide rows.

### Cancellation and shutdown

If a coroutine is cancelled mid-`await` on a queryset call, the connection
is released back to the pool by its async-context-manager (PG) or its SQL
finishes silently in the worker thread (SQLite). For deterministic cleanup
on process exit, await `close_all_async()` from a `lifespan`/shutdown hook
as shown above. Otherwise dorm relies on:

- aiosqlite worker threads being marked daemon (so the interpreter can
  exit even if you forget to close).
- An `atexit` hook that closes sync connections.

---

## Versioning and deprecation policy

djanorm follows [Semantic Versioning 2.0](https://semver.org/spec/v2.0.0.html):
``MAJOR.MINOR.PATCH``.

- **MAJOR** — breaking changes to the public API documented in this README.
  Removal of a field type, a backwards-incompatible change to model
  declarations, dropping support for a Python or DB version, or any rename
  that breaks `import dorm` symbols all qualify.
- **MINOR** — new features, performance improvements, and additions to the
  public API. Existing code keeps working.
- **PATCH** — bug fixes and internal refactors that don't touch the
  documented surface.

**Deprecation cycle.** Anything we plan to remove gets marked with a
``DeprecationWarning`` in a MINOR release and stays around for at least
**one full MINOR cycle** before disappearing in the next MAJOR. The
[CHANGELOG](CHANGELOG.md) flags every deprecation under a ``Deprecated``
heading so users can audit before upgrading.

**Stability scopes.**

| Scope | Stability |
|---|---|
| Top-level ``dorm.*`` exports listed in ``__all__`` | Stable — full SemVer guarantee |
| ``dorm.transaction``, ``dorm.signals``, ``dorm.contrib.pydantic`` | Stable |
| Model meta API (``Model._meta``) | Stable |
| ``dorm.db.connection`` (``get_connection``, ``get_async_connection``, ``close_all_async``) | Stable |
| ``dorm.db.backends.*`` internal cursor/pool plumbing | **Unstable** — refactor freely between minors |
| Names prefixed with ``_`` anywhere | Private — change without notice |

If you're pinning, ``djanorm>=X.0,<X+1.0`` is safe within a major.

---

## Dependencies

| Extra | Package | Purpose |
|---|---|---|
| `sqlite` | `aiosqlite` | Async SQLite |
| `postgresql` | `psycopg[binary,pool]` | Sync/Async PostgreSQL with connection pool |

## License

MIT
