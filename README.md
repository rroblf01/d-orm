# djanorm

[![PyPI version](https://img.shields.io/pypi/v/djanorm.svg)](https://pypi.org/project/djanorm/)
[![Python versions](https://img.shields.io/pypi/pyversions/djanorm.svg)](https://pypi.org/project/djanorm/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docs](https://img.shields.io/badge/docs-rroblf01.github.io-blue.svg)](https://rroblf01.github.io/d-orm/)
[![Typing: Typed](https://img.shields.io/badge/typing-PEP%20561-brightgreen.svg)](https://peps.python.org/pep-0561/)

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

Works with **SQLite**, **PostgreSQL**, **CockroachDB** (4.1+), **MySQL / MariaDB**, **libsql / Turso** and (4.0+) **DuckDB** for embedded analytics. Ships with migrations + linter, atomic transactions, signals, validation, relationship loading (`select_related` / `prefetch_related` / `FilteredRelation`), aggregations, DB functions, async-native ORM path, queryset & row caching, audit-trail tracking (`@track_history`), multi-tenancy (schema-level + row-level + PG Row-Level Security ops), recursive CTEs, full-text search, GIS, and Pydantic interop — all with real static typing (`Field[T]`).

**Production primitives**: query budget (HTTP SLA), circuit breaker, outbox pattern, hash sharding, idempotency keys, lag-aware read routing, async pool task affinity, online (zero-downtime) migrations, schema drift detection (`dorm diff`), **PgBouncer transaction-pool compatibility** (4.1+), **PII field tagging** (4.1+), **framework-agnostic ASGI middleware** + **Litestar plugin** (4.1+).

**Sibling packages**: [`pytest-djanorm`](pytest-djanorm/) (test fixtures) and [`djanorm-mypy`](djanorm-mypy/) (mypy plugin) ship in their own packages so the main wheel never pulls dev tooling.

Release notes for every version live in [CHANGELOG.md](CHANGELOG.md). For the **4.3** highlights see [docs/v4_3.md](docs/v4_3.en.md) + the [upgrading guide](docs/upgrading-to-4.3.en.md); the **4.2** highlights live in [docs/v4_2.md](docs/v4_2.en.md); the **4.1** highlights live in [docs/v4_1.md](docs/v4_1.en.md); the **4.0** highlights live in [docs/v4_0.md](docs/v4_0.en.md); upgrading from 3.3 → 4.0 is documented in [docs/upgrading-to-4.0.md](docs/upgrading-to-4.0.en.md). 4.2 → 4.3 needs no code changes.

## Installation

```bash
# SQLite
pip install "djanorm[sqlite]"

# PostgreSQL
pip install "djanorm[postgresql]"

# libsql / Turso (local, embedded replica or remote)
pip install "djanorm[libsql]"

# DuckDB (embedded analytics, 4.0+)
pip install "djanorm[duckdb]"

# CockroachDB (distributed SQL, 4.1+ — reuses the psycopg pipeline)
pip install "djanorm[cockroachdb]"

# Optional extras
pip install "djanorm[redis]"      # queryset + row cache backend
pip install "djanorm[encrypted]"  # AES-GCM EncryptedCharField/TextField
pip install "djanorm[pydantic]"   # FastAPI-friendly DormSchema
pip install "djanorm[s3]"         # FileField on AWS S3 / MinIO / R2 / B2
pip install "djanorm[litestar]"   # DormPlugin: ASGI middleware + lifespan hooks (4.1+)
pip install "djanorm[parquet]"    # QueryLog.dump_parquet via pyarrow (4.1+)

# Dev tooling (sibling packages)
pip install pytest-djanorm djanorm-mypy
```

## Quick start

### 1. Scaffold a project

```bash
dorm init blog
```

That creates:

- `settings.py` — uncomment the `DATABASES` block matching your backend.
- `blog/` — an app package with an empty `models.py`.

A minimal `settings.py` looks like:

```python
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "db.sqlite3",
    },
}
INSTALLED_APPS = ["blog"]
```

### 2. Define a model

```python
# blog/models.py
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    is_active = dorm.BooleanField(default=True)


class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    published_at = dorm.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-published_at"]
```

### 3. Generate and apply migrations

```bash
dorm makemigrations blog
dorm migrate
```

### 4. Use it

Open a shell with `dorm shell` (IPython auto-detected) or import
the models from your own script.

```python
from blog.models import Author, Post

# Create
alice = Author.objects.create(name="Alice", email="alice@example.com")
post = Post.objects.create(
    title="Hello world",
    body="First post body.",
    author=alice,
)

# Bulk create
Post.objects.bulk_create([
    Post(title=f"Draft {i}", body="...", author=alice)
    for i in range(5)
])

# Filter / exclude / Q / F
from dorm import Q, F

active_authors = Author.objects.filter(is_active=True)
some_posts = Post.objects.filter(
    Q(title__icontains="hello") | Q(title__startswith="Draft")
).exclude(published_at__isnull=True)

# Lookups across relations
alices_posts = Post.objects.filter(author__name="Alice")

# select_related / prefetch_related to dodge N+1
for post in Post.objects.select_related("author"):
    print(post.author.name, post.title)   # 1 query, JOIN

# Get one
post = Post.objects.get(pk=1)

# Update — single instance
post.title = "Renamed"
post.save()

# Update — bulk via queryset
Post.objects.filter(author=alice).update(title=F("title") + " (by Alice)")

# Delete — single instance
post.delete()

# Delete — bulk
Post.objects.filter(published_at__isnull=True).delete()
```

### Async API (same names with `a` prefix)

```python
from blog.models import Author, Post

async def main():
    alice = await Author.objects.acreate(name="Alice", email="a@x.com")
    post = await Post.objects.acreate(title="Hi", body="...", author=alice)

    async for p in Post.objects.filter(author=alice):
        print(p.title)

    await Post.objects.filter(pk=post.pk).aupdate(title="Hi!")
    await post.adelete()
```

### Atomic transactions

```python
from dorm import transaction

with transaction.atomic():
    alice = Author.objects.create(name="Alice", email="a@x.com")
    Post.objects.create(title="t", body="b", author=alice)
    # any exception here rolls back both inserts
```

## Documentation

The full documentation, tutorials and API reference are published at:

**https://rroblf01.github.io/d-orm/**

You will find the getting-started guide, complete examples, the API reference and production deployment notes there.

## Contributing

Everyone is welcome to get involved! If you want to suggest changes, propose improvements or discuss the direction of the project, open an issue or a pull request on this repository. Discussions, ideas and critiques are very welcome.

## License

See [LICENSE](LICENSE).
