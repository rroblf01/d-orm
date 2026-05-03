# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

Works with **SQLite**, **PostgreSQL** and **libsql / Turso**. Ships with migrations + linter, atomic transactions, signals, validation, relationship loading (`select_related` / `prefetch_related`), aggregations, DB functions, async-native ORM path, queryset & row caching, and Pydantic interop — all with real static typing (`Field[T]`).

## What's new in 3.1

- **`settings.USE_TZ = True`** — Django ≥4-compatible timezone-aware datetimes (UTC normalisation on insert, `TIMESTAMP WITH TIME ZONE` on PG).
- **`Meta.proxy = True`** — proxy models share the parent's table; autodetector skips them so `makemigrations` doesn't emit a phantom `CreateModel`.
- **`QuerySet.dates(field, kind)` / `datetimes(...)`** — distinct truncated values for archive listings.
- **`dorm migrate --fake` / `--fake-initial`** — record migrations as applied without running operations. Ideal for adopting dorm against a legacy schema.
- **JSONField PG operators** — `__contained_by`, `__has_key`, `__has_keys`, `__has_any_keys`, `__overlap`, `__len`. Same spelling as Django's `contrib.postgres`.
- **`Field.deconstruct()`** — base-class implementation for migration serialisation. Custom field subclasses get it for free.
- **`Model.from_db(db, field_names, values)`** — Django-parity hydration hook; stamps `_state.db` with the alias.
- **`dorm.transaction.savepoint()` / `savepoint_commit()` / `savepoint_rollback()`** — manual savepoints inside `atomic()`.
- **`dorm.contrib.auth.tokens`** — stateless HMAC-signed reset tokens for password-reset / email-verification flows.
- **`Meta.permissions = [...]`** + **`sync_permissions()`** — declare custom permissions, materialise into `auth_permission`.
- **`dorm.contrib.tenants`** — `TenantContext` / `aTenantContext` for PostgreSQL `search_path` switching.
- **MySQL / MariaDB scaffold** — `ENGINE = "mysql"` parses through `parse_database_url`; the connection wrapper raises `ImproperlyConfigured` pointing at v3.1 for the full implementation.
- **MySQL / MariaDB vector support** — `VectorField` returns `VECTOR(N)` and distance expressions compile to `VEC_DISTANCE_EUCLIDEAN` / `VEC_DISTANCE_COSINE`.

## What's new in 3.0

- **`dorm.contrib.auth`** — `User` / `Group` / `Permission` with stdlib PBKDF2 hashing. Same shape as Django, no `passlib` dependency.
- **`dorm.contrib.encrypted`** — `EncryptedCharField` / `EncryptedTextField` (AES-GCM with key rotation; `pip install 'djanorm[encrypted]'`).
- **`dorm.contrib.asyncguard`** — surfaces sync ORM calls inside an event loop as warnings or exceptions.
- **`dorm.contrib.querylog`** + **`dorm.contrib.querycount`** — request-scoped collectors for SQL traffic and N+1 guards.
- **`dorm.contrib.prometheus`** — stdlib-only metrics exposer for the `/metrics` endpoint.
- **`dorm lint-migrations`** — pre-merge gate that flags online-deploy footguns (full-table backfills, missing `concurrently=True`, irreversible `RunPython`).
- **`LocMemCache`** + **`Manager.cache_get(pk=…)`** / `cache_get_many(pks=[…])` — in-process LRU + single-row caching that piggy-backs on the same invalidation signal as queryset cache.
- **Sticky read-after-write window** for the DB router — no stale replica reads after a write on the same request.
- **`settings.SLOW_QUERY_MS`** — slow-query WARNING; `settings.RETRY_ATTEMPTS` / `RETRY_BACKOFF` — transient-error retry knobs (resolution: explicit setting > env var > default).
- **`dorm.test.assertNumQueries`** + `assertMaxQueries` — context-manager and decorator forms (sync + async).
- **Async-aware `dorm shell`** — top-level `await` works in the stdlib REPL fallback.
- **Math / string DB functions** — `Power`, `Sqrt`, `Mod`, `Sign`, `Ceil`, `Floor`, `Log`, `Ln`, `Exp`, `Random`, `Trim`, `LTrim`, `RTrim`, `NullIf`.

Full notes in [CHANGELOG.md](CHANGELOG.md). **Zero breaking changes vs 2.5** — every addition is opt-in or zero-cost when unused.

## Installation

```bash
# SQLite
pip install "djanorm[sqlite]"

# PostgreSQL
pip install "djanorm[postgresql]"

# libsql / Turso (local, embedded replica or remote)
pip install "djanorm[libsql]"

# Optional extras
pip install "djanorm[redis]"      # queryset + row cache backend
pip install "djanorm[encrypted]"  # AES-GCM EncryptedCharField/TextField
pip install "djanorm[pydantic]"   # FastAPI-friendly DormSchema
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
