# Migrating from Django ORM

dorm's surface area is intentionally close to Django's, so most code
ports with renames + import changes. This page collects the
differences you'll actually hit.

## Imports

```python
# Django
from django.db import models, transaction

class User(models.Model):
    name = models.CharField(max_length=100)

# dorm
import dorm

class User(dorm.Model):
    name = dorm.CharField(max_length=100)
```

`dorm.transaction.atomic` matches `django.db.transaction.atomic`
(both as context manager and decorator).

## Settings

There's no `INSTALLED_APPS = ["django.contrib.auth", ...]`-style setup.
Either:

- Drop a `settings.py` next to your app packages and let `dorm`
  autodiscover, or
- Call `dorm.configure(DATABASES={...}, INSTALLED_APPS=["myapp"])`
  programmatically.

`dorm` doesn't ship `auth`, `admin`, `staticfiles`, or any of Django's
batteries — bring your own.

## Field cheat sheet

| Django | dorm |
|---|---|
| `models.CharField(max_length=N)` | `dorm.CharField(max_length=N)` |
| `models.TextField()` | `dorm.TextField()` |
| `models.IntegerField()` | `dorm.IntegerField()` |
| `models.BigIntegerField()` | `dorm.BigIntegerField()` |
| `models.DecimalField(...)` | `dorm.DecimalField(...)` |
| `models.BooleanField()` | `dorm.BooleanField()` |
| `models.DateField()` / `DateTimeField()` | same |
| `models.JSONField()` | `dorm.JSONField()` |
| `models.UUIDField()` | `dorm.UUIDField()` |
| `models.EmailField()` | `dorm.EmailField()` (validates on assignment) |
| `models.ForeignKey(To, on_delete=CASCADE)` | `dorm.ForeignKey(To, on_delete=dorm.CASCADE)` |
| `models.OneToOneField(...)` | `dorm.OneToOneField(...)` |
| `models.ManyToManyField(...)` | `dorm.ManyToManyField(...)` |
| `ArrayField` (postgres contrib) | `dorm.ArrayField(base_field)` |
| `BinaryField` | `dorm.BinaryField()` |
| `models.SlugField` | `dorm.SlugField()` |
| `auto_now=True` / `auto_now_add=True` | same on `DateTimeField` |
| `default=`, `null=`, `blank=` | same |
| `validators=[...]` | same |

## QuerySet cheat sheet

| Django | dorm | Notes |
|---|---|---|
| `qs.filter(x=1)` | `qs.filter(x=1)` | identical |
| `qs.exclude(x=1)` | `qs.exclude(x=1)` | identical |
| `qs.get(pk=1)` | `qs.get(pk=1)` | raises `Model.DoesNotExist` |
| `qs.aget(pk=1)` *(Django 4.2+)* | `qs.aget(pk=1)` | identical |
| `qs.values("a", "b")` | `qs.values("a", "b")` | returns chainable QS of dicts |
| `qs.count()` | `qs.count()` | identical |
| `qs.aggregate(Sum(...))` | `qs.aggregate(Sum(...))` | identical |
| `qs.bulk_create(objs)` | `qs.bulk_create(objs)` | identical |
| `qs.bulk_update(objs, fields)` | `qs.bulk_update(objs, fields)` | dorm uses one CASE WHEN per batch |
| `qs.iterator(chunk_size=N)` | `qs.iterator(chunk_size=N)` | server-side cursor on PG |
| `qs.explain()` | `qs.explain(analyze=True)` | dorm extra: print plan |
| `qs.using("replica")` | `qs.using("replica")` | identical |
| `qs.select_for_update()` | `qs.select_for_update()` | identical |
| `Q(a=1) | Q(b=2)` | `Q(a=1) | Q(b=2)` | identical |
| `F("col")` | `F("col")` | identical |

Methods you have in dorm and **not** in Django (yet):

- `qs.aexplain(analyze=True)` — async EXPLAIN.
- `await qs` — every QuerySet is awaitable; equivalent to materializing
  via `[x async for x in qs]`.

## Migrations

`makemigrations`, `migrate`, `showmigrations`, `squashmigrations`
behave like their Django siblings. New in dorm:

- `dorm migrate --dry-run` — print SQL without executing.
- `dorm dbcheck` — diff each model against the live schema.
- `dorm sql users.User` — print the `CREATE TABLE` for a model.

There's **no `--fake`** flag yet; if you need to mark a migration
applied without running it, do it manually via the recorder table.

## What's missing on purpose

- **No admin site.** dorm is an ORM, not a CMS framework.
- **No `auth` / `contrib.*`.** Build identity / sessions / etc. with
  whatever your framework provides.
- **No request/response middleware.** dorm has no HTTP layer.
- **No timezone-aware datetimes** yet. `TIME_ZONE` / `USE_TZ` are
  reserved settings; datetimes are stored exactly as you provide them.
- **No content types / GenericForeignKey.** Specific to Django's app
  registry model.

## What's better than Django

- **Async pool** with retry on transient errors and slow-query
  detection — works with FastAPI / Starlette out of the box.
- **`Field[T]` generics** — your IDE knows `user.name` is `str` and
  flags `user.naem`.
- **`DormSchema`** for FastAPI — single-source-of-truth schemas with
  `class Meta: model = User`, including nested relations.
- **Tiny dependency footprint**: `psycopg` + `aiosqlite`, optionally
  `pydantic`. No Django.
