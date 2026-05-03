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

``dorm migrate --fake`` and ``dorm migrate --fake-initial`` (3.0+)
record migrations as applied without running their operations —
useful when adopting dorm against a hand-managed legacy database.

If you really need to mark a single legacy migration
applied without running it, ``--fake`` does exactly that.

## You don't need `asgiref` (3.0+)

Django ships `asgiref.sync` because the Django ORM was sync-only for
years — every model call inside an async view had to be wrapped in
`sync_to_async(...)` to avoid blocking the event loop. dorm has a
**native async path from day one**, so the bridge is unnecessary.

| Django (sync ORM) | dorm (async-native) |
|---|---|
| `await sync_to_async(User.objects.get)(pk=1)` | `await User.objects.aget(pk=1)` |
| `await sync_to_async(list)(qs)` | `[u async for u in qs.aiterator()]` or `await qs` |
| `await sync_to_async(User.objects.create)(...)` | `await User.objects.acreate(...)` |
| `await sync_to_async(user.save)()` | `await user.asave()` |
| `await sync_to_async(qs.update)(...)` | `await qs.aupdate(...)` |
| `with transaction.atomic(): ...` | `async with aatomic(): ...` |

Every queryset / manager method in dorm has an `a*` counterpart that
runs through the async backend wrapper — no thread pool, no
sync-async bridge, no per-call Token allocation. **Don't import
`asgiref` for ORM code.** If you find yourself reaching for
`sync_to_async` around a `Model.objects` call, switch to the
matching `a*` method instead.

To catch this at dev / test time, opt into the async-guard:

```python
# conftest.py or app startup (development only)
from dorm.contrib.asyncguard import enable_async_guard
enable_async_guard(mode="warn")     # WARNING per offending call site
# enable_async_guard(mode="raise")  # raise on every offender
```

The guard hooks `pre_query` and walks the call stack — sync ORM
calls inside a running event loop trigger the configured action,
async calls stay silent.

## What's missing on purpose

- **No admin site.** dorm is an ORM, not a CMS framework.
- **No request/response middleware.** dorm has no HTTP layer.
- **Timezone-aware datetimes** ship in 3.0+: set
  `settings.USE_TZ = True` to enable Django ≥4-compatible behaviour
  (naive→aware conversion, UTC normalisation on insert,
  ``TIMESTAMP WITH TIME ZONE`` on PG). Default ``False`` keeps
  pre-3.0+ behaviour.
- **Optional `dorm.contrib.auth`** (3.0+). User / Group / Permission
  models with stdlib PBKDF2 hashing. Stateless reset tokens land
  in 3.0+ (``dorm.contrib.auth.tokens.PasswordResetTokenGenerator``)
  for the password-reset / email-verification flow.
- **`Meta.permissions = [...]`** (3.0+) — declare custom
  permissions on a model and surface them in the ``auth_permission``
  table via ``dorm.contrib.auth.management.sync_permissions()``.
- **`Meta.proxy = True`** (3.0+) — proxy models share the parent's
  table; the autodetector skips them so ``makemigrations`` doesn't
  emit a phantom ``CreateModel``.
- **`Model.from_db(db, field_names, values)`** (3.0+) — Django-parity
  hook for custom hydration. Stamps the resulting instance's
  ``_state.db`` with the alias the row came from.
- **`QuerySet.dates(field, kind)` / `datetimes(field, kind)`** (3.0+) —
  return ``list[date]`` / ``list[datetime]`` of distinct truncated
  values, suitable for archive listings.
- **`dorm.transaction.savepoint()` / `savepoint_commit()` /
  `savepoint_rollback()`** (3.0+) — manual savepoints inside an
  ``atomic()`` block. Mirror Django's
  ``django.db.transaction.savepoint`` family.
- **JSONField PG operators** (3.0+): ``__contained_by``,
  ``__has_key``, ``__has_keys``, ``__has_any_keys``, ``__overlap``,
  ``__len``. Same spelling as Django's ``contrib.postgres``.
- **`GenericForeignKey`** lives in `dorm.contrib.contenttypes`,
  same shape as Django's.
- **Optional encryption** (3.0+) via ``dorm.contrib.encrypted``
  (`EncryptedCharField` / `EncryptedTextField`). AES-GCM,
  deterministic mode for equality lookups, key rotation. Requires
  ``pip install 'djanorm[encrypted]'``.
- **Optional Prometheus exporter** (3.0+) via
  ``dorm.contrib.prometheus`` — counters + histograms in plain
  text-exposition format, no third-party scraper SDK.
- **Multi-tenant `dorm.contrib.tenants`** (3.0+) — PostgreSQL
  ``search_path`` switching via ``TenantContext`` /
  ``aTenantContext`` context managers; per-tenant migration
  runner lands with v3.1.
- **MySQL / MariaDB scaffold** (3.0+). ``ENGINE = "mysql"`` parses
  through ``parse_database_url`` and the connection wrapper
  raises ``ImproperlyConfigured`` pointing at the v3.1
  implementation milestone. Lets users pin on a forward-compatible
  config string today.

## What's better than Django

- **Async pool** with retry on transient errors and slow-query
  detection — works with FastAPI / Starlette out of the box.
- **`Field[T]` generics** — your IDE knows `user.name` is `str` and
  flags `user.naem`.
- **`DormSchema`** for FastAPI — single-source-of-truth schemas with
  `class Meta: model = User`, including nested relations.
- **Tiny dependency footprint**: `psycopg` + `aiosqlite`, optionally
  `pydantic`. No Django.
