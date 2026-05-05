# Migrar desde el ORM de Django

La superficie de dorm está intencionadamente cerca de la de Django,
así que la mayoría del código se porta con renames + cambios de
import. Esta página recoge las diferencias con las que te vas a
topar de verdad.

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

`dorm.transaction.atomic` se corresponde con
`django.db.transaction.atomic` (tanto context manager como
decorador).

## Settings

No hay setup tipo `INSTALLED_APPS = ["django.contrib.auth", ...]`.
O bien:

- Pones un `settings.py` junto a tus paquetes y dejas que dorm haga
  autodiscover, o
- Llamas a `dorm.configure(DATABASES={...}, INSTALLED_APPS=["miapp"])`
  programáticamente.

dorm no trae `auth`, `admin`, `staticfiles`, ni ninguna otra
batería de Django — tráete las tuyas.

## Cheat sheet de campos

| Django | dorm |
|---|---|
| `models.CharField(max_length=N)` | `dorm.CharField(max_length=N)` |
| `models.TextField()` | `dorm.TextField()` |
| `models.IntegerField()` | `dorm.IntegerField()` |
| `models.BigIntegerField()` | `dorm.BigIntegerField()` |
| `models.DecimalField(...)` | `dorm.DecimalField(...)` |
| `models.BooleanField()` | `dorm.BooleanField()` |
| `models.DateField()` / `DateTimeField()` | igual |
| `models.JSONField()` | `dorm.JSONField()` |
| `models.UUIDField()` | `dorm.UUIDField()` |
| `models.EmailField()` | `dorm.EmailField()` (valida al asignar) |
| `models.ForeignKey(To, on_delete=CASCADE)` | `dorm.ForeignKey(To, on_delete=dorm.CASCADE)` |
| `models.OneToOneField(...)` | `dorm.OneToOneField(...)` |
| `models.ManyToManyField(...)` | `dorm.ManyToManyField(...)` |
| `ArrayField` (contrib postgres) | `dorm.ArrayField(base_field)` |
| `BinaryField` | `dorm.BinaryField()` |
| `models.SlugField` | `dorm.SlugField()` |
| `auto_now=True` / `auto_now_add=True` | igual en `DateTimeField` |
| `default=`, `null=`, `blank=` | igual |
| `validators=[...]` | igual |

## Cheat sheet de QuerySet

| Django | dorm | Notas |
|---|---|---|
| `qs.filter(x=1)` | `qs.filter(x=1)` | idéntico |
| `qs.exclude(x=1)` | `qs.exclude(x=1)` | idéntico |
| `qs.get(pk=1)` | `qs.get(pk=1)` | lanza `Model.DoesNotExist` |
| `qs.aget(pk=1)` *(Django 4.2+)* | `qs.aget(pk=1)` | idéntico |
| `qs.values("a", "b")` | `qs.values("a", "b")` | devuelve QS encadenable de dicts |
| `qs.count()` | `qs.count()` | idéntico |
| `qs.aggregate(Sum(...))` | `qs.aggregate(Sum(...))` | idéntico |
| `qs.bulk_create(objs)` | `qs.bulk_create(objs)` | idéntico |
| `qs.bulk_update(objs, fields)` | `qs.bulk_update(objs, fields)` | dorm usa un CASE WHEN por batch |
| `qs.iterator(chunk_size=N)` | `qs.iterator(chunk_size=N)` | cursor server-side en PG |
| `qs.explain()` | `qs.explain(analyze=True)` | extra de dorm: imprime el plan |
| `qs.using("replica")` | `qs.using("replica")` | idéntico |
| `qs.select_for_update()` | `qs.select_for_update()` | idéntico |
| `Q(a=1) | Q(b=2)` | `Q(a=1) | Q(b=2)` | idéntico |
| `F("col")` | `F("col")` | idéntico |

Métodos que tienes en dorm y **no** (todavía) en Django:

- `qs.aexplain(analyze=True)` — EXPLAIN async.
- `await qs` — cada QuerySet es awaitable; equivalente a
  materializar con `[x async for x in qs]`.

## Migraciones

`makemigrations`, `migrate`, `showmigrations`, `squashmigrations` se
comportan como sus hermanos de Django. Nuevo en dorm:

- `dorm migrate --dry-run` — imprime SQL sin ejecutar.
- `dorm dbcheck` — diff de cada modelo contra el esquema vivo.
- `dorm sql users.User` — imprime el `CREATE TABLE` de un modelo.

``dorm migrate --fake`` y ``dorm migrate --fake-initial`` (3.0+)
registran migraciones como aplicadas sin ejecutar sus operaciones —
útil al adoptar dorm contra una base legacy administrada a mano.

## No necesitas `asgiref` (3.0+)

Django incluye `asgiref.sync` porque el ORM de Django fue sync-only
durante años — toda llamada al modelo dentro de una vista async
necesitaba envolverse en `sync_to_async(...)` para no bloquear el
event loop. dorm tiene **path async nativo desde el día uno**, así
que el puente es innecesario.

| Django (ORM sync) | dorm (async-nativo) |
|---|---|
| `await sync_to_async(User.objects.get)(pk=1)` | `await User.objects.aget(pk=1)` |
| `await sync_to_async(list)(qs)` | `[u async for u in qs.aiterator()]` o `await qs` |
| `await sync_to_async(User.objects.create)(...)` | `await User.objects.acreate(...)` |
| `await sync_to_async(user.save)()` | `await user.asave()` |
| `await sync_to_async(qs.update)(...)` | `await qs.aupdate(...)` |
| `with transaction.atomic(): ...` | `async with aatomic(): ...` |

Cada método de queryset / manager en dorm tiene contraparte `a*`
que pasa por el wrapper async del backend — sin thread pool, sin
puente sync-async, sin alloc de Token por llamada. **No importes
`asgiref` para código de ORM.** Si te ves alcanzando `sync_to_async`
alrededor de un `Model.objects.*`, cambia al método `a*` que toca.

Para detectarlo en dev / tests, activa el async-guard:

```python
# conftest.py o startup de app (solo desarrollo)
from dorm.contrib.asyncguard import enable_async_guard
enable_async_guard(mode="warn")     # WARNING por call site infractor
# enable_async_guard(mode="raise")  # raise en todos los infractores
```

El guard engancha `pre_query` y recorre el call stack — llamadas
sync al ORM dentro de un event loop disparan la acción configurada;
las llamadas async pasan en silencio.

## Lo que falta a propósito

- **Sin admin.** dorm es un ORM, no un framework CMS.
- **Sin middleware request/response.** dorm no tiene capa HTTP.
- **Datetimes con timezone** llegan en 3.0+: pon
  `settings.USE_TZ = True` para activar el comportamiento Django ≥4
  (conversión naive→aware, normalización UTC en INSERT,
  ``TIMESTAMP WITH TIME ZONE`` en PG). Default ``False`` mantiene
  el comportamiento previo a 3.0.
- **`dorm.contrib.auth` opcional** (3.0+). Modelos User / Group /
  Permission con hashing PBKDF2 stdlib. Tokens de reset stateless
  llegan en 3.0
  (``dorm.contrib.auth.tokens.PasswordResetTokenGenerator``) para
  el flujo de password-reset / email-verification.
- **`Meta.permissions = [...]`** (3.0+) — declara permisos
  custom y materialízalos en ``auth_permission`` con
  ``dorm.contrib.auth.management.sync_permissions()``.
- **`Meta.proxy = True`** (3.0+) — proxy models comparten la
  tabla del padre; el autodetector los salta así
  ``makemigrations`` no emite un ``CreateModel`` fantasma.
- **`Model.from_db(db, field_names, values)`** (3.0+) — hook de
  Django para hidratación custom. Stampa el alias en
  ``_state.db``.
- **`QuerySet.dates(field, kind)` / `datetimes(field, kind)`** (3.0+)
  — devuelven ``list[date]`` / ``list[datetime]`` truncados
  distinct para listings de archivo.
- **`dorm.transaction.savepoint()` / `savepoint_commit()` /
  `savepoint_rollback()`** (3.0+) — savepoints manuales dentro
  de un ``atomic()``. Misma forma que la familia
  ``django.db.transaction.savepoint``.
- **Operadores PG sobre JSONField** (3.0+): ``__contained_by``,
  ``__has_key``, ``__has_keys``, ``__has_any_keys``,
  ``__overlap``, ``__len``. Misma forma que ``contrib.postgres``
  de Django.
- **`GenericForeignKey`** vive en `dorm.contrib.contenttypes`,
  con la misma forma que Django.
- **Cifrado opcional** (3.0+) via ``dorm.contrib.encrypted``
  (`EncryptedCharField` / `EncryptedTextField`). AES-GCM, modo
  determinista para lookups de igualdad, rotación de claves.
  Requiere ``pip install 'djanorm[encrypted]'``.
- **Exporter Prometheus opcional** (3.0+) via
  ``dorm.contrib.prometheus`` — contadores + histogramas en
  formato text-exposition plano, sin SDK externo.
- **Multi-tenant `dorm.contrib.tenants`** (3.0+) — switch de
  PostgreSQL ``search_path`` via ``TenantContext`` /
  ``aTenantContext``; runner de migraciones por tenant llega en v3.1.
- **Scaffold MySQL / MariaDB** (3.0+). ``ENGINE = "mysql"`` parsea
  por ``parse_database_url`` y el wrapper de conexión raisea
  ``ImproperlyConfigured`` apuntando al milestone v3.1. Permite
  pinear con un config string forward-compatible hoy.

## Lo que es mejor que Django

- **Pool async** con retry ante errores transitorios y detección de
  slow queries — funciona con FastAPI / Starlette out of the box.
- **Generics `Field[T]`** — tu IDE sabe que `user.name` es `str` y
  flagea `user.naem`. Plugin `djanorm-mypy` lo extiende a kwargs
  `filter()` y suffixes lookup en compile-time.
- **`DormSchema`** para FastAPI — esquemas single-source-of-truth
  con `class Meta: model = User`, incluyendo relaciones anidadas.
- **Footprint diminuto de dependencias**: `psycopg` + `aiosqlite`,
  opcionalmente `pydantic`. Sin Django.
- **Production hardening built-in** — circuit breaker, query
  budget, lag-aware routing, outbox, sharding, idempotency keys.

## Tabla equivalencias rápidas (4.0)

### Imports cotidianos

| Django | dorm |
|---|---|
| `from django.db import models` | `import dorm` |
| `models.Model` | `dorm.Model` |
| `models.CharField` / `IntegerField` / etc. | `dorm.CharField` / etc. |
| `models.Q`, `models.F`, `models.Subquery`, `models.Exists` | `dorm.Q`, `dorm.F`, `dorm.Subquery`, `dorm.Exists` |
| `models.Count`, `Sum`, `Avg`, ... | `dorm.Count`, `dorm.Sum`, `dorm.Avg`, ... |
| `models.OuterRef` | `dorm.OuterRef` |
| `models.FilteredRelation` | `dorm.FilteredRelation` |
| `models.UniqueConstraint`, `CheckConstraint` | `dorm.UniqueConstraint`, `dorm.CheckConstraint` |
| `models.Index` | `dorm.Index` |
| `from django.db import transaction` | `from dorm import transaction` |
| `transaction.atomic()` | `dorm.transaction.atomic()` |
| `from django.db import connection` | `from dorm.db.connection import get_connection` |

### `contrib.postgres`

| Django | dorm |
|---|---|
| `contrib.postgres.fields.ArrayField` | `dorm.ArrayField` |
| `contrib.postgres.fields.JSONField` | `dorm.JSONField` (también en SQLite) |
| `contrib.postgres.fields.HStoreField` | `dorm.HStoreField` (4.0+; fallback TEXT en SQLite) |
| `contrib.postgres.fields.RangeField` | `dorm.RangeField` y subclases |
| `contrib.postgres.search.SearchVector` | `dorm.search.SearchVector` |
| `contrib.postgres.search.SearchQuery` | `dorm.search.SearchQuery` |
| `contrib.postgres.search.SearchRank` | `dorm.search.SearchRank` |
| `contrib.postgres.search.SearchHeadline` | `dorm.search.SearchHeadline` |
| `contrib.postgres.search.TrigramSimilarity` | `dorm.search.TrigramSimilarity` (4.0+) |
| `contrib.postgres.aggregates.StringAgg` | `dorm.StringAgg` |
| `contrib.postgres.aggregates.ArrayAgg` | `dorm.ArrayAgg` |
| `contrib.postgres.aggregates.BoolAnd/BoolOr` | `dorm.BoolAnd` / `dorm.BoolOr` |

### `Choices` / Enums

```python
# Django
class Status(models.TextChoices):
    ACTIVE = "active", "Active"
    ARCHIVED = "archived", "Archived"

class Article(models.Model):
    status = models.CharField(max_length=10, choices=Status.choices)

# dorm — usa enum.Enum + EnumField
import enum

class Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"

class Article(dorm.Model):
    status = dorm.EnumField(Status, default=Status.ACTIVE)
    # O ENUM nativo PG (4.0+):
    # status = dorm.EnumField(Status, native=True, type_name="article_status")
```

### Forms

Django tiene `ModelForm`. dorm **no tiene forms** intencionalmente
— el target es 99% FastAPI / Litestar / aiohttp donde la
validación va a Pydantic. Para schemas input/output usa
`dorm.contrib.pydantic`:

```python
from dorm.contrib.pydantic import (
    schema_for, create_schema_for, update_schema_for,
)

AuthorOut = schema_for(Author)
AuthorCreate = create_schema_for(Author)            # POST body
AuthorUpdate = update_schema_for(Author)            # PATCH body, todo opcional
```

### Admin

Django tiene `contrib.admin`. dorm **no tiene admin** built-in. El
target FastAPI tiende a `sqladmin` o dashboard custom. Para
generar JSON Schema desde modelos (input para tools de admin
externas) usa `dorm export-json-schema --out schemas/`.

### `select_for_update` / señales / migraciones

Mismos APIs que Django. Operaciones de migración nuevas en 4.0
que Django no tiene: `AddFieldOnline`, `BackfillBatch`,
`SetNotNullOnline`, `CreateMaterializedView`,
`CreatePartitionedTable`, `CreatePGEnum`.

### Multi-tenancy

| Django | dorm |
|---|---|
| `django-tenants` (3rd party, schema) | `dorm.contrib.tenants` (built-in, schema) |
| Manager middleware con filter manual | `dorm.contrib.tenants_row.TenantModel` (4.0+) |

### GIS

| Django | dorm |
|---|---|
| `contrib.gis.db.models.PointField` | `dorm.contrib.gis.PointField` (4.0+) |
| `contrib.gis.db.models.PolygonField` | `dorm.contrib.gis.PolygonField` (4.0+) |
| `__intersects`, `__within`, `__contains`, `__distance_lte` | mismo, en `dorm.contrib.gis` |

### Tooling dev

| Django | dorm |
|---|---|
| `django-stubs` (mypy plugin) | `djanorm-mypy` (paquete hermano) |
| `pytest-django` | `pytest-djanorm` (paquete hermano) |
