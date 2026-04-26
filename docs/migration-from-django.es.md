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

Aún **no hay flag `--fake`**; si necesitas marcar una migración como
aplicada sin ejecutarla, hazlo manualmente vía la tabla del recorder.

## Lo que falta a propósito

- **Sin admin.** dorm es un ORM, no un framework CMS.
- **Sin `auth` / `contrib.*`.** Construye identidad / sesiones /
  etc. con lo que provea tu framework.
- **Sin middleware request/response.** dorm no tiene capa HTTP.
- **Sin datetimes con timezone** todavía. `TIME_ZONE` / `USE_TZ`
  son settings reservados; los datetimes se almacenan exactamente
  como los pasas.
- **Sin content types / GenericForeignKey.** Específicos del modelo
  de app registry de Django.

## Lo que es mejor que Django

- **Pool async** con retry ante errores transitorios y detección de
  slow queries — funciona con FastAPI / Starlette out of the box.
- **Generics `Field[T]`** — tu IDE sabe que `user.name` es `str` y
  flagea `user.naem`.
- **`DormSchema`** para FastAPI — esquemas single-source-of-truth
  con `class Meta: model = User`, incluyendo relaciones anidadas.
- **Footprint diminuto de dependencias**: `psycopg` + `aiosqlite`,
  opcionalmente `pydantic`. Sin Django.
