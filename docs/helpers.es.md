# Helpers framework-agnósticos (3.4)

Helpers pensados sobre todo para stacks FastAPI / Litestar /
Starlette / aiohttp pero que **no acoplan** ningún framework.
Core sigue puro: ningún `import fastapi` ni `import django` en
`dorm/`.

Los plugins **mypy** y **pytest** viven en paquetes separados
(``djanorm-mypy`` y ``pytest-djanorm``) — ver
[paquetes hermanos](sibling-packages.md) para el motivo.

## Query budget

```python
import dorm

# Sync — máximo 200ms wall-clock + máximo 10k filas materializadas.
with dorm.budget(timeout_ms=200, max_rows=10_000):
    rows = list(Author.objects.filter(active=True))

# Async equivalente.
async with dorm.abudget(timeout_ms=200):
    authors = await Author.objects.afilter(active=True)
```

`timeout_ms` en PG abre `atomic()` implícito + `SET LOCAL statement_timeout`.
`max_rows` chequea client-side post-fetch en cualquier backend.
`BudgetExceeded` subclase `DatabaseError`.

Bloques anidados → mínimo gana (inner aprieta, nunca relaja).

## Streaming primitives

```python
from dorm.contrib.streaming import stream_jsonl, astream_csv

# Cualquier framework: la salida es bytes.
async def export(qs):
    async for chunk in astream_csv(qs.values("name", "age")):
        yield chunk

# Sync con FastAPI / Starlette `StreamingResponse`.
return StreamingResponse(stream_jsonl(qs), media_type="application/x-ndjson")
```

Formatos: `stream_json` (array), `stream_jsonl` (NDJSON),
`stream_csv` (RFC-4180), `stream_ndjson_pretty` (humano).
`chunk_size` configurable (default 1000).

Tipos especiales (datetime, Decimal, UUID, Enum, bytes) serializan
limpiamente. Memory-bounded — backed por `iterator()`/`aiterator()`.

## Pydantic adapter expansión

```python
from dorm.contrib.pydantic import (
    schema_for, list_response_schema, schema_with_computed,
    schema_for_with_examples, nested_schema_for,
)

AuthorOut = schema_for(Author)
AuthorList = list_response_schema(AuthorOut)  # {items, total, next_cursor, has_more}

# Con propiedades calculadas
AuthorWithFlag = schema_with_computed(
    Author, computed={"is_adult": bool}
)

# OpenAPI examples auto desde DB
AuthorDoc = schema_for_with_examples(Author, sample_count=2)

# Anidado FK + M2M depth=2
AuthorDeep = nested_schema_for(Author, depth=2)
```

## N+1 detector como context manager

```python
from dorm.contrib.nplusone import detect

# Middleware típico
with detect(raise_on_detect=False) as d:
    response = handle_request()
if d.findings:
    log.warning("N+1 detected: %s", d.report())

# Test estricto
from dorm.contrib.nplusone import assert_no_nplusone
def test_view():
    with assert_no_nplusone():
        list(Author.objects.all())  # raises if N+1
```

Async: `async with adetect(): ...`.

## AsyncModel — async-only

```python
from dorm.contrib.asyncmodel import AsyncModel, AsyncOnlyError

class Author(AsyncModel):
    name = dorm.CharField(max_length=100)

# Síncrono → falla rápido
Author.objects.create(name="x")  # AsyncOnlyError

# Async funciona
await Author.objects.acreate(name="x")
```

## Idempotency primitive

```python
from dorm.contrib.idempotency import IdempotencyRecord, idempotency_key

class IdpEntry(IdempotencyRecord):
    class Meta:
        db_table = "idempotency"

# Handler
with idempotency_key(request.headers["Idempotency-Key"], model=IdpEntry) as ctx:
    if ctx.replay:
        return ctx.cached_response
    result = process_payment(...)
    ctx.store(result, status_code=201)
    return result
```

Block envuelto en `atomic()` — outbox row + business write commit
juntos.

## CLI: dorm diff (drift detection)

```bash
dorm diff --apps myapp.models  # exit 0 si limpio, 1 si drift
dorm diff --json > drift.json  # CI-friendly
```

Compara modelos vs `information_schema`/`sqlite_master`. Detecta:
tabla faltante, tabla extra, columna faltante, tipo mismatch.

## CLI: dorm purge-deleted

```bash
dorm purge-deleted --older-than 30d
dorm purge-deleted --older-than 12h --dry-run
dorm purge-deleted --older-than 90d --apps myapp.models
```

Hard-delete en `SoftDeleteModel` rows con `deleted_at < now() - DURATION`.
Cron-friendly. Sufijos: `s`/`m`/`h`/`d`/`w`.

## CLI: dorm export-json-schema

```bash
dorm export-json-schema > schemas.json
dorm export-json-schema --out schemas/ --apps myapp.models
dorm export-json-schema --include-relations --out schemas/
```

Genera Draft 2020-12 JSON Schema por modelo. Mapea formatos:
`uuid`, `email`, `uri`, `date-time`, `date`, `time`. `maxLength`
desde `CharField.max_length`.

## Lag-aware read router

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASES = {"primary": {...}, "r1": {...}, "r2": {...}}
DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["r1", "r2"],
        max_lag_seconds=2.0,
        cache_seconds=5.0,
    ),
]
```

Consulta `pg_last_xact_replay_timestamp()` antes de cada read.
Replicas con lag > umbral son evitadas; cuando todas están lagged,
deflecta al primary. Cache 5 s amortiza el coste del check.

## GIS

```python
from dorm.contrib.gis import Geom, PointField, PolygonField

class Store(dorm.Model):
    location = PointField(srid=4326)
    zone = PolygonField(srid=4326)

# Crear
Store.objects.create(
    location=Geom.point(2.17, 41.38),  # Barcelona
    zone=Geom.polygon([[[0, 0], [1, 0], [1, 1], [0, 0]]]),
)

# Lookups espaciales
Store.objects.filter(zone__intersects=Geom.point(0.5, 0.5))
Store.objects.filter(location__distance_lte=(target, 1000))
```

PostGIS en PG, BLOB+spatialite en SQLite. Lookups disponibles:
`intersects`, `within`, `contains`, `distance_lte`, `distance_gte`.

## Meta.read_only

```python
class AuditLog(dorm.Model):
    event = dorm.CharField(max_length=100)
    payload = dorm.JSONField()

    class Meta:
        db_table = "audit_log"
        read_only = True

# Reads OK, writes bloqueados
list(AuditLog.objects.filter(event="login"))   # ok
AuditLog(event="x").save()                      # ReadOnlyModelError
```

## Sibling packages

### pytest-djanorm

```bash
pip install pytest-djanorm
pip install 'pytest-djanorm[postgres]'  # + testcontainers PG
```

Fixtures auto-loaded: `djanorm_settings`, `pg_container`,
`transactional_db`, `atransactional_db`, `nplusone_guard`.

### djanorm-mypy

```bash
pip install djanorm-mypy
```

`pyproject.toml`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

Valida kwargs `filter()`/`exclude()`/`get()` contra el modelo,
suffixes lookup, sintetiza `pk`/`id` en cada Model subclass.
