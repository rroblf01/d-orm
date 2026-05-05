# Framework-agnostic helpers (3.4)

Helpers tuned for FastAPI / Litestar / Starlette / aiohttp stacks
but **not coupling** any framework. Core stays pure: no
``import fastapi`` or ``import django`` anywhere in ``dorm/``.

The **mypy** and **pytest** plugins ship as separate packages
(``djanorm-mypy`` and ``pytest-djanorm``) — see
[sibling packages](sibling-packages.md) for the rationale.

## Query budget

```python
import dorm

with dorm.budget(timeout_ms=200, max_rows=10_000):
    rows = list(Author.objects.filter(active=True))

async with dorm.abudget(timeout_ms=200):
    authors = await Author.objects.afilter(active=True)
```

`timeout_ms` on PG opens an implicit `atomic()` + `SET LOCAL
statement_timeout`. `max_rows` is checked client-side post-fetch on
every backend. `BudgetExceeded` subclasses `DatabaseError`.

Nested blocks combine via the strictest active value.

## Streaming primitives

```python
from dorm.contrib.streaming import stream_jsonl, astream_csv

async def export(qs):
    async for chunk in astream_csv(qs.values("name", "age")):
        yield chunk

return StreamingResponse(stream_jsonl(qs), media_type="application/x-ndjson")
```

Formats: `stream_json` (array), `stream_jsonl` (NDJSON),
`stream_csv` (RFC-4180), `stream_ndjson_pretty` (human).

datetime / Decimal / UUID / Enum / bytes serialise cleanly.

## Pydantic adapter expansion

```python
from dorm.contrib.pydantic import (
    schema_for, list_response_schema, schema_with_computed,
    schema_for_with_examples, nested_schema_for,
)

AuthorOut = schema_for(Author)
AuthorList = list_response_schema(AuthorOut)
AuthorWithFlag = schema_with_computed(Author, computed={"is_adult": bool})
AuthorDoc = schema_for_with_examples(Author, sample_count=2)
AuthorDeep = nested_schema_for(Author, depth=2)
```

## N+1 detector

```python
from dorm.contrib.nplusone import detect, adetect

with detect(raise_on_detect=False) as d:
    response = handle_request()
if d.findings:
    log.warning("N+1 detected: %s", d.report())
```

## AsyncModel

```python
from dorm.contrib.asyncmodel import AsyncModel

class Author(AsyncModel):
    name = dorm.CharField(max_length=100)

Author.objects.create(name="x")  # AsyncOnlyError
await Author.objects.acreate(name="x")  # ok
```

## Idempotency

```python
from dorm.contrib.idempotency import IdempotencyRecord, idempotency_key

class IdpEntry(IdempotencyRecord):
    class Meta:
        db_table = "idempotency"

with idempotency_key(key, model=IdpEntry) as ctx:
    if ctx.replay:
        return ctx.cached_response
    result = process_payment(...)
    ctx.store(result, status_code=201)
    return result
```

## CLI: dorm diff

```bash
dorm diff --apps myapp.models
dorm diff --json > drift.json
```

Exits 0 = clean, 1 = drift. JSON output for CI gates.

## CLI: dorm purge-deleted

```bash
dorm purge-deleted --older-than 30d
dorm purge-deleted --older-than 12h --dry-run
```

Hard-deletes `SoftDeleteModel` rows older than DURATION.
Suffixes: `s`/`m`/`h`/`d`/`w`.

## CLI: dorm export-json-schema

```bash
dorm export-json-schema > schemas.json
dorm export-json-schema --out schemas/ --apps myapp.models
```

Draft 2020-12 JSON Schema per Model.

## Lag-aware read router

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["r1", "r2"],
        max_lag_seconds=2.0,
    ),
]
```

Skips replicas above lag threshold; deflects to primary when all
are lagging.

## GIS

```python
from dorm.contrib.gis import Geom, PointField, PolygonField

class Store(dorm.Model):
    location = PointField(srid=4326)
    zone = PolygonField(srid=4326)

Store.objects.filter(zone__intersects=Geom.point(0.5, 0.5))
Store.objects.filter(location__distance_lte=(target, 1000))
```

Spatial lookups: `intersects`, `within`, `contains`,
`distance_lte`, `distance_gte`.

## Meta.read_only

```python
class AuditLog(dorm.Model):
    class Meta:
        db_table = "audit_log"
        read_only = True

list(AuditLog.objects.all())   # ok
AuditLog(...).save()           # ReadOnlyModelError
```

## Sibling packages

### pytest-djanorm

```bash
pip install pytest-djanorm
pip install 'pytest-djanorm[postgres]'
```

Fixtures: `djanorm_settings`, `pg_container`, `transactional_db`,
`atransactional_db`, `nplusone_guard`.

### djanorm-mypy

```bash
pip install djanorm-mypy
```

`pyproject.toml`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

Validates `filter()` kwargs against the model's fields, recognises
lookup suffixes, synthesises `pk` / `id` on Model subclasses.
