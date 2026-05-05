# djanorm

Un ORM al estilo Django para Python con **async de primera clase**,
schemas de Pydantic listos para FastAPI y un CLI `dorm` ligero.
Sin dependencia del runtime de Django.

```python
import dorm

class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

# Síncrono
alice = Author.objects.create(name="Alice", age=30)
adultos = Author.objects.filter(age__gte=18).order_by("name")

# Asíncrono — cada método tiene su variante `a*`
alice = await Author.objects.acreate(name="Alice", age=30)
async for a in Author.objects.filter(age__gte=18):
    print(a.name)
```

## Por dónde empezar

| Si eres… | Lee… |
|---|---|
| nuevo del todo | [Empezando](getting-started.md) |
| montas una app con FastAPI | [Tutorial: tu primera API en 5 min](tutorial.md) |
| vienes de Django | [Migración desde Django ORM](migration-from-django.md) |
| buscas qué cambió en 4.0 | [Novedades 4.0](v4_0.md) |
| no sabes qué helper usar | [Cuándo usar qué](when-to-use-what.md) |
| buscas un método | Referencia API (barra lateral) |
| vas a desplegar a producción | [Despliegue en producción](production.md) |

## Por qué dorm

- **La misma API de QuerySet que Django** — `filter`, `exclude`, `Q`,
  `F`, `bulk_create`, `select_related`, `prefetch_related`, señales,
  todo. Si conoces Django, ya conoces dorm.
- **Sync **y** async** — cada método tiene una variante `a*`. El pool
  async reintenta errores transitorios y registra consultas lentas
  sin configurar nada. Modelos `AsyncModel` rechazan API sync para
  stacks async puros.
- **Con tipos** — `Field[T]` genérico + `Manager[Self]`. Plugin
  [`djanorm-mypy`](sibling-packages.md) valida kwargs `filter()`
  contra el modelo y suffixes lookup en compile-time.
- **Listo para FastAPI** — `DormSchema` + `list_response_schema` +
  `nested_schema_for` + `schema_with_computed`. Streaming
  `StreamingResponse` directo via `dorm.contrib.streaming`. Sin
  pegamento.
- **Hardening de producción incluido** — circuit breaker, query
  budget (timeout SLA), pool task affinity, lag-aware read routing,
  outbox pattern, sharding por hash, idempotency keys, schema drift
  detection (`dorm diff`).
- **Multi-backend** — **SQLite**, **PostgreSQL**, **MySQL/MariaDB**,
  **libsql/Turso** y desde 4.0 **DuckDB** (analítica embarcada).
  Mismos modelos, mismas migraciones.
- **PG features de primera** — `COPY FROM/TO`, materialised views,
  particionamiento declarativo (RANGE/LIST/HASH), `LISTEN/NOTIFY`
  async pub/sub, `pgvector`, `HStoreField`, ENUM nativo, full-text
  search con trigram + GIN.
- **Migraciones zero-downtime** — `AddFieldOnline` +
  `BackfillBatch` + `SetNotNullOnline` para tablas grandes en prod.
- **Multi-tenancy** — schema-level (`dorm.contrib.tenants`) o
  row-level (`TenantModel` + `current_tenant()`).
- **Tooling separado** — `pytest-djanorm` y `djanorm-mypy` viven en
  paquetes hermanos para no contaminar el wheel principal con deps
  dev-only ([rationale](sibling-packages.md)).
- **Almacenamiento pluggable** — `FileField` a disco local o
  **AWS S3** / **MinIO** / **Cloudflare R2** / **Backblaze B2**
  cambiando `settings.STORAGES`. Sin tocar código.

## Instalación

```bash
pip install "djanorm[sqlite]"
pip install "djanorm[postgresql]"
pip install "djanorm[duckdb]"                 # analítica embarcada (4.0+)
pip install "djanorm[sqlite,postgresql,pydantic]"

# Uploads en AWS S3 / MinIO / R2 / B2
pip install "djanorm[postgresql,s3]"

# Tooling dev (paquetes hermanos)
pip install pytest-djanorm djanorm-mypy
```

## Referencia rápida

- Definición de modelos → [Modelos y campos](models.md)
- API de consultas → [Consultas](queries.md)
- Patrones async → [Patrones async](async.md)
- Migraciones de schema → [Migraciones](migrations.md)
- Migraciones zero-downtime → [Migraciones online](online-migrations.md)
- Transacciones → [Transacciones](transactions.md)
- FastAPI / Pydantic → [Integración con FastAPI](fastapi.md)
- Backend DuckDB → [DuckDB](duckdb.md)
- Multi-tenancy fila → [Tenancy fila](tenants-row.md)
- CTEs recursivos / árboles → [Recursive CTE](recursive-cte.md)
- Helpers framework-agnósticos → [Helpers](helpers.md)
- Features avanzadas PG → [Avanzado](advanced.md)
- Paquetes hermanos (mypy / pytest) → [Sibling packages](sibling-packages.md)
- Benchmark vs otros ORMs → [Bench](bench.md)
- CLI `dorm` → [Referencia del CLI](cli.md)
- Subida de archivos (disco local / S3 / MinIO) → [Modelos: Archivos](models.md#archivos)
- Pasar a producción → [Despliegue en producción](production.md)
- Atascado con algo → [Resolución de problemas](troubleshooting.md)
