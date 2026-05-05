# Cuándo usar qué

dorm ofrece varias opciones para resolver problemas similares.
Esta página resume los trade-offs.

## Backend

| Si... | Usa |
|---|---|
| OLTP web app | **PostgreSQL** |
| App pequeña / single-file deploy / móvil / edge | **SQLite** |
| Réplica embebida edge / Turso Cloud | **libsql** |
| Stack 100% MySQL/MariaDB legacy | **MySQL/MariaDB** |
| Dashboards / ETL / analítica embarcada | **DuckDB** (4.0+) |
| Test/CI rápido aislado | SQLite `:memory:` |

PG sigue siendo la elección por defecto. SQLite para apps cuyo
deploy es un único proceso. DuckDB es la novedad — analítica
columnar in-process. Ninguno reemplaza a PG para OLTP.

Detalles backend → [DuckDB](duckdb.md), [libsql](libsql.md).

## Multi-tenancy

| Si... | Usa |
|---|---|
| 5-50 tenants enterprise | **schema-level** (`dorm.contrib.tenants`) |
| 5000+ tenants pequeños | **row-level** (`dorm.contrib.tenants_row`) |
| Cumplimiento exige aislamiento físico | schema-level |
| Backend MySQL / SQLite / DuckDB | row-level (schema-level es PG-only) |
| Backups por tenant | schema-level |
| One-tenant-one-deploy | bare metal — ni siquiera multi-tenant |

[tenants-row](tenants-row.md) detalla el row-level.

## Bulk inserts

| Si... | Usa |
|---|---|
| <1000 filas | `bulk_create` |
| 1k-10k filas, signals importantes | `bulk_create(returning=...)` |
| 10k-1M filas, sin signals | `bulk_copy_from` (PG-only) |
| >1M filas | `bulk_copy_from` con CSV pre-generado |
| ETL desde Parquet | DuckDB + `read_parquet` |

[bulk-copy](bulk-copy.md) explica COPY.

## Atomic vs autocommit

| Si... | Usa |
|---|---|
| Operaciones que deben commit/rollback juntas | `atomic()` |
| Tarea único-write idempotente | autocommit (default) |
| Anidado dentro de tx existente | `atomic()` (savepoint automático) |
| Long-running batch que no debe bloquear | autocommit + lotes pequeños |
| DuckDB anidado | atomic outer; inner es no-op |

## Migraciones zero-downtime

| Si... | Usa |
|---|---|
| Tabla pequeña (<100k filas) | `AddField` plano |
| Tabla mediana (100k-10M filas), PG ≥ 12 | `AddField` con default no-volátil |
| Tabla grande, PG cualquier versión | `AddFieldOnline` + `BackfillBatch` + `SetNotNullOnline` |
| Backend MySQL | `AddFieldOnline` (DDL no es transaccional, plan rollback) |

[online-migrations](online-migrations.md) detalla la receta.

## Pub/sub / eventos

| Si... | Usa |
|---|---|
| Real-time low-volume (~docenas/s) | `dorm.contrib.listen_notify` |
| Garantía de entrega + atomic con tx | **Outbox pattern** (`dorm.contrib.outbox`) |
| CDC sin tabla extra | logical replication PG (no built-in dorm) |
| Alto volumen (>1k msg/s) | NATS / Kafka / Redis Pub/Sub externos |

[listen-notify](listen-notify.md) y [outbox](outbox.md) detallan
los dos primarios.

## Retry vs Circuit breaker

| Si... | Usa |
|---|---|
| Falla transitoria ocasional (network blip) | retry (built-in via `RETRY_ATTEMPTS`) |
| BD caída minutos | **Circuit breaker** (`dorm.contrib.circuit_breaker`) |
| Ambos | retry **dentro** del breaker |
| Cliente reintenta operación no-idempotente | **Idempotency keys** (`dorm.contrib.idempotency`) |

[circuit-breaker](circuit-breaker.md) y
[idempotency](idempotency.md).

## Scaling reads

| Si... | Usa |
|---|---|
| Leve aumento de carga | `MAX_POOL_SIZE` mayor |
| Reads >> writes, bajo lag tolerable | **read replicas** (`DATABASE_ROUTERS`) |
| Reads >> writes, lag intolerable | `LagAwareReadRouter` (4.0+) |
| Stale tolerable + cache hit alto | `cache(timeout=...)` queryset |
| Workload completamente leído | mat-view + `RefreshMaterializedView` |

[lag-router](lag-router.md), [advanced](advanced.md).

## Scaling writes

| Si... | Usa |
|---|---|
| Vertical scaling agotado, single primary | **Sharding** (`HashShardRouter`) |
| Transacciones globales raras | sharding |
| Frecuentes JOINs cross-shard | refactor schema antes de shardar |
| Backend PG con CockroachDB / Citus | usa el sharding nativo de la herramienta |

[sharding](sharding.md).

## Streaming exports

| Si... | Usa |
|---|---|
| <10k filas, JSON simple | `JSONResponse` con lista materializada |
| >10k filas, JSON | `astream_jsonl` + `StreamingResponse` |
| CSV exports | `astream_csv` |
| Custom format (Avro, Parquet) | `aiterator()` + serializador custom |
| Múltiples GB | `bulk_copy_from(copy_to=...)` directo a archivo |

[helpers](helpers.md#streaming-primitives).

## Async vs sync

| Si... | Usa |
|---|---|
| FastAPI / Litestar / aiohttp | métodos `a*` (`acreate`, `aget`, `afilter`) |
| Stack mixto sync/async (Celery + FastAPI) | ambos según contexto |
| Async puro, prevenir errores | `dorm.contrib.asyncmodel.AsyncModel` |
| Script CLI / cron | sync (más simple) |
| Test con pytest-asyncio | async para handlers async, sync para resto |

## Forms vs Pydantic

| Si... | Usa |
|---|---|
| FastAPI / Litestar / API JSON | **Pydantic** (`dorm.contrib.pydantic`) |
| Server-rendered HTML | escribir tus forms manualmente; dorm no incluye |
| Admin panel | `sqladmin` o similar — dorm no incluye |
| Validación cross-field compleja | Pydantic `model_validator` |
| Schema-first desarrollo | `dorm export-json-schema` + tooling externo |

dorm es API-first. Si vienes de Django ModelForm,
[migration-from-django](migration-from-django.md) tiene la
equivalencia.

## Tooling dev

| Para... | Usa |
|---|---|
| Type-checking estricto en queries | **`djanorm-mypy`** (paquete hermano) |
| Tests con fixtures de BD | **`pytest-djanorm`** (paquete hermano) |
| Validar drift post-deploy | `dorm diff` |
| Detectar N+1 en CI | `dorm.contrib.nplusone.assert_no_nplusone` |
| Detectar N+1 en prod (log) | `dorm.contrib.nplusone.detect()` middleware |
| Validar config | `dorm doctor` |

Por qué dev tooling vive en paquetes separados →
[sibling-packages](sibling-packages.md).
