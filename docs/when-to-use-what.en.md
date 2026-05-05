# When to use what

dorm offers several options for similar problems. This page
summarises the trade-offs.

## Backend

| If... | Use |
|---|---|
| OLTP web app | **PostgreSQL** |
| Small app / single-file deploy / mobile / edge | **SQLite** |
| Embedded edge replica / Turso Cloud | **libsql** |
| 100% MySQL/MariaDB legacy stack | **MySQL/MariaDB** |
| Dashboards / ETL / embedded analytics | **DuckDB** (4.0+) |
| Fast isolated CI tests | SQLite `:memory:` |

PG remains the default. SQLite for apps whose deploy is a single
process. DuckDB is the new entry — columnar in-process
analytics. None replaces PG for OLTP.

Backend details → [DuckDB](duckdb.md), [libsql](libsql.md).

## Multi-tenancy

| If... | Use |
|---|---|
| 5-50 enterprise tenants | **schema-level** (`dorm.contrib.tenants`) |
| 5000+ small tenants | **row-level** (`dorm.contrib.tenants_row`) |
| Compliance demands physical isolation | schema-level |
| MySQL / SQLite / DuckDB backend | row-level (schema-level is PG-only) |
| Per-tenant backups | schema-level |
| One-tenant-per-deploy | bare metal — not even multi-tenant |

[tenants-row](tenants-row.md) has the row-level details.

## Bulk inserts

| If... | Use |
|---|---|
| <1000 rows | `bulk_create` |
| 1k-10k rows, signals matter | `bulk_create(returning=...)` |
| 10k-1M rows, no signals | `bulk_copy_from` (PG-only) |
| >1M rows | `bulk_copy_from` with a pre-generated CSV |
| ETL from Parquet | DuckDB + `read_parquet` |

[bulk-copy](bulk-copy.md) explains COPY.

## Atomic vs autocommit

| If... | Use |
|---|---|
| Operations that must commit/rollback together | `atomic()` |
| Single-write idempotent task | autocommit (default) |
| Nested inside an existing tx | `atomic()` (auto savepoint) |
| Long-running batch that mustn't block | autocommit + small chunks |
| Nested DuckDB | atomic outer; inner is no-op |

## Zero-downtime migrations

| If... | Use |
|---|---|
| Small table (<100k rows) | plain `AddField` |
| Medium table (100k-10M rows), PG ≥ 12 | `AddField` with non-volatile default |
| Large table, any PG version | `AddFieldOnline` + `BackfillBatch` + `SetNotNullOnline` |
| MySQL backend | `AddFieldOnline` (DDL is not transactional, plan rollback) |

[online-migrations](online-migrations.md) details the recipe.

## Pub/sub / events

| If... | Use |
|---|---|
| Real-time low volume (~dozens/s) | `dorm.contrib.listen_notify` |
| Delivery guarantee + atomic with tx | **Outbox pattern** (`dorm.contrib.outbox`) |
| CDC without an extra table | PG logical replication (not built-in to dorm) |
| High volume (>1k msg/s) | external NATS / Kafka / Redis Pub/Sub |

[listen-notify](listen-notify.md) and [outbox](outbox.md) detail
the two primary options.

## Retry vs Circuit breaker

| If... | Use |
|---|---|
| Occasional transient failure (network blip) | retry (built-in via `RETRY_ATTEMPTS`) |
| DB down for minutes | **Circuit breaker** (`dorm.contrib.circuit_breaker`) |
| Both | retry **inside** the breaker |
| Client retries a non-idempotent op | **Idempotency keys** (`dorm.contrib.idempotency`) |

[circuit-breaker](circuit-breaker.md) and
[idempotency](idempotency.md).

## Scaling reads

| If... | Use |
|---|---|
| Mild load increase | bigger `MAX_POOL_SIZE` |
| Reads >> writes, mild lag tolerable | **read replicas** (`DATABASE_ROUTERS`) |
| Reads >> writes, lag intolerable | `LagAwareReadRouter` (4.0+) |
| Stale tolerable + high cache hit | queryset `cache(timeout=...)` |
| Read-only workload | mat-view + `RefreshMaterializedView` |

[lag-router](lag-router.md), [advanced](advanced.md).

## Scaling writes

| If... | Use |
|---|---|
| Vertical scaling exhausted, single primary | **Sharding** (`HashShardRouter`) |
| Rare global transactions | sharding |
| Frequent cross-shard JOINs | reshape schema before sharding |
| PG backend with CockroachDB / Citus | use that tool's native sharding |

[sharding](sharding.md).

## Streaming exports

| If... | Use |
|---|---|
| <10k rows, simple JSON | `JSONResponse` with materialised list |
| >10k rows, JSON | `astream_jsonl` + `StreamingResponse` |
| CSV exports | `astream_csv` |
| Custom format (Avro, Parquet) | `aiterator()` + custom serializer |
| Multi-GB | `bulk_copy_from(copy_to=...)` straight to a file |

[helpers](helpers.md#streaming-primitives).

## Async vs sync

| If... | Use |
|---|---|
| FastAPI / Litestar / aiohttp | `a*` methods (`acreate`, `aget`, `afilter`) |
| Mixed sync/async stack (Celery + FastAPI) | both, by context |
| Pure async, prevent mistakes | `dorm.contrib.asyncmodel.AsyncModel` |
| CLI script / cron | sync (simpler) |
| Tests with pytest-asyncio | async for async handlers, sync elsewhere |

## Forms vs Pydantic

| If... | Use |
|---|---|
| FastAPI / Litestar / JSON API | **Pydantic** (`dorm.contrib.pydantic`) |
| Server-rendered HTML | write your forms manually; dorm doesn't ship them |
| Admin panel | `sqladmin` or similar — dorm doesn't ship it |
| Complex cross-field validation | Pydantic `model_validator` |
| Schema-first development | `dorm export-json-schema` + external tooling |

dorm is API-first. Coming from Django ModelForm,
[migration-from-django](migration-from-django.md) has the
equivalents.

## Dev tooling

| For... | Use |
|---|---|
| Strict type-checking on queries | **`djanorm-mypy`** (sibling package) |
| Tests with DB fixtures | **`pytest-djanorm`** (sibling package) |
| Post-deploy drift validation | `dorm diff` |
| Catching N+1 in CI | `dorm.contrib.nplusone.assert_no_nplusone` |
| Detecting N+1 in prod (log) | `dorm.contrib.nplusone.detect()` middleware |
| Validating config | `dorm doctor` |

Why dev tooling lives in separate packages →
[sibling-packages](sibling-packages.md).
