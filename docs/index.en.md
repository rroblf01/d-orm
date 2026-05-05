# djanorm

A Django-style ORM for Python with **first-class async**, FastAPI-ready
Pydantic schemas, and a tiny `dorm` CLI. No Django runtime needed.

```python
import dorm

class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

# Sync
alice = Author.objects.create(name="Alice", age=30)
adults = Author.objects.filter(age__gte=18).order_by("name")

# Async — every method has an `a*` variant
alice = await Author.objects.acreate(name="Alice", age=30)
async for a in Author.objects.filter(age__gte=18):
    print(a.name)
```

## Where to start

| If you're… | Read… |
|---|---|
| brand new | [Getting started](getting-started.md) |
| building a FastAPI app | [Tutorial: 5 minutes to your first API](tutorial.md) |
| coming from Django | [Migration from Django ORM](migration-from-django.md) |
| looking up what changed in 4.0 | [What's new in 4.0](v4_0.md) |
| not sure which helper to use | [When to use what](when-to-use-what.md) |
| looking up a method | API Reference (sidebar) |
| putting this in prod | [Production deployment](production.md) |

## Why dorm

- **Same QuerySet API as Django** — `filter`, `exclude`, `Q`, `F`,
  `bulk_create`, `select_related`, `prefetch_related`, signals, the
  works. If you know Django, you already know dorm.
- **Sync **and** async** — every method has an `a*` variant. The async
  pool retries on transient errors and logs slow queries out of the
  box. `AsyncModel` rejects sync API for async-only stacks.
- **Type-safe** — `Field[T]` generics + `Manager[Self]`. The
  [`djanorm-mypy`](sibling-packages.md) plugin validates `filter()`
  kwargs against the model and lookup suffixes at compile time.
- **FastAPI-friendly** — `DormSchema` + `list_response_schema` +
  `nested_schema_for` + `schema_with_computed`. Streaming
  `StreamingResponse` directly via `dorm.contrib.streaming`. No glue.
- **Production hardening built in** — circuit breaker, query budget
  (HTTP SLA timeout), pool task affinity, lag-aware read routing,
  outbox pattern, hash sharding, idempotency keys, schema-drift
  detection (`dorm diff`).
- **Multi-backend** — **SQLite**, **PostgreSQL**, **MySQL/MariaDB**,
  **libsql/Turso**, and from 4.0 **DuckDB** (embedded analytics).
  Same models, same migrations.
- **First-class PG features** — `COPY FROM/TO`, materialised views,
  declarative partitioning (RANGE/LIST/HASH), async `LISTEN/NOTIFY`
  pub-sub, `pgvector`, `HStoreField`, native ENUM types, full-text
  search with trigram + GIN.
- **Zero-downtime migrations** — `AddFieldOnline` +
  `BackfillBatch` + `SetNotNullOnline` for big tables in production.
- **Multi-tenancy** — schema-level (`dorm.contrib.tenants`) or
  row-level (`TenantModel` + `current_tenant()`).
- **Tooling extracted** — `pytest-djanorm` and `djanorm-mypy` ship
  as sibling packages so the main wheel never pulls dev tooling
  ([rationale](sibling-packages.md)).
- **Pluggable file storage** — `FileField` writes to local disk by
  default and to **AWS S3** / **MinIO** / **Cloudflare R2** /
  **Backblaze B2** by changing `settings.STORAGES`. Application
  code doesn't change.

## Install

```bash
pip install "djanorm[sqlite]"
pip install "djanorm[postgresql]"
pip install "djanorm[duckdb]"                 # embedded analytics (4.0+)
pip install "djanorm[sqlite,postgresql,pydantic]"

# Uploads on AWS S3 / MinIO / R2 / B2
pip install "djanorm[postgresql,s3]"

# Dev tooling (sibling packages)
pip install pytest-djanorm djanorm-mypy
```

## Quick reference

- Model definition → [Models & fields](models.md)
- Query API → [Querying](queries.md)
- Async patterns → [Async patterns](async.md)
- Schema migrations → [Migrations](migrations.md)
- Zero-downtime migrations → [Online migrations](online-migrations.md)
- Transactions → [Transactions](transactions.md)
- FastAPI / Pydantic → [FastAPI integration](fastapi.md)
- DuckDB backend → [DuckDB](duckdb.md)
- Row-level multi-tenancy → [Row tenancy](tenants-row.md)
- Recursive CTEs / trees → [Recursive CTE](recursive-cte.md)
- Framework-agnostic helpers → [Helpers](helpers.md)
- Advanced PG features → [Advanced](advanced.md)
- Sibling packages (mypy / pytest) → [Sibling packages](sibling-packages.md)
- Benchmark vs other ORMs → [Bench](bench.md)
- `dorm` command line → [CLI reference](cli.md)
- File uploads (local disk / S3 / MinIO) → [Models: Files](models.md#files)
- Going live → [Production deployment](production.md)
- Stuck on something → [Troubleshooting](troubleshooting.md)
