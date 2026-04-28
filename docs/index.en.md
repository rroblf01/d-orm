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
| looking up a method | API Reference (sidebar) |
| putting this in prod | [Production deployment](production.md) |

## Why dorm

- **Same QuerySet API as Django** — `filter`, `exclude`, `Q`, `F`,
  `bulk_create`, `select_related`, `prefetch_related`, signals, the
  works. If you know Django, you already know dorm.
- **Sync **and** async** — every method has an `a*` variant. The async
  pool retries on transient errors and logs slow queries out of the
  box.
- **Type-safe** — `Field[T]` generics + `Manager[Self]`. Your IDE
  knows `user.name` is `str`, not `Any`, and flags `user.naem` as a
  typo.
- **FastAPI-friendly** — `DormSchema` with `class Meta: model = User`
  generates a Pydantic v2 schema mirroring your model, including
  nested FK / M2M serialization. No glue.
- **Production hardening built in** — health-check helper, advisory-
  locked migrations, transient retry, query observability hooks for
  OpenTelemetry / Datadog / Prometheus, slow-query logs.
- **Both PostgreSQL and SQLite** — same model code, same migrations,
  switch by editing one line.
- **Pluggable file storage** — `FileField` writes to local disk by
  default and to **AWS S3** (or any S3-compatible service:
  **MinIO**, **Cloudflare R2**, **Backblaze B2**) by changing
  `settings.STORAGES`. Application code doesn't change.

## Install

```bash
pip install "djanorm[sqlite]"
pip install "djanorm[postgresql]"
pip install "djanorm[sqlite,postgresql,pydantic]"

# Add the s3 extra when storing uploads on AWS S3 / MinIO / R2 / B2
pip install "djanorm[postgresql,s3]"
```

## Quick reference

- Model definition → [Models & fields](models.md)
- Query API → [Querying](queries.md)
- Async patterns → [Async patterns](async.md)
- Schema migrations → [Migrations](migrations.md)
- Transactions → [Transactions](transactions.md)
- FastAPI / Pydantic → [FastAPI integration](fastapi.md)
- `dorm` command line → [CLI reference](cli.md)
- File uploads (local disk / S3 / MinIO) → [Models: Files](models.md#files)
- Going live → [Production deployment](production.md)
- Stuck on something → [Troubleshooting](troubleshooting.md)
