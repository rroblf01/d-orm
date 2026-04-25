# djanorm

Django-style ORM for Python with sync **and** async, FastAPI-ready
Pydantic schemas, and a tiny `dorm` CLI.

```python
import dorm

class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

# Sync
alice = Author.objects.create(name="Alice", age=30)
adults = Author.objects.filter(age__gte=18).order_by("name")

# Async
alice = await Author.objects.acreate(name="Alice", age=30)
async for a in Author.objects.filter(age__gte=18):
    print(a.name)
```

## Where to start

- **First time?** → [Tutorial: your first FastAPI app in 5 minutes](tutorial.md)
- **Coming from Django?** → [Migration from Django ORM](migration-from-django.md)
- **Looking up a method?** → API Reference (sidebar)
- **Going to production?** → [Production deployment guide](https://github.com/rroblf01/d-orm#production-deployment) in the README

## Why dorm

- **Same API as Django ORM**, no Django runtime dependency.
- **First-class async** — every method has an `a*` variant.
- **Type-safe** — `Field[T]` generic + `Manager[Self]` so `user.name`
  is `str` and `Author.objects.first()` is `Author | None`.
- **FastAPI-friendly** — `DormSchema` (Django-REST-style `Meta`) makes
  Pydantic schemas one liner per response.
- **Production hardening built in** — transient retry, health check,
  pool stats, advisory-locked migrations, slow-query logs, query
  observability hooks.

## Install

```bash
pip install "djanorm[sqlite]"
pip install "djanorm[postgresql]"
pip install "djanorm[sqlite,postgresql,pydantic]"
```
