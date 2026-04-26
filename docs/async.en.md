# Async patterns

Every method on `QuerySet` and `Manager` has an `a*` variant. Same
SQL, same semantics — just awaitable.

## The naming convention

| Sync | Async |
|---|---|
| `Author.objects.create(...)` | `await Author.objects.acreate(...)` |
| `Author.objects.get(...)` | `await Author.objects.aget(...)` |
| `Author.objects.filter(...).count()` | `await Author.objects.filter(...).acount()` |
| `for a in Author.objects.all():` | `async for a in Author.objects.all():` |
| `qs.first()` / `last()` | `await qs.afirst()` / `alast()` |
| `qs.exists()` | `await qs.aexists()` |
| `qs.update(...)` | `await qs.aupdate(...)` |
| `qs.delete()` | `await qs.adelete()` |
| `qs.bulk_create(...)` | `await qs.abulk_create(...)` |
| `qs.bulk_update(...)` | `await qs.abulk_update(...)` |
| `qs.aggregate(...)` | `await qs.aaggregate(...)` |
| `obj.save()` / `delete()` | `await obj.asave()` / `adelete()` |
| `with transaction.atomic():` | `async with transaction.aatomic():` |

## Awaiting a queryset directly

QuerySets are awaitable — handy for chained `values()` / filters:

```python
# Materialise the whole queryset in one go
rows = await Author.objects.values("name", "age").filter(age__gte=18)
# rows is list[dict[str, Any]]
```

Use `aiterator()` when you don't want to load everything in memory:

```python
async for a in Author.objects.iterator(chunk_size=1000):
    await process(a)
```

## Atomic blocks

```python
from dorm.transaction import aatomic

# Context manager
async with aatomic():
    a = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="...", author=a)

# Decorator
@aatomic
async def transfer(src_id: int, dst_id: int, amount: int) -> None:
    await Account.objects.filter(pk=src_id).aupdate(balance=F("balance") - amount)
    await Account.objects.filter(pk=dst_id).aupdate(balance=F("balance") + amount)

# Per-alias
@aatomic("replica_writer")
async def replica_op(): ...
```

Nested `aatomic()` calls open savepoints so the inner block can fail
without rolling back the outer one.

## Concurrency caveats

- **Don't share a model instance across coroutines** that mutate it.
  `obj.save()` reads `obj.__dict__` — concurrent writers will lose
  changes.
- **Don't mix sync and async on the same alias** within a single
  request. They go through separate pools; cross-pool transactions are
  not coordinated.
- **`asyncio.wait_for` cancellation is safe**: the pool's context
  manager always returns the connection. dorm's tests assert this on
  every release.

## Performance notes

- Async pool retries `OperationalError` / `InterfaceError` (network
  blip, server restart) up to `DORM_RETRY_ATTEMPTS` (default 3) with
  exponential backoff. Disabled inside transactions.
- Async tests should set `asyncio_default_test_loop_scope = "session"`
  in pyproject so a single event loop is shared, otherwise pools
  accumulate one stale set of connections per test.
- See [Production deployment](production.md) for pool sizing.

## FastAPI integration

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager

import dorm

@asynccontextmanager
async def lifespan(app: FastAPI):
    dorm.configure(DATABASES={"default": {...}})
    yield
    from dorm.db.connection import close_all_async
    await close_all_async()

app = FastAPI(lifespan=lifespan)

@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

For schemas, see [FastAPI integration](fastapi.md).
