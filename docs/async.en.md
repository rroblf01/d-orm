# Async patterns

Every method on `QuerySet` and `Manager` has an `a*` variant. Same
SQL, same semantics — just awaitable.

## The naming convention

| Sync | Async |
|---|---|
| `Author.objects.create(...)` | `await Author.objects.acreate(...)` |
| `Author.objects.get(...)` | `await Author.objects.aget(...)` |
| `Author.objects.filter(...).count()` | `await Author.objects.filter(...).acount()` |
| `list(Author.objects.all())` | `[a async for a in Author.objects.all()]` or `await Author.objects.all()` |
| `for a in Author.objects.all():` | `async for a in Author.objects.all():` |
| `qs.values("name")` | `await qs.avalues("name")` (or `await qs.values(...)`) |
| `qs.values_list("name", flat=True)` | `await qs.avalues_list("name", flat=True)` |
| `qs.first()` / `last()` | `await qs.afirst()` / `alast()` |
| `qs.exists()` | `await qs.aexists()` |
| `qs.in_bulk([...])` | `await qs.ain_bulk([...])` |
| `qs.update(...)` | `await qs.aupdate(...)` |
| `qs.delete()` | `await qs.adelete()` |
| `qs.bulk_create(...)` | `await qs.abulk_create(...)` |
| `qs.bulk_update(...)` | `await qs.abulk_update(...)` |
| `qs.aggregate(...)` | `await qs.aaggregate(...)` |
| `qs.iterator(chunk_size=N)` | `qs.aiterator(chunk_size=N)` (use with `async for`) |
| `qs.explain(analyze=True)` | `await qs.aexplain(analyze=True)` |
| `qs.raw(sql, params)` | `await qs.araw(sql, params)` |
| `obj.save()` / `delete()` | `await obj.asave()` / `adelete()` |
| `with transaction.atomic():` | `async with transaction.aatomic():` |

## Awaiting a queryset directly

QuerySets are awaitable — `await qs` materializes the queryset in
one round-trip, which is convenient when you've already chained
filters or `values()`:

```python
# All Author instances
authors = await Author.objects.all()                      # list[Author]
authors = await Author.objects.filter(age__gte=18)

# As dicts — equivalent to await qs.avalues(...)
rows = await Author.objects.values("name", "age")         # list[dict]

# As tuples — equivalent to await qs.avalues_list(...)
names = await Author.objects.values_list("name", flat=True)  # list[str]
```

`avalues()` / `avalues_list()` are the explicit method form; both
hit the DB exactly once.

Use `aiterator()` when you don't want to load everything in memory:

```python
async for a in Author.objects.aiterator(chunk_size=1000):
    await process(a)
```

`aiterator()` opens a server-side cursor on PostgreSQL and streams
in chunks on SQLite, so memory stays flat regardless of result-set
size.

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
