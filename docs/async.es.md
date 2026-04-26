# Patrones async

Cada método de `QuerySet` y `Manager` tiene una variante `a*`. Misma
SQL, misma semántica — solo que awaitable.

## Convención de nombres

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

## Await directo a un queryset

Los QuerySets son awaitable — útil para encadenar `values()` / filtros:

```python
# Materializa todo el queryset de una sola vez
rows = await Author.objects.values("name", "age").filter(age__gte=18)
# rows es list[dict[str, Any]]
```

Usa `aiterator()` cuando no quieras cargar todo en memoria:

```python
async for a in Author.objects.iterator(chunk_size=1000):
    await process(a)
```

## Bloques atómicos

```python
from dorm.transaction import aatomic

# Como context manager
async with aatomic():
    a = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="...", author=a)

# Como decorador
@aatomic
async def transfer(src_id: int, dst_id: int, amount: int) -> None:
    await Account.objects.filter(pk=src_id).aupdate(balance=F("balance") - amount)
    await Account.objects.filter(pk=dst_id).aupdate(balance=F("balance") + amount)

# Por alias
@aatomic("replica_writer")
async def replica_op(): ...
```

Las llamadas anidadas a `aatomic()` abren savepoints, así que el bloque
interno puede fallar sin tumbar el externo.

## Avisos sobre concurrencia

- **No compartas una instancia de modelo entre corrutinas** que la
  muten. `obj.save()` lee `obj.__dict__` — escrituras concurrentes
  perderán cambios.
- **No mezcles sync y async en el mismo alias** dentro de una misma
  request. Pasan por pools separados; las transacciones cross-pool no
  se coordinan.
- **La cancelación con `asyncio.wait_for` es segura**: el context
  manager del pool siempre devuelve la conexión. Los tests de dorm lo
  verifican en cada release.

## Notas de rendimiento

- El pool async reintenta `OperationalError` / `InterfaceError`
  (corte de red, reinicio del servidor) hasta `DORM_RETRY_ATTEMPTS`
  (default 3) con backoff exponencial. Desactivado dentro de
  transacciones.
- Los tests async deberían fijar
  `asyncio_default_test_loop_scope = "session"` en pyproject para que
  todos compartan un único event loop, si no los pools acumulan un set
  de conexiones obsoletas por test.
- Para dimensionar el pool, mira [Despliegue en producción](production.md).

## Integración con FastAPI

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

Para los esquemas, mira [Integración con FastAPI](fastapi.md).
