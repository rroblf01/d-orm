# Patrones async

Cada método de `QuerySet` y `Manager` tiene una variante `a*`. Misma
SQL, misma semántica — solo que awaitable.

## Convención de nombres

| Sync | Async |
|---|---|
| `Author.objects.create(...)` | `await Author.objects.acreate(...)` |
| `Author.objects.get(...)` | `await Author.objects.aget(...)` |
| `Author.objects.filter(...).count()` | `await Author.objects.filter(...).acount()` |
| `list(Author.objects.all())` | `[a async for a in Author.objects.all()]` o `await Author.objects.all()` |
| `for a in Author.objects.all():` | `async for a in Author.objects.all():` |
| `qs.values("name")` | `await qs.avalues("name")` (o `await qs.values(...)`) |
| `qs.values_list("name", flat=True)` | `await qs.avalues_list("name", flat=True)` |
| `qs.first()` / `last()` | `await qs.afirst()` / `alast()` |
| `qs.exists()` | `await qs.aexists()` |
| `qs.in_bulk([...])` | `await qs.ain_bulk([...])` |
| `qs.update(...)` | `await qs.aupdate(...)` |
| `qs.delete()` | `await qs.adelete()` |
| `qs.bulk_create(...)` | `await qs.abulk_create(...)` |
| `qs.bulk_update(...)` | `await qs.abulk_update(...)` |
| `qs.aggregate(...)` | `await qs.aaggregate(...)` |
| `qs.iterator(chunk_size=N)` | `qs.aiterator(chunk_size=N)` (úsalo con `async for`) |
| `qs.explain(analyze=True)` | `await qs.aexplain(analyze=True)` |
| `qs.raw(sql, params)` | `await qs.araw(sql, params)` |
| `obj.save()` / `delete()` | `await obj.asave()` / `adelete()` |
| `with transaction.atomic():` | `async with transaction.aatomic():` |

## Await directo a un queryset

Los QuerySets son awaitable — `await qs` materializa el queryset
en un único round-trip, lo cual va bien cuando ya has encadenado
filtros o `values()`:

```python
# Todas las instancias Author
authors = await Author.objects.all()                      # list[Author]
authors = await Author.objects.filter(age__gte=18)

# Como dicts — equivalente a await qs.avalues(...)
rows = await Author.objects.values("name", "age")         # list[dict]

# Como tuplas — equivalente a await qs.avalues_list(...)
names = await Author.objects.values_list("name", flat=True)  # list[str]
```

`avalues()` / `avalues_list()` son la forma explícita; ambas
versiones tocan la BD exactamente una vez.

Usa `aiterator()` cuando no quieras cargar todo en memoria:

```python
async for a in Author.objects.aiterator(chunk_size=1000):
    await process(a)
```

`aiterator()` abre un cursor server-side en PostgreSQL y hace
streaming por chunks en SQLite, así la memoria se mantiene plana
sin importar el tamaño del resultset.

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
