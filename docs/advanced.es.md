# Características avanzadas (3.4)

Recetas para los helpers añadidos en la versión 3.4. Todos opt-in
mediante `dorm.contrib.*`; ningún coste en runtime cuando no se usan.

## `COPY FROM` / `COPY TO` (PostgreSQL)

Ingesta masiva 10-100× más rápida que `bulk_create` para tablas con
decenas o centenas de miles de filas.

```python
import dorm
from dorm.contrib.bulk_copy import bulk_copy_from, copy_to


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()


# Sync — instancias modelo, dicts o tuples ya preparados.
n: int = bulk_copy_from(
    Author,
    [Author(name=f"a-{i}", age=i) for i in range(50_000)],
)
print(f"insertadas {n} filas")

# Exporta a stdout / fichero
for row in copy_to('SELECT id, name FROM "authors"'):
    process(row)
```

API async: `await abulk_copy_from(Author, generator())`,
`async for row in acopy_to(sql)`. Acepta tanto iterables sync como
async. PostgreSQL-only — otros backends lanzan `NotImplementedError`.

## Vistas materializadas

```python
from dorm.migrations.operations import (
    CreateMaterializedView,
    RefreshMaterializedView,
    DropMaterializedView,
)

operations = [
    CreateMaterializedView(
        "active_authors",
        'SELECT id, name FROM "authors" WHERE is_active = true',
    ),
    RefreshMaterializedView("active_authors", concurrently=False),
]
```

`concurrently=True` requiere índice único en la vista (constraint PG).

## Particionamiento declarativo

```python
from dorm.migrations.operations import (
    CreatePartitionedTable,
    CreatePartition,
    AttachPartition,
    DetachPartition,
)

operations = [
    CreatePartitionedTable(
        "events",
        columns_sql=(
            'id BIGSERIAL, occurred_at TIMESTAMPTZ NOT NULL, '
            'payload JSONB, PRIMARY KEY (id, occurred_at)'
        ),
        method="RANGE",
        key="occurred_at",
    ),
    CreatePartition(
        parent="events",
        name="events_2026_q1",
        for_values="FROM ('2026-01-01') TO ('2026-04-01')",
    ),
]
```

Métodos soportados: `RANGE`, `LIST`, `HASH`. PG ≥ 11.

## `LISTEN` / `NOTIFY`

Publicación / suscripción sin broker — ideal para invalidación de
caché o despertar workers.

```python
from dorm.contrib.listen_notify import listen, anotify


async def consumer():
    async with listen("orders") as channel:
        async for n in channel:
            print(f"{n.channel}: {n.payload} (pid={n.pid})")
            if n.payload == "stop":
                break


# Publicador (puede ser otra task, otro proceso, otro servidor).
await anotify("orders", '{"id": 42}')
```

`listen()` mantiene una conexión PG dedicada durante el bloque.
PostgreSQL-only.

## `SELECT ... FOR UPDATE SKIP LOCKED`

Patrón cola de jobs sin broker:

```python
from dorm import transaction


def claim_jobs(worker_id: str) -> int:
    n = 0
    with transaction.atomic():
        for job in (
            Job.objects.select_for_update(skip_locked=True)
            .filter(status="pending")
            .order_by("created_at")[:10]
        ):
            job.status = "running"
            job.worker = worker_id
            job.save()
            n += 1
    return n
```

Múltiples workers ejecutan `claim_jobs()` en paralelo y obtienen
filas disjuntas — sin coordinación externa, sin filas duplicadas.

## Pool con afinidad por tarea

`pinned_connection()` reutiliza una sola conexión PG para todas las
queries dentro de la task — ahorra checkout/return en handlers que
emiten muchas queries cortas.

```python
from dorm.contrib.task_pool import pinned_connection


async def handler():
    async with pinned_connection():
        authors = await Author.objects.acount()
        await Author.objects.acreate(name="x", age=1)
        # Misma conexión en ambas líneas.
```

`assert_no_concurrent_gather()` detecta el antipatrón de compartir un
pin entre siblings de `asyncio.gather`. PostgreSQL-only — no-op en
otros backends.

## Circuit breaker

Evita el thundering-herd cuando la BD lleva minutos caída.

```python
from dorm.contrib.circuit_breaker import circuit_breaker, CircuitOpenError


cb = circuit_breaker("default", failure_threshold=5, open_window_s=30.0)


def safe_count() -> int | None:
    try:
        with cb:
            return Author.objects.count()
    except CircuitOpenError:
        return None  # Devuelve cache, valor por defecto, etc.
```

Estados: `CLOSED` → `OPEN` (tras N fallos consecutivos) → `HALF_OPEN`
(tras `open_window_s`) → `CLOSED` o `OPEN` según el siguiente probe.
Por proceso — usa Redis encima si necesitas coordinación cross-worker.

## Patrón Outbox

Resuelve el problema de la doble escritura (BD + broker) con una
transacción atómica.

```python
from dorm.contrib.outbox import OutboxEvent, OutboxRelay, record_event
from dorm import transaction


class Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox"


# Dentro del handler:
with transaction.atomic():
    order = Order.objects.create(...)
    record_event(Outbox, "order.created", {"order_id": order.id})


# Worker independiente (proceso separado):
def publish_to_kafka(row):
    kafka_client.send("orders", row.payload)
    return True


relay = OutboxRelay(Outbox, batch_size=100)
relay.run(handler=publish_to_kafka)
```

El relay usa `SELECT ... FOR UPDATE SKIP LOCKED` en PG para
escalar horizontalmente. Filas con `attempts >= max_attempts` van a
`status='dead'` para revisión manual.

## Sharding horizontal por hash

```python
from dorm.contrib.sharding import HashShardRouter, with_shard_key


# settings.py
DATABASES = {
    "default": {...},
    "shard_0": {...},
    "shard_1": {...},
    "shard_2": {...},
    "shard_3": {...},
}
DATABASE_ROUTERS = [
    HashShardRouter(num_shards=4, shard_models={Order, Customer}),
]


# Request handler
with with_shard_key(request.user.tenant_id):
    order = Order.objects.create(...)
```

Hash determinista (BLAKE2b con salt configurable) — no depende de
`hash()` para evitar randomización por proceso.

`for_each_shard(fn, num_shards=4)` ejecuta `fn(alias)` contra cada
shard secuencialmente — útil para conteos globales.

## Plugin de backends de terceros

Registra un backend custom vía entry-points en tu `pyproject.toml`:

```toml
[project.entry-points."djanorm.backends"]
mybackend = "mypkg.backend:MyBackendWrapper"

[project.entry-points."djanorm.async_backends"]
mybackend = "mypkg.backend:MyAsyncBackendWrapper"
```

Y úsalo:

```python
DATABASES = {"default": {"ENGINE": "mybackend", ...}}
```

`reset_backend_cache()` permite recargar el registro tras instalación
en caliente (uso típico: tests).

## Bench comparativo

```bash
python -m bench.compare --runs 3 --ops 1000
```

Compara dorm contra Django ORM, SQLAlchemy 2.0 y Tortoise ORM en los
mismos cinco escenarios, sobre SQLite en proceso. Salta el ORM cuando
no esté instalado.
