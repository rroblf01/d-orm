# Advanced features (3.4)

Recipes for the helpers added in 3.4. All opt-in via `dorm.contrib.*`;
zero runtime cost when unused.

## `COPY FROM` / `COPY TO` (PostgreSQL)

Bulk-load 10-100× faster than `bulk_create` for tens-of-thousands of rows.

```python
import dorm
from dorm.contrib.bulk_copy import bulk_copy_from, copy_to


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()


n: int = bulk_copy_from(
    Author,
    [Author(name=f"a-{i}", age=i) for i in range(50_000)],
)


for row in copy_to('SELECT id, name FROM "authors"'):
    process(row)
```

Async: `await abulk_copy_from(Author, generator())`,
`async for row in acopy_to(sql)`. PostgreSQL-only.

## Materialized views

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

`concurrently=True` requires a unique index on the view (PG constraint).

## Declarative partitioning

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

Methods: `RANGE`, `LIST`, `HASH`. PG ≥ 11.

## `LISTEN` / `NOTIFY`

Broker-less pub/sub — perfect for cache invalidation or worker wake-ups.

```python
from dorm.contrib.listen_notify import listen, anotify


async def consumer():
    async with listen("orders") as channel:
        async for n in channel:
            print(f"{n.channel}: {n.payload} (pid={n.pid})")
            if n.payload == "stop":
                break


await anotify("orders", '{"id": 42}')
```

`listen()` holds a dedicated PG connection for the block lifetime.
PostgreSQL-only.

## `SELECT ... FOR UPDATE SKIP LOCKED`

Job-queue worker pattern, no broker required:

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

N workers run `claim_jobs()` in parallel and pick disjoint rows.

## Task-affinity pool

```python
from dorm.contrib.task_pool import pinned_connection


async def handler():
    async with pinned_connection():
        authors = await Author.objects.acount()
        await Author.objects.acreate(name="x", age=1)
```

`assert_no_concurrent_gather()` detects pin sharing across
`asyncio.gather` siblings. PostgreSQL-only.

## Circuit breaker

```python
from dorm.contrib.circuit_breaker import circuit_breaker, CircuitOpenError


cb = circuit_breaker("default", failure_threshold=5, open_window_s=30.0)


def safe_count() -> int | None:
    try:
        with cb:
            return Author.objects.count()
    except CircuitOpenError:
        return None
```

States: `CLOSED` → `OPEN` (after N consecutive failures) → `HALF_OPEN`
(after `open_window_s`) → `CLOSED` or `OPEN` based on probe result.

## Outbox pattern

```python
from dorm.contrib.outbox import OutboxEvent, OutboxRelay, record_event
from dorm import transaction


class Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox"


with transaction.atomic():
    order = Order.objects.create(...)
    record_event(Outbox, "order.created", {"order_id": order.id})


def publish_to_kafka(row):
    kafka_client.send("orders", row.payload)
    return True


relay = OutboxRelay(Outbox, batch_size=100)
relay.run(handler=publish_to_kafka)
```

Uses `SELECT ... FOR UPDATE SKIP LOCKED` on PG to scale horizontally.

## Hash-based horizontal sharding

```python
from dorm.contrib.sharding import HashShardRouter, with_shard_key


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


with with_shard_key(request.user.tenant_id):
    order = Order.objects.create(...)
```

Deterministic BLAKE2b hash with configurable salt — does NOT use
Python's randomised built-in `hash()`.

`for_each_shard(fn, num_shards=4)` runs `fn(alias)` against each shard
sequentially.

## Third-party backend plugins

Register via entry-points in your `pyproject.toml`:

```toml
[project.entry-points."djanorm.backends"]
mybackend = "mypkg.backend:MyBackendWrapper"

[project.entry-points."djanorm.async_backends"]
mybackend = "mypkg.backend:MyAsyncBackendWrapper"
```

Then:

```python
DATABASES = {"default": {"ENGINE": "mybackend", ...}}
```

## Comparative bench

```bash
python -m bench.compare --runs 3 --ops 1000
```

Side-by-side: dorm vs Django ORM vs SQLAlchemy 2.0 vs Tortoise ORM,
same five scenarios on in-process SQLite. Skips ORMs that are not
installed.
