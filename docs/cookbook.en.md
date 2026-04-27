# Cookbook

Practical recipes for common situations. Each one is a complete,
copy-pastable snippet.

## Idempotent get-or-create with a unique constraint

```python
from dorm.exceptions import IntegrityError

def get_or_create_email(email: str, name: str) -> Author:
    obj, _ = Author.objects.get_or_create(
        email=email,
        defaults={"name": name},
    )
    return obj
```

`get_or_create` runs inside a transaction so two concurrent callers
can't both insert. If the unique constraint isn't on the lookup keys
the safer pattern is to catch `IntegrityError` and re-`get`.

## Atomic counter increment

```python
from dorm import F

# Race-free, single SQL UPDATE.
Post.objects.filter(pk=42).update(views=F("views") + 1)
```

Never `post.views += 1; post.save()` — that's read-modify-write and
loses concurrent increments.

## Pagination

```python
def paginate(qs, page: int, page_size: int = 20):
    start = (page - 1) * page_size
    return list(qs.order_by("id")[start:start + page_size])
```

Always pair a slice with `order_by(...)` — without explicit ordering
the same row can show up on two pages or none.

For large datasets, **keyset pagination** is far faster than `OFFSET`:

```python
def page_after(qs, last_id: int, page_size: int = 20):
    return list(
        qs.filter(id__gt=last_id).order_by("id")[:page_size]
    )
```

## Soft delete

There's a built-in mixin in `dorm.contrib.softdelete`:

```python
from dorm.contrib.softdelete import SoftDeleteModel
import dorm

class Post(SoftDeleteModel):
    title = dorm.CharField(max_length=200)

# Three managers:
Post.objects                          # only live rows
Post.all_objects                      # everything
Post.deleted_objects                  # only soft-deleted

# Soft delete by default; pass hard=True for a real DELETE:
post.delete()                         # UPDATE … SET deleted_at = now()
post.delete(hard=True)                # DELETE FROM …
post.restore()                        # clear deleted_at

# Async parity:
await post.adelete()
await post.arestore()
```

Caveats: `on_delete=CASCADE` does **not** cascade through soft
deletes (children remain visible to `Post.objects`). And database-
level UNIQUE constraints don't know about `deleted_at` — use a
partial index (`UNIQUE … WHERE deleted_at IS NULL`) at the schema
level if you need "unique among live rows only".

## Audit log via signals

```python
from dorm.signals import post_save, post_delete

def write_audit(sender, instance, created, **kwargs):
    AuditLog.objects.create(
        model=sender.__name__,
        pk=instance.pk,
        action="created" if created else "updated",
        payload=model_to_dict(instance),
    )

post_save.connect(write_audit, sender=Article)
post_delete.connect(write_audit, sender=Article)
```

For high-volume tables prefer the database-level WAL → Kafka pattern
rather than Python signals — signals fire in-process and add latency.

## Bulk insert with deduplication

```python
existing = set(
    User.objects.filter(email__in=[u.email for u in batch])
                .values_list("email", flat=True)
)
new_users = [u for u in batch if u.email not in existing]
User.objects.bulk_create(new_users, batch_size=500)
```

Or, on PG only, push the dedup into the database:

```python
from dorm.db.connection import get_connection
get_connection().execute(
    "INSERT INTO users (email, name) VALUES %s ON CONFLICT (email) DO NOTHING",
    rows,
)
```

## `select_for_update` lock

```python
with transaction.atomic():
    account = (
        Account.objects.select_for_update()
                       .get(pk=account_id)
    )
    account.balance -= amount
    account.save(update_fields=["balance"])
```

`SELECT ... FOR UPDATE` holds row locks until the transaction
commits — use it whenever you do read-then-write under contention.

## Custom manager / queryset method

```python
class PublishedQuerySet(QuerySet):
    def published(self):
        return self.filter(published=True, published_at__lte=Now())

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    published = dorm.BooleanField(default=False)
    published_at = dorm.DateTimeField(null=True)

    objects = PublishedQuerySet.as_manager()

# Article.objects.published().filter(author=...)
```

The result is still a `QuerySet`, so you keep chaining.

## Streaming a million rows for an export

```python
import csv

with open("authors.csv", "w") as f:
    w = csv.writer(f)
    w.writerow(["id", "name", "email"])
    for a in Author.objects.order_by("id").iterator(chunk_size=5000):
        w.writerow([a.id, a.name, a.email])
```

`iterator(chunk_size=N)` opens a server-side cursor on PG and
arraysize-streams on SQLite, so memory stays flat.

## Multi-tenant per-schema routing

```python
class TenantRouter:
    def db_for_read(self, model, **hints):
        return _current_tenant_alias()
    def db_for_write(self, model, **hints):
        return _current_tenant_alias()
```

Set the tenant alias from a middleware (web) or context manager
(workers) and route everything through it. Combine with one set of
migrations applied per alias.

## Read-after-write with a replica

```python
# Routing ensures reads go to the replica — but right after a write,
# the replica may not have the row yet. Read from the primary explicitly:
new = Post.objects.using("default").get(pk=new_pk)
```

Always read your own writes from the primary; rely on the replica
for everything older than a few seconds.

## Testing fixtures

```python
import pytest, dorm
from dorm.db.connection import close_all
from dorm.test import transactional_db, atransactional_db  # noqa: F401

@pytest.fixture(scope="session", autouse=True)
def configure_dorm():
    dorm.configure(DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}})
    yield
    close_all()

@pytest.fixture
def author():
    return Author.objects.create(name="Alice", age=30)


def test_something(transactional_db, author):
    Author.objects.create(name="Mallory", age=99)
    assert Author.objects.count() == 2
    # Both rows are rolled back automatically when the test exits.
```

`transactional_db` (sync) and `atransactional_db` (pytest-asyncio)
wrap each test in an `atomic()` block that rolls back on exit, so
you avoid the `DROP TABLE` / `CREATE TABLE` churn between tests. For
unittest-style suites use `dorm.test.DormTestCase` as a mixin:

```python
import unittest
from dorm.test import DormTestCase

class AuthorTests(DormTestCase, unittest.TestCase):
    def test_create(self):
        Author.objects.create(name="Alice", age=30)
        # rolled back at tearDown
```

For PostgreSQL integration tests, use
[`testcontainers`](https://testcontainers-python.readthedocs.io/) to
spin a throwaway Postgres per session.

## OpenTelemetry instrumentation

`dorm.contrib.otel` hooks the `pre_query` / `post_query` signals so
every ORM query becomes an OTel span without per-call-site changes.

### Setup

Install the OTel API (and an SDK if you want to ship spans
anywhere):

```bash
pip install opentelemetry-api opentelemetry-sdk
# Optional exporters:
pip install opentelemetry-exporter-otlp     # → OTLP collector / Jaeger / Honeycomb
pip install opentelemetry-exporter-jaeger   # → Jaeger directly
```

In your app startup:

```python
from dorm.contrib.otel import instrument

instrument()                                 # default tracer name "dorm"
# instrument(tracer_name="myapp.dorm")       # custom tracer
```

That's it — every query the ORM runs now produces a span.

### Span attributes

Each span carries:

| Attribute | Value |
|---|---|
| span name | `db.<vendor>` (`db.postgresql` / `db.sqlite`) |
| `db.system` | `"postgresql"` or `"sqlite"` |
| `db.statement` | the SQL text, truncated to 1024 chars |
| `db.dorm.elapsed_ms` | wall-clock duration (set on `post_query`, before `end()`) |
| `db.dorm.error` | exception class name (only on failure) |
| span status | `ERROR` on failure, default `UNSET` on success |

The 1KB statement truncation keeps massive `bulk_create` SQL out of
your traces; if you need the full SQL, log it separately via
`dorm.queries`.

### Wiring an exporter (Jaeger via OTLP)

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider(resource=Resource({SERVICE_NAME: "myapp"}))
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
)
trace.set_tracer_provider(provider)

# Now wire dorm into it:
from dorm.contrib.otel import instrument
instrument()
```

`instrument()` is idempotent — calling it twice replaces the
previous wiring instead of producing double spans, so it's safe to
call again on hot reload.

### Disconnecting

```python
from dorm.contrib.otel import uninstrument
uninstrument()  # detaches the receivers; future queries are not traced
```

Useful in test teardown or in libraries that want to opt out for a
specific code path.

### Caveats

- **Optional dependency.** `instrument()` raises a clear `ImportError`
  if `opentelemetry-api` isn't installed. Production deploys without
  the package keep working — just no spans.
- **Spans are produced synchronously on the calling thread.** No
  dedicated background pump. The BatchSpanProcessor (typical SDK
  config) absorbs the export latency, so the caller sees nanosecond
  overhead per span.
- **Sensitive params land in `db.statement`.** The SQL is the literal
  query text (`%s` placeholders, params separate). The OTel
  attribute does NOT include the bound parameters — those go through
  `dorm.db.utils._mask_params` in DEBUG logs only. If you also want
  the params on the span, write a custom receiver instead of
  calling `instrument()`.

## PostgreSQL pub/sub via `LISTEN` / `NOTIFY`

PostgreSQL's `NOTIFY` / `LISTEN` give you pub-sub on the database
itself — no Redis required for fan-out workloads at modest scale.
The `dorm` async wrapper exposes both ends.

### Publishing

```python
from dorm import transaction
from dorm.db.connection import get_async_connection

conn = get_async_connection()

async def create_order(payload):
    async with transaction.aatomic():
        order = await Order.objects.acreate(**payload)
        await conn.notify("orders.created", str(order.pk))
        # NOTIFY is delivered AFTER COMMIT — subscribers never see
        # work that ends up rolled back.
```

`channel` is validated as a SQL identifier (`[A-Za-z_][A-Za-z0-9_]*`,
≤ 63 chars), so a channel name from user input can't smuggle SQL.
The payload is bound as a parameter, so no escaping needed.

**Payload limits.** PostgreSQL's `NOTIFY` payload is capped at
8000 bytes by default (compile-time `NOTIFY_PAYLOAD_MAX`). Don't
serialise the whole row — pass a pk and let the listener fetch.

### Subscribing

```python
async def consume_orders():
    conn = get_async_connection()
    async for msg in conn.listen("orders.created"):
        order = await Order.objects.aget(pk=int(msg.payload))
        await dispatch(order)
```

`msg` is a `psycopg.Notify` with `channel`, `payload`, and `pid`
attributes. The async iterator never returns by itself — break out
when you want to stop:

```python
async for msg in conn.listen("orders.created"):
    if shutdown_event.is_set():
        break
    await dispatch(msg)
```

### Connection ownership

`listen()` opens its **own** dedicated psycopg connection (LISTEN
registers a session-scoped notification handler — the connection
must outlive the subscription). It does NOT pull from the pool, so
a long-lived listener doesn't tie up a pool slot.

When you break out of the iterator, the connection is closed in a
`finally` block. If your worker dies hard, the listener connection
gets reaped by PG's idle-connection timeout.

### Reconnection / retries

The current `listen()` does not auto-reconnect on connection loss.
Wrap it in your own retry loop if you need that:

```python
async def reliable_listener(channel: str, handler):
    while True:
        try:
            async for msg in get_async_connection().listen(channel):
                await handler(msg)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            logger.warning("listener disconnected: %s — reconnecting", exc)
            await asyncio.sleep(1.0)
```

Keep in mind: notifications fired *while disconnected* are lost.
PG doesn't queue them. For at-least-once semantics, pair NOTIFY
with an outbox table you poll on reconnect.

### Common patterns

**Cache invalidation across replicas:**

```python
async with transaction.aatomic():
    await User.objects.filter(pk=user_id).aupdate(**changes)
    await conn.notify("user.invalidate", str(user_id))
# Every node listening drops its local cache for that user.
```

**Worker queue (one-of-N consumers):**

NOTIFY broadcasts to *all* listeners. For exclusive-consumer
patterns, use a row-based queue with `select_for_update(skip_locked=True)`
and use NOTIFY just as a wakeup signal:

```python
# Producer
async with transaction.aatomic():
    job = await Job.objects.acreate(payload=payload, status="pending")
    await conn.notify("jobs.wakeup", "")

# Consumer
async for _ in conn.listen("jobs.wakeup"):
    while True:
        async with transaction.aatomic():
            job = await (
                Job.objects.filter(status="pending")
                .select_for_update(skip_locked=True)
                .afirst()
            )
            if job is None:
                break
            await job.run()
            await job.adelete()
```
