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

```python
from dorm.contrib.otel import instrument

instrument()                # default tracer name "dorm"
# instrument(tracer_name="myapp.dorm")
```

Every query becomes a span with `db.system` (`"postgresql"` /
`"sqlite"`), `db.statement` (truncated to 1KB), and
`db.dorm.elapsed_ms`. Failed queries get span status `ERROR` and the
exception class in `db.dorm.error`. Idempotent — calling twice
replaces the previous wiring (no double-spans). Optional dependency
on `opentelemetry-api`.

## PostgreSQL pub/sub via `LISTEN` / `NOTIFY`

```python
from dorm.db.connection import get_async_connection

conn = get_async_connection()

# Publish:
async with transaction.aatomic():
    Order.objects.create(...)
    await conn.notify("orders", str(order.pk))
# NOTIFY is delivered after COMMIT — listeners only see committed work.

# Subscribe:
async for msg in conn.listen("orders"):
    print(msg.channel, msg.payload, msg.pid)
```

Channel names are validated as SQL identifiers. The `listen()` async
iterator opens its own dedicated connection (LISTEN holds the
connection for the lifetime of the subscription) and closes it
cleanly when you break out.
