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

```python
class SoftDeleteQuerySet(QuerySet):
    def alive(self):
        return self.filter(deleted_at__isnull=True)
    def soft_delete(self):
        from datetime import datetime, timezone
        return self.update(deleted_at=datetime.now(timezone.utc))

class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    deleted_at = dorm.DateTimeField(null=True, blank=True, db_index=True)

    objects = SoftDeleteQuerySet.as_manager()
```

Always go through `Post.objects.alive()` for normal queries and
reserve `Post.objects.all()` for admin/audit code.

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
from dorm.db.connection import close_all, reset_connections

@pytest.fixture(scope="session", autouse=True)
def configure_dorm():
    dorm.configure(DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}})
    yield
    close_all()

@pytest.fixture
def author():
    return Author.objects.create(name="Alice", age=30)
```

For PostgreSQL integration tests, use
[`testcontainers`](https://testcontainers-python.readthedocs.io/) to
spin a throwaway Postgres per session.
