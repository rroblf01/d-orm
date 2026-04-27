# Soft delete

`dorm.contrib.softdelete` swaps the default `DELETE FROM` for a
timestamp-based "soft delete": rows stay in the table but get a
`deleted_at` column set to the current UTC time. The default manager
hides them automatically; opt-in managers expose them when needed.

It's a contrib module (not core) because soft delete carries trade-
offs that some projects can't accept — read the [caveats](#caveats)
before adopting it project-wide.

## Quick start

```python
from dorm.contrib.softdelete import SoftDeleteModel
import dorm

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()

    class Meta:
        db_table = "articles"
```

Inherit from `SoftDeleteModel` instead of `dorm.Model`. The mixin
contributes:

- A `deleted_at` `DateTimeField(null=True, blank=True, db_index=True)`
- Three managers: `objects`, `all_objects`, `deleted_objects`
- `delete(hard=False)` / `restore()` instance methods (sync + async)

You still need to run `dorm makemigrations` / `dorm migrate` so the
table picks up the `deleted_at` column.

## Three managers

```python
Article.objects                # only live rows (deleted_at IS NULL)
Article.all_objects            # everything, including soft-deleted
Article.deleted_objects        # only soft-deleted (deleted_at IS NOT NULL)
```

The default `objects` is a `SoftDeleteManager` whose `get_queryset`
filters `deleted_at__isnull=True`. Every method on the queryset
inherits that filter — `.filter()`, `.count()`, `.exists()`,
aggregates, async iteration, prefetch, and so on. You don't have
to remember to add `.alive()` everywhere; that's the whole point.

`all_objects` and `deleted_objects` are intended for admin tooling,
audit dashboards, GDPR exports, and undelete flows.

## Deleting

```python
article = Article.objects.get(pk=1)
article.delete()                 # UPDATE … SET deleted_at = now()
article.delete(hard=True)        # actual DELETE FROM …
```

`delete()` is a soft delete by default. Pass `hard=True` to bypass
the soft path entirely — useful for GDPR purges, abuse cleanup, or
periodic compaction of long-deleted rows.

The async equivalent works the same way:

```python
await article.adelete()
await article.adelete(hard=True)
```

`delete()` returns a `(total, by_model)` tuple matching the regular
`Model.delete` contract, so existing call sites keep working:

```python
n, by_model = article.delete()
# n == 1
# by_model == {"myapp.Article": 1}
```

## Restoring

```python
article = Article.deleted_objects.get(pk=1)
article.restore()
# Now visible again to Article.objects
```

`restore()` clears the `deleted_at` slot and saves. No-op if the
row was never soft-deleted. Async: `await article.arestore()`.

## Custom managers

`SoftDeleteManager` is just a `Manager` with one extra filter, so you
can subclass it for custom default scoping:

```python
from dorm.contrib.softdelete import SoftDeleteManager

class TenantSoftDeleteManager(SoftDeleteManager):
    def get_queryset(self):
        from .middleware import current_tenant_id
        return super().get_queryset().filter(tenant_id=current_tenant_id())

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    tenant_id = dorm.IntegerField(db_index=True)

    objects = TenantSoftDeleteManager()
    # all_objects / deleted_objects inherited from SoftDeleteModel
```

## Caveats

### `on_delete=CASCADE` does NOT cascade through soft deletes

```python
class Author(SoftDeleteModel):
    name = dorm.CharField(max_length=100)

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
```

When you `author.delete()` (soft), the author's `deleted_at` is set
but the `Article` rows stay live and visible to `Article.objects` —
they still have the FK pointing at the now-soft-deleted author.

If you need cascading soft deletes, override `delete()` in the
parent to walk relations explicitly:

```python
class Author(SoftDeleteModel):
    name = dorm.CharField(max_length=100)

    def delete(self, using="default", *, hard=False):
        if not hard:
            for art in self.article_set.all():
                art.delete()
        return super().delete(using=using, hard=hard)
```

### UNIQUE constraints don't know about `deleted_at`

A `unique=True` column rejects re-inserting a value that matches a
soft-deleted row. If you need "unique among live rows only", create
a partial index at the schema level:

```sql
-- PostgreSQL
CREATE UNIQUE INDEX articles_slug_live
    ON articles (slug) WHERE deleted_at IS NULL;
```

```sql
-- SQLite ≥ 3.8
CREATE UNIQUE INDEX articles_slug_live
    ON articles (slug) WHERE deleted_at IS NULL;
```

Add this through a `RunSQL` migration — the autodetector doesn't
emit partial indexes yet.

### Foreign keys don't know about `deleted_at` either

A FK pointing at a soft-deleted row stays valid. Reading
`article.author` returns the soft-deleted author instance. Code that
assumes "if I can dereference the FK, the parent is alive" breaks
silently. Either:

- Filter explicitly: `Article.objects.filter(author__deleted_at__isnull=True)`
- Cascade soft deletes (see above)
- Use a `Q(...)` mixin in the queryset

### Disk usage

Soft-deleted rows stay on disk forever unless you periodically purge
them. For high-churn tables (sessions, events) this can balloon. A
common pattern:

```python
# Run nightly via cron / Celery beat:
threshold = datetime.now(timezone.utc) - timedelta(days=90)
Article.deleted_objects.filter(deleted_at__lt=threshold).delete(hard=True)
```

## Testing

`SoftDeleteModel` plays nicely with `dorm.test.transactional_db`:
each test starts in a transaction that rolls back, so soft deletes
in one test don't leak to the next.

```python
def test_soft_delete_hides(transactional_db):
    a = Article.objects.create(title="x")
    a.delete()
    assert not Article.objects.filter(pk=a.pk).exists()
    assert Article.deleted_objects.filter(pk=a.pk).exists()
```

## API reference

- **`SoftDeleteModel`** — abstract model. Inherit instead of
  `dorm.Model`. Contributes `deleted_at` field and the three
  managers.
- **`SoftDeleteManager`** — manager filtering `deleted_at IS NULL`.
  Subclass for custom default scoping.
- **`SoftDeleteModel.delete(using="default", *, hard=False)`** — soft
  delete by default; `hard=True` bypasses to a real `DELETE`.
- **`SoftDeleteModel.adelete(...)`** — async counterpart, same args.
- **`SoftDeleteModel.restore(using="default")`** — clears
  `deleted_at`; no-op if the row wasn't soft-deleted.
- **`SoftDeleteModel.arestore(...)`** — async counterpart.
