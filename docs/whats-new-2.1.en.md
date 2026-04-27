# What's new in djanorm 2.1

The 2.1 release closes the biggest gap left in 2.0 for production
reporting workloads — *querying* — and tightens the migration story
for tables large enough that an `ALTER TABLE` would page someone.
Every feature below ships with tests against both SQLite and
PostgreSQL.

## Querying

### `Subquery()` and `Exists()` — correlated subqueries

```python
from dorm import Exists, OuterRef, Subquery

# "Authors with at least one published book"
qs = Author.objects.filter(
    Exists(Book.objects.filter(author=OuterRef("pk"), published=True))
)

# Annotate each Author with the title of their latest book
latest = (
    Book.objects
        .filter(author=OuterRef("pk"))
        .order_by("-published_on")
        .values("title")[:1]
)
qs = Author.objects.annotate(latest=Subquery(latest))
```

`OuterRef("pk")` resolves to the outer model's primary-key column
when the subquery compiles. Negate with `~Exists(...)`.

### Window functions

```python
from dorm import Sum, Window, RowNumber, Lag

# Top 3 books per author by pages
qs = (
    Book.objects
        .annotate(
            rk=Window(RowNumber(), partition_by=["author_id"], order_by="-pages")
        )
        .filter(rk__lte=3)
)

# Running total of book pages, ordered by publish date
qs = Book.objects.annotate(
    running_pages=Window(
        Sum("pages"), partition_by=["author_id"], order_by="published_on"
    )
)

# Delta vs the previous row in the partition
qs = Book.objects.annotate(
    prev_pages=Window(Lag("pages"), partition_by=["author_id"], order_by="published_on")
)
```

The full set: `RowNumber`, `Rank`, `DenseRank`, `NTile`, `Lag`,
`Lead`, `FirstValue`, `LastValue`. Ranking constructors *require* an
`order_by` — building a `Window(RowNumber())` without one raises at
queryset build time, because the SQL would parse but return
implementation-defined results.

### CTEs (`WITH ... AS (...)`)

```python
recent = Book.objects.filter(published_on__gte=one_week_ago)
qs = Book.objects.with_cte(recent_books=recent).filter(...)
```

Non-recursive only. CTE bodies share the outer query's
placeholder rewrite, so PG prepared-statement caches still hit.

### New scalar functions

| Function | Maps to | Notes |
| --- | --- | --- |
| `Greatest(a, b, ...)` | `GREATEST(...)` PG / `MAX(a, b)` SQLite | Vendor-aware |
| `Least(a, b, ...)` | `LEAST(...)` PG / `MIN(a, b)` SQLite | Vendor-aware |
| `Round(expr, places)` | `ROUND(...)` | |
| `Trunc(expr, "month")` | `DATE_TRUNC('month', expr)` | PG; unit allow-listed |
| `Extract(expr, "year")` | `EXTRACT(YEAR FROM expr)` | PG; unit allow-listed |
| `Substr(expr, pos, len)` | `SUBSTR(...)` | 1-indexed |
| `Replace(expr, old, new)` | `REPLACE(...)` | |
| `StrIndex(haystack, needle)` | `STRPOS(...)` PG / `INSTR(...)` SQLite | 1-based |

### Cursor pagination (keyset)

```python
page = Author.objects.cursor_paginate(order_by="-created_at", page_size=20)
# page.items, page.next_cursor, page.has_next
next_page = Author.objects.cursor_paginate(
    order_by="-created_at", page_size=20, after=page.next_cursor,
)
```

Stable across writes. O(1) deep-page cost vs `OFFSET`'s O(N). Async
counterpart: `acursor_paginate`. Returns `CursorPage`, which iterates
over its `items` and exposes `has_next`.

### Full-text search (PostgreSQL)

```python
from dorm.search import SearchVector, SearchQuery, SearchRank

# Simple: a __search lookup using the canonical idiom
qs = Article.objects.filter(title__search="postgres tuning")

# Ranked with explicit vector / query
qs = (
    Article.objects
        .annotate(
            rank=SearchRank(
                SearchVector("title", "body", config="english"),
                SearchQuery("postgres tuning", search_type="websearch"),
            )
        )
        .filter(rank__gt=0)
        .order_by("-rank")
)
```

`search_type="websearch"` accepts `"quoted phrase"`, `OR`, and
`-exclude`. `cover_density=True` switches `SearchRank` to
`ts_rank_cd`. SQLite is unsupported — use FTS5 virtual tables.

## Schema

### `CheckConstraint` and `UniqueConstraint`

```python
from dorm import CheckConstraint, UniqueConstraint, Q

class Order(dorm.Model):
    quantity = dorm.IntegerField()
    user_id = dorm.IntegerField()
    is_active = dorm.BooleanField(default=True)

    class Meta:
        constraints = [
            CheckConstraint(
                check=Q(quantity__gt=0),
                name="order_qty_positive",
            ),
            # Partial unique index — only one *active* order per user.
            UniqueConstraint(
                fields=["user_id"],
                condition=Q(is_active=True),
                name="uniq_active_order_per_user",
            ),
        ]
```

The autodetector emits `AddConstraint` / `RemoveConstraint`
operations. Partial unique constraints render to
`CREATE UNIQUE INDEX ... WHERE predicate` (PostgreSQL + SQLite ≥ 3.8).

### `GeneratedField`

```python
class Order(dorm.Model):
    quantity = dorm.IntegerField()
    price    = dorm.DecimalField(max_digits=10, decimal_places=2)
    total    = dorm.GeneratedField(
        expression="quantity * price",
        output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
    )
```

Computed at write time by the database (PG ≥ 12, SQLite ≥ 3.31).
Python writes are rejected — the database is authoritative. The
expression grammar is allow-listed.

### Index extensions

```python
from dorm import Index, Q

class Article(dorm.Model):
    ...
    class Meta:
        indexes = [
            # Partial index — only active rows participate.
            Index(
                fields=["email"],
                name="ix_active_email",
                condition=Q(deleted_at__isnull=True),
            ),
            # GIN index for JSONB containment queries.
            Index(fields=["payload"], method="gin", name="ix_payload_gin"),
            # Expression index for case-insensitive lookups.
            Index(fields=["LOWER(email)"], name="ix_email_lower"),
            # Composite descending.
            Index(fields=["-created_at", "user_id"], name="ix_recent_per_user"),
        ]
```

`method` accepts `"btree"` (default), `"hash"`, `"gin"`, `"gist"`,
`"brin"`, `"spgist"`, `"bloom"`. SQLite silently uses B-tree.

## Migration safety

### Online (concurrent) index creation

```python
from dorm.migrations.operations import AddIndex
from dorm import Index

operations = [
    AddIndex(
        "Article",
        Index(fields=["email"], name="ix_email"),
        concurrently=True,
    ),
]
```

Emits `CREATE INDEX CONCURRENTLY` on PostgreSQL — no
`AccessExclusiveLock`, no downtime. Must be the only DDL in its
migration file (the executor enforces this so the surrounding atomic
can be skipped). SQLite ignores the flag.

### `SetLockTimeout` and `ValidateConstraint`

```python
from dorm.migrations.operations import RunSQL, SetLockTimeout, ValidateConstraint

operations = [
    # Cap how long any DDL waits for its lock.
    SetLockTimeout(ms=2000),

    # Add the FK without scanning the table.
    RunSQL(
        "ALTER TABLE orders ADD CONSTRAINT fk_orders_user "
        "FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID",
        reverse_sql="ALTER TABLE orders DROP CONSTRAINT fk_orders_user",
    ),

    # Validate online — ShareUpdateExclusive lock only.
    ValidateConstraint(table="orders", name="fk_orders_user"),
]
```

The `NOT VALID` + `VALIDATE CONSTRAINT` pattern lets you add foreign
keys / CHECK constraints to a billion-row table with zero downtime.

## Operations and tooling

### `dorm inspectdb`

```bash
$ dorm inspectdb > legacy/models.py
```

Reverse-engineers `models.py` from the connected database. Best-effort:
field types, FK detection, `db_table` are recovered; constraints,
indexes, `related_name`, validators are not. Diff and edit before
committing.

### `dorm doctor`

```bash
$ dorm doctor
dorm doctor — 2 warning(s), 1 note(s)

warnings:
  ! DATABASES['default']: MAX_POOL_SIZE=2 is small for production; raise to 10–20...
  ! Order.user: ForeignKey without db_index; joins on this FK will sequentially scan...

notes:
  · DORM_RETRY_ATTEMPTS not set or set to 0/1: transient PG errors will surface...
```

Audits the running configuration for production footguns. Exits non-zero
on warnings — usable as a pre-deploy gate.

### URL / DSN in `DATABASES`

```python
import os, dorm

# Direct URL form
dorm.configure(DATABASES={
    "default": "postgres://u:p@host:5432/db?sslmode=require&MAX_POOL_SIZE=20",
})

# Or with overrides — the dict's keys win over the URL
dorm.configure(DATABASES={
    "default": {
        "URL": os.environ["DATABASE_URL"],
        "MAX_POOL_SIZE": 30,
    },
})

# Or use the parser directly
cfg = dorm.parse_database_url(os.environ["DATABASE_URL"])
```

Well-known pool knobs (`MAX_POOL_SIZE`, `POOL_TIMEOUT`, `POOL_CHECK`,
`MAX_IDLE`, `MAX_LIFETIME`, `PREPARE_THRESHOLD`) are lifted to top-level
keys; everything else lands in `OPTIONS`.

## New field types

Four field types fill in long-standing gaps in the type catalogue:

```python
import datetime, enum

class Priority(enum.Enum):
    LOW, MEDIUM, HIGH = "low", "medium", "high"

class Job(dorm.Model):
    name = dorm.CharField(max_length=100)
    timeout = dorm.DurationField()                       # INTERVAL / BIGINT µs
    priority = dorm.EnumField(Priority, default=Priority.LOW)
    owner_email = dorm.CITextField(unique=True)          # CITEXT / TEXT NOCASE
    seats = dorm.IntegerRangeField(null=True, blank=True)  # PG int4range
```

- **`DurationField`** stores `datetime.timedelta`. Native `INTERVAL`
  on PostgreSQL; SQLite registers a sqlite3 adapter so the same
  `timedelta` round-trips as integer microseconds in a `BIGINT`.
- **`EnumField(enum_cls)`** stores an `enum.Enum` member. Column type
  is derived from the underlying value type (string → `VARCHAR`,
  int → `INTEGER`); `choices` is auto-populated from the enum.
- **`CITextField`** maps to PostgreSQL's `CITEXT` (requires the
  extension) and falls back to `TEXT COLLATE NOCASE` on SQLite.
- **Range fields** — `IntegerRangeField`, `BigIntegerRangeField`,
  `DecimalRangeField`, `DateRangeField`, `DateTimeRangeField`. The
  Python value is `dorm.Range(lower, upper, bounds="[)")`. SQLite
  raises `NotImplementedError` from `db_type()` so the limitation
  surfaces at migrate time.

## `FileField` and storage backends

A pluggable file-storage layer with a local-disk default and an
optional S3 backend behind the `s3` extra:

```python
class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

doc = Document(name="Q1")
doc.attachment = dorm.ContentFile(b"PDF bytes", name="q1.pdf")
doc.save()
doc.attachment.url      # → "/media/docs/2026/04/q1.pdf"
```

Configuration mirrors `DATABASES`:

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.storage.FileSystemStorage",
        "OPTIONS": {"location": "/var/app/media", "base_url": "/media/"},
    },
}
```

Switch the default to S3 by installing the extra and pointing the
backend at `dorm.contrib.storage.s3.S3Storage`:

```bash
pip install 'djanorm[s3]'
```

Multiple aliases are supported (`storage="cold"` etc.), so a single
project can mix local + S3. Custom backends only need to subclass
`dorm.storage.Storage` and implement six methods. See the
[Models & fields](models.md#files) doc for the full reference.

## Async signal receivers

Signals now accept `async def` receivers. Connect them the same way:

```python
async def reindex(sender, instance, **kw):
    await search_client.upsert(instance)

dorm.signals.post_save.connect(reindex, sender=Article, weak=False)

await Article(...).asave()        # async receiver awaited sequentially
Article(...).save()               # async receiver skipped + WARNING logged
```

`Signal.asend()` is the new entry point used by `Model.asave` /
`Model.adelete`. Sync receivers stay sync; coroutine receivers are
awaited sequentially in connect order. Sync `send()` keeps its
existing semantics — async receivers connected to it are skipped
with a single `WARNING` on `dorm.signals` so missed work is visible.

## `dorm dumpdata` / `dorm loaddata`

JSON fixtures, compatible with Django's shape:

```bash
dorm dumpdata blog.Author > fixtures/authors.json
dorm dumpdata --output fixtures/seed.json --indent 2
dorm loaddata fixtures/authors.json
```

The output is `[{"model": "<app.Model>", "pk": <pk>, "fields": {...}}, …]`.
FKs serialise as the target's PK; M2M as a list of related PKs. The
loader inserts inside an `atomic()` block, so a malformed record
rolls back the whole file. `save()` and signals are bypassed so
seeding stays deterministic and fast.

## Migration from 2.0.x

Almost every 2.1 feature is additive — no code changes are needed
unless you previously declared a custom `Aggregate` subclass that
overrides `as_sql`. Those should grow a `**kwargs` parameter so the
new `connection=` thread doesn't `TypeError` them at compile time:

```python
# Before
def as_sql(self, table_alias=None, *, model=None):
    ...

# After
def as_sql(self, table_alias=None, *, model=None, **kwargs):
    ...
```

`Index(fields=["-foo"])` is now validated more strictly — the leading
`-` is honoured (DESC), but bare strings with other punctuation will
raise. Move them to the `expressions=` form (e.g. `["LOWER(name)"]`).
