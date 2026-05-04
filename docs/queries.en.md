# Querying

dorm's QuerySet is lazy: building one doesn't hit the database. The
SQL only runs when you iterate, slice, or call a terminal method
(`first()`, `count()`, `exists()`, …).

## Filter, exclude, get

```python
# Equality
Author.objects.filter(name="Alice")

# Lookups: __gt, __gte, __lt, __lte, __contains, __icontains,
#          __startswith, __endswith, __in, __isnull, __range, __regex
Author.objects.filter(age__gte=18, name__icontains="al")
Author.objects.exclude(email__isnull=True)

# Single object — raises DoesNotExist / MultipleObjectsReturned
alice = Author.objects.get(email="alice@example.com")

# Same but returns None instead of raising
alice = Author.objects.get_or_none(email="missing@example.com")
```

### Lookups across relations

```python
# Forward FK chain: books whose author's name starts with "Al".
Book.objects.filter(author__name__startswith="Al")

# Reverse relation via the default ``<model_lower>_set`` accessor —
# no ``related_name`` declared on the FK.
Author.objects.filter(book_set__title="alpha").distinct()

# Same query via custom ``related_name="books"``.
Author.objects.filter(books__published=True).distinct()

# Reverse-FK aggregation — ``Count`` walks the reverse accessor and
# auto-emits ``GROUP BY`` over the outer columns. Authors with zero
# books surface with ``book_count = 0`` (LEFT OUTER JOIN).
from dorm import Count

Author.objects.annotate(book_count=Count("book_set")).order_by("-book_count")

# Reverse one-to-one accessor and many-to-many descriptor work the
# same way.
Profile.objects.filter(acct__email="ace@example.com")        # OneToOne reverse
Article.objects.filter(tags__name="python").distinct()       # M2M
```

### JSON path lookups

```python
# JSONField supports nested-key traversal in lookups. The compiler
# emits the vendor's JSON-path operator — ``#>>`` on PostgreSQL,
# ``json_extract`` on SQLite.
class Doc(dorm.Model):
    data = dorm.JSONField()

Doc.objects.filter(data__name="alice")
# PG:    SELECT ... WHERE "data" #>> '{name}' = %s
# SQLite: SELECT ... WHERE json_extract("data", '$.name') = %s

Doc.objects.filter(data__address__city="Lisbon")
# PG:    "data" #>> '{address,city}' = %s
# SQLite: json_extract("data", '$.address.city') = %s
```

The PG ``#>>`` operator returns ``text``. Pair with ``Cast`` for
typed comparisons (``Cast(F("data__age"), "INTEGER")__gt=18``).

### Trigram + unaccent lookups (3.1+, PG only)

PostgreSQL ships the `pg_trgm` and `unaccent` extensions
out-of-the-box; enable them once per database, then use the
matching dorm lookups:

```sql
-- One-time DDL (or via migration RunSQL):
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
```

```python
# Approximate / fuzzy matches via the % operator family.
Author.objects.filter(name__trigram_similar="alise")        # "Alice" matches
Author.objects.filter(name__trigram_word_similar="ali")
Author.objects.filter(name__trigram_strict_word_similar="ali")

# Diacritic-insensitive equality.
Author.objects.filter(name__unaccent="Cafe")  # matches "Café"
```

### Custom lookups via `register_lookup` (3.1+)

Plug in a project-specific lookup name without subclassing every
field:

```python
from dorm.lookups import register_lookup

register_lookup(
    "zipcode_us",
    "{col} ~ '^[0-9]{{5}}(-[0-9]{{4}})?$'",
    value_transform=None,
)

Address.objects.filter(zip_code__zipcode_us=None)
```

The transform runs over the queryset value before binding;
pass `None` for value-ignoring lookups (regex shape, etc.).
Names colliding with built-ins raise `ValueError`.

### Q objects — complex boolean logic

```python
from dorm import Q

Author.objects.filter(
    Q(age__gte=18) & (Q(name__startswith="A") | Q(email__contains="@hq."))
)
```

### F expressions — reference other columns

```python
from dorm import F

# Increment in-place (no race vs read-modify-write):
Post.objects.filter(pk=42).update(views=F("views") + 1)

# Compare two columns
Post.objects.filter(updated_at__gt=F("created_at"))
```

## Slicing & ordering

```python
# OFFSET / LIMIT — lazy, no SQL until you iterate
qs = Author.objects.order_by("name")[10:20]

# Reverse a queryset
Author.objects.order_by("-age")

# Override the model's Meta.ordering with .order_by(...) explicitly,
# or fall back to the default with .order_by()
```

## Counting and existence

```python
Author.objects.count()                       # SELECT COUNT(*)
Author.objects.filter(active=True).exists()  # SELECT 1 ... LIMIT 1
Author.objects.first()                       # SELECT ... LIMIT 1
Author.objects.last()                        # opposite ordering, LIMIT 1
```

## Materializing the whole queryset

`all()` returns a fresh `QuerySet` — it doesn't hit the DB until you
iterate, slice, or call a terminal method.

```python
# Sync
authors = list(Author.objects.all())
for a in Author.objects.all():
    ...

# Async — three equivalent ways
authors = [a async for a in Author.objects.all()]
authors = await Author.objects.all()           # QuerySets are awaitable
async for a in Author.objects.all():
    ...
```

Use `iterator()` / `aiterator()` (see [Streaming](#streaming-for-huge-result-sets))
when you don't want every row in memory at once.

## Values and value lists

```python
# Sync — list[dict[str, Any]] — chainable (filter, order_by) before iterating
Author.objects.values("name", "age")

# Async — same shape, awaitable
await Author.objects.avalues("name", "age")
# or, since QuerySets are awaitable:
await Author.objects.values("name", "age")

# Sync — list[tuple]; flat=True with a single column returns list[value]
Author.objects.values_list("name", flat=True)

# Async — same shape, awaitable
await Author.objects.avalues_list("name", flat=True)
await Author.objects.values_list("name", flat=True)
```

`avalues` / `avalues_list` materialize the whole queryset in one
round-trip; for huge sets prefer streaming via `aiterator()`.

## Aggregations & annotations

```python
from dorm import Sum, Avg, Count, Max, Min

# Whole-queryset aggregation
Author.objects.aggregate(total=Sum("age"), avg=Avg("age"))
# → {"total": 137, "avg": 27.4}

# Per-row annotation (computed column)
Author.objects.annotate(post_count=Count("books"))
```

### `alias()` — annotate without selecting

`alias()` declares a named expression usable in `filter()` /
`exclude()` / `order_by()` but **not** projected into the result
rows — skip the bandwidth and per-row hydration cost when you only
need the value to build a predicate or a sort key:

```python
authors = (
    Author.objects
    .alias(book_count=Count("books"))
    .filter(book_count__gte=5)        # uses the alias
    .order_by("name")
)
# SELECT only the regular Author columns; the COUNT() participates
# in the WHERE clause but isn't returned.
```

Promote an alias to a real projection by re-declaring it via
`annotate(name=...)` later in the chain — Django parity.

### PostgreSQL aggregates (3.1+)

```python
from dorm import (
    StringAgg, ArrayAgg, JSONBAgg,
    BoolOr, BoolAnd, BitOr, BitAnd,
)

# String / Array / JSON collection
Tag.objects.annotate(article_titles=StringAgg("articles__title", ", "))
Tag.objects.annotate(article_ids=ArrayAgg("articles__id"))
Tag.objects.annotate(payload=JSONBAgg("articles__id"))

# Boolean reduction across the group
User.objects.aggregate(any_active=BoolOr("is_active"))
User.objects.aggregate(all_active=BoolAnd("is_active"))

# Bitwise reduction
Setting.objects.aggregate(merged_flags=BitOr("flags"))
```

`JSONBAgg`, `BoolOr`, `BoolAnd` are PostgreSQL-only at the SQL
level. `BitOr` / `BitAnd` work on PG and MySQL; SQLite needs an
extension.

## DB functions

```python
from dorm import Case, When, Coalesce, Lower, Upper, Length, Concat, Now, Cast, Abs

Author.objects.annotate(
    label=Case(
        When(age__lt=18, then="minor"),
        When(age__gte=65, then="senior"),
        default="adult",
    ),
    full_name=Concat(Lower("first_name"), " ", Lower("last_name")),
)
```

## Set operations

```python
qs_a = Author.objects.filter(active=True)
qs_b = Author.objects.filter(books__published=True)

qs_a.union(qs_b)          # UNION (distinct)
qs_a.union(qs_b, all=True)
qs_a.intersection(qs_b)
qs_a.difference(qs_b)
```

## Updates and deletes

```python
# Bulk update — single SQL UPDATE, returns rowcount
n = Author.objects.filter(active=False).update(active=True)

# Bulk delete — handles on_delete CASCADE chains
n, by_model = Author.objects.filter(age__lt=10).delete()
```

For mass updates of *different* values per row, use `bulk_update`:

```python
authors = list(Author.objects.all())
for a in authors:
    a.score = compute_score(a)
Author.objects.bulk_update(authors, fields=["score"], batch_size=500)
# 1 UPDATE statement per batch (CASE WHEN), not N statements.
```

## Inserting

```python
Author.objects.create(name="Alice", age=30)   # INSERT
Author.objects.bulk_create([
    Author(name=f"User{i}", age=i) for i in range(1_000)
], batch_size=500)
# 1 multi-row INSERT per batch.
```

### Upsert (`bulk_create` with conflict handling)

`bulk_create` accepts two upsert flags, mapping to PostgreSQL /
SQLite `ON CONFLICT` semantics:

```python
# Skip duplicates entirely (ON CONFLICT DO NOTHING)
Tag.objects.bulk_create(
    [Tag(name="alpha"), Tag(name="beta")],
    ignore_conflicts=True,
)

# Update on conflict (ON CONFLICT (...) DO UPDATE SET ...)
Author.objects.bulk_create(
    [Author(email="x@y.com", name="Updated", age=42)],
    update_conflicts=True,
    update_fields=["name", "age"],     # what to refresh on conflict
    unique_fields=["email"],            # which constraint identifies the conflict
)
```

`unique_fields=` is **required** with `update_conflicts=True`.
`update_fields=` defaults to every non-PK / non-unique column when
omitted — usually what you want for an idempotent sync from an
external source. Async counterpart: `abulk_create(...)` with the
same flags.

When conflicts may have skipped rows, returned PKs are not
back-filled on the input objects — the database doesn't report which
rows actually wrote. Re-fetch by `unique_fields` if you need the
final PK set.

### Returning DB-side defaults (`bulk_create(returning=…)`)

```python
import dorm
from dorm.expressions import RawSQL


class Item(dorm.Model):
    name: str = dorm.CharField(max_length=80)
    rev: int = dorm.IntegerField(db_default=1)
    created_at = dorm.DateTimeField(db_default=RawSQL("now()"))


items: list[Item] = [Item(name="a"), Item(name="b")]
Item.objects.bulk_create(items, returning=["rev", "created_at"])

# Each obj now carries the values the DB actually wrote — no follow-up SELECT.
print(items[0].rev, items[0].created_at)
```

`returning=[<field>, …]` asks the database to send back the listed
columns for each newly-inserted row and back-fill them on the
corresponding object. Useful when the column carries a server-side
default (`db_default=…`), is a `GeneratedField`, or is otherwise
populated by a trigger.

- **PostgreSQL** and **SQLite ≥ 3.35** support `RETURNING` on
  `INSERT` — both back the feature.
- **MySQL** has no `RETURNING` on `INSERT`; the call raises
  `NotImplementedError` (re-fetch by primary key instead — PKs
  are already back-filled).
- Cannot be combined with `ignore_conflicts` / `update_conflicts`:
  when conflicts skip or update existing rows the returned-row
  count no longer aligns 1:1 with the input list. Validation
  raises `ValueError` up-front so the failure mode is obvious.

Bug-fix bundled with this feature: `bulk_create` no longer sends
`NULL` for columns the user left unset when the column DDL declares
its own `DEFAULT …`. The column is omitted from the `INSERT` so the
DB applies its own default — matching Django's behaviour.

Async counterpart: `await Item.objects.abulk_create(items,
returning=["rev"])`.

## get_or_create / update_or_create

```python
obj, created = Author.objects.get_or_create(
    email="x@y.com",
    defaults={"name": "X", "age": 0},
)

obj, created = Author.objects.update_or_create(
    email="x@y.com",
    defaults={"name": "Updated", "age": 99},
)
```

Both run inside a transaction so concurrent callers don't double-insert.

## Relationship loading

### `select_related` — JOIN

```python
# 1 query with a JOIN — author preloaded
for book in Book.objects.select_related("author"):
    print(book.author.name)         # no extra query
```

### `prefetch_related` — separate query, batched

```python
# 2 queries total: posts + (1 IN-query for all author rows)
for author in Author.objects.prefetch_related("books"):
    print(author.books.all())       # no extra query
```

For M2M, `prefetch_related` issues a single JOIN against the through
table (no separate "fetch through then fetch targets" round-trip).

#### Polymorphic FKs (`GenericForeignKey`)

`prefetch_related("target")` works on a `GenericForeignKey` too.
Without it, every descriptor read does its own `get(pk=…)` — N+1
across a queryset of N tags pointing at K distinct content types.
With it, dorm groups instances by `content_type_id`, fetches every
referenced `ContentType` in a single SELECT, and then issues one
`filter(pk__in=…)` per content type — total: **1 + 1 + K** queries.

```python
# 3 tags pointing at 2 articles + 2 books
# = 1 (tags) + 1 (content_types) + 2 (one per CT) = 4 queries
for tag in Tag.objects.prefetch_related("target"):
    print(tag.target)        # served from cache, no extra query
```

Two compatibility notes:

- A custom `Prefetch("target", queryset=…)` is **not supported** —
  one queryset can't filter all targets of a heterogeneous GFK. If
  you need filtering, prefetch each concrete relation explicitly with
  its own `Prefetch`.
- `to_attr=…` is also unsupported on a GFK; the descriptor's own
  cache slot is what dorm fills, so `instance.target` returns the
  resolved object without a second query.

#### Reverse generic relations (`GenericRelation`)

Symmetric: `prefetch_related` over a reverse `GenericRelation`
(`Article.objects.prefetch_related("tags")`) groups every target
instance by its PK, runs **one** SELECT against the related model
filtered by `content_type` + `object_id__in`, and stamps each owner's
manager cache slot. `article.tags.all()` then reads from memory.

```python
# 3 articles + 5 tags pointing at them = 1 (articles) + 1 (tags) = 2 queries
for article in Article.objects.prefetch_related("tags"):
    for tag in article.tags.all():     # served from cache
        ...
```

`Prefetch("tags", queryset=Tag.objects.filter(label="urgent"))` is
honoured — the user-supplied queryset is AND-ed with the
`content_type` predicate.

## Partial loading

```python
Author.objects.only("name", "email")     # SELECT name, email
Author.objects.defer("bio")              # SELECT everything except bio
```

### Composing with `select_related`

`only()` / `defer()` accept dotted paths to restrict the projection
of a `select_related`-joined relation as well:

```python
# JOINs publishers, but only pulls publisher.name (plus PK for identity).
Author.objects.select_related("publisher").only("name", "publisher__name")

# Same JOIN, but drop publisher.bio from the SELECT — keep everything else.
Author.objects.select_related("publisher").defer("publisher__bio")
```

Bare names restrict the parent model (legacy behaviour); dotted
names restrict the named relation. The PK of the related model is
always implicitly included so the hydrated instance keeps its
identity. The two methods write to different state buckets so
mixing them works:

```python
Author.objects.select_related("publisher").only("name").defer("publisher__bio")
# parent: id, name. publisher: every column except bio.
```

## Row locking: `select_for_update`

Lock rows for the surrounding transaction. Must be called inside an
`atomic()` / `aatomic()` block — otherwise PostgreSQL releases the
lock immediately at autocommit and the call is effectively a no-op.

```python
from dorm import transaction

with transaction.atomic():
    a = Author.objects.select_for_update().get(pk=1)
    a.balance -= 100
    a.save()
```

Three flags map to PostgreSQL's row-level lock variants:

```python
# Task-queue pattern: each worker pops the next *unlocked* row.
job = (
    Job.objects
    .filter(status="pending")
    .select_for_update(skip_locked=True)
    .first()
)

# Bail fast on contention instead of waiting.
qs.select_for_update(no_wait=True)

# Lock only specific tables when joining (avoid locking parents
# in a select_related chain).
qs.select_related("publisher").select_for_update(of=("authors",))
```

`skip_locked` and `no_wait` are mutually exclusive. All three flags
are PostgreSQL-only — passing them on SQLite raises
`NotImplementedError` (SQLite serialises writers via the file lock,
so row-level lock variants don't translate).

## Streaming for huge result sets

```python
# Default: fetch all rows, iterate in memory (fine for thousands).
for a in Author.objects.iterator():
    process(a)

# chunk_size → server-side cursor on PG, arraysize on SQLite.
# Use this for million-row scans.
for a in Author.objects.order_by("id").iterator(chunk_size=5000):
    process(a)
```

## EXPLAIN

```python
slow_qs = Author.objects.filter(age__gte=18).select_related("publisher")
print(slow_qs.explain(analyze=True))
```

PG returns the full plan; SQLite returns `EXPLAIN QUERY PLAN`. Use
this when a route is slow in prod and you need to tell what the
planner chose.

## Raw SQL escape hatch

```python
authors = Author.objects.raw(
    "SELECT * FROM authors WHERE age > %s ORDER BY name",
    [18],
)
for a in authors:
    print(a.name)
```

`raw()` returns a `RawQuerySet` that hydrates rows back into model
instances. For results that don't map to a model, drop down to
`get_connection().execute(...)`.

!!! danger "Use placeholders, never f-strings"

    `raw()` sends `raw_sql` to the database verbatim — values must be
    bound via the `params` list, never spliced into the SQL string:

    ```python
    # SAFE — value goes through psycopg / sqlite3 binding
    Author.objects.raw("SELECT * FROM authors WHERE id = %s", [user_id])

    # UNSAFE — turns user input into SQL
    Author.objects.raw(f"SELECT * FROM authors WHERE id = {user_id}")
    ```

    As a defensive check, dorm counts the placeholders (`%s` and `$N`,
    skipping ones inside quoted literals) and refuses to construct the
    `RawQuerySet` if the number doesn't match `len(params)`. That
    catches the most common slip — building the SQL with `f""` and
    forgetting to pass values — at construction time instead of
    surfacing as a confusing database error.

    For dynamic identifiers (table or column names that aren't fixed
    at coding time), validate them against an allowlist before
    splicing — placeholders only bind values, not identifiers.

### `Cast(...)` accepts a fixed set of SQL types

`Cast(expr, output_field=...)` splices its second argument into SQL
(no bind exists for type names), so `output_field` is validated
against an allowlist:

```python
from dorm import Cast, F

Author.objects.annotate(age_str=Cast(F("age"), output_field="TEXT"))
```

Allowed base types include `INTEGER`, `BIGINT`, `SMALLINT`, `REAL`,
`DOUBLE PRECISION`, `FLOAT`, `NUMERIC`, `DECIMAL`, `TEXT`,
`VARCHAR`, `CHAR`, `BLOB`, `BYTEA`, `BOOLEAN`, `BOOL`, `DATE`,
`TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`, `JSON`, `JSONB`,
`UUID`. An optional length / precision spec (`VARCHAR(255)` or
`NUMERIC(10, 2)`) is accepted. Any other value raises
`ImproperlyConfigured` immediately at queryset build time, so a
typo or unsanitised input can never reach the SQL.


## Advanced querying

Building blocks for non-trivial reporting queries — what you'd
otherwise drop to `RawQuerySet` for:

- **`Subquery(qs)` / `Exists(qs)` / `OuterRef("col")`** — correlated
  subqueries that compose with `filter()` / `annotate()`.
- **`Window(expr, partition_by=, order_by=)`** plus `RowNumber`,
  `Rank`, `DenseRank`, `NTile`, `Lag`, `Lead`, `FirstValue`,
  `LastValue`, `NthValue`, `PercentRank`, `CumeDist` — ranking,
  running totals, deltas, percentile bucketing without bailing to
  raw SQL.
- **`QuerySet.with_cte(name=qs)`** — non-recursive CTEs.
- **Scalar functions**: `Greatest`, `Least`, `Round`, `Trunc`,
  `Extract`, `Substr`, `Replace`, `StrIndex`.
- **Full-text search (PostgreSQL)** via `dorm.search.SearchVector` /
  `SearchQuery` / `SearchRank` and the `__search` lookup.
- **`QuerySet.cursor_paginate(...)` / `acursor_paginate(...)`** —
  keyset pagination with stable ordering, O(1) deep-page cost.
