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
# Books whose author's name starts with "Al":
Book.objects.filter(author__name__startswith="Al")

# Reverse relation via related_name
Author.objects.filter(books__published=True).distinct()
```

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

## Partial loading

```python
Author.objects.only("name", "email")     # SELECT name, email
Author.objects.defer("bio")              # SELECT everything except bio
```

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
