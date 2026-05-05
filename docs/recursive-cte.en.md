# Recursive CTEs (trees, graphs)

`dorm.tree` (4.0+) generates recursive CTEs for the two patterns
that show up in 95% of schemas with self-referential relations:

- **Descendants** of a node (every transitive child).
- **Ancestors** of a node (path to the root).

PG, SQLite ≥ 3.8.3 and MySQL ≥ 8 all support `WITH RECURSIVE`.

## The problem

An adjacency-list table — every row has `parent_id` pointing at
another row in the same table:

```python
class Category(dorm.Model):
    name = dorm.CharField(max_length=100)
    parent_id = dorm.IntegerField(null=True, db_index=True)
```

Listing every descendant of `pk=42` needs recursive SQL. Without
helpers you'd write:

```sql
WITH RECURSIVE descendants AS (
    SELECT id, parent_id, name FROM categories WHERE parent_id = 42
    UNION ALL
    SELECT c.id, c.parent_id, c.name FROM categories c
    JOIN descendants d ON c.parent_id = d.id
)
SELECT * FROM descendants;
```

## Helper

```python
from dorm.tree import descendants

rows = descendants(Category, parent_field="parent_id", root_pk=42)
# [{"pk": 100, "parent_id": 42}, {"pk": 101, "parent_id": 42}, ...]
```

`descendants()` runs the CTE and returns rows as dicts. Each dict
has `pk` and `parent_id`. For more columns, build the CTE
manually.

`ancestors()` is the inverse:

```python
from dorm.tree import ancestors

# Path from category 999 to the root (root excluded).
rows = ancestors(Category, parent_field="parent_id", leaf_pk=999)
```

## Building the CTE manually

To compose with a regular queryset:

```python
from dorm.tree import descendants_cte

cte = descendants_cte(
    Category,
    parent_field="parent_id",
    root_pk=42,
    fields=["id", "parent_id", "name"],   # columns to project
    cte_name="subtree",                    # name inside WITH
)

# Compose with with_cte() — load rows as Category instances:
qs = (
    Category.objects
    .with_cte(subtree=cte)
    .raw('SELECT * FROM "subtree" WHERE name LIKE %s', ["Books%"])
)
for cat in qs:
    print(cat.name)
```

## Cycle detection (PG)

If your graph can have cycles (rare in trees, common in general
graphs) use `cycle_field`:

```python
cte = descendants_cte(
    Category,
    parent_field="parent_id",
    root_pk=42,
    cycle_field="path",
)
```

PG-only — uses `ARRAY[id]` to accumulate the path and a boolean
`is_cycle` flag that becomes `TRUE` when a row was already
visited. Recursion stops automatically.

SQLite has no array literals; for cycle detection on SQLite, emit
a custom CTE with a depth counter and a `WHERE depth < N`.

## Caveats

- **Unbounded depth by default**: if your tree is millions of
  levels deep (unlikely), the CTE consumes server-side memory
  proportionally. Add a `WHERE depth < N` in a custom CTE.
- **`UNION ALL` doesn't dedupe**: on non-tree graphs, the same
  row can appear N times (one per path leading to it). Use
  `cycle_field` on PG; on other backends, manually accumulate a
  `path` field of concatenated pks.
- **`fields=` must be valid**: the op validates identifiers.
  Injection isn't possible through this API.

## Use case: comment tree

```python
class Comment(dorm.Model):
    body = dorm.TextField()
    parent_id = dorm.IntegerField(null=True, db_index=True)
    article_id = dorm.IntegerField(db_index=True)

# Full thread under root comment 555:
ids = [r["pk"] for r in descendants(
    Comment, parent_field="parent_id", root_pk=555
)]
thread = list(Comment.objects.filter(pk__in=ids).order_by("created_at"))
```

## Use case: breadcrumb path

```python
# From the leaf category up to the root, ordered leaf-to-root:
breadcrumbs_ids = [r["pk"] for r in ancestors(
    Category, parent_field="parent_id", leaf_pk=current.pk
)]
# Fetch names in one query:
crumbs_by_id = {
    c.pk: c for c in Category.objects.filter(pk__in=breadcrumbs_ids)
}
crumbs = [crumbs_by_id[pk] for pk in breadcrumbs_ids]
```

## More

- [Querying](queries.md) — `with_cte()` and the `CTE` literal
- [API: tree](api/tree.md)
