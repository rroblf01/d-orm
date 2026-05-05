# Bulk COPY (PostgreSQL)

`dorm.contrib.bulk_copy` (3.4+) uses PostgreSQL's `COPY` protocol
to ingest tens/hundreds of thousands of rows **10-100× faster**
than `bulk_create`. Essential for ETL, seeding, replication and
large data migrations.

PostgreSQL-only. Other backends raise `NotImplementedError` on
purpose (see "design" below).

## Quick API

```python
from dorm.contrib.bulk_copy import (
    bulk_copy_from, abulk_copy_from,
    copy_to, acopy_to,
)

# INGEST — model instances
n = bulk_copy_from(
    Author,
    [Author(name=f"a-{i}", age=i) for i in range(50_000)],
)
print(f"inserted {n} rows")

# INGEST — dicts
bulk_copy_from(
    Author,
    [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 40}],
    columns=["name", "age"],
)

# INGEST — async from a generator
async def feed():
    for line in open("data.csv"):
        name, age = line.strip().split(",")
        yield {"name": name, "age": int(age)}

await abulk_copy_from(Author, feed(), columns=["name", "age"])

# EXPORT — yield rows
for row in copy_to('SELECT id, name FROM "authors"'):
    process(row)

# EXPORT async
async for row in acopy_to('SELECT id, name FROM "authors"'):
    process(row)
```

## When to use

- Nightly ETL moving millions of rows.
- Seeding large fixtures for tests / dev.
- Cross-instance data replication.
- Importing CSV / Parquet into dorm tables.

## When NOT to use

- Small inserts (<1000 rows) — COPY's overhead doesn't pay vs
  `bulk_create`.
- Data with FK dependencies that need pre-insert validation
  (COPY skips signals).
- When you need `pre_save` / `post_save` signals to fire — COPY
  bypasses every signal for performance.

## Modes: text vs binary

```python
bulk_copy_from(Author, objs)               # text (default)
bulk_copy_from(Author, objs, binary=True)  # binary
```

- **Text**: the client converts each value to string, PostgreSQL
  parses. More forgiving on Python types (`None` → NULL,
  `datetime` → ISO 8601 string).
- **Binary**: the client sends bytes in the exact format PG
  expects. ~2× faster but **type-strict** — an `int` when PG
  expects `bigint` fails. Reserve for when types are guaranteed.

## Design: why no fallback to `bulk_create`

If a user calls `bulk_copy_from(Author, ...)` on SQLite we raise:

```
NotImplementedError: bulk_copy_from() is PostgreSQL-only — the COPY
protocol has no portable equivalent on other backends.
```

No silent fallback to `bulk_create`. Reason: if you called
`bulk_copy_from` it's because performance matters. Falling back to
`bulk_create` when you swap `ENGINE` gives you a hidden bottleneck
without warning. Better to fail fast and clearly.

## Pitfalls

- **Doesn't reuse the autocommit pool connection** to avoid long
  blocks. Uses a dedicated pool checkout.
- **Bypasses signals**: `pre_save` / `post_save` do NOT fire.
  If your audit logic depends on them, use `bulk_create`.
- **No auto-PK fill**: COPY doesn't return generated PKs. If you
  need them, follow up with `SELECT pk FROM ... WHERE
  unique_field IN (...)`.
- **Constraint violations**: any row that violates a constraint
  aborts the entire COPY. Pre-filter or use a staging table +
  `INSERT ... ON CONFLICT DO NOTHING` afterwards.

## More

- [Advanced](advanced.md#copy-frompostgresql)
- API: `dorm.contrib.bulk_copy`
