# DuckDB backend

DuckDB is an embedded analytical database — single-process, no
server, columnar storage, vectorised execution engine. The same
mental model as SQLite (one file, no daemons) with PostgreSQL-style
SQL and OLAP performance.

Added in **4.0**.

## When to use it

- **Dashboards**: fast aggregations on medium-sized tables
  (~10⁹ rows) without standing up Postgres.
- **Local ETL / staging**: ingest CSV/Parquet, transform, export.
  Faster than pandas for large joins.
- **ML feature stores**: iterative training with repeated queries
  over the same dataset.
- **CI tests**: like SQLite but with PG-flavoured SQL (window
  functions, CTEs, lateral joins).

## When NOT to use it

- **Serious OLTP**: DuckDB isn't built for concurrent writes. No
  SAVEPOINT, no replication, no connection pool; one writer at a
  time.
- **Multi-process**: each process opens its own connection to the
  same file, but isolation between concurrent writers isn't robust.
- **Public APIs**: typical request/response with many writes and
  small reads — PostgreSQL wins.

## Install

```bash
pip install 'djanorm[duckdb]'
```

Brings in the `duckdb` Python client (engine bundled in the wheel).
No system packages.

## Configure

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "duckdb",
        "NAME": "analytics.duckdb",   # ":memory:" for volatile in-process
    }
}
INSTALLED_APPS = ["dashboards"]
```

`ENGINE` resolves to
`dorm.db.backends.duckdb.DuckDBDatabaseWrapper`.

## Capabilities

- **Full CRUD**: `Model.objects.create`, `filter`, `bulk_create`,
  `delete`, etc.
- **Migrations**: `dorm makemigrations` / `dorm migrate` work the
  same way as SQLite.
- **Streaming**: `qs.iterator(chunk_size=N)` uses DuckDB's
  `cursor.fetchmany(N)`.
- **Atomic transactions**: `with transaction.atomic():` wraps
  `BEGIN`/`COMMIT`. Caveat below.
- **Async wrapper**: `await Model.objects.acreate(...)` routes to
  a thread executor (DuckDB is sync internally).
- **`information_schema`**: `dorm diff` works unchanged.
- **`__search`**: runs `LIKE`/`ILIKE` (DuckDB has no `tsvector`).
  For full-text, use pattern matching or pull the workload to
  another backend.

## Limitations to know

### No `SAVEPOINT`

DuckDB doesn't support savepoints. Nested `atomic()` blocks
degrade to a no-op boundary — the outer rollback discards
everything:

```python
with transaction.atomic():           # BEGIN
    Author.objects.create(name="x")
    try:
        with transaction.atomic():   # nested → no-op
            Author.objects.create(name="bad")
            raise RuntimeError       # rolls back all
    except RuntimeError:
        pass
# final state: Author count = 0 (everything reverted)
```

Equivalent to a try/except where the "all or nothing" pattern
holds. If you need real savepoints, switch to PostgreSQL/SQLite.

### `RETURNING` requires the right pk alias

DuckDB accepts `RETURNING <pk_col>` on INSERT, but the column
must be the declared PK, not the auto `id` alias. dorm handles
this internally using the configured `pk_col`.

### Async = thread executor

DuckDB has no native async API. `DuckDBAsyncDatabaseWrapper`
delegates every call to `asyncio.to_thread`. It works but isn't
"async-native" — for real concurrency (event-loop with thousands
of simultaneous connections) use libsql or PG.

DuckDB connections are **per-thread**: with a file-on-disk DB
that's fine (each thread opens its own handle to the same file);
with `:memory:` each thread would get its own in-memory DB, so
avoid `:memory:` in async code.

## Recipe: quick dashboard

```python
import dorm

dorm.configure(
    DATABASES={"default": {"ENGINE": "duckdb", "NAME": "analytics.duckdb"}},
    INSTALLED_APPS=["dash"],
)

class PageView(dorm.Model):
    path = dorm.CharField(max_length=200)
    user_id = dorm.IntegerField()
    ts = dorm.DateTimeField()

# Top 10 paths by unique visitors (PG-style SQL — DuckDB digests it)
from dorm import Count

top = (
    PageView.objects
    .values("path")
    .annotate(uniques=Count("user_id", distinct=True))
    .order_by("-uniques")[:10]
)
```

## Recipe: read Parquet directly

DuckDB can read Parquet/CSV without importing — useful for
staging without migrating to dorm tables. Bypass the ORM with
raw SQL:

```python
from dorm.db.connection import get_connection

conn = get_connection()
rows = conn.execute(
    "SELECT region, COUNT(*) AS n "
    "FROM 'sales_2026.parquet' "
    "GROUP BY region ORDER BY n DESC"
)
for r in rows:
    print(r["region"], r["n"])
```

## Migrating SQLite → DuckDB

The same migrations work (DuckDB accepts SQLite-like syntax).
Change `ENGINE` and re-apply:

```bash
# settings with ENGINE=sqlite → switch to ENGINE=duckdb
dorm migrate
```

Watch out: types like `BOOLEAN` and `TIMESTAMP` map automatically;
`BLOB`/`TEXT` too. But if you have `PRAGMA`-specific logic (typical
SQLite), audit it.

## More

- [What's new in 4.0](v4_0.md#6-duckdb-backend) — release overview
- [When to use what](when-to-use-what.md) — DuckDB vs PostgreSQL vs SQLite
- [Migrations](migrations.md) — every op works
