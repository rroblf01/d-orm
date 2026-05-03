# Vector search with djanorm

`dorm.contrib.pgvector` covers vector similarity search on **four**
backends — the same model + queryset code runs against any of them
because the field decides the wire format from the active
connection's vendor:

| Backend                 | Column type    | Distance functions                          |
|-------------------------|---------------|---------------------------------------------|
| PostgreSQL (pgvector)   | `vector(N)`   | `<->` / `<=>` / `<#>` operators             |
| SQLite (sqlite-vec)     | `BLOB`        | `vec_distance_L2` / `vec_distance_cosine`   |
| libsql / Turso (native) | `F32_BLOB(N)` | `vector_distance_l2` / `vector_distance_cos` |
| MariaDB 11.7+ / MySQL 9.0+ (3.0+) | `VECTOR(N)` | `VEC_DISTANCE_EUCLIDEAN` / `VEC_DISTANCE_COSINE` |

The module exposes:

- **`VectorField(dimensions=N)`** — the column type.
- **`L2Distance` / `CosineDistance` / `MaxInnerProduct`** —
  distance expressions that compose with `annotate()` + `order_by()`.
- **`HnswIndex` / `IvfflatIndex`** — index helpers (PostgreSQL
  only — other backends use different index models that aren't
  wrapped yet).
- **`VectorExtension`** — the migration operation that enables
  pgvector / sqlite-vec where needed; a no-op on libsql / MariaDB
  / MySQL because the engine ships vector functions natively.

> **Note on `MaxInnerProduct`** — pgvector ships it
> (operator `<#>`). sqlite-vec, libsql and MariaDB / MySQL don't:
> use `CosineDistance` over L2-normalised embeddings instead
> (mathematically equivalent up to a constant).
>
> **Note on the MySQL backend (3.0+)** — the Python wrapper for
> the MySQL / MariaDB engine is a scaffold today (raises
> `ImproperlyConfigured` until v3.1 ships the full implementation).
> ``VectorField`` and the distance expressions emit the right SQL
> already, so once the wrapper lands the same vector code keeps
> working without changes. The ``VECTOR`` row above is in the
> table now to pin the contract.

## Step-by-step (PostgreSQL)

### 1. Install pgvector on your PostgreSQL server

pgvector is shipped as a binary extension. On a Debian / Ubuntu
host running PostgreSQL 16:

```bash
sudo apt install postgresql-16-pgvector
```

For other distros / managed services see [the upstream README](https://github.com/pgvector/pgvector#installation-notes).
On AWS RDS / Aurora the extension is preinstalled — you only need
to enable it (step 3).

### 2. Install the Python extra

```bash
pip install 'djanorm[postgresql,pgvector]'
```

The `[pgvector]` extra is **PostgreSQL only** — it pulls the
`pgvector` Python package, which registers a psycopg adapter so
`list[float]` and `numpy.ndarray` values bind transparently.
Without it you can still use the field, you just lose the numpy
convenience.

If your project targets *both* PostgreSQL and SQLite (CI runs
SQLite, prod runs PG), install the convenience meta-extra
`[vector]` instead — it pulls `[pgvector]` *and* `[sqlite-vec]`
in one go:

```bash
pip install 'djanorm[postgresql,sqlite,vector]'
```

### 3. Generate the extension migration

```bash
dorm makemigrations --enable-pgvector myapp
```

That writes `myapp/migrations/0001_enable_pgvector.py`:

```python
from dorm.contrib.pgvector import VectorExtension

dependencies = []
operations = [VectorExtension()]
```

`VectorExtension` runs `CREATE EXTENSION IF NOT EXISTS "vector"`
on apply and `DROP EXTENSION IF EXISTS "vector"` on rollback.
On non-PostgreSQL backends the operation is a no-op so the same
migration applies cleanly under SQLite (your test runs keep
working).

### 4. Add a `VectorField` to your model

```python
import dorm
from dorm.contrib.pgvector import VectorField


class Document(dorm.Model):
    title = dorm.CharField(max_length=200)
    content = dorm.TextField()
    embedding = VectorField(dimensions=1536)   # OpenAI text-embedding-3-small

    class Meta:
        db_table = "documents"
```

`dimensions=` is mandatory and must match your embedding model. The
column is declared `vector(1536)` and pgvector rejects inserts whose
length differs — the field mirrors the check in Python so the
ValidationError fires with your stack frame, not deep inside libpq.

### 5. Run `makemigrations` + `migrate`

```bash
dorm makemigrations myapp
dorm migrate
```

The autodetector picks up the new column and emits an `AddField`
operation against the existing extension migration.

### 6. Insert and query

```python
import openai

resp = openai.embeddings.create(
    model="text-embedding-3-small",
    input="hello world",
)
emb = resp.data[0].embedding   # list[float] length 1536

doc = Document.objects.create(
    title="hello",
    content="hello world",
    embedding=emb,
)
```

To retrieve the *k* nearest neighbours, annotate with a distance
expression then order by it:

```python
from dorm.contrib.pgvector import L2Distance

query_emb = openai.embeddings.create(
    model="text-embedding-3-small",
    input="greetings",
).data[0].embedding

nearest = list(
    Document.objects
    .annotate(score=L2Distance("embedding", query_emb))
    .order_by("score")[:10]
)
for doc in nearest:
    print(doc.title, doc.score)   # type: ignore — runtime attribute
```

The three distance expressions correspond exactly to pgvector's
three operators:

| Class             | Operator | Meaning                                  |
|-------------------|----------|------------------------------------------|
| `L2Distance`      | `<->`    | Euclidean (L2). Smaller = more similar.  |
| `CosineDistance`  | `<=>`    | `1 - cosine_similarity`. Smaller = closer. |
| `MaxInnerProduct` | `<#>`    | Negated inner product (smaller = closer). |

### 7. Add an index — *required* for production-grade kNN

Without an index, every kNN query is a sequential scan. For more
than a few thousand rows that's seconds-per-request territory. Two
methods are available:

```python
from dorm.contrib.pgvector import HnswIndex, IvfflatIndex


class Document(dorm.Model):
    embedding = VectorField(dimensions=1536)

    class Meta:
        db_table = "documents"
        indexes = [
            HnswIndex(
                fields=["embedding"],
                name="doc_emb_hnsw",
                opclass="vector_l2_ops",
                m=16,
                ef_construction=64,
            ),
        ]
```

After adding this, run `dorm makemigrations` + `dorm migrate` to
emit the `CREATE INDEX … USING hnsw …` statement.

#### Picking an index method

| Method     | Build time | Recall    | Memory   | When to use                                 |
|------------|-----------:|----------:|---------:|---------------------------------------------|
| HNSW       | minutes    | excellent | high     | Default. Better recall, paid in disk + RAM. |
| IVFFlat    | seconds    | good      | low      | Tight memory, big tables, build-time critical. |

#### `opclass` matters

Pick the operator class that matches the distance you query with —
otherwise the planner can't use the index and silently falls back
to seq scan:

| Distance         | Opclass              |
|------------------|----------------------|
| `L2Distance`     | `vector_l2_ops`      |
| `CosineDistance` | `vector_cosine_ops`  |
| `MaxInnerProduct`| `vector_ip_ops`      |

#### Tuning at query time

Both methods expose recall-vs-latency knobs that live outside the
index definition (they're per-session GUCs):

```python
# HNSW: ef_search defaults to 40; raise for better recall.
get_connection().execute("SET hnsw.ef_search = 100")

# IVFFlat: probes defaults to 1; range is 1..lists.
get_connection().execute("SET ivfflat.probes = 10")
```

Set these at request entry (FastAPI dependency, Django middleware)
so every kNN query in the request honours the same target.

## Step-by-step (SQLite)

### 1. Install sqlite-vec

sqlite-vec is a client-side loadable extension — no server-side
installation required. The PyPI package bundles compiled binaries
for Linux / macOS / Windows:

```bash
pip install 'djanorm[sqlite,sqlite-vec]'
```

The `[sqlite-vec]` extra is **SQLite only** — it pulls just the
`sqlite-vec` package without pulling psycopg's `pgvector`
adapter. Use `[pgvector]` for the PostgreSQL side, or
`[vector]` for both at once if your project ships against
both backends.

### 2. Verify your Python build supports `enable_load_extension`

Most CPython distributions ship with `sqlite3` compiled against a
SQLite that allows loading external extensions. A few don't —
notably some Ubuntu / Debian system Pythons before Python 3.11.
Quick check:

```python
import sqlite3
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)   # AttributeError → unsupported build
```

If this raises, install Python from python.org / pyenv / uv —
those builds enable extension loading.

### 3. Generate the extension migration

Same command as PostgreSQL:

```bash
dorm makemigrations --enable-pgvector myapp
```

The generated migration calls `VectorExtension()`, which on
SQLite:

- Loads sqlite-vec into the migration's connection.
- Marks the wrapper so every *future* connection (re-opens, new
  threads) auto-loads the extension too.

The marker lives on the wrapper instance, not in the database,
so a process restart needs to hit the migration code path again —
either re-run the migration once at startup, or call
`load_sqlite_vec_extension(raw_sqlite3_conn)` from your app's
boot sequence.

### 4. Define the model exactly the same way

```python
import dorm
from dorm.contrib.pgvector import VectorField


class Document(dorm.Model):
    title = dorm.CharField(max_length=200)
    embedding = VectorField(dimensions=384)   # smaller for SQLite

    class Meta:
        db_table = "documents"
```

On SQLite, `db_type()` returns `BLOB`. The field packs values as
little-endian float32 bytes — that's what sqlite-vec stores
natively and the form `vec_distance_L2(col, ?)` accepts directly.

### 5. Query the same way

```python
from dorm.contrib.pgvector import L2Distance

nearest = list(
    Document.objects
    .annotate(score=L2Distance("embedding", query_emb))
    .order_by("score")[:10]
)
```

The expression detects the active backend at compile time and
emits either `embedding <-> %s::vector` (PG) or
`vec_distance_L2(embedding, %s)` (SQLite).

### Index support (SQLite)

sqlite-vec's index model is built on virtual tables (`vec0`),
which doesn't fit the regular-table workflow djanorm exposes
today. **Sequential scan with `vec_distance_L2` is fine up to a
few hundred thousand vectors** on commodity hardware; if you need
ANN at SQLite scale, drop down to `RunSQL` to create a `vec0`
virtual table mirroring the column. We may wrap that in a future
release once the sqlite-vec API stabilises.

## Common gotchas

* **Dimensions must match the model that produced the embedding.**
  OpenAI `text-embedding-3-small` is 1536, `…3-large` is 3072,
  `text-embedding-ada-002` is also 1536. A mismatch fires
  `ValidationError` with the offending size.
* **pgvector caps `vector` at 16000 dimensions.** For higher-dim
  vectors use `halfvec` (16-bit floats, 32k cap) or `sparsevec`
  in pgvector ≥ 0.7. Those types aren't yet wrapped by djanorm.
* **First HNSW build on a big table is slow.** Either build the
  index *after* bulk-loading rows, or accept a long migration
  window. IVFFlat is faster but plateaus lower on recall.
* **Don't mix opclasses across the same column.** One index per
  column per opclass is the rule.

## Reference

- [`VectorField`](api/pgvector.md#vectorfield)
- [`L2Distance` / `CosineDistance` / `MaxInnerProduct`](api/pgvector.md#distance-expressions)
- [`HnswIndex` / `IvfflatIndex`](api/pgvector.md#index-helpers)
- [`VectorExtension`](api/pgvector.md#vectorextension)
- pgvector upstream: <https://github.com/pgvector/pgvector>
