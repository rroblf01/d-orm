# libsql backend

`djanorm` ships a `libsql` engine that talks to:

- **Local SQLite-compatible files** — drop-in replacement for the
  built-in `sqlite` backend. No server, no auth.
- **Self-hosted [`sqld`](https://github.com/tursodatabase/libsql)
  on a VPS** — the typical "I want a managed-ish SQLite that
  multiple processes share" pattern. Run `sqld` behind a
  reverse proxy with TLS, point djanorm at the HTTPS endpoint.
- **Embedded replica** — local file kept in sync with the
  remote master. Reads land on the local replica
  (sub-millisecond); writes round-trip to the master and
  replicate back.
- **Turso Cloud** — same wire protocol as self-hosted `sqld`,
  so the same configuration shape works against
  `https://<db>-<org>.turso.io`. Useful when you don't want to
  run the server yourself.

The libsql client is **optional**. Enable it with:

```bash
pip install 'djanorm[libsql]'
```

This pulls [`pyturso`](https://pypi.org/project/pyturso/) — the
official Turso Python SDK. Without it, `djanorm` itself imports
cleanly; only the moment you actually open a libsql connection
do you see a clear `ImproperlyConfigured` error pointing at the
install command.

## Configuration

Three knobs in your `DATABASES` config:

| Key | Meaning |
|-----|---------|
| `ENGINE` | `"libsql"` |
| `NAME` | Local file path. Defaults to `:memory:`. With `SYNC_URL` set, this becomes the embedded-replica file. |
| `SYNC_URL` | Remote endpoint URL — typically `https://libsql.your-vps.com` for a self-hosted `sqld` or `https://<db>-<org>.turso.io` for Turso Cloud. Setting it turns the connection into an embedded replica. |
| `AUTH_TOKEN` | Bearer token sent as `Authorization: Bearer <token>` on every sync round-trip. Optional for self-hosted `sqld` running on a private network; required by Turso Cloud and recommended for any internet-exposed `sqld`. |

### Local file

```python
import dorm

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "libsql",
            "NAME": "/var/app/data.db",
        },
    },
    INSTALLED_APPS=["myapp"],
)
```

Behaviour identical to the SQLite backend — same SQL dialect,
same `PRAGMA foreign_keys = ON`, same migration tooling.

### Self-hosted sqld on a VPS

The most common production layout. Spin up `sqld` on your VPS,
expose it through nginx / Caddy with HTTPS:

```bash
# On your VPS:
docker run -d \
    -p 8080:8080 \
    -v /var/lib/sqld:/var/lib/sqld \
    -e SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080 \
    -e SQLD_AUTH_JWT_KEY="$(cat /etc/sqld/jwt.pub)" \
    ghcr.io/tursodatabase/libsql-server:latest
```

Point djanorm at the HTTPS endpoint:

```python
import os

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "libsql",
            "NAME": "/var/app/local-replica.db",  # embedded replica
            "SYNC_URL": "https://libsql.your-vps.com",
            "AUTH_TOKEN": os.environ["LIBSQL_TOKEN"],
        },
    },
)
```

Reads come from `local-replica.db` (zero network round-trip);
writes flush to the VPS and replicate back. Force a pull from
the master (e.g. after a write made by another process) with:

```python
from dorm.db.connection import get_connection

get_connection().sync_replica()
```

If you want **remote-only** mode (no local replica, every read
hits the VPS), keep `NAME=":memory:"` — the embedded replica
becomes ephemeral but the wire protocol is the same.

```python
DATABASES = {
    "default": {
        "ENGINE": "libsql",
        "NAME": ":memory:",
        "SYNC_URL": "https://libsql.your-vps.com",
        "AUTH_TOKEN": os.environ["LIBSQL_TOKEN"],
    },
}
```

### URL form

`parse_database_url` understands `libsql://` and the explicit
scheme variants `libsql+http://`, `libsql+https://`,
`libsql+ws://`, `libsql+wss://`. Pass the URL straight from
`os.environ`:

```python
from dorm.conf import parse_database_url

cfg = parse_database_url(os.environ["DATABASE_URL"])
dorm.configure(DATABASES={"default": cfg})
```

```
libsql://libsql.your-vps.com?authToken=…&NAME=local-replica.db
libsql+https://libsql.your-vps.com?authToken=…
libsql:///relative/path.db        # three slashes → relative path
libsql:////var/data/abs.db        # four slashes  → absolute path
```

Slash count matters — three slashes mean a path relative to the
working directory, four slashes mean an absolute path. Same
convention as the `sqlite://` URLs you may already know.

### Turso Cloud (managed)

Same configuration shape; only the host changes:

```python
DATABASES = {
    "default": {
        "ENGINE": "libsql",
        "NAME": "/var/app/local-replica.db",
        "SYNC_URL": "libsql://your-db-your-org.turso.io",
        "AUTH_TOKEN": os.environ["TURSO_AUTH_TOKEN"],
    },
}
```

Generate the auth token via the Turso CLI
(`turso db tokens create your-db`) and stash it in your env.

## Async usage

`djanorm`'s async path works against libsql with two paths:

- **Local-only** (no `SYNC_URL`) — uses `turso.aio.connect` for
  native async I/O. Each cursor call is awaited directly with
  no thread bouncing.
- **Embedded replica / remote-only** (`SYNC_URL` set) — pyturso's
  async API is local-only today, so the wrapper falls back to
  the sync client running on a dedicated single-thread worker.
  Single thread matters: pyturso connections are NOT thread-safe,
  and the default `asyncio.to_thread` pool would fan calls out
  across multiple workers and produce native-code crashes.

Either way, the user-facing API is the same:

```python
async def list_books():
    return [b async for b in Book.objects.all()]
```

## Vector support

`VectorField` (from `dorm.contrib.pgvector`) recognises the
`libsql` vendor and emits libsql-native types and functions —
**no `sqlite-vec` extension is needed**. The wrapper opens
every connection with `experimental_features="vector"` so
`F32_BLOB(N)` columns and the `vector_distance_*` SQL
functions are available out of the box.

| Backend | Column type | Distance functions |
|---------|-------------|--------------------|
| PostgreSQL | `vector(N)` | `<->` / `<=>` / `<#>` |
| SQLite (sqlite-vec) | `BLOB` | `vec_distance_L2` / `vec_distance_cosine` |
| **libsql / pyturso** | **`F32_BLOB(N)`** | **`vector_distance_l2` / `vector_distance_cos`** |

Example — kNN over a self-hosted libsql with a hosted index:

```python
from dorm import F
from dorm.contrib.pgvector import VectorField, CosineDistance


class Doc(dorm.Model):
    title = dorm.CharField(max_length=200)
    embedding = VectorField(dimensions=384)


# Top-10 nearest neighbours by cosine distance.
nearest = (
    Doc.objects
       .annotate(score=CosineDistance("embedding", query_vector))
       .order_by("score")[:10]
)
```

The annotation compiles to:

```sql
SELECT …, vector_distance_cos("docs"."embedding", vector32(?)) AS "score"
FROM "docs"
ORDER BY "score" ASC
LIMIT 10
```

`MaxInnerProduct` is **not** supported on libsql today — fall
back to `CosineDistance` over L2-normalised embeddings.

## Migrations

The migration tooling is unchanged. `dorm makemigrations` /
`dorm migrate` generate the same operations they would for
SQLite; the rebuild recipe used by `AlterField` works against
libsql too.

For a vector column on libsql you don't need
`VectorExtension()` (that operation is sqlite-vec / pgvector
specific) — just declare the field:

```python
class Doc(dorm.Model):
    embedding = dorm.contrib.pgvector.VectorField(dimensions=1536)
```

The generated migration emits `F32_BLOB(1536)` and you're done.

## Limitations

- The async wrapper for **embedded-replica / remote** mode
  serialises onto a single worker thread (pyturso's async API
  is local-only). Throughput is fine for most apps; PostgreSQL
  with `psycopg.AsyncConnection` remains the recommended option
  for heavy fan-out async workloads.
- `MaxInnerProduct` is unimplemented on libsql; use
  `CosineDistance` on normalised vectors.
- `journal_mode` is a no-op for remote-mode sessions (the
  master controls journaling).
- pyturso connections are NOT thread-safe — keep the wrapper
  scoped to the thread / event loop that opened it. The async
  wrapper enforces this by pinning to a dedicated executor;
  the sync wrapper relies on the parent SQLite wrapper's
  thread-local connection cache.
