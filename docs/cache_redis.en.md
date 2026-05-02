# Result cache (Redis)

`djanorm` ships an opt-in result-cache layer for hot querysets.
The default backend is Redis, but the contract is pluggable —
any class that implements `dorm.cache.BaseCache` is a valid
backend.

The Redis client is **optional**:

```bash
pip install 'djanorm[redis]'
```

`djanorm` itself imports without `redis-py`. The helpful error
surfaces only when you actually instantiate the cache backend.

## Security — payloads are HMAC-signed

!!! warning "Trust boundary"
    Cached payloads are deserialised with `pickle.loads`, which
    executes `__reduce__` on whatever bytes come back from the
    backend. A Redis instance writeable by an attacker
    (multi-tenant cluster, leaky ACL, no-auth deployment)
    would let that attacker inject a malicious blob and
    trigger arbitrary code execution at queryset materialisation
    time.

`dorm.cache` therefore signs every payload with HMAC-SHA256
before it leaves the process and verifies the signature on the
way back in. Unsigned / tampered / truncated blobs are dropped
silently; the queryset falls through to the database as if
the entry didn't exist.

The signing key reads from these settings, in priority order:

1. `CACHE_SIGNING_KEY` — recommended explicit setting.
2. `SECRET_KEY` — Django convention; reused if present.
3. A per-process random key — entries don't survive a process
   restart (signed with the old key won't verify against the
   new one), but the cache stays unforgeable. A one-time warning
   logs to the `dorm.cache` logger so the operator knows the
   cache isn't shared across workers.

```python
dorm.configure(
    DATABASES={"default": {...}},
    CACHES={"default": {"BACKEND": "dorm.cache.redis.RedisCache", ...}},
    CACHE_SIGNING_KEY=os.environ["DORM_CACHE_KEY"],  # 32+ random bytes
)
```

To disable signing (only for migrating an unsigned legacy cache
on a private trusted network), set
`CACHE_INSECURE_PICKLE = True`. Don't.

### Multi-worker production

In a multi-worker deployment (gunicorn, uvicorn `--workers >1`,
multi-process ASGI servers) every worker that falls back to
the per-process random key generates its OWN key. Payloads
written by one worker can't be verified by another → cache
hit-rate collapses to per-worker visibility, silently. To
catch this misconfiguration loudly, set:

```python
dorm.configure(
    ...,
    CACHE_REQUIRE_SIGNING_KEY=True,
)
```

The first cache use in a worker without an explicit
`CACHE_SIGNING_KEY` (or `SECRET_KEY`) will then raise
`ImproperlyConfigured` with a clear pointer at the misconfig.
Recommended for any production-shaped deployment.

## Configuration

```python
import dorm

dorm.configure(
    DATABASES={"default": {...}},
    CACHES={
        "default": {
            "BACKEND": "dorm.cache.redis.RedisCache",
            "LOCATION": "redis://localhost:6379/0",
            "OPTIONS": {"socket_timeout": 1.0},
            # default TTL in seconds for ``qs.cache()`` calls
            # that don't pass ``timeout=``.
            "TTL": 300,
        },
    },
    CACHE_SIGNING_KEY=os.environ["DORM_CACHE_KEY"],
)
```

`LOCATION` accepts every URL form `redis-py` understands:

- `redis://host:port/db` — TCP, no TLS.
- `rediss://host:port/db` — TCP + TLS.
- `unix:///path/to/redis.sock` — Unix socket.

`OPTIONS` is forwarded to `Redis.from_url(...)`. Common keys:
`socket_timeout`, `socket_connect_timeout`, `health_check_interval`,
`retry_on_timeout`, `password`.

## Caching a queryset

Chain `.cache(timeout=…)` onto any queryset:

```python
# 30-second cache.
hot_books = Book.objects.filter(featured=True).cache(timeout=30)

for b in hot_books:
    print(b.title)
```

The first iteration runs the query and stores the materialised
rows under a SHA-1 key derived from the model name + final SQL +
bound parameters. Subsequent iterations within `timeout`
seconds hydrate model instances from the cached bytes — no DB
round-trip.

`timeout=None` falls back to the backend's `TTL` setting.
`timeout=0` caches indefinitely (until invalidated by a write).

### Async

`await qs.cache(...)` works the same way:

```python
hot_books = await Book.objects.filter(featured=True).cache(timeout=30)
```

The async path uses `redis.asyncio.Redis` under the hood; the
sync and async clients have separate connection pools but share
the same cache keys, so a sync writer and an async reader see
the same view.

## Auto-invalidation

Every `Model.save()` / `Model.delete()` (and the matching async
variants) fires the `post_save` / `post_delete` signal. The
cache layer hooks into both signals on first `qs.cache()` call
and runs:

```python
backend.delete_pattern(f"dormqs:{app_label}.{ModelName}:*")
```

So a writer never observes a stale cached read. The eviction
is **coarse-grained**: a single save invalidates *every*
cached queryset for the model, including ones that wouldn't
have matched the new row. For typical apps this is fine; if
you cache a hot list page that rebuilds on every write, prefer
a smaller TTL or a manual key scheme.

Cross-model writes (e.g. saving an `Author` while a queryset
on `Book` is cached) are **not** auto-invalidated — only the
saved model's namespace is dropped. Use FK-aware invalidation
in your application layer (or a shorter TTL) when you cache
joined queries.

### Stale-read race protection

The naïve "read → fetch → store" flow has a subtle race: a
writer that invalidates a key BETWEEN a reader's fetch and
store steps would leave the reader's stale rows cached for one
TTL window. `dorm.cache` closes that window with a per-model
in-memory version counter. Every `post_save` / `post_delete`
bumps the counter; the cache key includes `:vN:`; the store
step re-reads the version after the DB fetch and lands the
bytes under the (possibly bumped) key. A racing writer's bump
points later readers at a key the racing reader never wrote.

The counter is process-local. Cross-process invalidation still
goes through `delete_pattern`. Helpers exposed on
`dorm.cache`:

- `model_cache_version(model)` → current counter value.
- `bump_model_cache_version(model)` → atomic increment;
  returns the new value. Called by the signal handler before
  it issues `delete_pattern`.

## Known gaps and edge cases

A few scenarios are intentionally NOT handled — flag them at
review time so you don't trip over them in production:

### Multi-process version-counter drift

The per-model version counter is **process-local**. Workers
carry independent counters, so a save in worker A doesn't bump
worker B's counter. Cross-process invalidation still works
because both ends share the same Redis namespace and
``delete_pattern`` wipes every version-prefixed key. The
practical consequences:

- After a save, the writer's `:vN+1:` key is the next
  consumer in the same worker; other workers keep using
  `:vN:` until their own next write or read.
- Stale `:v0:`, `:v1:`, … entries can accumulate in Redis
  between writes; the next ``delete_pattern`` from any worker
  cleans them. Set a sensible TTL (default 300 s) so
  long-cold keys don't pile up.

If you need cross-process version coherence (rare — the
``delete_pattern`` mechanism normally suffices), implement a
custom backend whose ``model_cache_version`` reads / writes a
shared atomic counter (Redis ``INCR``).

### Multi-table inheritance

Saving a child instance fires ``post_save`` for the child
class; querysets cached on the **parent** model use the
parent's namespace and are NOT invalidated. Avoid caching
queries on a parent of a multi-table inheritance hierarchy if
the children change frequently.

### `count()` / `exists()` / `aggregate()` are not cached

The cache hook lives in ``QuerySet._fetch_all`` (the path
``__iter__`` / ``await qs`` use). ``count()``, ``exists()``,
``aggregate()`` and the explain helpers issue their own SQL
and bypass the cache entirely. To cache a row count, cache the
materialised list (``len(qs)`` after ``.cache(...)``), or
manage a separate counter via ``set`` / ``get`` on the cache
backend directly.

### M2M relation mutations

``manager.add(...)`` / ``set(...)`` / ``clear(...)`` on a
``ManyToManyField`` write through the junction table; they do
NOT fire ``post_save`` on the parent. Cached querysets that
filter on the M2M relation stay populated until the next save
on the parent or until the TTL expires. Wrap M2M mutations in
an explicit ``Model.save()`` call when consistency matters.

### `_cache_key` fallback when params are unpicklable

The key digest pickles bind parameters; if a parameter type
can't be pickled (custom expression, lambda) the wrapper
falls back to ``repr(params)``. Distinct unpicklable values
sharing the same ``repr`` would collide on the cache key —
edge case (you'd need a deliberately misleading ``__repr__``)
but worth knowing about.

## Cache outages don't break queries

The Redis backend wraps every operation in a broad `try /
except`. A connection error, a timeout, or a `WRONGTYPE`
response causes the cache miss path to take over: the queryset
runs against the database as if no cache was configured. This
is intentional — caching is best-effort, and a cache outage
must never take down a request.

You'll see standard Redis client warnings in your logs, but
the request itself succeeds.

## Backend protocol

Implement your own backend by subclassing
`dorm.cache.BaseCache`:

```python
from dorm.cache import BaseCache


class MyCache(BaseCache):
    def get(self, key): ...
    def set(self, key, value, timeout=None): ...
    def delete(self, key): ...
    def delete_pattern(self, pattern): ...
    async def aget(self, key): ...
    async def aset(self, key, value, timeout=None): ...
    async def adelete(self, key): ...
    async def adelete_pattern(self, pattern): ...
```

Then register the dotted path in `CACHES.BACKEND`:

```python
CACHES = {
    "default": {
        "BACKEND": "myapp.cache.MyCache",
        "LOCATION": "...",
    },
}
```

## When to cache

- **Reference data** — countries, currencies, feature flags.
  Read-mostly, small, expensive to look up across services.
- **Listing pages** — homepage, search results, leaderboards.
  Read-heavy, written by background jobs.
- **Foreign-key lookups** — chained `.select_related(...)` that
  return the same row repeatedly under a single request can
  benefit from a 10-second cache.

When **not** to cache:

- **User-specific reads** that vary every request — the cache
  hit rate stays near 0 % and you pay the serialisation cost
  for nothing.
- **Strongly-consistent counters** — auto-invalidation is
  coarse, so a fast-write counter would invalidate constantly
  and hammer the cache.

## In-process LRU: `LocMemCache` (3.0+)

For tests, single-process scripts, or as a layer in front of Redis,
use the bundled in-process LRU instead of pulling in `redis-py`:

```python
CACHES = {
    "default": {
        "BACKEND": "dorm.cache.locmem.LocMemCache",
        "OPTIONS": {"maxsize": 1024},  # entries; LRU eviction beyond
        "TTL": 300,
    }
}
```

Same contract as `RedisCache` — sync + async helpers, `delete_pattern`
for signal-driven invalidation. NOT shared across worker processes:
each gunicorn / uvicorn worker holds its own dict.

## Row-cache: `Manager.cache_get(pk=…)` (3.0+)

Single-row lookup that goes through the cache before hitting the DB.
Uses the same per-model invalidation version as `QuerySet.cache(...)`,
so a `post_save` from any path invalidates both:

```python
user = User.objects.cache_get(pk=42, timeout=60)
# Async parity:
user = await User.objects.acache_get(pk=42)
```

Cache misses fall through silently. Cache outages also fall
through — the row from the database is the source of truth.

### Batch row-cache: `cache_get_many(pks=[...])` (3.0+)

Fetch many rows by primary key in a single round-trip. Hits go
through the cache; misses are batched into one ``WHERE pk IN (...)``
query and written back to the cache afterwards:

```python
users = User.objects.cache_get_many(pks=[1, 2, 3, 4])
# Returns {1: <User>, 2: <User>, 3: <User>}
# (pk=4 absent if not in the DB)

# Async parity:
users = await User.objects.acache_get_many(pks=[1, 2, 3, 4])
```

PKs not found in the database are simply absent from the returned
dict. Pair with `select_related` on a follow-up query if you need
the FKs eager-loaded — the cache stores the row exactly as
``Manager.get(pk=…)`` returned it.
