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
