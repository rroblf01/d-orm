# `dorm.cache`

Pluggable result-cache layer for queryset + single-row reads.
HMAC-SHA256 signed payloads on top of pickle so a writeable Redis
isn't an RCE vector; per-model invalidation version closes the
classic read-then-write race.

## Backends

::: dorm.cache.BaseCache
::: dorm.cache.redis.RedisCache
::: dorm.cache.locmem.LocMemCache

`RedisCache` is the production default (multi-worker, multi-host).
`LocMemCache` is the in-process LRU — useful for tests, single-
process scripts, or as a cheap layer in front of Redis.

## Helpers

::: dorm.cache.get_cache
::: dorm.cache.reset_caches
::: dorm.cache.model_cache_namespace
::: dorm.cache.model_cache_version
::: dorm.cache.bump_model_cache_version
::: dorm.cache.sign_payload
::: dorm.cache.verify_payload

## QuerySet integration

`QuerySet.cache(timeout=…)` opts a single queryset into result
caching. `Manager.cache_get(pk=…)` / `cache_get_many(pks=[…])`
read individual rows through the cache before the DB.

```python
# Queryset cache — N+1 friendly read of a hot listing.
top = Article.objects.filter(published=True).order_by("-rank")[:20].cache(60)

# Row cache — hot single-instance reads.
user = User.objects.cache_get(pk=42, timeout=300)
users = User.objects.cache_get_many(pks=[1, 2, 3])
```

Cache miss / outage falls through silently — the row from the
database is the source of truth.

See [Result cache (Redis)](../cache_redis.md) for the full
configuration / invalidation story.
