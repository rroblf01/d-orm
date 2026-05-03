# `dorm.cache`

Capa de result-cache pluggable para lecturas queryset + single-row.
Payloads firmados con HMAC-SHA256 sobre pickle así un Redis
escribible no es vector RCE; versión de invalidación per-modelo
cierra la race clásica read-then-write.

## Backends

::: dorm.cache.BaseCache
::: dorm.cache.redis.RedisCache
::: dorm.cache.locmem.LocMemCache

`RedisCache` es el default producción (multi-worker, multi-host).
`LocMemCache` es el LRU en proceso — útil para tests, scripts
single-process o capa barata frente a Redis.

## Helpers

::: dorm.cache.get_cache
::: dorm.cache.reset_caches
::: dorm.cache.model_cache_namespace
::: dorm.cache.model_cache_version
::: dorm.cache.bump_model_cache_version
::: dorm.cache.sign_payload
::: dorm.cache.verify_payload

## Integración QuerySet

`QuerySet.cache(timeout=…)` opta una sola queryset a result cache.
`Manager.cache_get(pk=…)` / `cache_get_many(pks=[…])` leen filas
individuales por cache antes de la DB.

```python
# Cache de queryset — lectura N+1 friendly de listing caliente.
top = Article.objects.filter(published=True).order_by("-rank")[:20].cache(60)

# Cache de fila — lecturas hot single-instance.
user = User.objects.cache_get(pk=42, timeout=300)
users = User.objects.cache_get_many(pks=[1, 2, 3])
```

Miss de cache / caída cae silencioso a DB — la fila de la DB es
fuente de verdad.

Ver [Caché de resultados (Redis)](../cache_redis.md) para configuración
/ invalidación completa.
