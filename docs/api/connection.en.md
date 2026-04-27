# Connection management & health

::: dorm.db.connection.health_check
::: dorm.db.connection.ahealth_check
::: dorm.db.connection.pool_stats
::: dorm.db.connection.get_connection
::: dorm.db.connection.get_async_connection
::: dorm.db.connection.close_all
::: dorm.db.connection.close_all_async
::: dorm.db.connection.router_db_for_read
::: dorm.db.connection.router_db_for_write
::: dorm.db.utils.with_transient_retry
::: dorm.db.utils.awith_transient_retry

## `health_check(deep=True)` — combined readiness + observability

Both `health_check` and `ahealth_check` accept a `deep=True` flag that
adds the live pool snapshot under the `pool` key — handy when the
same `/healthz` endpoint must serve both readiness probes and
observability scrapers:

```python
import dorm

@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check(deep=True)
# {
#   "status": "ok", "alias": "default", "elapsed_ms": 0.42,
#   "pool": {
#     "alias": "default", "vendor": "postgresql", "has_pool": True,
#     "pool_min": 1, "pool_max": 10,
#     "pool_size": 7, "pool_available": 4, "requests_waiting": 0,
#     "requests_num": 18234, "usage_ms": 412.3, "connections_ms": 1.1,
#   }
# }
```

`pool_stats(alias)` is also exposed standalone for Prometheus / OTel
exporters that want only the pool view:

```python
from dorm import pool_stats
metrics.gauge("db.pool.in_use", pool_stats("default")["pool_size"])
```

`pool_stats` never raises — for a never-used alias it returns
`{"alias": ..., "status": "uninitialised"}`, so calling it from a
healthz handler before the first query is safe.
