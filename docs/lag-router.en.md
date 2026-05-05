# Lag-aware read routing

`dorm.contrib.lag_router.LagAwareReadRouter` (3.4+) consults
`pg_last_xact_replay_timestamp()` and deflects reads to the
primary when a replica is too lagged. Key difference: dorm
doesn't just round-robin between replicas — each replica's
health is evaluated before each batch of reads.

PostgreSQL-only.

## The problem

```
[primary] --- WAL stream ---> [replica_1]   lag: 0.4s ✓
                              [replica_2]   lag: 12.0s ✗
```

If your app sends reads round-robin without a health check,
`replica_2` returns data *12 seconds in the past*. For balance
queries, payment status, etc., that's a correctness bug.

## Configure

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASES = {
    "primary": {...},
    "replica_1": {...},
    "replica_2": {...},
}

DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["replica_1", "replica_2"],
        max_lag_seconds=2.0,    # deflection threshold
        cache_seconds=5.0,      # how long we cache the lag reading
    ),
]
```

`max_lag_seconds=2.0` means: replicas with lag > 2s are
avoided; reads go straight to the primary. The 5s cache
amortises the check cost (probing
`pg_last_xact_replay_timestamp()` per query would saturate PG).

## Behaviour

```python
reads = Order.objects.filter(...)
# 1. Router asks each replica for its lag (if cache expired).
# 2. If replica_1 has 0.4s lag → route there.
# 3. If every replica is over the threshold → log WARNING + primary.

writes = Order.objects.create(...)
# Always primary.
```

## Inspection

```python
router = DATABASE_ROUTERS[0]
print(router.snapshot())
# {
#   "replica_1": {"lag_seconds": 0.4, "healthy": True, "checked_at": 12345.6},
#   "replica_2": {"lag_seconds": 12.0, "healthy": False, "checked_at": 12345.6},
# }
```

Useful for Prometheus exporters / dashboards.

## Caveats

- **`pg_last_xact_replay_timestamp()` can be NULL** on idle
  replicas (no replay pending). The router treats it as "0s
  lag" — fully caught up by definition.
- **Queries with `using("replica_1")` skip the router** —
  literal alias forced. Useful for occasional overrides.
- **Cache is per-process**: each worker maintains its own
  state. For cross-worker coordination, layer Redis on top.
- **Primary failover**: when the primary changes, the router
  doesn't know — use an additional layer (HAProxy, Patroni)
  for infra-level failover.

## Recipe: FastAPI

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["replica_eu", "replica_us"],
        max_lag_seconds=2.0,
    ),
]

@app.get("/orders/{pk}")
async def get_order(pk: int):
    order = await Order.objects.aget(pk=pk)   # auto-routed
    return order

# Special case: right after a write, read from primary
@app.post("/orders")
async def create_order(body):
    order = await Order.objects.using("primary").acreate(...)
    fresh = await Order.objects.using("primary").aget(pk=order.pk)
    return fresh
```

## Versus alternatives

| Pattern | When |
|---|---|
| **Lag-aware router** (this) | Classic read-replica with a safety net |
| Plain read replicas (no lag check) | When stale tolerance is high (analytics) |
| Sticky read-after-write window (3.0+) | Single primary, avoid replica for recent reads |
| Manual `using("primary")` | Per-endpoint override |

## More

- [Helpers](helpers.md#lag-aware-read-router)
- [Production: replicas](production.md#read-replicas)
- API: `dorm.contrib.lag_router`
