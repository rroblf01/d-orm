# Hash-based sharding

When a dataset grows past what fits on a single DB, the pattern
is **horizontal sharding**: split rows across N physical servers
by a *shard key* (typically tenant_id, user_id, org_id).

`dorm.contrib.sharding` (3.4+).

## When to use

- Main table is past the TB; vertical scaling exhausted.
- Tenants distributed geographically (US-east, EU-west).
- Write-heavy load saturating a single primary.

## When NOT to

- <100GB per table and <5000 QPS — vertical scaling is much
  simpler.
- If your queries do frequent cross-shard JOINs — sharding breaks
  those. Reshape your data model first.
- No clear partitioning key (queries fan out across arbitrary
  subsets).

## API

```python
from dorm.contrib.sharding import (
    HashShardRouter, with_shard_key, shard_for, for_each_shard,
)

# settings.py
DATABASES = {
    "default": {...},
    "shard_0": {...},
    "shard_1": {...},
    "shard_2": {...},
    "shard_3": {...},
}
DATABASE_ROUTERS = [
    HashShardRouter(num_shards=4, shard_models={Order, Customer}),
]
```

```python
# Request handler
from dorm.contrib.sharding import with_shard_key

@app.post("/orders")
async def create_order(request, body):
    with with_shard_key(request.user.tenant_id):
        order = await Order.objects.acreate(...)   # routed to shard_N
    return order
```

## Deterministic hash

`shard_for(key, num_shards)` uses `hashlib.blake2b` with a
configurable salt, **NOT** Python's built-in `hash()` (which is
randomised per-process since Python 3.3 — that would put the same
row on different shards in different workers).

```python
from dorm.contrib.sharding import shard_for

assert shard_for("user-42", 4) == shard_for("user-42", 4)   # deterministic
# Some callers prefer their own salt for security:
shard_for("user-42", 4, salt=b"my-secret-salt")
```

## `for_each_shard` — fan-out

For global queries (total count, batch jobs per shard):

```python
from dorm.contrib.sharding import for_each_shard

results = for_each_shard(
    lambda alias: Order.objects.using(alias).count(),
    num_shards=4,
)
# {"shard_0": 1234, "shard_1": 1209, ...}

total = sum(results.values())
```

Sequential; wrap in `asyncio.gather` or threads for parallelism
if needed.

## Compose with row-level multi-tenancy

`HashShardRouter` + `TenantModel` compose elegantly — the shard
key is usually **the** tenant id:

```python
with current_tenant(tenant_id), with_shard_key(tenant_id):
    Note.objects.create(title="hi")
    # → tenant_id auto-filled + routed to the right shard
```

## No active shard key

If your model is sharded and no `with_shard_key()` is active:

```
RuntimeError: HashShardRouter: no active shard key for sharded model 'Order'
```

By design. Silent fallback to `default` would scatter rows
inconsistently across shards.

## Rebalancing (shard splits)

dorm does **not** rebalance automatically. If you go 4 → 8
shards:

1. Create the new shards (empty).
2. Switch `num_shards=8` in production — new rows go to the new
   distribution.
3. For each old shard, migrate rows to their new destination:
   ```python
   for row in OldShard.objects.using("shard_0").all():
       new_alias = shard_for(row.tenant_id, 8)
       row.save(using=new_alias)
       row.delete(using="shard_0")
   ```
4. Whether to pause traffic during migration is an ops / business
   call.

To avoid this pain, **consistent hashing** (a ring) instead of
modulo. dorm doesn't ship that out of the box; consider Citus or
Vitess if you need it.

## Pitfalls

- **Cross-shard JOINs impossible** — each shard is an independent
  DB. Model your data before sharding.
- **`allow_relation` rejects cross-shard FKs**: the router
  returns `False` when obj1 / obj2 live on different aliases.
  Catches bugs in code before runtime.
- **Migrations**: `dorm migrate` only runs on `default` by
  default. To run on every shard:
  ```bash
  for alias in shard_0 shard_1 shard_2 shard_3; do
    dorm migrate --database $alias
  done
  ```

## More

- [Helpers](helpers.md#hash-based-horizontal-sharding)
- [Row-level multi-tenancy](tenants-row.md) — natural pairing
- API: `dorm.contrib.sharding`
