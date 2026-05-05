# Row-level multi-tenancy

dorm offers two flavours of multi-tenancy:

| Flavour | Module | Isolation | Cost |
|---|---|---|---|
| Schema-level (PG) | `dorm.contrib.tenants` | `search_path` per tenant | one migration per tenant |
| Row-level (any backend) | `dorm.contrib.tenants_row` | `tenant_id` column + auto filter | one migration total |

This page covers the second. Added in **4.0**.

## Quick trade-off

| If… | Use… |
|---|---|
| You have 5 large enterprise tenants | schema-level |
| You have 5000 small tenants | row-level |
| Compliance demands physical isolation | schema-level |
| Same schema is fine; isolation in code | row-level |
| Backend is MySQL/SQLite/DuckDB | row-level (schema-level is PG-only) |

## The contract

```python
import dorm
from dorm.contrib.tenants_row import TenantModel, current_tenant

class Note(TenantModel):
    title = dorm.CharField(max_length=200)
```

`TenantModel` adds:

- A `tenant_id` column (`CharField(max_length=64, db_index=True)`).
  Override in subclasses for `UUIDField` / `IntegerField` / etc.
- An `objects` manager that automatically filters every queryset
  by `tenant_id = <active tenant>`.
- An `unscoped` manager — escape hatch with no filter.
- `save()` / `asave()` auto-fill `tenant_id` from the active
  context.

## Activating a tenant

Wrap each handler / job in:

```python
from dorm.contrib.tenants_row import current_tenant

with current_tenant(request.user.tenant_id):
    Note.objects.create(title="hi")            # tenant_id auto-filled
    notes = list(Note.objects.all())           # auto-filtered
```

`current_tenant()` uses `contextvars` — per-task in asyncio,
per-thread in sync. No leak between requests.

Nested:

```python
with current_tenant("a"):
    with current_tenant("b"):
        # b active here
        ...
    # a active here
```

## No active tenant → error

Calling the manager outside `current_tenant(...)` raises:

```python
>>> list(Note.objects.all())
NoActiveTenantError: No active tenant — wrap the call in
`with current_tenant(<tenant_id>):` or use `Note.unscoped` for a
deliberate cross-tenant query.
```

By design. A silent fallback to "every tenant" would leak data
between customers.

## Escape hatch — `unscoped`

For reports, admin, cross-tenant dashboards:

```python
all_notes = list(Note.unscoped.all())
note_count_by_tenant = (
    Note.unscoped
    .values("tenant_id")
    .annotate(n=dorm.Count("id"))
)
```

`unscoped` is deliberately verbose — every use surfaces in code
review.

## FastAPI middleware

```python
from fastapi import Request
from dorm.contrib.tenants_row import current_tenant

@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    tenant = request.headers.get("X-Tenant-ID")
    if tenant is None:
        return JSONResponse({"detail": "missing tenant"}, status_code=400)
    with current_tenant(tenant):
        return await call_next(request)
```

Any ORM query inside the handler picks up the tenant automatically.

## Worker job (Celery / arq)

```python
@app.task
def send_digest(tenant_id: str):
    with current_tenant(tenant_id):
        notes = list(Note.objects.filter(...))
        # ...
```

## Override the column name

If your domain uses `org_id` instead of `tenant_id`:

```python
from dorm.contrib.tenants_row import TenantManager, TenantModel

class OrgScopedManager(TenantManager):
    tenant_field = "org_id"

class Note(TenantModel):
    org_id = dorm.CharField(max_length=64, db_index=True)
    title = dorm.CharField(max_length=200)

    objects = OrgScopedManager()
```

(The inherited `tenant_id` can stay or be redeclared as
`IntegerField` if your org_id is integer — override the field and
the manager.)

## Compose with sharding

`HashShardRouter` (3.4+) and `TenantModel` compose well: the shard
key is usually **the** tenant id.

```python
from dorm.contrib.sharding import HashShardRouter, with_shard_key

DATABASE_ROUTERS = [
    HashShardRouter(num_shards=4, shard_models={Note}),
]

with current_tenant(tenant_id), with_shard_key(tenant_id):
    Note.objects.create(title="hi")   # tenant filter + shard routing
```

## Caveats

- **Unique constraints**: `UNIQUE(name)` is not per-tenant. Use
  `UNIQUE(tenant_id, name)` or a partial index
  `UNIQUE(name) WHERE tenant_id = ...`.
- **Cross-tenant foreign keys**: nothing stops `Note.author_id`
  from pointing at a User in another tenant. Validate at the code
  path or add a CHECK constraint.
- **Per-tenant backups**: impossible with row-level — every
  tenant shares the tables. For backup-per-tenant use
  schema-level.
- **Dropping a tenant**: with row-level it's `Note.unscoped.filter(
  tenant_id=X).delete()` — costly on large tables. Consider
  partitioning by `tenant_id` if churn is high.

## More

- [Schema-level tenants](https://github.com/rroblf01/d-orm) — `dorm.contrib.tenants`
- [Sharding](sharding.md) —
  combining with multi-tenancy
- [When to use what](when-to-use-what.md) — schema-level vs row-level
