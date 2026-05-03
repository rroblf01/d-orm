# `dorm.contrib.tenants`

Schema-per-tenant routing for PostgreSQL — switches the
connection's `search_path` per request / context so the same
model classes route to a tenant-specific schema.

## How it works

PostgreSQL's `search_path` controls which schemas the planner
walks when resolving an unqualified table reference. Setting it
to `<tenant_schema>, public` lets the same `Article` model write
to `acme.articles` for one tenant and `globex.articles` for another
— no per-tenant table prefix, no model swap, no router gymnastics.

`TenantContext(name)` wraps a request body with `SET search_path`
on enter / restore on exit. State lives in a `ContextVar` so
concurrent ASGI / asyncio tasks routed to different tenants don't
bleed.

## Quick start

```python
from dorm.contrib.tenants import TenantContext, register_tenant

# Once at startup — feeds the future per-tenant migration runner.
register_tenant("acme")
register_tenant("globex")

# In a FastAPI / Starlette middleware:
@app.middleware("http")
async def tenant_middleware(request, call_next):
    tenant = resolve_tenant_from_host(request)  # your logic
    async with aTenantContext(tenant):
        return await call_next(request)
```

Each tenant ends up reading / writing against its own schema; the
public schema (`public`) keeps shared / cross-tenant tables.

## Bootstrap

The runtime context manager assumes the schema already exists.
Create + migrate per tenant manually for now (the per-tenant
migration runner lands with v3.1):

```sql
CREATE SCHEMA IF NOT EXISTS acme;
```

```bash
# Then within a TenantContext("acme"):
PGOPTIONS='-c search_path=acme,public' dorm migrate
```

## API

::: dorm.contrib.tenants.TenantContext
::: dorm.contrib.tenants.aTenantContext
::: dorm.contrib.tenants.current_tenant
::: dorm.contrib.tenants.register_tenant
::: dorm.contrib.tenants.registered_tenants

## Backend support

PostgreSQL only. Other backends would need a different routing
model (per-tenant database, per-tenant prefix on a shared table,
…) and `TenantContext` raises `NotImplementedError` against them
loudly so the user doesn't think it worked.
