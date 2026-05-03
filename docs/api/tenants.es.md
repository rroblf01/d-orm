# `dorm.contrib.tenants`

Routing schema-por-tenant para PostgreSQL — cambia el
`search_path` de la conexión por request / contexto así que los
mismos modelos enrutan a un schema tenant-specific.

## Cómo funciona

`search_path` de PostgreSQL controla qué schemas recorre el
planner al resolver referencias de tabla sin calificar. Ponerlo a
`<tenant_schema>, public` permite que el mismo modelo `Article`
escriba a `acme.articles` para un tenant y `globex.articles` para
otro — sin prefijo de tabla, sin swap de modelo, sin gymnastics
de router.

`TenantContext(name)` envuelve el body de request con `SET
search_path` al entrar / restore al salir. State vive en un
`ContextVar` así tasks ASGI / asyncio concurrentes enrutadas a
distintos tenants no se mezclan.

## Quick start

```python
from dorm.contrib.tenants import TenantContext, register_tenant

# Una vez al startup — alimenta al futuro runner de migraciones.
register_tenant("acme")
register_tenant("globex")

# En middleware FastAPI / Starlette:
@app.middleware("http")
async def tenant_middleware(request, call_next):
    tenant = resolve_tenant_from_host(request)  # tu lógica
    async with aTenantContext(tenant):
        return await call_next(request)
```

Cada tenant lee / escribe contra su propio schema; el schema
`public` mantiene tablas compartidas / cross-tenant.

## Bootstrap

El context manager runtime asume que el schema ya existe. Crea
+ migra por tenant a mano por ahora (el runner per-tenant llega
en v3.1):

```sql
CREATE SCHEMA IF NOT EXISTS acme;
```

```bash
# Luego dentro de un TenantContext("acme"):
PGOPTIONS='-c search_path=acme,public' dorm migrate
```

## API


## Backends soportados

Solo PostgreSQL. Otros backends necesitarían modelo de routing
distinto (BD por tenant, prefijo en tabla compartida, …) y
`TenantContext` raisea `NotImplementedError` contra ellos en
voz alta así el user no cree que funcionó.
