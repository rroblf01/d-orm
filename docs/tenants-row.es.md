# Multi-tenancy a nivel fila

dorm ofrece dos sabores de multi-tenancy:

| Sabor | Módulo | Cómo aísla | Coste |
|---|---|---|---|
| Schema-level (PG) | `dorm.contrib.tenants` | `search_path` por tenant | una migración por tenant |
| Row-level (cualquier backend) | `dorm.contrib.tenants_row` | columna `tenant_id` + filtro auto | una migración total |

Esta página cubre el segundo. Añadido en **4.0**.

## Trade-off rápido

| Si... | Usa... |
|---|---|
| Tienes 5 tenants enterprise grandes | schema-level |
| Tienes 5000 tenants pequeños | row-level |
| Cumplimiento exige aislamiento físico | schema-level |
| Mismo schema OK; aislamiento en código | row-level |
| Backend MySQL/SQLite/DuckDB | row-level (schema-level es PG-only) |

## El contrato

```python
import dorm
from dorm.contrib.tenants_row import TenantModel, current_tenant

class Note(TenantModel):
    title = dorm.CharField(max_length=200)
```

`TenantModel` añade:

- Columna `tenant_id` (`CharField(max_length=64, db_index=True)`).
  Override en subclase si quieres `UUIDField` / `IntegerField`.
- Manager `objects` que filtra automáticamente por
  `tenant_id = <active tenant>`.
- Manager `unscoped` — escape hatch sin filtro.
- `save()` / `asave()` autorrellenan `tenant_id` desde el contexto.

## Activar tenant

Envuelve cada handler / job con:

```python
from dorm.contrib.tenants_row import current_tenant

with current_tenant(request.user.tenant_id):
    Note.objects.create(title="hi")            # tenant_id auto-fill
    notes = list(Note.objects.all())           # filtro auto
```

`current_tenant()` usa `contextvars` — es per-task en asyncio,
per-thread en sync. No hay leak entre requests.

Anidado:

```python
with current_tenant("a"):
    with current_tenant("b"):
        # b activo aquí
        ...
    # a activo aquí
```

## Sin tenant activo → error

Llamar el manager fuera de `current_tenant(...)` lanza:

```python
>>> list(Note.objects.all())
NoActiveTenantError: No active tenant — wrap the call in
`with current_tenant(<tenant_id>):` or use `Note.unscoped` for a
deliberate cross-tenant query.
```

Por diseño. Un fallback silencioso a "todas las tenants" sería un
leak entre clientes.

## Escape hatch — `unscoped`

Para reportes, admin, dashboards cross-tenant:

```python
all_notes = list(Note.unscoped.all())
note_count_by_tenant = (
    Note.unscoped
    .values("tenant_id")
    .annotate(n=dorm.Count("id"))
)
```

`unscoped` es deliberadamente verbose — cada uso aparece en code
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

Cualquier query ORM dentro del handler hereda el tenant
automáticamente.

## Job worker (Celery / arq)

```python
@app.task
def send_digest(tenant_id: str):
    with current_tenant(tenant_id):
        notes = list(Note.objects.filter(...))
        # ...
```

## Override del nombre de columna

Si tu dominio usa `org_id` en lugar de `tenant_id`:

```python
from dorm.contrib.tenants_row import TenantManager, TenantModel

class OrgScopedManager(TenantManager):
    tenant_field = "org_id"

class Note(TenantModel):
    org_id = dorm.CharField(max_length=64, db_index=True)
    title = dorm.CharField(max_length=200)

    objects = OrgScopedManager()
```

(El `tenant_id` heredado puede dejarse o redeclarar como `IntegerField`
si tu org_id es entero — sobrescribe el campo y el manager.)

## Combina con sharding

`HashShardRouter` (3.4+) y `TenantModel` componen bien: el shard
key suele **ser** el tenant id.

```python
from dorm.contrib.sharding import HashShardRouter, with_shard_key

DATABASE_ROUTERS = [
    HashShardRouter(num_shards=4, shard_models={Note}),
]

with current_tenant(tenant_id), with_shard_key(tenant_id):
    Note.objects.create(title="hi")   # filtro tenant + ruteo shard
```

## Caveats

- **Constraints únicas**: `UNIQUE(name)` no es por-tenant. Usa
  `UNIQUE(tenant_id, name)` o partial index
  `UNIQUE(name) WHERE tenant_id = ...`.
- **Foreign keys cross-tenant**: nada impide a `Note.author_id`
  apuntar a un User de otra tenant. Valida en el code path o
  añade un CHECK constraint.
- **Backups por tenant**: imposible con row-level — todos los
  tenants comparten tablas. Para backup-per-tenant usa
  schema-level.
- **DROP de un tenant**: con row-level es `Note.unscoped.filter(
  tenant_id=X).delete()` — caro en tablas grandes. Considera
  particionado por tenant_id si el churn es alto.

## Más

- [Schema-level tenants](https://github.com/rroblf01/d-orm) — `dorm.contrib.tenants`
- [Sharding](sharding.md) —
  combinación con multi-tenancy
- [Cuándo usar qué](when-to-use-what.md) — schema-level vs row-level
