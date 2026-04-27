# Despliegue en producción

Esta página recoge todo lo que deberías considerar al ejecutar dorm
fuera del portátil: pooling de conexiones, réplicas, reintentos,
observabilidad y workflow de deploy.

## Dimensionar el pool de conexiones

Settings de PostgreSQL (`DATABASES["default"]`):

| Clave | Default | Notas |
|---|---|---|
| `MIN_POOL_SIZE` | `1` | conexiones idle a mantener abiertas |
| `MAX_POOL_SIZE` | `10` | tope duro; los checkouts adicionales esperan |
| `POOL_TIMEOUT` | `30.0` | segundos antes de que un checkout lance `PoolTimeout` |
| `POOL_CHECK` | `True` | `SELECT 1` al checkout para descartar conexiones rotas |
| `PREPARE_THRESHOLD` | default de psycopg (5) | tras cuántas ejecuciones del mismo SQL psycopg lo prepara en el servidor. Pon `0` para "siempre preparar" en apps dominadas por queries repetidas; súbelo si tu workload genera muchas queries únicas |
| `MAX_IDLE` | `600.0` | recicla conexiones que llevan idle más de N segundos |
| `MAX_LIFETIME` | `3600.0` | recicla cada conexión tras N segundos, independientemente de la actividad |

**Regla de oro**: `MAX_POOL_SIZE = vCPU * 2` por proceso.
Multiplica por el número de workers (gunicorn, uvicorn) para
obtener el footprint total de conexiones, y asegúrate de que cabe
en el `max_connections` de PostgreSQL con margen para slots de
replicación y sesiones admin.

```python
DATABASES = {
    "default": {
        "ENGINE": "postgresql",
        "NAME": "myapp",
        "USER": "myapp",
        "PASSWORD": "...",
        "HOST": "primary.internal",
        "PORT": 5432,
        "MIN_POOL_SIZE": 4,
        "MAX_POOL_SIZE": 20,
        "POOL_TIMEOUT": 10.0,
    }
}
```

Si estás detrás de PgBouncer en modo transaction, baja
`MIN_POOL_SIZE` a 1 — el bouncer es el pool de verdad, dorm solo
necesita checkouts baratos.

## Réplicas de lectura

Define cada alias en `DATABASES` y enruta vía `DATABASE_ROUTERS`:

```python
DATABASES = {
    "default": {"ENGINE": "postgresql", "HOST": "primary.internal", ...},
    "replica": {"ENGINE": "postgresql", "HOST": "replica.internal", ...},
}

class PrimaryReplicaRouter:
    def db_for_read(self, model, **hints):
        return "replica"
    def db_for_write(self, model, **hints):
        return "default"

DATABASE_ROUTERS = [PrimaryReplicaRouter()]
```

Los routers también pueden ramificar por modelo:

```python
class AuditRouter:
    def db_for_write(self, model, **hints):
        if model._meta.app_label == "audit":
            return "audit_writer"
        return None      # deja que decidan otros routers / default
```

Para un override puntual, usa `Manager.using("alias")` — saltea los
routers para esa única query.

## Reintento ante errores transitorios

dorm reintenta `OperationalError` e `InterfaceError` (cortes de red,
reinicio del servidor) tanto en pools sync como async. Tuneable vía
variables de entorno:

| Var | Default | Efecto |
|---|---|---|
| `DORM_RETRY_ATTEMPTS` | `3` | intentos totales incluyendo el primero |
| `DORM_RETRY_BACKOFF` | `0.1` | segundos, multiplicados por `2^intento` |

Los retries están **desactivados dentro de transacciones** — el
pool no puede reproducir con seguridad un `BEGIN` a medio commitear.
Envuelve secuencias externas "must-succeed" en tu propio bucle de
retry con claves de idempotencia.

## Health checks

```python
import dorm

@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

`health_check()` (sync) y `ahealth_check()` (async) ejecutan un
`SELECT 1` sobre el alias configurado y devuelven:

```python
{"status": "ok", "alias": "default", "elapsed_ms": 0.42}
{"status": "error", "alias": "default", "elapsed_ms": 5012.0,
 "error": "OperationalError: connection refused"}
```

Ninguno lanza excepción — siempre responden, incluso cuando la BD
está caída, que es lo que necesita una sonda de readiness en un
orquestador.

## Despliegue de migraciones

El orden recomendado:

1. Construye el nuevo código (artifact inmutable).
2. `dorm migrate --dry-run` contra producción — revisa el SQL.
3. `dorm migrate` (los advisory locks hacen segura la ejecución
   concurrente).
4. Despliega el código nuevo.

Para cambios de esquema sin downtime, sigue el playbook estándar
expand/contract:

| Paso | Migración | Código |
|---|---|---|
| Expand | añade columna nullable | el código viejo la ignora |
| Backfill | data migration en chunks | viejo y nuevo conviven |
| Contract | NOT NULL, borra la vieja | solo código nuevo |

`dorm dbcheck` en tu CI caza el caso en el que un dev olvidó
commitear una migración: sale non-zero ante drift.

## Observabilidad

### Hooks por query

```python
from dorm.signals import pre_query, post_query

def trace(sender, sql, params, alias, duration_ms=None, **kwargs):
    log.info("query", sql=sql, params=params, alias=alias, ms=duration_ms)

pre_query.connect(trace)
post_query.connect(trace)
```

Conéctalos a OpenTelemetry, structlog, o lo que uses. La señal
`post_query` incluye `duration_ms`, que es lo que querrás meter en
tu APM.

### Stats del pool

```python
from dorm.db.connection import get_connection
stats = get_connection("default").pool_stats()
# {"size": 7, "idle": 4, "in_use": 3, "max_size": 20, ...}
```

Expón esto en `/metrics` vía Prometheus para graficar la saturación.
Un pool que toca `in_use == max_size` durante periodos sostenidos
es el indicador líder de una app constreñida por conexiones.

### EXPLAIN

Para debugging puntual, `qs.explain(analyze=True)` devuelve la
salida del planner. Engánchalo a un endpoint solo-dev o úsalo en
`dorm shell`.

## Compartir el event loop async

Si ejecutas código async (FastAPI, scripts asyncio), asegúrate de
que todos tus tests comparten **un** event loop:

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

Sin esto, cada test crea un loop nuevo y un pool nuevo. Los pools
viejos quedan colgando como conexiones dangling, max_connections
sube, y al final CI empieza a colgarse.

## Logging

dorm usa el módulo stdlib `logging` bajo el namespace `dorm`.
Loggers útiles:

| Logger | Qué emite |
|---|---|
| `dorm.db.pool` | INFO en open/close del pool, WARNING en agotamiento |
| `dorm.db.lifecycle.postgresql` | INFO en open/close del pool PG (tamaño/timeout); el nombre de BD y host se emiten solo a DEBUG, así no se filtran metadatos por-tenant en un sink INFO sin habilitarlo explícitamente |
| `dorm.migrations` | INFO por migración aplicada |
| `dorm.queries` | DEBUG por SQL ejecutada (off por defecto) |
| `dorm.signals` | ERROR por excepción de receiver (con traceback completo) — conéctalo a Sentry / tu pipeline de alertas para que un `post_save` roto sea observable |
| `dorm.conf` | INFO cuando un `settings.py` se autodescubre (auditoría de qué archivo conformó la configuración) |

```python
import logging
logging.getLogger("dorm.queries").setLevel(logging.DEBUG)
# Enrutar fallos de señales a tu handler de alertas:
logging.getLogger("dorm.signals").addHandler(tu_handler_alerta)
```

## Checklist

- [ ] `MAX_POOL_SIZE × workers ≤ max_connections de Postgres / 2`
- [ ] `dorm dbcheck` corre en CI
- [ ] `dorm migrate --dry-run` corre como gate de deploy en prod
- [ ] `/healthz` cableado a la sonda de readiness
- [ ] `pre_query` / `post_query` trazado a tu APM
- [ ] Tests async con event loop session-scoped
- [ ] Router de réplica definido si el tráfico supera una caja
