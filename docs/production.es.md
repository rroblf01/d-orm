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

Pasa `deep=True` para incluir además estadísticas en vivo del pool,
así el mismo endpoint puede servir readiness *y* observabilidad:

```python
await dorm.ahealth_check(deep=True)
# {
#   "status": "ok", "alias": "default", "elapsed_ms": 0.42,
#   "pool": {
#     "alias": "default", "vendor": "postgresql", "has_pool": True,
#     "pool_min": 1, "pool_max": 10,
#     "pool_size": 7, "pool_available": 4, "requests_waiting": 0,
#     "requests_num": 18234, "usage_ms": 412.3, "connections_ms": 1.1,
#     ...
#   }
# }
```

O llama a `dorm.pool_stats(alias)` directamente si solo te interesa
la vista del pool (p.ej. en un exporter de Prometheus):

```python
from dorm.db.connection import pool_stats
stats = pool_stats("default")
```

Un pool cuyo `pool_available` se queda a cero con
`requests_waiting > 0` durante periodos prolongados es el indicador
adelantado de una app limitada por conexiones.

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

## Notas de seguridad

Algunos puntos a tener en cuenta en despliegues de producción:

- **El auto-discovery de `settings.py` ejecuta código Python.** Si no
  pasas `--settings=` ni `DORM_SETTINGS=`, dorm recorre el `cwd` y el
  directorio de `sys.argv[0]` buscando un `settings.py` y ejecuta
  el primero que encuentra (`exec_module()`). Es el comportamiento
  diseñado (imita el de `manage.py` de Django) pero implica que un
  `settings.py` que termine en tu directorio de trabajo se ejecutará
  como código. **Pasa explícitamente `--settings=miproyecto.settings`
  en los runners de producción** para evitar ambigüedad, y revisa tus
  imágenes de contenedor por archivos `settings.py` espurios.
- **Los logs DEBUG de queries enmascaran valores ligados a columnas
  cuyo nombre coincida con `password`, `token`, `api_key`, `secret`…**
  El resto se imprime literal para ayudar al debugging. Si rediriges
  los logs DEBUG a un sink compartido (Datadog, Loki), asegúrate de
  que la lista de redacción cubra tus columnas de credenciales
  específicas del dominio; si no, extiéndela vía la tupla
  `dorm.db.utils._SENSITIVE_COLUMN_PATTERNS`, o filtra en el handler
  del logger. Las señales `pre_query` / `post_query` siempre reciben
  los params crudos; si los reenvías a sinks externos, sanitiza ahí
  también.
- **Las migraciones son atómicas por archivo.** Un fallo en la op N
  hace rollback de las ops 1..N-1 y la migración *no* queda
  registrada como aplicada — así un `dorm migrate` reintentado vuelve
  a aplicar limpiamente. La misma garantía cubre el rollback y
  `migrate_to`. En SQLite esto requirió forzar un `BEGIN` explícito
  (el módulo `sqlite3` de Python no auto-inicia transacción antes de
  DDL); en PostgreSQL todo el DDL pasa por la conexión fijada por el
  bloque `atomic()` activo.
- **`execute_streaming()` se niega a correr dentro de `atomic()`.**
  Los cursores server-side de PostgreSQL necesitan su propia
  transacción; el fallback silencioso anterior cargaba todo el
  resultado en memoria. Si necesitas streaming dentro de una
  transacción, reestructura: lee las PKs a una lista fuera del bloque
  y luego itera sobre ellas.

## Checklist

- [ ] `MAX_POOL_SIZE × workers ≤ max_connections de Postgres / 2`
- [ ] `dorm dbcheck` corre en CI
- [ ] `dorm migrate --dry-run` corre como gate de deploy en prod
- [ ] `--settings=` o `DORM_SETTINGS=` explícitos en los runners de producción
- [ ] `/healthz` cableado a la sonda de readiness
- [ ] `pre_query` / `post_query` trazado a tu APM
- [ ] Tests async con event loop session-scoped
- [ ] Router de réplica definido si el tráfico supera una caja

## Configuración por URL/DSN (2.1+)

Las entradas de `DATABASES` aceptan ahora un string URL o un dict
con clave `URL`. Útil para sacar la cadena de conexión directamente
de `DATABASE_URL` sin escribir el cableado de
``HOST/PORT/USER/PASSWORD``::

    import os, dorm

    dorm.configure(DATABASES={
        "default": os.environ["DATABASE_URL"],
        # O con overrides:
        # "default": {"URL": os.environ["DATABASE_URL"], "MAX_POOL_SIZE": 30},
    })

Los parámetros conocidos del query string (`MAX_POOL_SIZE`,
`POOL_TIMEOUT`, `POOL_CHECK`, `MAX_IDLE`, `MAX_LIFETIME`,
`PREPARE_THRESHOLD`) se elevan como claves top-level de
`DATABASES`. El resto cae en `OPTIONS`.

## Puerta pre-despliegue: `dorm doctor` (2.1+)

Ejecuta `dorm doctor` en CI para fallar builds cuya configuración
tropiece con un footgun conocido de producción. Ejemplos que pilla:
tamaño de pool pequeño, falta de `sslmode` en host PG remoto, FKs
sin índice, retry de errores transitorios desactivado.
