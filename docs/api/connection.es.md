# Conexión y health-check

Funciones para gestionar conexiones, hacer probes de salud y resolver
routers entre bases de datos.

> **Nota:** las firmas de funciones son código Python — no se traducen.
> Las descripciones aquí son traducción manual de las docstrings
> originales en inglés. Si algo difiere, el código fuente manda.

## Health checks

### `health_check`

```python
def health_check(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]
```

Ejecuta un `SELECT 1` trivial contra el backend configurado y devuelve
un dict de estado apto para una sonda de readiness de Kubernetes / ECS
/ Render.

**Devuelve** un `dict` con:

- `status` — `"ok"` o `"error"`
- `alias` — el alias consultado
- `elapsed_ms` — duración del probe (float)
- `error` — solo en fallo: `"<TipoExcepción>: <mensaje>"`

**Nunca lanza excepción** — los health-checks tienen que responder
incluso cuando la BD está caída (es lo que el orquestador necesita
para decidir si reiniciarte).

```python
import dorm
result = dorm.health_check("default")
# {"status": "ok", "alias": "default", "elapsed_ms": 0.42}
```

### `ahealth_check`

```python
async def ahealth_check(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]
```

Equivalente async de `health_check`, pensado para rutas de FastAPI /
Starlette / Sanic. Aplica `asyncio.wait_for(timeout=...)` al `SELECT 1`
para que un Postgres colgado no bloquee al worker indefinidamente.

```python
@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

## Acceso a conexiones

### `get_connection`

```python
def get_connection(alias: str = "default")
```

Devuelve el wrapper de conexión sync para *alias*. Lazy: la primera
llamada construye el pool / connection; las siguientes devuelven la
misma instancia desde caché. El wrapper expone `execute`,
`execute_write`, `execute_insert`, `atomic`, etc.

### `get_async_connection`

```python
def get_async_connection(alias: str = "default")
```

Mismo concepto pero para el pool async. La primera llamada crea el
`psycopg_pool.AsyncConnectionPool` (PostgreSQL) o el wrapper de
`aiosqlite` (SQLite).

### `close_all`

```python
def close_all()
```

Cierra cada conexión sync cacheada y vacía el caché. Útil al final
de scripts / tests sync.

### `close_all_async`

```python
async def close_all_async()
```

Drena cada pool async cacheado y vacía el caché. **Llámalo en el
shutdown** de tu app FastAPI / Starlette para que las conexiones se
cierren limpiamente:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await dorm.db.connection.close_all_async()
```

## Routing entre bases de datos

### `router_db_for_read`

```python
def router_db_for_read(model, *, default: str = "default", **hints) -> str
```

Consulta `settings.DATABASE_ROUTERS` y devuelve el alias para *leer*
filas de *model*. Gana el primer router que devuelva un string truthy;
si ninguno responde, devuelve *default*. Las excepciones lanzadas por
un router se ignoran (se prueba el siguiente).

### `router_db_for_write`

```python
def router_db_for_write(model, *, default: str = "default", **hints) -> str
```

Espejo de `router_db_for_read` para escrituras.

```python
class PrimaryReplicaRouter:
    def db_for_read(self, model, **hints):
        return "replica"
    def db_for_write(self, model, **hints):
        return "default"

dorm.configure(
    DATABASES={...},
    DATABASE_ROUTERS=[PrimaryReplicaRouter()],
)
```

## Reintento ante errores transitorios

### `with_transient_retry`

```python
def with_transient_retry(
    func,
    *,
    in_transaction: bool = False,
    attempts: int | None = None,
    backoff: float | None = None,
)
```

Ejecuta `func()` con backoff exponencial ante errores de BD
transitorios (corte de red, reinicio del servidor, "database is
locked" en SQLite). Salta los reintentos si `in_transaction=True`
(volver a aplicar trabajo ya committeado sería incorrecto).

| Argumento | Default | Efecto |
|---|---|---|
| `attempts` | `DORM_RETRY_ATTEMPTS` (env, def. `3`) | intentos totales |
| `backoff` | `DORM_RETRY_BACKOFF` (env, def. `0.1`) | segundos × `2^intento` |

### `awith_transient_retry`

```python
async def awith_transient_retry(
    coro_factory,
    *,
    in_transaction: bool = False,
    attempts: int | None = None,
    backoff: float | None = None,
)
```

Equivalente async. **`coro_factory` debe ser un callable de 0
argumentos** que devuelva una corrutina nueva en cada llamada — las
corrutinas en Python solo se pueden `await` una vez.

```python
result = await awith_transient_retry(
    lambda: get_async_connection().execute("SELECT 1"),
)
```

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [Connection / health (English)](../../api/connection/).
