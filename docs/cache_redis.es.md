# Caché de resultados (Redis)

`djanorm` incluye una capa opt-in de caché para querysets
calientes. Backend por defecto: Redis. Contrato pluggable —
cualquier clase que implemente `dorm.cache.BaseCache` vale.

El cliente Redis es **opcional**:

```bash
pip install 'djanorm[redis]'
```

Sin la extra, `djanorm` importa limpio. El error útil sale solo
al instanciar el backend.

## Seguridad — payloads firmados con HMAC

!!! warning "Trust boundary"
    Los payloads cacheados se deserializan con `pickle.loads`,
    que ejecuta `__reduce__` sobre cualquier byte que devuelva
    el backend. Una instancia Redis escribible por un atacante
    (cluster multi-tenant, ACL leaky, deployment sin auth)
    permitiría inyectar un blob malicioso → arbitrary code
    execution al hidratar el queryset.

`dorm.cache` firma cada payload con HMAC-SHA256 antes de
salir del proceso y verifica la firma al volver. Blobs sin
firma / manipulados / truncados se descartan silenciosamente;
el queryset cae a la base de datos como si no existiera entry.

La signing key viene de estos settings, en orden de prioridad:

1. `CACHE_SIGNING_KEY` — recomendado, explícito.
2. `SECRET_KEY` — convención Django; reusado si está.
3. Clave random per-proceso — entries no sobreviven restart
   (firma con clave vieja no verifica), pero caché sigue
   inforjable. Warning logged una vez al logger `dorm.cache`
   para que el operador sepa que caché no se comparte entre
   workers.

```python
dorm.configure(
    DATABASES={"default": {...}},
    CACHES={"default": {"BACKEND": "dorm.cache.redis.RedisCache", ...}},
    CACHE_SIGNING_KEY=os.environ["DORM_CACHE_KEY"],  # 32+ bytes random
)
```

Para desactivar firma (sólo migrando caché legacy sin firmar
en red privada de confianza), usar `CACHE_INSECURE_PICKLE = True`.
No lo hagas.

### Producción multi-worker

En deployment multi-worker (gunicorn, uvicorn `--workers >1`,
ASGI multi-proceso) cada worker que cae al random key per-proceso
genera SU PROPIA key. Payloads escritos por un worker no
verifican en otro → cache hit-rate cae a visibilidad
per-worker, silenciosamente. Para detectar esta misconfig
ruidosamente:

```python
dorm.configure(
    ...,
    CACHE_REQUIRE_SIGNING_KEY=True,
)
```

El primer uso de caché en un worker sin `CACHE_SIGNING_KEY`
(o `SECRET_KEY`) explícito lanzará `ImproperlyConfigured` con
puntero claro al fallo. Recomendado para cualquier deployment
de producción.

## Configuración

```python
import dorm

dorm.configure(
    DATABASES={"default": {...}},
    CACHES={
        "default": {
            "BACKEND": "dorm.cache.redis.RedisCache",
            "LOCATION": "redis://localhost:6379/0",
            "OPTIONS": {"socket_timeout": 1.0},
            # TTL por defecto en segundos para qs.cache() sin timeout=.
            "TTL": 300,
        },
    },
)
```

`LOCATION` acepta todas las formas de `redis-py`:

- `redis://host:port/db` — TCP sin TLS.
- `rediss://host:port/db` — TCP + TLS.
- `unix:///path/to/redis.sock` — socket Unix.

`OPTIONS` se reenvía a `Redis.from_url(...)`. Claves comunes:
`socket_timeout`, `socket_connect_timeout`, `health_check_interval`,
`retry_on_timeout`, `password`.

## Cachear un queryset

Encadena `.cache(timeout=…)`:

```python
# Caché 30 segundos.
hot_books = Book.objects.filter(featured=True).cache(timeout=30)

for b in hot_books:
    print(b.title)
```

Primera iteración: query + store. Bytes pickleados bajo clave
SHA-1 de modelo + SQL final + parámetros. Iteraciones
posteriores dentro de `timeout`: hidratan instancias desde
caché. Cero round-trip a DB.

`timeout=None` usa el `TTL` del backend.
`timeout=0` cachea hasta invalidación.

### Async

```python
hot_books = await Book.objects.filter(featured=True).cache(timeout=30)
```

Pool sync y async separados. Mismas claves — un writer sync y
un reader async ven la misma vista.

## Invalidación automática

`Model.save()` / `Model.delete()` (y variantes async) disparan
`post_save` / `post_delete`. La capa de caché se engancha la
primera vez que llamas `qs.cache()` y ejecuta:

```python
backend.delete_pattern(f"dormqs:{app_label}.{ModelName}:*")
```

Writer nunca ve lectura cacheada vieja. La invalidación es
**coarse-grained**: un save invalida TODO queryset cacheado
del modelo. Para listings calientes con writes frecuentes:
TTL menor o esquema manual de claves.

Writes cross-model (guardar un `Author` con queryset cacheado
sobre `Book`) **no** se invalidan automáticamente — solo el
namespace del modelo guardado. Maneja FK-aware invalidation
en app o usa TTL corto en queries con JOIN.

### Protección stale-read race

El flujo naïve "read → fetch → store" tiene race sutil: un
writer que invalida key ENTRE fetch y store del reader dejaría
las rows viejas cacheadas durante un TTL completo.
`dorm.cache` cierra la ventana con un contador de versión
in-memory por modelo. Cada `post_save`/`post_delete` lo bump-ea;
la cache key incluye `:vN:`; el step de store re-lee la
versión POST-fetch y guarda los bytes bajo la key (posiblemente
bumpeada). El bump del writer apunta lecturas posteriores a
una key que el racer nunca escribió.

Contador es process-local. Invalidación cross-process sigue
yendo por `delete_pattern`. Helpers en `dorm.cache`:

- `model_cache_version(model)` → valor actual.
- `bump_model_cache_version(model)` → increment atómico;
  devuelve nuevo valor. El signal handler lo llama antes de
  emitir `delete_pattern`.

## Gaps conocidos y edge cases

Algunos escenarios NO se manejan a propósito — flags para que
no te tropieces en producción:

### Drift del contador de versión multi-proceso

El contador de versión per-modelo es **process-local**.
Workers tienen contadores independientes, así un save en
worker A no bumpea el contador de worker B. La invalidación
cross-process sigue funcionando porque comparten Redis y
``delete_pattern`` limpia todas las keys con prefijo de
versión. Consecuencias prácticas:

- Tras un save, la `:vN+1:` del writer es la nueva key en su
  worker; otros workers siguen usando `:vN:` hasta su próximo
  write o read.
- Entradas `:v0:`, `:v1:`, … pueden acumularse en Redis entre
  writes; el siguiente ``delete_pattern`` de cualquier worker
  las limpia. Pon TTL razonable (default 300s) para que keys
  fríos no se amontonen.

Si necesitas coherencia cross-process (raro — ``delete_pattern``
suele bastar), implementa backend custom cuyo
``model_cache_version`` lea/escriba contador atómico
compartido (Redis ``INCR``).

### Multi-table inheritance

Guardar instancia child dispara ``post_save`` del child;
querysets cacheados sobre el **parent** usan namespace del
parent y NO se invalidan. Evita cachear queries sobre parent
de jerarquía MTI si los children cambian frecuente.

### `count()` / `exists()` / `aggregate()` NO se cachean

El hook de caché vive en ``QuerySet._fetch_all`` (path que
usan ``__iter__`` / ``await qs``). ``count()``, ``exists()``,
``aggregate()`` y los helpers explain lanzan su propio SQL y
saltan la caché. Para cachear un count, materializa la lista
(``len(qs)`` tras ``.cache(...)``) o gestiona contador aparte
con ``set`` / ``get`` directo sobre el backend.

### Mutaciones M2M

``manager.add(...)`` / ``set(...)`` / ``clear(...)`` en
``ManyToManyField`` escriben sobre la tabla de junction; NO
disparan ``post_save`` del parent. Querysets cacheados que
filtran por M2M siguen poblados hasta próximo save del parent
o expiración de TTL. Envuelve mutaciones M2M en ``save()``
del parent si la consistencia importa.

### Fallback de `_cache_key` con params no-picklables

El digest pickle-a los bind parameters; si algún param no
sobrevive pickle (expression custom, lambda) el wrapper cae a
``repr(params)``. Valores distintos no-picklables con mismo
``repr`` colisionarían en la cache key — edge case (necesitas
``__repr__`` malicioso) pero conviene saber.

## Caídas de caché no rompen queries

`RedisCache` envuelve cada operación en `try / except`. Error
de conexión, timeout, `WRONGTYPE` → miss path → query a DB
normal. Cache es best-effort.

Verás warnings del cliente en logs pero la request funciona.

## Protocolo backend

Subclase `dorm.cache.BaseCache`:

```python
from dorm.cache import BaseCache


class MyCache(BaseCache):
    def get(self, key): ...
    def set(self, key, value, timeout=None): ...
    def delete(self, key): ...
    def delete_pattern(self, pattern): ...
    async def aget(self, key): ...
    async def aset(self, key, value, timeout=None): ...
    async def adelete(self, key): ...
    async def adelete_pattern(self, pattern): ...
```

Registra el dotted path:

```python
CACHES = {
    "default": {
        "BACKEND": "myapp.cache.MyCache",
        "LOCATION": "...",
    },
}
```

## Cuándo cachear

- **Datos de referencia** — países, monedas, feature flags.
  Read-heavy, pequeños, caros de obtener.
- **Páginas listing** — homepage, búsquedas, rankings.
  Lecturas dominan, escrituras desde jobs.
- **Lookups FK** — `.select_related(...)` que devuelve la
  misma fila varias veces en una request: caché 10s gana.

Cuándo **no**:

- **Lecturas user-specific** que cambian cada request — hit
  rate ~0%, pagas serialización para nada.
- **Counters de consistencia fuerte** — invalidación coarse
  los machaca constantemente.

## LRU en proceso: `LocMemCache` (2.6+)

Para tests, scripts mono-proceso o como capa local delante de Redis,
usa el LRU en proceso sin pulling de `redis-py`:

```python
CACHES = {
    "default": {
        "BACKEND": "dorm.cache.locmem.LocMemCache",
        "OPTIONS": {"maxsize": 1024},
        "TTL": 300,
    }
}
```

Mismo contrato que `RedisCache` — sync + async, `delete_pattern` para
invalidación por señal. NO compartido entre procesos worker: cada
gunicorn / uvicorn tiene su dict.

## Row-cache: `Manager.cache_get(pk=…)` (2.6+)

Lookup individual por PK que pasa por la cache antes de la DB.
Usa la misma versión de invalidación por modelo que
`QuerySet.cache(...)`, así que un `post_save` invalida ambos:

```python
user = User.objects.cache_get(pk=42, timeout=60)
# Paridad async:
user = await User.objects.acache_get(pk=42)
```

Misses caen silenciosamente a DB. Caída de cache también — la fila
en DB es la fuente de verdad.
