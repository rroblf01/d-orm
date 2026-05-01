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
