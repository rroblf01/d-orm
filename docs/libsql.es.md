# Backend libsql

`djanorm` incluye un motor `libsql` que habla con:

- **Ficheros locales SQLite-compatibles** — sustituto directo del
  backend `sqlite`. Sin servidor, sin auth.
- **`sqld` auto-alojado en VPS** — el caso típico "quiero un
  SQLite tipo gestionado que comparten varios procesos". Corre
  `sqld` detrás de un reverse proxy con TLS, apunta djanorm al
  endpoint HTTPS.
- **Réplica embebida** — fichero local sincronizado con el
  master remoto. Lecturas en sub-milisegundos sobre la réplica;
  escrituras viajan al master y se replican de vuelta.
- **Turso Cloud** — mismo wire protocol que `sqld` propio, así
  que la misma config funciona contra
  `https://<db>-<org>.turso.io`. Útil si no quieres mantener
  servidor.

El cliente libsql es **opcional**:

```bash
pip install 'djanorm[libsql]'
```

Tira de [`pyturso`](https://pypi.org/project/pyturso/) — SDK
oficial Turso para Python. Sin la extra, `djanorm` importa
limpio; al abrir conexión libsql sin pyturso instalado verás
un `ImproperlyConfigured` con el comando de instalación.

## Configuración

Tres claves en `DATABASES`:

| Clave | Significado |
|-------|-------------|
| `ENGINE` | `"libsql"` |
| `NAME` | Ruta al fichero local. Por defecto `:memory:`. Con `SYNC_URL` se convierte en réplica embebida. |
| `SYNC_URL` | Endpoint remoto — típicamente `https://libsql.tu-vps.com` para `sqld` propio o `https://<db>-<org>.turso.io` para Turso Cloud. Activa modo réplica embebida. |
| `AUTH_TOKEN` | Token Bearer (`Authorization: Bearer <token>`) en cada round-trip de sync. Opcional para `sqld` en red privada; obligatorio en Turso Cloud y recomendado para cualquier `sqld` expuesto a internet. |

### Local

```python
import dorm

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "libsql",
            "NAME": "/var/app/data.db",
        },
    },
    INSTALLED_APPS=["myapp"],
)
```

Comportamiento idéntico al backend SQLite.

### sqld auto-alojado en VPS

Layout más común en producción. Levanta `sqld` en tu VPS,
expón con nginx / Caddy + HTTPS:

```bash
# En tu VPS:
docker run -d \
    -p 8080:8080 \
    -v /var/lib/sqld:/var/lib/sqld \
    -e SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080 \
    -e SQLD_AUTH_JWT_KEY="$(cat /etc/sqld/jwt.pub)" \
    ghcr.io/tursodatabase/libsql-server:latest
```

Apunta djanorm al HTTPS:

```python
import os

dorm.configure(
    DATABASES={
        "default": {
            "ENGINE": "libsql",
            "NAME": "/var/app/local-replica.db",  # réplica embebida
            "SYNC_URL": "https://libsql.tu-vps.com",
            "AUTH_TOKEN": os.environ["LIBSQL_TOKEN"],
        },
    },
)
```

Lecturas → `local-replica.db` (cero round-trip de red);
escrituras → master → replicación. Forzar pull del master
(p.ej. tras un write hecho por otro proceso):

```python
from dorm.db.connection import get_connection

get_connection().sync_replica()
```

Para modo **solo-remoto** (sin réplica local, cada lectura
viaja al VPS), deja `NAME=":memory:"`:

```python
DATABASES = {
    "default": {
        "ENGINE": "libsql",
        "NAME": ":memory:",
        "SYNC_URL": "https://libsql.tu-vps.com",
        "AUTH_TOKEN": os.environ["LIBSQL_TOKEN"],
    },
}
```

### Forma URL

`parse_database_url` entiende `libsql://` y las variantes
explícitas `libsql+http://`, `libsql+https://`,
`libsql+ws://`, `libsql+wss://`. Pásalo directo desde
`os.environ`:

```python
from dorm.conf import parse_database_url

cfg = parse_database_url(os.environ["DATABASE_URL"])
dorm.configure(DATABASES={"default": cfg})
```

```
libsql://libsql.tu-vps.com?authToken=…&NAME=local-replica.db
libsql+https://libsql.tu-vps.com?authToken=…
libsql:///path/to/local.db
```

### Turso Cloud (gestionado)

Misma config; solo cambia el host:

```python
DATABASES = {
    "default": {
        "ENGINE": "libsql",
        "NAME": "/var/app/local-replica.db",
        "SYNC_URL": "libsql://your-db-your-org.turso.io",
        "AUTH_TOKEN": os.environ["TURSO_AUTH_TOKEN"],
    },
}
```

Genera el token con el CLI Turso
(`turso db tokens create your-db`) y guárdalo en env.

## Async

Dos paths:

- **Solo local** (sin `SYNC_URL`) — usa `turso.aio.connect`,
  I/O async nativa. Cada llamada al cursor se await directo
  sin worker thread.
- **Réplica embebida / solo remoto** (`SYNC_URL` set) — la API
  async de pyturso es solo-local hoy, así que el wrapper cae
  al cliente sync sobre un single-thread worker dedicado.
  Single-thread importa: las conexiones pyturso NO son
  thread-safe; el pool default de `asyncio.to_thread`
  produciría crashes nativos.

API user-facing idéntica:

```python
async def list_books():
    return [b async for b in Book.objects.all()]
```

## Soporte vectorial

`VectorField` (de `dorm.contrib.pgvector`) detecta el vendor
`libsql` y emite tipos / funciones nativas — **no necesitas la
extensión `sqlite-vec`**. El wrapper abre cada conexión con
`experimental_features="vector"` así que `F32_BLOB(N)` y las
funciones `vector_distance_*` están disponibles.

| Backend | Tipo columna | Funciones distancia |
|---------|--------------|---------------------|
| PostgreSQL | `vector(N)` | `<->` / `<=>` / `<#>` |
| SQLite (sqlite-vec) | `BLOB` | `vec_distance_L2` / `vec_distance_cosine` |
| **libsql / pyturso** | **`F32_BLOB(N)`** | **`vector_distance_l2` / `vector_distance_cos`** |

Ejemplo kNN:

```python
from dorm import F
from dorm.contrib.pgvector import VectorField, CosineDistance


class Doc(dorm.Model):
    title = dorm.CharField(max_length=200)
    embedding = VectorField(dimensions=384)


# Top-10 vecinos por distancia coseno.
nearest = (
    Doc.objects
       .annotate(score=CosineDistance("embedding", query_vector))
       .order_by("score")[:10]
)
```

Compila a:

```sql
SELECT …, vector_distance_cos("docs"."embedding", vector32(?)) AS "score"
FROM "docs"
ORDER BY "score" ASC
LIMIT 10
```

`MaxInnerProduct` **no** está soportado en libsql todavía; usa
`CosineDistance` sobre vectores L2-normalizados.

## Migraciones

Mismo flujo. `dorm makemigrations` / `dorm migrate` funcionan
sin cambios. Para campo vectorial en libsql NO necesitas
`VectorExtension()`:

```python
class Doc(dorm.Model):
    embedding = dorm.contrib.pgvector.VectorField(dimensions=1536)
```

La migración emite `F32_BLOB(1536)`.

## Limitaciones

- El wrapper async para **réplica embebida / remoto** serializa
  en un único worker thread (la API async de pyturso es
  solo-local). Throughput suficiente para la mayoría de apps;
  PostgreSQL con `psycopg.AsyncConnection` sigue siendo más
  paralelo en cargas con mucho fan-out.
- `MaxInnerProduct` no implementado en libsql.
- `journal_mode` es no-op en sesiones remoto-mode.
- Conexiones pyturso NO son thread-safe — mantén el wrapper
  scoped al thread / event loop que lo abrió. El wrapper
  async lo refuerza con executor dedicado; el sync usa el
  cache de conexiones thread-local del wrapper SQLite padre.
