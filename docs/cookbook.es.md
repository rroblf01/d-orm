# Recetario

Recetas prácticas para situaciones comunes. Cada una es un snippet
completo y listo para copiar.

## get-or-create idempotente con un constraint único

```python
from dorm.exceptions import IntegrityError

def get_or_create_email(email: str, name: str) -> Author:
    obj, _ = Author.objects.get_or_create(
        email=email,
        defaults={"name": name},
    )
    return obj
```

`get_or_create` corre dentro de una transacción para que dos
llamadas concurrentes no inserten ambas. Si el constraint único no
está en las claves de lookup, el patrón seguro es capturar
`IntegrityError` y volver a hacer `get`.

## Incremento atómico de contador

```python
from dorm import F

# Sin race, un único UPDATE SQL.
Post.objects.filter(pk=42).update(views=F("views") + 1)
```

Nunca hagas `post.views += 1; post.save()` — eso es
read-modify-write y pierde incrementos concurrentes.

## Paginación

```python
def paginate(qs, page: int, page_size: int = 20):
    start = (page - 1) * page_size
    return list(qs.order_by("id")[start:start + page_size])
```

Acompaña siempre un slice con `order_by(...)` — sin orden explícito
la misma fila puede aparecer en dos páginas o en ninguna.

Para datasets grandes, la **paginación por keyset** es mucho más
rápida que `OFFSET`:

```python
def page_after(qs, last_id: int, page_size: int = 20):
    return list(
        qs.filter(id__gt=last_id).order_by("id")[:page_size]
    )
```

## Soft delete

Hay un mixin listo en `dorm.contrib.softdelete`:

```python
from dorm.contrib.softdelete import SoftDeleteModel
import dorm

class Post(SoftDeleteModel):
    title = dorm.CharField(max_length=200)

# Tres managers:
Post.objects                          # solo filas vivas
Post.all_objects                      # todo
Post.deleted_objects                  # solo soft-deleted

# Soft delete por defecto; pasa hard=True para un DELETE real:
post.delete()                         # UPDATE … SET deleted_at = now()
post.delete(hard=True)                # DELETE FROM …
post.restore()                        # limpia deleted_at

# Paridad async:
await post.adelete()
await post.arestore()
```

Avisos: `on_delete=CASCADE` **no** cascadea por soft delete (los
hijos siguen visibles en `Post.objects`). Y los UNIQUE de la BD no
saben de `deleted_at` — usa un índice parcial
(`UNIQUE … WHERE deleted_at IS NULL`) en el schema si necesitas
"único entre filas vivas".

## Audit log via señales

```python
from dorm.signals import post_save, post_delete

def write_audit(sender, instance, created, **kwargs):
    AuditLog.objects.create(
        model=sender.__name__,
        pk=instance.pk,
        action="created" if created else "updated",
        payload=model_to_dict(instance),
    )

post_save.connect(write_audit, sender=Article)
post_delete.connect(write_audit, sender=Article)
```

Para tablas con mucho volumen, prefiere el patrón WAL → Kafka a
nivel de base de datos en vez de señales Python — las señales
disparan in-process y suman latencia.

## Bulk insert con deduplicación

```python
existing = set(
    User.objects.filter(email__in=[u.email for u in batch])
                .values_list("email", flat=True)
)
new_users = [u for u in batch if u.email not in existing]
User.objects.bulk_create(new_users, batch_size=500)
```

O, solo en PG, empuja la dedup a la BD:

```python
from dorm.db.connection import get_connection
get_connection().execute(
    "INSERT INTO users (email, name) VALUES %s ON CONFLICT (email) DO NOTHING",
    rows,
)
```

## Lock con `select_for_update`

```python
with transaction.atomic():
    account = (
        Account.objects.select_for_update()
                       .get(pk=account_id)
    )
    account.balance -= amount
    account.save(update_fields=["balance"])
```

`SELECT ... FOR UPDATE` mantiene row locks hasta que la transacción
commitea — úsalo cuando hagas read-then-write bajo contención.

## Manager / queryset method propio

```python
class PublishedQuerySet(QuerySet):
    def published(self):
        return self.filter(published=True, published_at__lte=Now())

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    published = dorm.BooleanField(default=False)
    published_at = dorm.DateTimeField(null=True)

    objects = PublishedQuerySet.as_manager()

# Article.objects.published().filter(author=...)
```

El resultado sigue siendo un `QuerySet`, así que sigues encadenando.

## Stream de un millón de filas para un export

```python
import csv

with open("authors.csv", "w") as f:
    w = csv.writer(f)
    w.writerow(["id", "name", "email"])
    for a in Author.objects.order_by("id").iterator(chunk_size=5000):
        w.writerow([a.id, a.name, a.email])
```

`iterator(chunk_size=N)` abre un cursor server-side en PG y hace
streaming por arraysize en SQLite, así la memoria se mantiene plana.

## Multi-tenant con routing por schema

```python
class TenantRouter:
    def db_for_read(self, model, **hints):
        return _current_tenant_alias()
    def db_for_write(self, model, **hints):
        return _current_tenant_alias()
```

Fija el alias del tenant desde un middleware (web) o un context
manager (workers) y enruta todo por ahí. Combínalo con un set de
migraciones aplicado por alias.

## Read-after-write con réplica

```python
# El routing manda las lecturas a la réplica — pero justo tras una
# escritura, la réplica puede no tener aún la fila. Lee del primary
# explícitamente:
new = Post.objects.using("default").get(pk=new_pk)
```

Lee tus propias escrituras desde el primary; confía en la réplica
para todo lo más antiguo de unos segundos.

## Fixtures de testing

```python
import pytest, dorm
from dorm.db.connection import close_all
from dorm.test import transactional_db, atransactional_db  # noqa: F401

@pytest.fixture(scope="session", autouse=True)
def configure_dorm():
    dorm.configure(DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}})
    yield
    close_all()

@pytest.fixture
def author():
    return Author.objects.create(name="Alice", age=30)


def test_something(transactional_db, author):
    Author.objects.create(name="Mallory", age=99)
    assert Author.objects.count() == 2
    # Las dos filas se hacen rollback solas al salir el test.
```

`transactional_db` (sync) y `atransactional_db` (pytest-asyncio)
envuelven cada test en un `atomic()` que hace rollback al salir,
ahorrándote el `DROP TABLE` / `CREATE TABLE` entre tests. Para
suites con unittest usa el mixin `dorm.test.DormTestCase`:

```python
import unittest
from dorm.test import DormTestCase

class AuthorTests(DormTestCase, unittest.TestCase):
    def test_create(self):
        Author.objects.create(name="Alice", age=30)
        # rollback en tearDown
```

Para tests de integración con PostgreSQL, usa
[`testcontainers`](https://testcontainers-python.readthedocs.io/)
para levantar un Postgres efímero por sesión.

### Asertar conteo de queries (`assertNumQueries`, `assertMaxQueries`) (3.0+)

Fija el número exacto de queries de un code path — o el techo —
para defenderte de regresiones N+1. Tanto la forma context manager
como decorator funcionan en tests sync y ``async def``:

```python
from dorm.test import (
    assertNumQueries,
    assertMaxQueries,
    assertNumQueriesFactory,
    assertMaxQueriesFactory,
)

def test_list_view(transactional_db):
    with assertNumQueries(2) as ctx:
        list(Article.objects.select_related("author")[:10])
    assert ctx.count == 2  # también expuesto para asserciones extra

@assertMaxQueriesFactory(5)
def test_dashboard(transactional_db):
    # Sin N+1: dashboard tiene que emitir ≤ 5 queries.
    render_dashboard()

@assertNumQueriesFactory(1)
async def test_async_una_query():
    await Article.objects.acount()
```

Aislamiento per-task via ``ContextVar`` — tasks lanzadas con
``asyncio.gather`` ven contadores independientes, así dos tests
concurrentes bajo ``pytest-xdist`` no contaminan asserciones.

## Cambiar `FileField` entre disco local y S3 sin tocar código

`FileField` lee el storage backend desde `settings.STORAGES` en
*cada* acceso, así que pasar de disco local → MinIO → AWS S3 es un
cambio de configuración, no de código.

```python
class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)
```

**Disco local** — el default si `STORAGES` no se define, útil para
tests / SQLite / dev en una sola máquina:

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.storage.FileSystemStorage",
        "OPTIONS": {"location": "/var/app/media", "base_url": "/media/"},
    }
}
```

**MinIO** — para paridad de dev local con producción S3 sin cuenta
AWS. Lanza `docker run -d -p 9000:9000 minio/minio server /data` una
vez y:

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        "OPTIONS": {
            "bucket_name": "dev-uploads",
            "endpoint_url": "http://localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "region_name": "us-east-1",
            "signature_version": "s3v4",
            "addressing_style": "path",
        },
    }
}
```

**AWS S3 en producción** — boto3 toma las credenciales de la cadena
ambiente (rol IAM en EC2/ECS/Lambda, env vars, `~/.aws/`):

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        "OPTIONS": {
            "bucket_name": "myapp-prod-uploads",
            "region_name": "eu-west-1",
            "default_acl": "private",
            "querystring_auth": True,
            "querystring_expire": 3600,
        },
    }
}
```

El código de la aplicación — `doc.attachment = ContentFile(...)`,
`doc.save()`, `doc.attachment.url`, `doc.attachment.delete()` — es
idéntico en las tres configuraciones. Pilota la elección desde el
entorno para que la misma imagen despliegue en todos lados:

```python
# settings.py
import os

if os.environ.get("DORM_STORAGE") == "s3":
    STORAGES = {
        "default": {
            "BACKEND": "dorm.contrib.storage.s3.S3Storage",
            "OPTIONS": {
                "bucket_name": os.environ["S3_BUCKET"],
                "region_name": os.environ.get("AWS_REGION", "us-east-1"),
                "default_acl": "private",
            },
        }
    }
else:
    STORAGES = {
        "default": {
            "BACKEND": "dorm.storage.FileSystemStorage",
            "OPTIONS": {"location": os.environ.get("MEDIA_ROOT", "media")},
        }
    }
```

Trampa común: al cambiar a S3, **no esperes que las filas existentes
se migren solas** — los nombres se almacenan tal cual, así que una
fila apuntando a `docs/2026/04/q1.pdf` en disco local buscará la
misma clave en S3. O bien rellenas el bucket desde el volumen local
antes del cutover, o escribes una migración `RunPython` puntual que
re-suba cada archivo a través del nuevo storage.

## Instrumentación con OpenTelemetry

`dorm.contrib.otel` engancha las señales `pre_query` / `post_query`
para que cada query del ORM se convierta en un span de OTel sin
cambios en cada call site.

### Setup

Instala la API de OTel (y un SDK si quieres mandar spans a algún
sitio):

```bash
pip install opentelemetry-api opentelemetry-sdk
# Exporters opcionales:
pip install opentelemetry-exporter-otlp     # → collector OTLP / Jaeger / Honeycomb
pip install opentelemetry-exporter-jaeger   # → Jaeger directo
```

En el startup de tu app:

```python
from dorm.contrib.otel import instrument

instrument()                                 # tracer por defecto "dorm"
# instrument(tracer_name="miapp.dorm")       # tracer custom
```

Ya está — cada query del ORM produce un span.

### Atributos del span

Cada span lleva:

| Atributo | Valor |
|---|---|
| nombre del span | `db.<vendor>` (`db.postgresql` / `db.sqlite`) |
| `db.system` | `"postgresql"` o `"sqlite"` |
| `db.statement` | el SQL, truncado a 1024 chars |
| `db.dorm.elapsed_ms` | duración wall-clock (set en `post_query`, antes de `end()`) |
| `db.dorm.error` | nombre de clase de la excepción (solo en fallo) |
| status del span | `ERROR` en fallo, default `UNSET` en éxito |

El truncado a 1KB del statement mantiene el SQL gigante de
`bulk_create` fuera de tus traces; si necesitas el SQL completo,
loguéalo aparte vía `dorm.queries`.

### Cablear un exporter (Jaeger via OTLP)

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider(resource=Resource({SERVICE_NAME: "miapp"}))
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
)
trace.set_tracer_provider(provider)

# Ahora cablea dorm:
from dorm.contrib.otel import instrument
instrument()
```

`instrument()` es idempotente — llamarla dos veces reemplaza el
cableado anterior en lugar de producir spans duplicados, así que es
seguro en hot reload.

### Desconectar

```python
from dorm.contrib.otel import uninstrument
uninstrument()  # desengancha los receivers; futuras queries no se trazan
```

Útil en teardown de tests o en librerías que quieran opt-out para
un código path específico.

### Avisos

- **Dependencia opcional.** `instrument()` lanza un `ImportError`
  claro si `opentelemetry-api` no está instalado. Despliegues sin
  el paquete siguen funcionando — sin spans.
- **Los spans se producen sync en el thread llamante.** No hay pump
  background dedicado. El BatchSpanProcessor (config típica del
  SDK) absorbe la latencia del export, así que el caller ve
  overhead de nanosegundos por span.
- **Los params sensibles aterrizan en `db.statement`.** El SQL es
  el texto literal de la query (placeholders `%s`, params aparte).
  El atributo OTel **no** incluye los params bindeados — esos van
  por `dorm.db.utils._mask_params` solo en logs DEBUG. Si también
  quieres los params en el span, escribe un receiver custom en
  lugar de llamar a `instrument()`.

## Pub/sub PostgreSQL con `LISTEN` / `NOTIFY`

`NOTIFY` / `LISTEN` de PostgreSQL te dan pub/sub en la propia BD —
sin Redis para workloads de fan-out a escala modesta. El wrapper
async de `dorm` expone los dos lados.

### Publicar

```python
from dorm import transaction
from dorm.db.connection import get_async_connection

conn = get_async_connection()

async def crear_order(payload):
    async with transaction.aatomic():
        order = await Order.objects.acreate(**payload)
        await conn.notify("orders.created", str(order.pk))
        # El NOTIFY se entrega TRAS el COMMIT — los suscriptores
        # nunca ven trabajo que termine en rollback.
```

El `channel` se valida como identificador SQL
(`[A-Za-z_][A-Za-z0-9_]*`, ≤ 63 chars), así un nombre de canal que
venga de input del usuario no puede colar SQL. El payload va como
parámetro bound, así que no hay que escaparlo.

**Límite de payload.** El payload de `NOTIFY` está capado a 8000
bytes por defecto (compile-time `NOTIFY_PAYLOAD_MAX`). No
serialices la fila entera — manda un pk y deja que el listener la
busque.

### Suscribirse

```python
async def consumer_orders():
    conn = get_async_connection()
    async for msg in conn.listen("orders.created"):
        order = await Order.objects.aget(pk=int(msg.payload))
        await dispatch(order)
```

`msg` es un `psycopg.Notify` con atributos `channel`, `payload` y
`pid`. El iterador async no termina por sí solo — rompe el bucle
cuando quieras parar:

```python
async for msg in conn.listen("orders.created"):
    if shutdown_event.is_set():
        break
    await dispatch(msg)
```

### Propiedad de la conexión

`listen()` abre su **propia** conexión psycopg dedicada (LISTEN
registra un handler de notificación scoped a la sesión — la conexión
debe sobrevivir a la suscripción). NO tira del pool, así que un
listener de larga duración no ocupa un slot del pool.

Cuando rompes el bucle, la conexión se cierra en un bloque
`finally`. Si el worker muere a saco, la conexión del listener se
recolecta por el timeout de idle-connection de PG.

### Reconexión / reintentos

El `listen()` actual no se reconecta solo ante pérdida de conexión.
Envuélvelo en tu propio loop de retry si necesitas eso:

```python
async def listener_robusto(channel: str, handler):
    while True:
        try:
            async for msg in get_async_connection().listen(channel):
                await handler(msg)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            logger.warning("listener desconectado: %s — reconectando", exc)
            await asyncio.sleep(1.0)
```

Ten en cuenta: las notificaciones disparadas *mientras está
desconectado* se pierden. PG no las encola. Para semántica
at-least-once, combina NOTIFY con una tabla outbox que poll-eas al
reconectar.

### Patrones habituales

**Invalidación de caché entre réplicas:**

```python
async with transaction.aatomic():
    await User.objects.filter(pk=user_id).aupdate(**changes)
    await conn.notify("user.invalidate", str(user_id))
# Cada nodo escuchando tira su caché local para ese user.
```

**Cola de workers (consumidor uno-de-N):**

NOTIFY hace broadcast a *todos* los listeners. Para patrones de
consumer exclusivo, usa una cola row-based con
`select_for_update(skip_locked=True)` y NOTIFY solo como señal de
wake-up:

```python
# Productor
async with transaction.aatomic():
    job = await Job.objects.acreate(payload=payload, status="pending")
    await conn.notify("jobs.wakeup", "")

# Consumer
async for _ in conn.listen("jobs.wakeup"):
    while True:
        async with transaction.aatomic():
            job = await (
                Job.objects.filter(status="pending")
                .select_for_update(skip_locked=True)
                .afirst()
            )
            if job is None:
                break
            await job.run()
            await job.adelete()
```
