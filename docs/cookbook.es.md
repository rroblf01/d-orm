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

## Instrumentación con OpenTelemetry

```python
from dorm.contrib.otel import instrument

instrument()                # nombre de tracer por defecto: "dorm"
# instrument(tracer_name="miapp.dorm")
```

Cada query genera un span con `db.system` (`"postgresql"` /
`"sqlite"`), `db.statement` (truncado a 1KB) y
`db.dorm.elapsed_ms`. Las queries fallidas marcan el span con status
`ERROR` y la clase de la excepción en `db.dorm.error`. Idempotente —
llamarla dos veces reemplaza el cableado anterior (sin duplicar
spans). Dependencia opcional sobre `opentelemetry-api`.

## Pub/sub PostgreSQL con `LISTEN` / `NOTIFY`

```python
from dorm.db.connection import get_async_connection

conn = get_async_connection()

# Publicar:
async with transaction.aatomic():
    Order.objects.create(...)
    await conn.notify("orders", str(order.pk))
# El NOTIFY se entrega tras el COMMIT — los suscriptores solo ven
# trabajo commiteado.

# Suscribirse:
async for msg in conn.listen("orders"):
    print(msg.channel, msg.payload, msg.pid)
```

Los nombres de canal se validan como identificadores SQL. El
iterador async de `listen()` abre su propia conexión dedicada
(LISTEN retiene la conexión durante toda la suscripción) y la cierra
limpio cuando rompes el bucle.
