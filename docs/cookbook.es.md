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

```python
class SoftDeleteQuerySet(QuerySet):
    def alive(self):
        return self.filter(deleted_at__isnull=True)
    def soft_delete(self):
        from datetime import datetime, timezone
        return self.update(deleted_at=datetime.now(timezone.utc))

class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    deleted_at = dorm.DateTimeField(null=True, blank=True, db_index=True)

    objects = SoftDeleteQuerySet.as_manager()
```

Pasa siempre por `Post.objects.alive()` en queries normales y
reserva `Post.objects.all()` para código de admin/auditoría.

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
from dorm.db.connection import close_all, reset_connections

@pytest.fixture(scope="session", autouse=True)
def configure_dorm():
    dorm.configure(DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}})
    yield
    close_all()

@pytest.fixture
def author():
    return Author.objects.create(name="Alice", age=30)
```

Para tests de integración con PostgreSQL, usa
[`testcontainers`](https://testcontainers-python.readthedocs.io/)
para levantar un Postgres efímero por sesión.
