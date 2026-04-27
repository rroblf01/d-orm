# Soft delete

`dorm.contrib.softdelete` cambia el `DELETE FROM` por defecto por un
"soft delete" basado en timestamp: las filas siguen en la tabla pero
reciben una columna `deleted_at` con el UTC actual. El manager por
defecto las oculta automáticamente; managers opt-in las exponen
cuando hace falta.

Es un módulo contrib (no core) porque el soft delete tiene
trade-offs que algunos proyectos no pueden aceptar — léete los
[caveats](#caveats) antes de adoptarlo a nivel de proyecto.

## Quick start

```python
from dorm.contrib.softdelete import SoftDeleteModel
import dorm

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()

    class Meta:
        db_table = "articles"
```

Hereda de `SoftDeleteModel` en vez de `dorm.Model`. El mixin aporta:

- Un `deleted_at` — `DateTimeField(null=True, blank=True, db_index=True)`
- Tres managers: `objects`, `all_objects`, `deleted_objects`
- Métodos de instancia `delete(hard=False)` / `restore()` (sync + async)

Aún tienes que correr `dorm makemigrations` / `dorm migrate` para que
la tabla coja la columna `deleted_at`.

## Tres managers

```python
Article.objects                # solo filas vivas (deleted_at IS NULL)
Article.all_objects            # todo, incluso soft-deleted
Article.deleted_objects        # solo soft-deleted (deleted_at IS NOT NULL)
```

El `objects` por defecto es un `SoftDeleteManager` cuyo `get_queryset`
filtra por `deleted_at__isnull=True`. Cada método del queryset
hereda ese filtro — `.filter()`, `.count()`, `.exists()`, agregaciones,
iteración async, prefetch, etc. No tienes que acordarte de añadir
`.alive()` por todos lados; ese es el objetivo.

`all_objects` y `deleted_objects` están pensados para herramientas
admin, dashboards de auditoría, exports GDPR y flujos de undelete.

## Borrar

```python
article = Article.objects.get(pk=1)
article.delete()                 # UPDATE … SET deleted_at = now()
article.delete(hard=True)        # DELETE FROM … real
```

`delete()` es soft por defecto. Pasa `hard=True` para saltarte el
camino soft completamente — útil para purgas GDPR, limpieza de abuso
o compactación periódica.

La versión async funciona igual:

```python
await article.adelete()
await article.adelete(hard=True)
```

`delete()` devuelve la tupla `(total, by_model)` típica del contrato
`Model.delete`, así los call sites existentes siguen funcionando:

```python
n, by_model = article.delete()
# n == 1
# by_model == {"miapp.Article": 1}
```

## Restaurar

```python
article = Article.deleted_objects.get(pk=1)
article.restore()
# Ahora visible otra vez en Article.objects
```

`restore()` limpia el slot `deleted_at` y guarda. No-op si la fila
nunca fue soft-deleted. Async: `await article.arestore()`.

## Managers personalizados

`SoftDeleteManager` es solo un `Manager` con un filtro extra, así
que puedes subclassearlo para scoping por defecto custom:

```python
from dorm.contrib.softdelete import SoftDeleteManager

class TenantSoftDeleteManager(SoftDeleteManager):
    def get_queryset(self):
        from .middleware import current_tenant_id
        return super().get_queryset().filter(tenant_id=current_tenant_id())

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    tenant_id = dorm.IntegerField(db_index=True)

    objects = TenantSoftDeleteManager()
    # all_objects / deleted_objects se heredan de SoftDeleteModel
```

## Caveats

### `on_delete=CASCADE` NO cascadea por soft delete

```python
class Author(SoftDeleteModel):
    name = dorm.CharField(max_length=100)

class Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
```

Cuando haces `author.delete()` (soft), el `deleted_at` del autor se
pone, pero las filas `Article` siguen vivas y visibles en
`Article.objects` — todavía tienen la FK apuntando al author
soft-deleted.

Si necesitas cascadas de soft delete, sobreescribe `delete()` en el
padre para recorrer relaciones explícitamente:

```python
class Author(SoftDeleteModel):
    name = dorm.CharField(max_length=100)

    def delete(self, using="default", *, hard=False):
        if not hard:
            for art in self.article_set.all():
                art.delete()
        return super().delete(using=using, hard=hard)
```

### Los UNIQUE constraints no saben de `deleted_at`

Una columna `unique=True` rechaza re-insertar un valor que coincida
con una fila soft-deleted. Si necesitas "único entre filas vivas",
crea un índice parcial al nivel de schema:

```sql
-- PostgreSQL
CREATE UNIQUE INDEX articles_slug_live
    ON articles (slug) WHERE deleted_at IS NULL;
```

```sql
-- SQLite ≥ 3.8
CREATE UNIQUE INDEX articles_slug_live
    ON articles (slug) WHERE deleted_at IS NULL;
```

Añádelo via una migración con `RunSQL` — el autodetector aún no
emite índices parciales.

### Las FKs tampoco saben de `deleted_at`

Una FK que apunta a una fila soft-deleted sigue siendo válida. Leer
`article.author` devuelve el author soft-deleted. Código que asume
"si puedo desreferenciar la FK, el padre está vivo" se rompe en
silencio. Las opciones:

- Filtrar explícito: `Article.objects.filter(author__deleted_at__isnull=True)`
- Cascadear soft deletes (ver arriba)
- Usar un mixin de `Q(...)` en el queryset

### Uso de disco

Las filas soft-deleted se quedan en disco para siempre a menos que
las purgues periódicamente. Para tablas con mucho churn (sesiones,
eventos) esto puede explotar. Patrón habitual:

```python
# Correr cada noche via cron / Celery beat:
threshold = datetime.now(timezone.utc) - timedelta(days=90)
Article.deleted_objects.filter(deleted_at__lt=threshold).delete(hard=True)
```

## Testing

`SoftDeleteModel` se lleva bien con `dorm.test.transactional_db`:
cada test arranca dentro de una transacción que se hace rollback, así
los soft deletes de un test no se filtran al siguiente.

```python
def test_soft_delete_oculta(transactional_db):
    a = Article.objects.create(title="x")
    a.delete()
    assert not Article.objects.filter(pk=a.pk).exists()
    assert Article.deleted_objects.filter(pk=a.pk).exists()
```

## Referencia API

- **`SoftDeleteModel`** — modelo abstracto. Hereda en vez de
  `dorm.Model`. Aporta el campo `deleted_at` y los tres managers.
- **`SoftDeleteManager`** — manager que filtra `deleted_at IS NULL`.
  Subclassea para scoping por defecto custom.
- **`SoftDeleteModel.delete(using="default", *, hard=False)`** —
  soft delete por defecto; `hard=True` salta a un `DELETE` real.
- **`SoftDeleteModel.adelete(...)`** — versión async, mismos args.
- **`SoftDeleteModel.restore(using="default")`** — limpia
  `deleted_at`; no-op si la fila no estaba soft-deleted.
- **`SoftDeleteModel.arestore(...)`** — versión async.
