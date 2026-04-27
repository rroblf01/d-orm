# Novedades en djanorm 2.1

La 2.1 cierra el mayor hueco que dejaba la 2.0 para cargas de
*reporting* en producción — las **consultas** — y endurece la
historia de migraciones para tablas en las que un `ALTER TABLE`
despertaría a alguien de guardia. Cada feature viene con tests
contra SQLite y PostgreSQL.

## Consultas

### `Subquery()` y `Exists()` — subconsultas correlacionadas

```python
from dorm import Exists, OuterRef, Subquery

# "Autores con al menos un libro publicado"
qs = Author.objects.filter(
    Exists(Book.objects.filter(author=OuterRef("pk"), published=True))
)

# Anota cada Author con el título de su último libro
latest = (
    Book.objects
        .filter(author=OuterRef("pk"))
        .order_by("-published_on")
        .values("title")[:1]
)
qs = Author.objects.annotate(latest=Subquery(latest))
```

`OuterRef("pk")` se resuelve a la columna PK del modelo externo en el
momento de compilar la subconsulta. Negar con `~Exists(...)`.

### Window functions

```python
from dorm import Sum, Window, RowNumber, Lag

# Top 3 libros por autor según páginas
qs = (
    Book.objects
        .annotate(
            rk=Window(RowNumber(), partition_by=["author_id"], order_by="-pages")
        )
        .filter(rk__lte=3)
)

# Total acumulado de páginas, ordenado por fecha
qs = Book.objects.annotate(
    running_pages=Window(
        Sum("pages"), partition_by=["author_id"], order_by="published_on"
    )
)

# Diferencia con la fila anterior de la partición
qs = Book.objects.annotate(
    prev_pages=Window(Lag("pages"), partition_by=["author_id"], order_by="published_on")
)
```

Set completo: `RowNumber`, `Rank`, `DenseRank`, `NTile`, `Lag`,
`Lead`, `FirstValue`, `LastValue`. Las funciones de ranking
**requieren** `order_by` — construir un `Window(RowNumber())` sin él
levanta excepción al construir el queryset, porque el SQL parsearía
pero devolvería resultados *implementation-defined*.

### CTEs (`WITH ... AS (...)`)

```python
recent = Book.objects.filter(published_on__gte=hace_una_semana)
qs = Book.objects.with_cte(recent_books=recent).filter(...)
```

Solo no recursivos. El cuerpo del CTE comparte el mismo paso de
reescritura de placeholders que la query externa, así que la caché
de prepared statements de PG sigue acertando.

### Funciones escalares nuevas

| Función | Mapea a | Notas |
| --- | --- | --- |
| `Greatest(a, b, ...)` | `GREATEST(...)` PG / `MAX(a, b)` SQLite | Vendor-aware |
| `Least(a, b, ...)` | `LEAST(...)` PG / `MIN(a, b)` SQLite | Vendor-aware |
| `Round(expr, places)` | `ROUND(...)` | |
| `Trunc(expr, "month")` | `DATE_TRUNC('month', expr)` | PG; unidades en allow-list |
| `Extract(expr, "year")` | `EXTRACT(YEAR FROM expr)` | PG; unidades en allow-list |
| `Substr(expr, pos, len)` | `SUBSTR(...)` | Indexado en 1 |
| `Replace(expr, old, new)` | `REPLACE(...)` | |
| `StrIndex(haystack, needle)` | `STRPOS(...)` PG / `INSTR(...)` SQLite | Indexado en 1 |

### Paginación por cursor (keyset)

```python
page = Author.objects.cursor_paginate(order_by="-created_at", page_size=20)
# page.items, page.next_cursor, page.has_next
next_page = Author.objects.cursor_paginate(
    order_by="-created_at", page_size=20, after=page.next_cursor,
)
```

Estable bajo escrituras concurrentes. Coste O(1) en páginas
profundas frente al O(N) de `OFFSET`. Variante async:
`acursor_paginate`. Devuelve `CursorPage`, iterable sobre `items`
y con `has_next`.

### Búsqueda full-text (PostgreSQL)

```python
from dorm.search import SearchVector, SearchQuery, SearchRank

# Forma simple: lookup __search con el idiom canónico
qs = Article.objects.filter(title__search="postgres tuning")

# Con vector / query explícitos y ranking
qs = (
    Article.objects
        .annotate(
            rank=SearchRank(
                SearchVector("title", "body", config="english"),
                SearchQuery("postgres tuning", search_type="websearch"),
            )
        )
        .filter(rank__gt=0)
        .order_by("-rank")
)
```

`search_type="websearch"` acepta `"frase entre comillas"`, `OR` y
`-excluir`. `cover_density=True` cambia `SearchRank` a `ts_rank_cd`.
SQLite no soportado — usa tablas virtuales FTS5.

## Esquema

### `CheckConstraint` y `UniqueConstraint`

```python
from dorm import CheckConstraint, UniqueConstraint, Q

class Order(dorm.Model):
    quantity = dorm.IntegerField()
    user_id = dorm.IntegerField()
    is_active = dorm.BooleanField(default=True)

    class Meta:
        constraints = [
            CheckConstraint(
                check=Q(quantity__gt=0),
                name="order_qty_positive",
            ),
            # Índice único parcial — solo un pedido *activo* por usuario.
            UniqueConstraint(
                fields=["user_id"],
                condition=Q(is_active=True),
                name="uniq_active_order_per_user",
            ),
        ]
```

El autodetector emite operaciones `AddConstraint` /
`RemoveConstraint`. Las restricciones únicas parciales se renderizan
como `CREATE UNIQUE INDEX ... WHERE predicado` (PostgreSQL + SQLite
≥ 3.8).

### `GeneratedField`

```python
class Order(dorm.Model):
    quantity = dorm.IntegerField()
    price    = dorm.DecimalField(max_digits=10, decimal_places=2)
    total    = dorm.GeneratedField(
        expression="quantity * price",
        output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
    )
```

Calculado en escritura por la base de datos (PG ≥ 12, SQLite ≥ 3.31).
Las asignaciones desde Python se rechazan — la BD es la fuente de
verdad. La gramática del expression está en allow-list.

### Extensiones de `Index`

```python
from dorm import Index, Q

class Article(dorm.Model):
    ...
    class Meta:
        indexes = [
            # Índice parcial — sólo filas activas.
            Index(
                fields=["email"],
                name="ix_active_email",
                condition=Q(deleted_at__isnull=True),
            ),
            # Índice GIN para queries de containment sobre JSONB.
            Index(fields=["payload"], method="gin", name="ix_payload_gin"),
            # Índice por expresión para lookups case-insensitive.
            Index(fields=["LOWER(email)"], name="ix_email_lower"),
            # Compuesto descendente.
            Index(fields=["-created_at", "user_id"], name="ix_recent_per_user"),
        ]
```

`method` acepta `"btree"` (default), `"hash"`, `"gin"`, `"gist"`,
`"brin"`, `"spgist"`, `"bloom"`. SQLite ignora silenciosamente y usa
B-tree.

## Seguridad de migraciones

### Creación de índice online (concurrent)

```python
from dorm.migrations.operations import AddIndex
from dorm import Index

operations = [
    AddIndex(
        "Article",
        Index(fields=["email"], name="ix_email"),
        concurrently=True,
    ),
]
```

Emite `CREATE INDEX CONCURRENTLY` en PostgreSQL — sin
`AccessExclusiveLock`, sin downtime. Debe ser la única DDL en su
fichero de migración (el executor lo enforcea para poder saltarse
el atomic envolvente). SQLite ignora la flag.

### `SetLockTimeout` y `ValidateConstraint`

```python
from dorm.migrations.operations import RunSQL, SetLockTimeout, ValidateConstraint

operations = [
    # Limita cuánto espera cualquier DDL para conseguir su lock.
    SetLockTimeout(ms=2000),

    # Añade la FK sin escanear la tabla.
    RunSQL(
        "ALTER TABLE orders ADD CONSTRAINT fk_orders_user "
        "FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID",
        reverse_sql="ALTER TABLE orders DROP CONSTRAINT fk_orders_user",
    ),

    # Valida online — solo ShareUpdateExclusive lock.
    ValidateConstraint(table="orders", name="fk_orders_user"),
]
```

El patrón `NOT VALID` + `VALIDATE CONSTRAINT` te permite añadir
foreign keys / CHECK constraints a una tabla con miles de millones
de filas sin downtime.

## Operación y herramientas

### `dorm inspectdb`

```bash
$ dorm inspectdb > legacy/models.py
```

Reverse-engineering de `models.py` desde la base de datos conectada.
Best-effort: tipos de campo, detección de FK, `db_table`. No
recupera constraints, índices, `related_name`, ni validators.
Revisa y edita antes de hacer commit.

### `dorm doctor`

```bash
$ dorm doctor
dorm doctor — 2 warning(s), 1 note(s)

warnings:
  ! DATABASES['default']: MAX_POOL_SIZE=2 is small for production; raise to 10–20...
  ! Order.user: ForeignKey without db_index; joins on this FK will sequentially scan...

notes:
  · DORM_RETRY_ATTEMPTS not set or set to 0/1: transient PG errors will surface...
```

Auditoría del runtime para detectar pies de plomo de producción.
Termina con código distinto de cero ante warnings — sirve como gate
pre-despliegue.

### URL / DSN en `DATABASES`

```python
import os, dorm

# URL directa
dorm.configure(DATABASES={
    "default": "postgres://u:p@host:5432/db?sslmode=require&MAX_POOL_SIZE=20",
})

# O con overrides — las claves del dict ganan a las de la URL
dorm.configure(DATABASES={
    "default": {
        "URL": os.environ["DATABASE_URL"],
        "MAX_POOL_SIZE": 30,
    },
})

# O usa el parser directamente
cfg = dorm.parse_database_url(os.environ["DATABASE_URL"])
```

Las variables conocidas del pool (`MAX_POOL_SIZE`, `POOL_TIMEOUT`,
`POOL_CHECK`, `MAX_IDLE`, `MAX_LIFETIME`, `PREPARE_THRESHOLD`) se
suben como claves de primer nivel; el resto cae en `OPTIONS`.

## Nuevos tipos de campo

Cuatro tipos cubren huecos clásicos del catálogo:

```python
import datetime, enum

class Priority(enum.Enum):
    LOW, MEDIUM, HIGH = "low", "medium", "high"

class Job(dorm.Model):
    name = dorm.CharField(max_length=100)
    timeout = dorm.DurationField()                       # INTERVAL / BIGINT µs
    priority = dorm.EnumField(Priority, default=Priority.LOW)
    owner_email = dorm.CITextField(unique=True)          # CITEXT / TEXT NOCASE
    seats = dorm.IntegerRangeField(null=True, blank=True)  # int4range PG
```

- **`DurationField`** almacena `datetime.timedelta`. `INTERVAL`
  nativo en PostgreSQL; en SQLite dorm registra un adaptador de
  sqlite3 para que el mismo `timedelta` viaje como microsegundos
  enteros en un `BIGINT`.
- **`EnumField(enum_cls)`** almacena un miembro de `enum.Enum`. El
  tipo de columna se deriva del valor subyacente (string → `VARCHAR`,
  int → `INTEGER`); `choices` se autopobla a partir del enum.
- **`CITextField`** mapea al `CITEXT` de PostgreSQL (requiere la
  extensión) y cae a `TEXT COLLATE NOCASE` en SQLite.
- **Campos de rango** — `IntegerRangeField`, `BigIntegerRangeField`,
  `DecimalRangeField`, `DateRangeField`, `DateTimeRangeField`. El
  valor Python es `dorm.Range(lower, upper, bounds="[)")`. SQLite
  levanta `NotImplementedError` desde `db_type()` para que la
  limitación aparezca en `migrate`.

## `FileField` y storage backends

Una capa de almacenamiento de archivos pluggable con el disco local
por defecto y un backend S3 opcional detrás del extra `s3`:

```python
class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

doc = Document(name="Q1")
doc.attachment = dorm.ContentFile(b"bytes del PDF", name="q1.pdf")
doc.save()
doc.attachment.url      # → "/media/docs/2026/04/q1.pdf"
```

La configuración sigue el patrón de `DATABASES`:

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.storage.FileSystemStorage",
        "OPTIONS": {"location": "/var/app/media", "base_url": "/media/"},
    },
}
```

Cambia el default a S3 instalando el extra y apuntando el backend a
`dorm.contrib.storage.s3.S3Storage`:

```bash
pip install 'djanorm[s3]'
```

Soporta varios alias (`storage="cold"`, etc.), así que un mismo
proyecto puede mezclar local + S3. Los backends custom solo necesitan
heredar de `dorm.storage.Storage` e implementar seis métodos.
Referencia completa en [Models & fields](models.md#archivos).

## Receivers asíncronos en señales

Las señales aceptan ahora receivers `async def`. Se conectan igual:

```python
async def reindex(sender, instance, **kw):
    await search_client.upsert(instance)

dorm.signals.post_save.connect(reindex, sender=Article, weak=False)

await Article(...).asave()        # receiver async awaiteado en orden
Article(...).save()               # receiver async se salta + WARNING
```

`Signal.asend()` es el nuevo punto de entrada que usan
`Model.asave` / `Model.adelete`. Los receivers síncronos siguen
síncronos; las corrutinas se awaitean secuencialmente en el orden de
conexión. El `send()` síncrono mantiene su semántica — los receivers
async conectados ahí se saltan con un único `WARNING` en
`dorm.signals` para que el trabajo perdido sea visible.

## `dorm dumpdata` / `dorm loaddata`

Fixtures en JSON, compatibles con la forma de Django:

```bash
dorm dumpdata blog.Author > fixtures/authors.json
dorm dumpdata --output fixtures/seed.json --indent 2
dorm loaddata fixtures/authors.json
```

La salida es `[{"model": "<app.Model>", "pk": <pk>, "fields": {...}}, …]`.
Las FKs se serializan como el PK del objetivo; las M2M como lista de
PKs. El cargador inserta dentro de un `atomic()`, así que un
registro malformado revierte el archivo entero. Se omiten `save()` y
las señales para que el seed sea determinista y rápido.

## Migración desde 2.0.x

Casi todas las features de 2.1 son aditivas — no necesitas cambios
de código a menos que hubieras declarado una subclase custom de
`Aggregate` que sobrescriba `as_sql`. En ese caso, añade `**kwargs`
para que el nuevo paso de `connection=` no provoque un `TypeError`
en compilación:

```python
# Antes
def as_sql(self, table_alias=None, *, model=None):
    ...

# Después
def as_sql(self, table_alias=None, *, model=None, **kwargs):
    ...
```

`Index(fields=["-foo"])` ahora valida con más rigor — el `-` inicial
sigue funcionando (significa DESC), pero strings con otra puntuación
levantarán excepción. Pásalos al formato de expresión (por ejemplo
`["LOWER(name)"]`).
