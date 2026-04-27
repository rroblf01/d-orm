# Modelos y campos

Cada modelo dorm es una clase Python que hereda de `dorm.Model` y
declara un campo por columna. La metaclase construye un registro
`_meta` que la suite de migraciones, el query builder y el adaptador
Pydantic introspectan.

## Anatomía de un modelo

```python
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()
    email = dorm.EmailField(unique=True, null=True, blank=True)

    class Meta:
        db_table = "authors"      # default: "<applabel>_<lowercase_name>"
        ordering = ["name"]       # orden por defecto en cada queryset
```

Si no declaras una primary key, dorm añade automáticamente una `id`
implícita (un `BigAutoField`).

## Referencia de campos

### Strings

| Campo | Tipo BD | Notas |
|---|---|---|
| `CharField(max_length=N)` | `VARCHAR(N)` | `max_length` obligatorio |
| `TextField()` | `TEXT` | sin límite |
| `EmailField()` | `VARCHAR(254)` | valida el formato al asignar |
| `URLField()` | `VARCHAR(200)` | |
| `SlugField()` | `VARCHAR(50)` | letras/dígitos/`-`/`_`, indexado |
| `UUIDField()` | `UUID` (PG) / `CHAR(36)` (SQLite) | |
| `IPAddressField()` / `GenericIPAddressField()` | `VARCHAR(45)` | |

### Números

| Campo | Tipo BD |
|---|---|
| `IntegerField()` | `INTEGER` |
| `SmallIntegerField()` | `SMALLINT` |
| `BigIntegerField()` | `BIGINT` |
| `PositiveIntegerField()` / `PositiveSmallIntegerField()` | con `CHECK` |
| `FloatField()` | `DOUBLE PRECISION` / `REAL` |
| `DecimalField(max_digits=N, decimal_places=M)` | `DECIMAL(N, M)` |

### Tiempo

| Campo | Tipo BD |
|---|---|
| `DateField()` | `DATE` |
| `TimeField()` | `TIME` |
| `DateTimeField(auto_now_add=False, auto_now=False)` | `TIMESTAMP` |
| `DurationField()` | `INTERVAL` (PG) / `BIGINT` µs (SQLite) |

`auto_now_add` rellena al insertar; `auto_now` reescribe en cada save.

`DurationField` almacena un `datetime.timedelta`. En PostgreSQL mapea
a `INTERVAL` nativo (psycopg adapta `timedelta` directamente). SQLite
no tiene tipo intervalo, así que dorm registra un adaptador de
sqlite3 que guarda la duración como microsegundos enteros en un
`BIGINT` — el valor Python siempre es un `timedelta`, la codificación
es invisible.

```python
import datetime

class Job(dorm.Model):
    timeout = dorm.DurationField()
    grace = dorm.DurationField(null=True, blank=True)

Job.objects.create(timeout=datetime.timedelta(minutes=5))
```

### Booleanos

`BooleanField()` — `BOOLEAN` (PG) / `INTEGER 0|1` (SQLite). Los defaults
se emiten conscientes del vendor (`DEFAULT TRUE` vs `DEFAULT 1`).

### Enumeraciones

`EnumField(enum_cls, max_length=None)` almacena un miembro de
`enum.Enum`. El tipo de columna se deriva del tipo subyacente del
enum: enums con valores string mapean a `VARCHAR(max_length)`, enums
con valores int a `INTEGER`. La instancia Python siempre lleva el
*miembro* del enum; las lecturas desde BD rehidratan vía
`enum_cls(value)`. `choices` se autopobla para capas de admin /
formularios.

```python
import enum

class Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"

class Article(dorm.Model):
    status = dorm.EnumField(Status, default=Status.ACTIVE)

Article.objects.filter(status=Status.ACTIVE)   # miembro
Article.objects.filter(status="active")        # también acepta valor crudo
```

### Texto case-insensitive

`CITextField()` — columna de texto case-insensitive. Mapea a `CITEXT`
de PostgreSQL (la BD necesita la extensión `citext`; instálala con
`RunSQL("CREATE EXTENSION IF NOT EXISTS citext")` desde una
migración). En SQLite cae a `TEXT COLLATE NOCASE` para que las
comparaciones de igualdad / `LIKE` se comporten igual sin la
extensión.

```python
class User(dorm.Model):
    email = dorm.CITextField(unique=True)

# las dos triunfan y encuentran la misma fila:
User.objects.get(email="Alice@example.com")
User.objects.get(email="alice@example.com")
```

### Datos estructurados

| Campo | Tipo BD |
|---|---|
| `JSONField()` | `JSONB` (PG) / `TEXT` (SQLite) |
| `BinaryField()` | `BYTEA` / `BLOB` |
| `ArrayField(base_field)` | `<inner>[]` (solo PG — falla en SQLite) |

### Archivos

`FileField(upload_to="", *, storage=None, max_length=255)` almacena
un fichero vía un [storage pluggable](#storage-backends). La columna
en BD es un `VARCHAR(max_length)` que guarda el nombre del storage
(path relativo / clave S3); el valor Python es un wrapper
`FieldFile` que devuelve el descriptor.

```python
class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

doc = Document(name="Informe Q1")
doc.attachment = dorm.ContentFile(b"bytes del PDF", name="q1.pdf")
doc.save()                     # escribe en storage, guarda nombre en BD

doc.attachment.url             # storage.url(name) — path local o URL S3
doc.attachment.size            # storage.size(name)
with doc.attachment.open("rb") as fh:
    payload = fh.read()
doc.attachment.delete()        # borra el fichero + limpia la columna
```

`upload_to` acepta:

- un string estático (`"docs/"`).
- una plantilla `strftime` (`"docs/%Y/%m/"`, se expande al guardar).
- un callable `f(instance, filename) -> str` para paths totalmente
  dinámicos.

`storage` acepta una instancia `Storage`, un alias resuelto contra
`settings.STORAGES` (por defecto `"default"`), o `None` para diferir
la búsqueda hasta el primer uso. Sobre `null=True` mira el bloque de
config más abajo — muy recomendado.

#### Storage backends

La configuración sigue el mismo esquema `BACKEND + OPTIONS` que
`DATABASES`:

```python
# settings.py — sistema de archivos local (default si STORAGES no se define)
STORAGES = {
    "default": {
        "BACKEND": "dorm.storage.FileSystemStorage",
        "OPTIONS": {
            "location": "/var/app/media",
            "base_url": "/media/",
        },
    }
}
```

Para usar S3, instala el extra opcional y cambia el backend:

```bash
pip install 'djanorm[s3]'
```

```python
STORAGES = {
    "default": {
        "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        "OPTIONS": {
            "bucket_name": "my-app-uploads",
            "region_name": "eu-west-1",
            "default_acl": "private",
            "querystring_auth": True,     # URLs firmadas
            "querystring_expire": 3600,
            # endpoint_url= para MinIO / R2 / Backblaze B2.
        },
    }
}
```

Puedes mezclar backends — declara varios alias y eliges por campo:

```python
class Avatar(dorm.Model):
    image = dorm.FileField(upload_to="avatars/", storage="public")
    backup = dorm.FileField(upload_to="archive/", storage="cold")
```

De serie dorm trae:

| Backend | Módulo | Extra |
|---|---|---|
| `FileSystemStorage` | `dorm.storage` | core |
| `S3Storage` | `dorm.contrib.storage.s3` | `s3` (boto3) |

Para enchufar el tuyo (Azure Blob, GCS, encriptado en reposo),
hereda de `dorm.storage.Storage` e implementa `_save`, `_open`,
`delete`, `exists`, `size`, `url`. Los métodos async heredan de la
clase base (envuelven los sync con `asyncio.to_thread`);
sobrescríbelos si tu SDK es nativamente async.

#### Consejos

- **Declara siempre `null=True, blank=True`** en campos de archivo
  opcionales. Un `FileField` sin set bindea `NULL` al insertar; una
  columna no-null rechazaría la fila.
- **`MEDIA_URL` es una preocupación solo del ORM** — dorm no sirve
  los ficheros. Conecta tu framework (FastAPI `StaticFiles`, nginx
  `alias`, etc.) para exponer `location` en `base_url`.
- **`default_storage`** es un proxy a nivel de módulo que se re-resuelve
  en cada llamada, así que `dorm.configure(STORAGES=...)` después
  del import surte efecto al instante.

### Tipos de rango (solo PostgreSQL)

| Campo | Tipo BD |
|---|---|
| `IntegerRangeField()` | `int4range` |
| `BigIntegerRangeField()` | `int8range` |
| `DecimalRangeField()` | `numrange` |
| `DateRangeField()` | `daterange` |
| `DateTimeRangeField()` | `tstzrange` |

El tipo de valor Python es `dorm.Range(lower, upper, bounds="[)")`.
`bounds` son dos caracteres con la inclusividad de los extremos:
`"[)"` (el por defecto), `"(]"`, `"[]"` o `"()"`. Cualquiera de los
dos extremos puede ser `None` para indicar "sin cota por ese lado".

```python
import datetime

class Reservation(dorm.Model):
    during = dorm.DateTimeRangeField()
    seats = dorm.IntegerRangeField(null=True, blank=True)

Reservation.objects.create(
    during=dorm.Range(
        datetime.datetime(2026, 1, 1, 9, tzinfo=datetime.timezone.utc),
        datetime.datetime(2026, 1, 1, 17, tzinfo=datetime.timezone.utc),
    ),
    seats=dorm.Range(1, 10),
)
```

PostgreSQL canoniza los rangos *discretos* (`int4range`, `int8range`,
`daterange`) al salir — `(1, 5]` siempre vuelve como `[2, 6)`. Los
rangos continuos (`numrange`, `tstzrange`) preservan los bounds
escritos. SQLite no tiene tipo de rango nativo; usar uno de estos
campos contra una conexión SQLite levanta `NotImplementedError` desde
`db_type()` para que la limitación aparezca al hacer migrate, no en
la primera query.

### Relaciones

```python
class Book(dorm.Model):
    title = dorm.CharField(max_length=200)
    # uno-a-muchos
    author = dorm.ForeignKey(
        Author, on_delete=dorm.CASCADE, related_name="books"
    )
    # uno-a-uno
    cover = dorm.OneToOneField(
        "Cover", on_delete=dorm.SET_NULL, null=True
    )

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags = dorm.ManyToManyField("Tag", related_name="articles")
```

`on_delete` acepta `CASCADE`, `PROTECT`, `SET_NULL`, `SET_DEFAULT`,
`DO_NOTHING`, `RESTRICT` — semántica idéntica a Django.

El descriptor de FK expone:

- `book.author` → la instancia `Author` relacionada (fetch + caché)
- `book.author_id` → el PK entero crudo (tipado como `int | None`)

Para que el type-checker vea `<fk>_id`, añade una anotación de clase:

```python
class Book(dorm.Model):
    author = dorm.ForeignKey(Author, ...)
    author_id: int | None        # ← lo verán ty/mypy/pyright
```

## Opciones comunes de campo

Todo campo acepta:

| Opción | Efecto |
|---|---|
| `null=True` | la columna permite `NULL` (a nivel BD) |
| `blank=True` | string vacío permitido (validación, no BD) |
| `unique=True` | añade restricción `UNIQUE` |
| `db_index=True` | crea un índice |
| `db_column="x"` | override del nombre de la columna |
| `default=value` o `default=callable` | valor por defecto |
| `validators=[fn, ...]` | se ejecutan al asignar y en `full_clean()` |
| `choices=[(value, label), …]` | restringe a un conjunto fijo |
| `editable=False` | oculto a forms / serializers |
| `help_text="..."` | string de docs |

## Opciones Meta

```python
class Author(dorm.Model):
    ...
    class Meta:
        db_table = "authors"
        ordering = ["name", "-age"]            # orden por defecto
        unique_together = [("first_name", "last_name")]
        indexes = [dorm.Index(fields=["name"], name="author_name_idx")]
        abstract = False                       # True para mixins
        app_label = "blog"                     # rara vez necesario
```

### Clases base abstractas

```python
class TimestampedModel(dorm.Model):
    created_at = dorm.DateTimeField(auto_now_add=True)
    updated_at = dorm.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Post(TimestampedModel):                 # hereda timestamps
    title = dorm.CharField(max_length=200)
```

`abstract = True` significa: sin tabla en BD, sin migraciones; las
subclases concretas heredan los campos como si los hubieran declarado.

## Tipado

Cada campo es `Field[T]` (un `Generic` parametrizado por el tipo
Python que almacena). El `__get__` sobrecargado del descriptor hace que:

- `Author.name` → `Field[str]` (el descriptor en sí, para introspección
  de migraciones y `_meta`)
- `author.name` → `str` (el valor real)

Así `user.name + " hi"` está bien, `user.age + " hi"` lo flagea el
type-checker. Mismo truco que SQLAlchemy 2.0 introdujo con `Mapped[T]`.

## Validación

La validación a nivel de campo se ejecuta al asignar/construir:

```python
>>> Author(name="x", age=10, email="not-an-email")
ValidationError: {'email': "'not-an-email' is not a valid email address."}
```

Para lógica más rica, sobrescribe `clean()` en el modelo y llama a
`obj.full_clean()` antes de guardar:

```python
class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

    def clean(self):
        if self.age < 0:
            raise dorm.ValidationError({"age": "debe ser >= 0"})
```

`full_clean()` ejecuta `clean_fields()` → `clean()` → `validate_unique()`.

## Señales

```python
from dorm.signals import pre_save, post_save

def slugify(sender, instance, **kwargs):
    if not instance.slug:
        instance.slug = slugify(instance.title)

pre_save.connect(slugify, sender=Article)
```

Señales disponibles: `pre_save`, `post_save`, `pre_delete`,
`post_delete`, `pre_query`, `post_query`. Disparan tanto en operaciones
sync como async.

Para la referencia completa — kwargs que recibe cada señal, la
diferencia entre `sender` para save/delete (clase modelo) y `sender`
para señales de query (string del vendor), `dispatch_uid` para
registración idempotente, referencias débiles, y los pitfalls sobre
tragado de excepciones y recursión — mira la
[guía de Señales](signals.md).
