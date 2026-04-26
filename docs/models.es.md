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

`auto_now_add` rellena al insertar; `auto_now` reescribe en cada save.

### Booleanos

`BooleanField()` — `BOOLEAN` (PG) / `INTEGER 0|1` (SQLite). Los defaults
se emiten conscientes del vendor (`DEFAULT TRUE` vs `DEFAULT 1`).

### Datos estructurados

| Campo | Tipo BD |
|---|---|
| `JSONField()` | `JSONB` (PG) / `TEXT` (SQLite) |
| `BinaryField()` | `BYTEA` / `BLOB` |
| `ArrayField(base_field)` | `<inner>[]` (solo PG — falla en SQLite) |

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
