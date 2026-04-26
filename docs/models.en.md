# Models & fields

Every dorm model is a Python class that inherits from `dorm.Model` and
declares one field per column. The metaclass builds a `_meta` registry
that the migration system, query builder, and Pydantic adapter all
introspect.

## Anatomy of a model

```python
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()
    email = dorm.EmailField(unique=True, null=True, blank=True)

    class Meta:
        db_table = "authors"      # default: "<applabel>_<lowercase_name>"
        ordering = ["name"]       # default sort applied to every queryset
```

The implicit `id` PK is added automatically (a `BigAutoField`) unless
you declare your own primary key.

## Field reference

### Strings

| Field | DB type | Notes |
|---|---|---|
| `CharField(max_length=N)` | `VARCHAR(N)` | required `max_length` |
| `TextField()` | `TEXT` | unlimited |
| `EmailField()` | `VARCHAR(254)` | validates format on assignment |
| `URLField()` | `VARCHAR(200)` | |
| `SlugField()` | `VARCHAR(50)` | letters/digits/`-`/`_`, indexed |
| `UUIDField()` | `UUID` (PG) / `CHAR(36)` (SQLite) | |
| `IPAddressField()` / `GenericIPAddressField()` | `VARCHAR(45)` | |

### Numbers

| Field | DB type |
|---|---|
| `IntegerField()` | `INTEGER` |
| `SmallIntegerField()` | `SMALLINT` |
| `BigIntegerField()` | `BIGINT` |
| `PositiveIntegerField()` / `PositiveSmallIntegerField()` | with `CHECK` |
| `FloatField()` | `DOUBLE PRECISION` / `REAL` |
| `DecimalField(max_digits=N, decimal_places=M)` | `DECIMAL(N, M)` |

### Time

| Field | DB type |
|---|---|
| `DateField()` | `DATE` |
| `TimeField()` | `TIME` |
| `DateTimeField(auto_now_add=False, auto_now=False)` | `TIMESTAMP` |

`auto_now_add` populates on insert; `auto_now` overwrites on every save.

### Booleans

`BooleanField()` — `BOOLEAN` (PG) / `INTEGER 0|1` (SQLite). Defaults
are emitted vendor-aware (`DEFAULT TRUE` vs `DEFAULT 1`).

### Structured data

| Field | DB type |
|---|---|
| `JSONField()` | `JSONB` (PG) / `TEXT` (SQLite) |
| `BinaryField()` | `BYTEA` / `BLOB` |
| `ArrayField(base_field)` | `<inner>[]` (PG only — raises on SQLite) |

### Relationships

```python
class Book(dorm.Model):
    title = dorm.CharField(max_length=200)
    # one-to-many
    author = dorm.ForeignKey(
        Author, on_delete=dorm.CASCADE, related_name="books"
    )
    # one-to-one
    cover = dorm.OneToOneField(
        "Cover", on_delete=dorm.SET_NULL, null=True
    )

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags = dorm.ManyToManyField("Tag", related_name="articles")
```

`on_delete` accepts `CASCADE`, `PROTECT`, `SET_NULL`, `SET_DEFAULT`,
`DO_NOTHING`, `RESTRICT` — same semantics as Django.

The FK descriptor exposes:

- `book.author` → the related `Author` instance (lazy fetch + cache)
- `book.author_id` → the raw int PK (typed as `int | None`)

For static type checking on `<fk>_id`, add a class-level annotation:

```python
class Book(dorm.Model):
    author = dorm.ForeignKey(Author, ...)
    author_id: int | None        # ← lets ty/mypy/pyright see it
```

## Common field options

Every field accepts:

| Option | Effect |
|---|---|
| `null=True` | column allows `NULL` (DB-level) |
| `blank=True` | empty string OK (validation-level, not DB) |
| `unique=True` | adds `UNIQUE` constraint |
| `db_index=True` | adds an index |
| `db_column="x"` | override column name (default: field name) |
| `default=value` or `default=callable` | row-level default |
| `validators=[fn, ...]` | run on assignment + `full_clean()` |
| `choices=[(value, label), …]` | restrict to a fixed set |
| `editable=False` | hidden from forms / serializers |
| `help_text="..."` | docs string |

## Meta options

```python
class Author(dorm.Model):
    ...
    class Meta:
        db_table = "authors"
        ordering = ["name", "-age"]            # default sort
        unique_together = [("first_name", "last_name")]
        indexes = [dorm.Index(fields=["name"], name="author_name_idx")]
        abstract = False                       # set True for mixins
        app_label = "blog"                     # rarely needed
```

### Abstract base classes

```python
class TimestampedModel(dorm.Model):
    created_at = dorm.DateTimeField(auto_now_add=True)
    updated_at = dorm.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Post(TimestampedModel):                 # inherits the timestamps
    title = dorm.CharField(max_length=200)
```

`abstract = True` means: no DB table, no migrations; concrete subclasses
inherit the field declarations as if they had been written there.

## Type safety

Every field is `Field[T]` (a `Generic` parameterised by the stored
Python type). The descriptor's overloaded `__get__` means:

- `Author.name` → `Field[str]` (the descriptor itself, for migrations
  and `_meta` introspection)
- `author.name` → `str` (the actual value)

So `user.name + " hi"` is fine, `user.age + " hi"` is flagged by your
type checker. Same idea SQLAlchemy 2.0 introduced with `Mapped[T]`.

## Validation

Field-level validation runs when you assign or construct:

```python
>>> Author(name="x", age=10, email="not-an-email")
ValidationError: {'email': "'not-an-email' is not a valid email address."}
```

For richer logic, override `clean()` on the model and call
`obj.full_clean()` before saving:

```python
class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

    def clean(self):
        if self.age < 0:
            raise dorm.ValidationError({"age": "must be >= 0"})
```

`full_clean()` runs `clean_fields()` (per-field validation) → `clean()`
(custom) → `validate_unique()` (DB uniqueness check).

## Signals

```python
from dorm.signals import pre_save, post_save

def slugify(sender, instance, **kwargs):
    if not instance.slug:
        instance.slug = slugify(instance.title)

pre_save.connect(slugify, sender=Article)
```

Available signals: `pre_save`, `post_save`, `pre_delete`, `post_delete`,
`pre_query`, `post_query`. Signals fire for both sync and async
operations.
