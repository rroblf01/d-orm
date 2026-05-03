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
| `PositiveIntegerField()` / `PositiveSmallIntegerField()` / `PositiveBigIntegerField()` (3.1+) | with `CHECK` |
| `FloatField()` | `DOUBLE PRECISION` / `REAL` |
| `DecimalField(max_digits=N, decimal_places=M)` | `DECIMAL(N, M)` |

### Time

| Field | DB type |
|---|---|
| `DateField()` | `DATE` |
| `TimeField()` | `TIME` |
| `DateTimeField(auto_now_add=False, auto_now=False)` | `TIMESTAMP` |
| `DurationField()` | `INTERVAL` (PG) / `BIGINT` µs (SQLite) |

`auto_now_add` populates on insert; `auto_now` overwrites on every save.

`DurationField` stores a `datetime.timedelta`. On PostgreSQL it maps
to native `INTERVAL` (psycopg adapts `timedelta` directly). SQLite has
no interval type, so dorm registers a sqlite3 adapter that stores the
duration as integer microseconds in a `BIGINT` — the Python value is
always a `timedelta`, the encoding is invisible.

```python
import datetime

class Job(dorm.Model):
    timeout = dorm.DurationField()
    grace = dorm.DurationField(null=True, blank=True)

Job.objects.create(timeout=datetime.timedelta(minutes=5))
```

### Booleans

`BooleanField()` — `BOOLEAN` (PG) / `INTEGER 0|1` (SQLite). Defaults
are emitted vendor-aware (`DEFAULT TRUE` vs `DEFAULT 1`).

### Enumerations

`EnumField(enum_cls, max_length=None)` stores a `enum.Enum` member.
The column type is derived from the enum's underlying value:
string-valued enums become `VARCHAR(max_length)`, integer-valued enums
become `INTEGER`. The Python instance always carries the enum
*member*; reads from the DB rehydrate via `enum_cls(value)`.
`choices` is auto-populated for admin / form layers.

```python
import enum

class Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"

class Article(dorm.Model):
    status = dorm.EnumField(Status, default=Status.ACTIVE)

Article.objects.filter(status=Status.ACTIVE)   # member
Article.objects.filter(status="active")        # raw value also accepted
```

### Case-insensitive text

`CITextField()` — case-insensitive text column. Maps to PostgreSQL's
`CITEXT` (the database needs the `citext` extension; install via
`RunSQL("CREATE EXTENSION IF NOT EXISTS citext")` in a migration). On
SQLite, falls back to `TEXT COLLATE NOCASE` so equality / `LIKE`
queries behave the same way without the extension.

```python
class User(dorm.Model):
    email = dorm.CITextField(unique=True)

# both succeed and find the same row:
User.objects.get(email="Alice@example.com")
User.objects.get(email="alice@example.com")
```

### Structured data

| Field | DB type |
|---|---|
| `JSONField()` | `JSONB` (PG) / `TEXT` (SQLite) |
| `BinaryField()` | `BYTEA` / `BLOB` |
| `ArrayField(base_field)` | `<inner>[]` (PG only — raises on SQLite) |

### Files

`FileField(upload_to="", *, storage=None, max_length=255)` stores a
file via a pluggable [storage backend](#storage-backends). The DB
column itself is a `VARCHAR(max_length)` holding the storage name
(relative path / S3 key); the Python value is a `FieldFile` wrapper
returned by the descriptor.

```python
class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

doc = Document(name="Q1 report")
doc.attachment = dorm.ContentFile(b"PDF bytes here", name="q1.pdf")
doc.save()                     # writes to storage, stores name in DB

doc.attachment.url             # storage.url(name) — local path or S3 URL
doc.attachment.size            # storage.size(name)
with doc.attachment.open("rb") as fh:
    payload = fh.read()
doc.attachment.delete()        # removes file + clears column
```

`upload_to` accepts:

- a static string (`"docs/"`).
- a `strftime` template (`"docs/%Y/%m/"`, expanded at save time).
- a callable `f(instance, filename) -> str` for fully dynamic paths
  — see [Dynamic upload paths](#dynamic-upload-paths) below.

`storage` accepts a `Storage` instance, an alias resolved against
`settings.STORAGES` (default `"default"`), or `None` to defer the
lookup to first use. For `null=True` see the configuration block
below — it's strongly recommended.

#### Dynamic upload paths

When you need to compute the storage path from the model instance —
tenant-isolated folders, route-by-extension, content-addressed
layouts — pass a callable instead of a string. dorm invokes it as
`upload_to(instance, filename)` at save time and uses the returned
string as the full storage name.

```python
def upload_owner_scoped(instance, filename):
    """Each user's uploads live under their own prefix so a
    misconfigured ACL can't leak across accounts."""
    return f"users/{instance.owner_id}/{filename}"


class Document(dorm.Model):
    owner = dorm.ForeignKey(User, on_delete=dorm.CASCADE)
    attachment = dorm.FileField(upload_to=upload_owner_scoped, null=True)
```

The callable receives the *fully populated* model instance, so any
attribute that's set at save time is fair game:

```python
import os, hashlib

def upload_by_extension(instance, filename):
    """Route uploads to per-mime buckets so the CDN's cache rules
    can target each shape differently."""
    bucket = {".pdf": "documents", ".png": "images", ".jpg": "images"}
    _, ext = os.path.splitext(filename)
    return f"{bucket.get(ext.lower(), 'other')}/{filename}"


def upload_content_addressed(instance, filename):
    """Content-addressed layout — the storage name is the hash of
    the model's identity. Useful for dedup-friendly storage."""
    digest = hashlib.sha256(
        f"{instance.owner_id}|{filename}".encode()
    ).hexdigest()[:16]
    _, ext = os.path.splitext(filename)
    return f"cas/{digest}{ext}"
```

Lambdas work too:

```python
attachment = dorm.FileField(
    upload_to=lambda instance, filename: f"by-name/{instance.slug}/{filename}",
)
```

**Migration round-trip.** `dorm makemigrations` can serialise a
**module-level** callable by emitting `upload_to=upload_owner_scoped`
plus the matching `from yourapp.uploads import upload_owner_scoped`
import in the migration's header. **Lambdas and nested functions
can't be round-tripped** (they have no stable importable name); the
writer leaves a `FIXME` marker in the generated file and the user
edits it by hand. So if the model ever needs to round-trip through
makemigrations, declare the callable at module scope:

```python
# yourapp/uploads.py — module-level, importable.
def upload_owner_scoped(instance, filename):
    return f"users/{instance.owner_id}/{filename}"

# yourapp/models.py
from .uploads import upload_owner_scoped

class Document(dorm.Model):
    attachment = dorm.FileField(upload_to=upload_owner_scoped)
```

**Path safety.** The basename returned by the callable goes through
`Storage.get_valid_name` (strips path separators, normalises unsafe
chars), and `FileSystemStorage._resolve_path` rejects any final path
that escapes the storage root. So even if your callable accidentally
splices a user-controlled string into the directory portion, the
underlying writer can't be tricked into climbing out of `location`.

#### Storage backends

Configuration follows the same `BACKEND + OPTIONS` shape as
`DATABASES`:

```python
# settings.py — local filesystem (the default if STORAGES is unset)
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

To use S3, install the optional extra and switch the backend:

```bash
pip install 'djanorm[s3]'
```

```python
# settings.py — production AWS S3
STORAGES = {
    "default": {
        "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        "OPTIONS": {
            "bucket_name": "my-app-uploads",
            "region_name": "eu-west-1",
            # Keys are picked up from the IAM role / env vars / `~/.aws/`
            # by default — don't hardcode them in source. The
            # ``access_key`` / ``secret_key`` options exist for
            # development scenarios (MinIO below); production should
            # leave them unset so boto3 uses the ambient creds chain.
            "default_acl": "private",
            "querystring_auth": True,     # presigned URLs
            "querystring_expire": 3600,
        },
    }
}
```

The same `S3Storage` works against any S3-compatible service —
**MinIO** for local development, **Cloudflare R2**, **Backblaze B2**,
**DigitalOcean Spaces**. Set `endpoint_url` and force path-style
addressing (most non-AWS endpoints don't support virtual-hosted
sub-domains over IP):

```bash
# Spin up MinIO locally — no AWS account, no costs.
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"

# Create the bucket via the console at http://localhost:9001
# (login: minioadmin / minioadmin) or with `mc`.
```

```python
# settings.py — local development against MinIO.
STORAGES = {
    "default": {
        "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        "OPTIONS": {
            "bucket_name": "dev-uploads",
            "endpoint_url": "http://localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "region_name": "us-east-1",     # MinIO ignores it but boto3 needs *something*
            "signature_version": "s3v4",
            "addressing_style": "path",     # required: MinIO over IP can't do virtual-hosted
        },
    }
}
```

The application code is identical — same `FileField`, same
`obj.attachment.save(...)`, same `obj.attachment.url`. Switching
between local FileSystemStorage, MinIO and AWS is purely a
`STORAGES` change.

You can mix backends — declare multiple aliases and pick per field:

```python
class Avatar(dorm.Model):
    image = dorm.FileField(upload_to="avatars/", storage="public")
    backup = dorm.FileField(upload_to="archive/", storage="cold")
```

Out of the box dorm ships:

| Backend | Module | Extra |
|---|---|---|
| `FileSystemStorage` | `dorm.storage` | core |
| `S3Storage` | `dorm.contrib.storage.s3` | `s3` (boto3) |

To plug in your own (Azure Blob, GCS, encrypted-at-rest), subclass
`dorm.storage.Storage` and implement `_save`, `_open`, `delete`,
`exists`, `size`, `url`. Async methods inherit from the base class
(they wrap the sync ones in `asyncio.to_thread`); override them
directly if your SDK is natively async.

#### Tips

- **Always declare `null=True, blank=True`** on optional file fields.
  An unset `FileField` binds `NULL` on insert; a non-null column
  would reject the row.
- **`MEDIA_URL` is an ORM-side concern only** — dorm does not serve
  the files. Wire your web framework (FastAPI `StaticFiles`, nginx
  `alias`, etc.) to expose `location` at `base_url`.
- **`default_storage`** is a module-level proxy that re-resolves on
  every call, so `dorm.configure(STORAGES=...)` after import time
  takes effect immediately.
- **Files written inside `atomic()` are cleaned up on rollback.**
  `FileField.pre_save` registers an `on_rollback` hook that calls
  `storage.delete(name)` if the surrounding transaction rolls back,
  so a `BusinessRuleViolation` mid-block leaves no orphan bytes on
  disk / S3. Savepoint rollbacks clean up only the files written
  inside that savepoint; the outer commit (if any) preserves the
  rest. Outside `atomic()`, no cleanup is registered — saves are
  fire-and-forget. See [Transactions: cleanup on
  rollback](transactions.md#cleanup-on-rollback-on_rollback) for
  the underlying API.
- **Replacing a file does not delete the old one.** Reassigning
  `obj.attachment = ContentFile(...)` and saving writes the new
  file but leaves the previous one on storage. If you need
  delete-on-replace semantics, call `obj.attachment.delete(save=False)`
  before assigning the replacement, or schedule the cleanup yourself
  via `on_commit`.

#### `ImageField`

`ImageField(upload_to="", *, storage=None, max_length=255)` is a
specialised `FileField` that validates the upload is a real image
before writing it to storage — so a user can't slip a `.exe`
through with a renamed extension.

```python
class Avatar(dorm.Model):
    user = dorm.ForeignKey(User, on_delete=dorm.CASCADE)
    image = dorm.ImageField(upload_to="avatars/%Y/%m/", null=True, blank=True)
```

Validation uses **Pillow** when installed; otherwise it falls back
to a magic-bytes sniff that recognises PNG / JPEG / GIF / WebP /
TIFF / BMP. Install the optional `image` extra to make Pillow the
canonical validator (and unlock things like reading dimensions or
re-encoding before save in user code):

```bash
pip install 'djanorm[image]'
```

Everything you can do with `FileField` (dynamic `upload_to`,
`STORAGES` aliases, S3 / MinIO, atomic-rollback cleanup) works
identically on `ImageField` — the only difference is the upfront
content-type check at assignment time.

### Range types (PostgreSQL only)

| Field | DB type |
|---|---|
| `IntegerRangeField()` | `int4range` |
| `BigIntegerRangeField()` | `int8range` |
| `DecimalRangeField()` | `numrange` |
| `DateRangeField()` | `daterange` |
| `DateTimeRangeField()` | `tstzrange` |

The Python value type is `dorm.Range(lower, upper, bounds="[)")`.
`bounds` is two characters denoting endpoint inclusivity — `"[)"` (the
default), `"(]"`, `"[]"`, or `"()"`. Either endpoint may be `None` to
mean "unbounded on that side".

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

PostgreSQL canonicalises *discrete* ranges (`int4range`, `int8range`,
`daterange`) on the way out — `(1, 5]` always returns as `[2, 6)`.
Continuous ranges (`numrange`, `tstzrange`) preserve the bounds you
wrote. SQLite has no native range type; using one of these fields on
a SQLite connection raises `NotImplementedError` from `db_type()` so
the limitation surfaces at migrate time, not at first query.

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

### Composite primary keys

`CompositePrimaryKey(*field_names)` declares that the table's primary
key spans more than one column. The component fields are real,
concrete fields you also declare in the model body — the composite
just tells the migration writer to emit `PRIMARY KEY (col1, col2)`
and tells the ORM to address rows by tuple.

```python
class OrderLine(dorm.Model):
    order_id = dorm.IntegerField()
    line_no = dorm.IntegerField()
    sku = dorm.CharField(max_length=50)
    qty = dorm.IntegerField(default=1)

    pk = dorm.CompositePrimaryKey("order_id", "line_no")
```

CRUD by tuple `pk`:

```python
line = OrderLine.objects.create(order_id=1, line_no=1, sku="A", qty=2)
line.pk                             # (1, 1)

OrderLine.objects.get(pk=(1, 1))    # tuple lookup
OrderLine.objects.filter(pk=(1, 1)) # decomposed into per-component WHERE
line.delete()                       # uses (order_id=…, line_no=…)
```

Limitations to know up front:

- A `CompositePrimaryKey` cannot be the *target* of a `ForeignKey`
  — single-column FKs can't reference a multi-column key. If you
  need cross-table referencing, declare a synthetic surrogate PK
  and a `UniqueConstraint` over the composite columns.
- No component is auto-incrementing; you supply both values on
  insert.
- `filter(pk__in=[...])` over composite keys is not supported. Use
  `Q` objects with explicit per-component clauses.

### Generic relations (polymorphic FKs)

For the case where one model needs to point at "any other model"
— think tags, comments, audit-log entries — use the
``dorm.contrib.contenttypes`` helpers. They mirror Django's
``django.contrib.contenttypes``: a ``ContentType`` registry plus
two field types that compose ``content_type`` (FK to
``ContentType``) + ``object_id`` (integer column) into a
polymorphic FK.

Add the app to your settings and run the migrations once so the
``django_content_type`` table exists:

```python
# settings.py
INSTALLED_APPS = ["dorm.contrib.contenttypes", "myapp"]
```

```bash
dorm makemigrations
dorm migrate
```

Then declare the polymorphic side and the reverse accessor:

```python
import dorm
from dorm.contrib.contenttypes import (
    ContentType,
    GenericForeignKey,
    GenericRelation,
)

class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags = GenericRelation("Tag")          # reverse accessor — no column

class Book(dorm.Model):
    name = dorm.CharField(max_length=200)
    tags = GenericRelation("Tag")

class Tag(dorm.Model):
    label = dorm.CharField(max_length=50)
    content_type = dorm.ForeignKey(ContentType, on_delete=dorm.CASCADE)
    content_type_id: int | None
    object_id = dorm.PositiveIntegerField()
    target = GenericForeignKey("content_type", "object_id")
```

Forward access:

```python
article = Article.objects.create(title="Hello")
tag = Tag(label="featured")
tag.target = article                       # sets content_type + object_id
tag.save()

reloaded = Tag.objects.get(pk=tag.pk)
isinstance(reloaded.target, Article)        # True
```

Reverse access via `GenericRelation`:

```python
article.tags.create(label="urgent")
list(article.tags.all())                   # all Tags pointing at article
article.tags.filter(label__startswith="u").count()
```

Async paths exist alongside the sync ones:

```python
ct = await ContentType.objects.aget_for_model(Article)
target = await tag.target.aget(tag) if tag.target is None else tag.target
```

`ContentType.objects.get_for_model(MyModel)` memoises the row per
process — repeated polymorphic lookups don't pay a round-trip per
access. If your tests recreate models or truncate the table, call
`ContentType.objects.clear_cache()` to invalidate.

When you iterate a queryset of polymorphic-tagged rows, reach for
`prefetch_related("target")` — the descriptor's per-row `get(pk=…)`
collapses to **1 + 1 + K** queries (one for the tags, one for every
referenced `ContentType` in bulk, one per concrete target model). See
[`prefetch_related` polymorphic FKs](queries.md#polymorphic-fks-genericforeignkey)
in the queries guide.

## Common field options

Every field accepts:

| Option | Effect |
|---|---|
| `null=True` | column allows `NULL` (DB-level) |
| `blank=True` | empty string OK (validation-level, not DB) |
| `unique=True` | adds `UNIQUE` constraint |
| `db_index=True` | adds an index |
| `db_column="x"` | override column name (default: field name) |
| `default=value` or `default=callable` | row-level default (Python-side, fires when constructor doesn't see a value) |
| `db_default=value` or `db_default=RawSQL("now()")` | server-side default — lands in `CREATE TABLE` as `DEFAULT <literal>`; covers raw INSERTs that omit the column |
| `db_comment="..."` (3.1+) | column-level comment for schema-archaeology tooling. PG / MySQL emit `COMMENT ON COLUMN`; SQLite ignores |
| `validators=[fn, ...]` | run on assignment + `full_clean()` |
| `choices=[(value, label), …]` | restrict to a fixed set |
| `editable=False` | hidden from forms / serializers |
| `help_text="..."` | docs string |

### `default` vs `db_default`

```python
import dorm
from dorm.expressions import RawSQL

class Event(dorm.Model):
    # Python default: fires when ``Event(...)`` is built without
    # the kwarg. Dynamic — runs every time on the application.
    correlation_id = dorm.UUIDField(default=uuid.uuid4)

    # Server-side default: lands in DDL as ``DEFAULT now()``. Raw
    # INSERTs that omit the column (think: a partner system writing
    # to the table directly) still get a sane value.
    created_at = dorm.DateTimeField(db_default=RawSQL("now()"))

    # Both at once: ``default`` covers Python writes, ``db_default``
    # covers raw SQL writes. They target different paths and don't
    # conflict.
    revision = dorm.IntegerField(default=1, db_default=1)
```

`RawSQL` is the escape hatch for vendor-specific server-side
defaults (`now()`, `gen_random_uuid()`, sequence calls). The string
is spliced verbatim — pick one your vendor recognises.

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

### Index extras (3.1+)

`Index(include=[...])` emits a PostgreSQL **covering index**:

```python
indexes = [
    dorm.Index(
        fields=["email"],
        name="ix_user_email_cover",
        include=["full_name", "is_active"],
    ),
]
# PG: CREATE INDEX ... ON "users" ("email") INCLUDE ("full_name", "is_active")
# SQLite / MySQL: silently ignore the INCLUDE clause
```

The included columns travel with the index pages, so the planner
satisfies index-only scans for `SELECT email, full_name, is_active
WHERE email = ?` without a heap fetch.

### `UniqueConstraint(deferrable=, include=)` (3.1+)

```python
constraints = [
    # Deferred unique check — evaluated at COMMIT, not statement-end.
    # Lets you swap two rows' unique values inside a single
    # transaction without tripping the constraint mid-flight.
    dorm.UniqueConstraint(
        fields=["slot"], name="uq_slot_deferred",
        deferrable="deferred",
    ),
    # Covering unique constraint — same INCLUDE pattern as Index.
    dorm.UniqueConstraint(
        fields=["email"], name="uq_email_cover",
        include=["last_login_at"],
    ),
]
```

`deferrable=` accepts `"deferred"` (default check at COMMIT) or
`"immediate"` (check at statement-end, switchable mid-tx with
`SET CONSTRAINTS ... DEFERRED`). PostgreSQL only — SQLite + MySQL
silently drop the clause.

### `ExclusionConstraint` (3.1+, PG only)

PostgreSQL `EXCLUDE` constraint — guarantees no two rows in the
table satisfy the same operator over the named expressions. The
canonical use case is range-overlap exclusion:

```python
import dorm

class Reservation(dorm.Model):
    room_id = dorm.IntegerField()
    slot = dorm.RangeField(...)  # tstzrange

    class Meta:
        constraints = [
            dorm.ExclusionConstraint(
                name="no_overlap_room",
                expressions=[("room_id", "="), ("slot", "&&")],
                index_type="gist",   # default
            ),
        ]
```

Any insert/update producing a `(room_id, slot)` pair that overlaps
an existing row's range raises `IntegrityError`. SQLite + MySQL
emit nothing — pick a different uniqueness strategy on those
backends.

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

### Custom fields with descriptors

A regular `Field` subclass writes its value straight into the
instance dict — `Model.__init__` calls `field.to_python(value)` and
stores the result. That's enough for 95% of column types.

Some fields, though, need to *react* to assignment: track a pending
upload, invalidate a cache, snapshot the previous value. For those,
override `__get__` and `__set__` and opt into the **class-descriptor
path** with one line:

```python
import dorm


class MyEncryptedField(dorm.CharField):
    uses_class_descriptor = True

    def contribute_to_class(self, cls, name):
        # Reinstall as a class-level descriptor — the metaclass would
        # otherwise strip Field instances out of class attrs.
        super().contribute_to_class(cls, name)
        setattr(cls, name, self)

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        ...

    def __set__(self, instance, value):
        # Custom logic — encryption, audit logging, lazy decryption.
        instance.__dict__[self.attname] = self._encrypt(value)
```

`uses_class_descriptor = True` is the documented opt-in: when
`Model.__init__` sees that flag (or finds the field installed
directly on the class), it routes `Model(field=value)` through
`setattr` so `__set__` fires. `FileField` is the canonical built-in
example — it stashes a pending `File` until `model.save()` flushes
it to storage.

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

For the full reference — kwargs each signal receives, the difference
between `sender` for save/delete (model class) vs `sender` for query
signals (vendor string), `dispatch_uid` for idempotent registration,
weak references, and the gotchas around exception swallowing and
recursion — see the [Signals guide](signals.md).
