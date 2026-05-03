# Getting started

A 10-minute tour from "I haven't installed it" to "I've inserted and
queried real rows". No FastAPI, no async — just the basics. For the
async / FastAPI flavor, jump to the [Tutorial](tutorial.md).

## 1. Install

```bash
pip install "djanorm[sqlite]"
# or with uv (recommended):
uv add "djanorm[sqlite]"
```

For PostgreSQL: `pip install "djanorm[postgresql]"`.
For MySQL / MariaDB (3.1+): `pip install "djanorm[mysql]"`
(pure-Python `pymysql` + `aiomysql`, no C toolchain).
For S3 file uploads: `pip install "djanorm[s3]"` (works with AWS S3,
MinIO, Cloudflare R2, Backblaze B2).

## 2. Scaffold a project

```bash
mkdir myapp && cd myapp
dorm init --app blog
```

This creates:

```
.
├── blog/
│   ├── __init__.py
│   └── models.py        # starter User model
└── settings.py          # commented-out DB and STORAGES blocks
```

The generated `settings.py` includes commented templates for both
SQLite/PostgreSQL and the file-storage `STORAGES` (local disk, AWS S3,
and S3-compatible MinIO). Uncomment whichever ones you need.

## 3. Configure the database

Open `settings.py` and uncomment the SQLite section:

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "blog.db",
    }
}
```

dorm autodiscovers any sibling directory that has `__init__.py` +
`models.py`, so you don't need an `INSTALLED_APPS` list for the simple
case.

## 4. Define your models

Edit `blog/models.py`:

```python title="blog/models.py"
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    bio = dorm.TextField(null=True, blank=True)

    class Meta:
        ordering = ["name"]


class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE, related_name="posts")
    published = dorm.BooleanField(default=False)
    created_at = dorm.DateTimeField(auto_now_add=True)
```

## 5. Create and apply migrations

```bash
dorm makemigrations
dorm migrate
```

You should see:

```
Detecting changes for 'blog'...
  Created migration: blog/migrations/0001_initial.py
  Applying blog.0001_initial... OK
```

## 6. Insert and query

Drop into the dorm shell — it pre-imports your models and runs
IPython if available:

```bash
dorm shell
```

```python
>>> alice = Author.objects.create(name="Alice", email="alice@example.com")
>>> Post.objects.create(title="Hello", body="World", author=alice, published=True)
<Post: pk=1>

>>> Author.objects.count()
1

>>> for p in Post.objects.filter(published=True).select_related("author"):
...     print(p.author.name, "—", p.title)
Alice — Hello

>>> # F expressions, Q objects, aggregates — all here
>>> from dorm import F, Q, Count
>>> Author.objects.annotate(post_count=Count("posts")).values_list("name", "post_count")
[('Alice', 1)]
```

## 7. Switch to PostgreSQL

When you're ready to leave SQLite, all you need to change is `settings.py`:

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "postgresql",
        "NAME": "blog",
        "USER": "postgres",
        "PASSWORD": "secret",
        "HOST": "localhost",
        "PORT": 5432,
    }
}
```

Re-run `dorm migrate` against the empty PG database. Your code, models,
and queries stay identical.

## 8. MySQL / MariaDB (3.1+)

Install the extra and point at the MySQL service:

```bash
pip install "djanorm[mysql]"
```

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "mysql",   # or "mariadb"
        "NAME": "blog",
        "USER": "root",
        "PASSWORD": "secret",
        "HOST": "localhost",
        "PORT": 3306,
    }
}
```

Caveats: DDL is not transactional on MySQL — wrapping
`ALTER TABLE` in `atomic()` won't roll it back. `RETURNING` works
on MariaDB 10.5+ but not on MySQL; the insert path uses
`cursor.lastrowid` for autoincrement PKs. The wrapper forces
`ANSI_QUOTES` mode so dorm's double-quoted identifiers parse the
same as PostgreSQL / SQLite.

## What next?

- [Models & fields](models.md) — every field type and their options
- [Querying](queries.md) — filter, exclude, Q, F, aggregations
- [Async patterns](async.md) — `acreate`, `aiterator`, `aatomic`
- [Tutorial](tutorial.md) — wire it up with FastAPI
- [File uploads with `FileField`](models.md#files) — local disk by
  default, switch to S3 / MinIO / R2 with a `STORAGES` change
