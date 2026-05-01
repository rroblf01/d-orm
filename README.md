# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

Works with **SQLite** and **PostgreSQL**, and ships with a migration system, atomic transactions, signals, validation, relationship loading (`select_related` / `prefetch_related`), aggregations, DB functions and much more — all with real static typing (`Field[T]`).

## Installation

```bash
# SQLite
pip install "djanorm[sqlite]"

# PostgreSQL
pip install "djanorm[postgresql]"
```

## Quick start

### 1. Scaffold a project

```bash
dorm init blog
```

That creates:

- `settings.py` — uncomment the `DATABASES` block matching your backend.
- `blog/` — an app package with an empty `models.py`.

A minimal `settings.py` looks like:

```python
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "db.sqlite3",
    },
}
INSTALLED_APPS = ["blog"]
```

### 2. Define a model

```python
# blog/models.py
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    is_active = dorm.BooleanField(default=True)


class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    published_at = dorm.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-published_at"]
```

### 3. Generate and apply migrations

```bash
dorm makemigrations blog
dorm migrate
```

### 4. Use it

Open a shell with `dorm shell` (IPython auto-detected) or import
the models from your own script.

```python
from blog.models import Author, Post

# Create
alice = Author.objects.create(name="Alice", email="alice@example.com")
post = Post.objects.create(
    title="Hello world",
    body="First post body.",
    author=alice,
)

# Bulk create
Post.objects.bulk_create([
    Post(title=f"Draft {i}", body="...", author=alice)
    for i in range(5)
])

# Filter / exclude / Q / F
from dorm import Q, F

active_authors = Author.objects.filter(is_active=True)
some_posts = Post.objects.filter(
    Q(title__icontains="hello") | Q(title__startswith="Draft")
).exclude(published_at__isnull=True)

# Lookups across relations
alices_posts = Post.objects.filter(author__name="Alice")

# select_related / prefetch_related to dodge N+1
for post in Post.objects.select_related("author"):
    print(post.author.name, post.title)   # 1 query, JOIN

# Get one
post = Post.objects.get(pk=1)

# Update — single instance
post.title = "Renamed"
post.save()

# Update — bulk via queryset
Post.objects.filter(author=alice).update(title=F("title") + " (by Alice)")

# Delete — single instance
post.delete()

# Delete — bulk
Post.objects.filter(published_at__isnull=True).delete()
```

### Async API (same names with `a` prefix)

```python
from blog.models import Author, Post

async def main():
    alice = await Author.objects.acreate(name="Alice", email="a@x.com")
    post = await Post.objects.acreate(title="Hi", body="...", author=alice)

    async for p in Post.objects.filter(author=alice):
        print(p.title)

    await Post.objects.filter(pk=post.pk).aupdate(title="Hi!")
    await post.adelete()
```

### Atomic transactions

```python
from dorm import transaction

with transaction.atomic():
    alice = Author.objects.create(name="Alice", email="a@x.com")
    Post.objects.create(title="t", body="b", author=alice)
    # any exception here rolls back both inserts
```

## Documentation

The full documentation, tutorials and API reference are published at:

**https://rroblf01.github.io/d-orm/**

You will find the getting-started guide, complete examples, the API reference and production deployment notes there.

## Contributing

Everyone is welcome to get involved! If you want to suggest changes, propose improvements or discuss the direction of the project, open an issue or a pull request on this repository. Discussions, ideas and critiques are very welcome.

## License

See [LICENSE](LICENSE).
