# Migrations

dorm's migration system follows the same pattern Django shipped: each
migration is a Python file with a list of `Operation` objects that
describe a single forward step. The autodetector compares your model
state to the latest migration and writes the diff for you.

## The day-to-day loop

```bash
# 1. Edit your models
# 2. Generate a migration
dorm makemigrations

# 3. Review the SQL it would emit (optional but recommended)
dorm migrate --dry-run

# 4. Apply
dorm migrate
```

Each migration file lives in `<app>/migrations/000N_<name>.py` and is
applied in order. dorm records applied migrations in a
`dorm_migrations` table inside your database, so re-running `migrate`
is always safe.

## What `makemigrations` detects

- New / removed models → `CreateModel` / `DeleteModel`
- New / removed columns → `AddField` / `RemoveField`
- Field option changes (max_length, null, default, ...) → `AlterField`
- Renamed models / fields → `RenameModel` / `RenameField` (asks for
  confirmation when a remove-then-add is ambiguous)
- New / removed `Meta.indexes` → `AddIndex` / `RemoveIndex`

The detector runs in pure Python over the model `_meta` registry — no
database call needed.

## Empty migrations for data work

```bash
dorm makemigrations --empty --name backfill_slugs blog
```

Produces a stub with `RunPython` and `RunSQL` you can fill in:

```python
from dorm.migrations.operations import RunPython

def fill_slugs(apps, connection):
    Article = apps.get_model("blog", "Article")
    for a in Article.objects.all():
        a.slug = slugify(a.title)
        a.save(update_fields=["slug"])

class Migration:
    dependencies = [("blog", "0003_add_slug")]
    operations = [RunPython(fill_slugs, reverse_code=RunPython.noop)]
```

`apps.get_model(app, name)` returns a *historical* model — i.e. the
model with the field shape it had **at this point in the migration
chain**. This is what protects data migrations from breaking when you
later edit the live model.

## `dorm migrate` targets

```bash
dorm migrate                       # apply everything pending
dorm migrate blog                  # only the blog app
dorm migrate blog 0005             # forward or roll back to 0005
dorm migrate blog 0005_add_index   # name prefix also works
dorm migrate blog zero             # roll back every migration
```

Rollback runs the operations in reverse using each operation's
`backwards()` method. `RunPython` requires a `reverse_code=` argument
to be reversible.

## `--dry-run`: preview before deploying

```bash
dorm migrate --dry-run
```

Prints the exact SQL each pending migration would execute, without
touching the database and without recording the migration as applied.
The recorder is **not** updated — your next `dorm migrate` still sees
the same set as pending. Use this as a pre-deploy review step on
production schemas.

## `dorm showmigrations`

```text
blog
 [X] 0001_initial
 [X] 0002_post_author
 [ ] 0003_add_slug
```

Crossed boxes are applied; empty boxes are pending. Useful for spotting
out-of-order or never-applied migrations after a long-lived branch
merges.

## Squashing

After a year of small migrations the chain gets long. `squashmigrations`
collapses a range into a single file:

```bash
dorm squashmigrations blog 0001 0042
```

Produces `blog/migrations/0042_squashed.py` with `replaces = [...]`
listing the originals. Once every environment has applied 0042, you can
delete the originals and the squashed file becomes the new starting
point.

## Schema drift detection

```bash
dorm dbcheck             # check every app
dorm dbcheck blog users  # only specific apps
```

Compares the live database schema (column names + types pulled from
`information_schema` / `pragma`) against what your models expect.
Reports drift like:

- columns the model declares but the DB lacks (forgotten migration)
- columns the DB has but the model doesn't (hand-edited table)
- type mismatches (someone ran `ALTER TYPE` outside the migration tool)

Exits non-zero on drift, so you can wire it into CI or a pre-deploy
gate. It does **not** fix anything — its job is to tell you.

## Concurrency: advisory locks

`dorm migrate` takes a PostgreSQL advisory lock (`pg_advisory_lock`)
before applying anything, so two CI workers racing each other won't
double-apply or corrupt the recorder. SQLite serializes through file
locking, which has the same effect for small dev setups.

## Manual migrations: `RunPython` / `RunSQL`

```python
from dorm.migrations.operations import RunPython, RunSQL

class Migration:
    dependencies = [("blog", "0007_add_slug")]
    operations = [
        RunSQL(
            "CREATE INDEX CONCURRENTLY blog_post_slug_lower ON blog_post (LOWER(slug));",
            reverse_sql="DROP INDEX IF EXISTS blog_post_slug_lower;",
        ),
        RunPython(my_python_function, reverse_code=my_undo_function),
    ]
```

`RunSQL` accepts a single statement or a list. For things like
`CREATE INDEX CONCURRENTLY` (which can't run inside a transaction),
mark the migration `atomic = False` at the class level.

## Common pitfalls

- **Forgetting `null=True` on a new field**: dorm refuses to add a
  `NOT NULL` column without a default to a non-empty table. Either
  give it a default, or split into two migrations: add nullable, then
  backfill, then alter to NOT NULL.
- **Renaming a model**: dorm asks "did you rename X to Y? [y/N]".
  Answering "no" creates remove + add, which **drops the table** —
  re-read before pressing y.
- **Editing an applied migration**: don't. The recorder hashes the
  content; if you really must, also delete the row from
  `dorm_migrations` on every environment.
