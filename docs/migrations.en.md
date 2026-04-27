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
from typing import Any

from dorm.migrations.operations import RunPython


def fill_slugs(app_label: str, registry: dict[str, Any]) -> None:
    Article = registry[f"{app_label}.Article"]
    for a in Article.objects.filter(slug=""):
        a.slug = slugify(a.title)
        a.save(update_fields=["slug"])


class Migration:
    dependencies = [("blog", "0003_add_slug")]
    operations = [RunPython(fill_slugs, reverse_code=RunPython.noop)]
```

### `RunPython` callable contract

dorm passes exactly **two positional arguments** to every callable
you hand to `RunPython(code=, reverse_code=)`. Type both of them so
your editor catches mistakes before you run the migration:

```python
def my_step(app_label: str, registry: dict[str, Any]) -> None: ...
```

| Position | Name | Type | What it is |
|---|---|---|---|
| 1 | `app_label` | `str` | The app label the migration belongs to (e.g. `"blog"`). Use it to build keys for `registry` instead of hard-coding the app name — lets the same callable be reused across forks of an app. |
| 2 | `registry` | `dict[str, type[dorm.Model]]` | The **live** model registry. Look up classes by either the bare class name (`registry["Post"]`) or the app-qualified key (`registry["blog.Post"]` — preferred, unambiguous when two apps declare the same name). |

What you **don't** get (intentional differences vs Django):

- **No `connection` / `schema_editor` argument.** If you need raw SQL
  inside a Python step, fetch the connection yourself:

  ```python
  from dorm.db.connection import get_connection
  get_connection().execute("UPDATE blog_post SET ...", [...])
  ```

  Most data-migration code shouldn't reach for this — `Model.objects.filter(...).update(...)`
  covers the common case and is portable.

- **No "historical" model.** dorm hands you the *current* model
  class, not a frozen snapshot of how the model looked at this point
  in the migration chain. The implication: a callable that references
  a column dropped in a later migration will break if you re-run
  history from scratch. Mitigation — keep `RunPython` steps small,
  scope them tightly to the columns they touch, and place them right
  after the schema migration that introduced those columns. If you
  need to be defensive against future schema changes, write the data
  step as `RunSQL` instead.

### `reverse_code=`

Always pass it. `RunPython` requires a reverse callable to be
considered reversible by `dorm migrate <app> <target>`; a forward
step without one will run, but the migration will refuse to roll
back and you'll be left with the data half of a partially-undone
migration. Two patterns:

- A real undo function, with the same `(app_label, registry)`
  signature, that reverses what the forward step did (e.g. clears
  the column the forward step backfilled).
- `RunPython.noop` — a built-in callable (matching dorm's
  contract) you pass when the forward step has no meaningful
  inverse. The classic case: a one-shot data backfill that
  tolerates being undone by simply leaving the rows in place.

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

## Manual migrations: `RunPython` + `RunSQL` together

When a single migration mixes raw SQL with a Python data step,
declare both inside `operations`. The `RunPython` callables follow
the same contract documented in
[Empty migrations for data work](#empty-migrations-for-data-work)
above — `(app_label: str, registry: dict[str, Any]) -> None`.

```python
from typing import Any

from dorm.migrations.operations import RunPython, RunSQL


def backfill_slug_lower(app_label: str, registry: dict[str, Any]) -> None:
    """Forward step: nothing to backfill — the index reads the column live."""
    return None


def clear_slug_overrides(app_label: str, registry: dict[str, Any]) -> None:
    """Reverse step: undo any data side-effect the forward did."""
    Post = registry[f"{app_label}.Post"]
    Post.objects.filter(slug__isnull=False).update(slug="")


class Migration:
    atomic = False  # required for CREATE INDEX CONCURRENTLY
    dependencies = [("blog", "0007_add_slug")]
    operations = [
        RunSQL(
            "CREATE INDEX CONCURRENTLY blog_post_slug_lower ON blog_post (LOWER(slug));",
            reverse_sql="DROP INDEX IF EXISTS blog_post_slug_lower;",
        ),
        RunPython(backfill_slug_lower, reverse_code=clear_slug_overrides),
    ]
```

`RunSQL` accepts a single statement or a list. For things like
`CREATE INDEX CONCURRENTLY` — which **cannot** run inside a
transaction — set `atomic = False` at the class level so the
executor skips the per-migration atomic wrap.

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

## Zero-downtime migrations (2.1+)

Three operations help you avoid `AccessExclusiveLock` on hot tables:

- **`AddIndex(..., concurrently=True)`** emits
  `CREATE INDEX CONCURRENTLY` on PostgreSQL. Must be the only DDL
  in its migration file (the executor needs to skip the surrounding
  atomic, since `CONCURRENTLY` cannot run in a transaction).
- **`SetLockTimeout(ms=...)`** sets PG's `lock_timeout` for the
  migration window so any DDL that can't acquire its lock fast
  enough fails loudly instead of blocking writers indefinitely.
- **`ValidateConstraint(table=, name=)`** runs `ALTER TABLE ...
  VALIDATE CONSTRAINT` — the second half of the canonical
  `NOT VALID` + `VALIDATE` pattern for adding FKs / CHECKs to large
  tables without an `AccessExclusiveLock`.

Worked example in [What's new in 2.1 → Migration safety](whats-new-2.1.md#migration-safety).

## Constraints and generated columns (2.1+)

`Meta.constraints` now accepts `CheckConstraint` and
`UniqueConstraint(condition=...)` (partial unique index — the
canonical "only one active row per user" pattern). The autodetector
emits `AddConstraint` / `RemoveConstraint`.

`GeneratedField` declares a database-computed column (PG ≥ 12,
SQLite ≥ 3.31). See [What's new in 2.1 →
Schema](whats-new-2.1.md#schema).
