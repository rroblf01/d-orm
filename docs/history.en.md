# Audit trail (history)

`dorm.contrib.history` provides an opt-in audit trail: every
`save()`, `asave()`, `delete()` and `adelete()` against a tracked
model writes a row to a parallel `<table>_history` table. The
history table records what changed, when, and (optionally) who.

It's a contrib module (not core) because tracking carries DDL +
write-amplification cost — read the [caveats](#caveats) before
turning it on for every model.

## Quick start

```python
import dorm
from dorm.contrib.history import track_history


@track_history
class Article(dorm.Model):
    title: str = dorm.CharField(max_length=200)
    body: str = dorm.TextField()

    class Meta:
        db_table = "articles"
```

The `@track_history` class decorator builds a sibling
`ArticleHistorical` model with the same fields plus four audit
columns and registers it with dorm's model registry — your next
`dorm makemigrations` picks up the new table automatically.

```python
art: Article = Article.objects.create(title="hello", body="world")
art.title = "hi"
art.save()
art.delete()

# Three rows: '+', '~', '-'
for row in Article.history.all().order_by("history_date"):
    print(row.history_type, row.title)
```

## What gets tracked

The decorator builds an `<Name>Historical` sibling model with:

- Every column from the source model. Primary keys are demoted to
  indexed regular columns (the history table has its own surrogate
  PK because the same source row can appear multiple times).
- `history_id: int` — `BigAutoField` primary key on the history
  row itself.
- `history_date: datetime` — UTC timestamp the change was
  recorded.
- `history_type: str` — single-character tag: `"+"` (insert),
  `"~"` (update), `"-"` (delete).
- `history_user_id: int | None` — optional integer attributed via
  `set_history_user()` (see [User attribution](#user-attribution)).

Default ordering on the history model is `["-history_date"]` so
`Article.history.all()` returns newest changes first.

## User attribution

Most production audit trails care about *who* triggered the change,
not just *what*. `set_history_user()` plants the actor id on a
`contextvars.ContextVar`; subsequent history rows pick it up.

```python
from dorm.contrib.history import (
    set_history_user,
    reset_history_user,
    current_history_user,
)

# Inside a FastAPI / Starlette middleware:
async def history_user_middleware(request, call_next):
    token = set_history_user(request.user.id)
    try:
        return await call_next(request)
    finally:
        reset_history_user(token)
```

`current_history_user() -> int | None` reads the active value back.
The default is `None`, so unattributed history rows have
`history_user_id IS NULL`.

## Manual recording

The automatic hooks fire on `save` / `delete` of *individual*
instances. Operations that bypass the per-row path (`QuerySet.update()`,
`bulk_create`, `bulk_update`) **do not** fire `post_save` /
`post_delete`, so they don't write history rows. Use
`record_history_for(instance, kind)` (or `arecord_history_for(...)`)
to record one manually:

```python
from dorm.contrib.history import record_history_for, arecord_history_for


# After a manual update bypassing save():
Article.objects.filter(pk=42).update(title="new")
art = Article.objects.get(pk=42)
record_history_for(art, "~", user_id=request.user.id)


# Or async equivalent:
await arecord_history_for(art, "~", user_id=request.user.id)
```

`kind` must be one of `"+"`, `"~"`, `"-"`. `user_id` falls back to
`current_history_user()` when omitted, so middleware-set actors
flow through automatically.

## Querying history

`Model.history` exposes a `Manager` over the history table — every
queryset method works exactly the same as on the source model.

```python
# Every change to article 42, oldest first:
changes = Article.history.filter(pk=42).order_by("history_date")

# Only deletions ever recorded for the table:
gone = Article.history.filter(history_type="-")

# Who deleted the row whose original pk was 42?
last_delete = (
    Article.history
    .filter(pk=42, history_type="-")
    .order_by("-history_date")
    .first()
)
print(last_delete.history_user_id, last_delete.history_date)
```

## Async parity

`asave()` and `adelete()` are wired the same way — each tracked
model registers an async receiver under `post_save.asend` /
`post_delete.asend`. The sync receiver detects the running event
loop and bails out, so async writes never double-record.

```python
import dorm
from dorm.contrib.history import track_history


@track_history
class Note(dorm.Model):
    body: str = dorm.TextField()

    class Meta:
        db_table = "notes"


async def write_note(body: str) -> None:
    note: Note = Note(body=body)
    await note.asave()
    # async '+' row written through aiosqlite / psycopg async.
```

## Caveats

- **Bulk paths bypass the hooks.** `QuerySet.update`, `bulk_create`,
  `bulk_update` don't emit `post_save` / `post_delete`. Call
  `record_history_for` manually if audit coverage matters there.
- **Schema drift.** The history table mirrors the source's columns
  at decoration time. After a column add / rename on the source,
  re-run `dorm makemigrations` so the autodetector re-syncs the
  history table.
- **Write amplification.** Every `save()` writes one history row.
  For high-throughput tables, factor that into your IOPS / disk
  budget before enabling.
- **No diff-only mode (yet).** v3.2 records a full snapshot on
  every change. A future version may add a column-diff mode for
  storage-sensitive deployments.
