# Troubleshooting

A collection of error messages you may hit and what they actually
mean. The format is symptom → cause → fix.

## `ImproperlyConfigured: DATABASES is not configured`

**Cause.** dorm couldn't find a settings module before the first
query.

**Fix.** Either:

- set `DORM_SETTINGS_MODULE=myproject.settings`, or
- call `dorm.configure(DATABASES={...})` at app start (e.g. in a
  FastAPI lifespan), or
- run from the directory containing `settings.py`.

## `ImproperlyConfigured: Database alias 'replica' not found`

**Cause.** A `using("replica")` call or a router returned an alias
not present in `DATABASES`.

**Fix.** Add the alias to `DATABASES`, or fix the router so it only
returns aliases that exist.

## `psycopg.errors.OperationalError: too many clients already`

**Cause.** Total connections from all your processes exceed
PostgreSQL's `max_connections`.

**Fix.** Lower `MAX_POOL_SIZE` or scale Postgres up. The
back-of-envelope rule: `MAX_POOL_SIZE × workers × pods ≤
max_connections / 2`. Drop a PgBouncer in front if you're scaling
out workers.

## `PoolTimeout: pool timeout`

**Cause.** Every connection in the pool is checked out, and a new
checkout waited longer than `POOL_TIMEOUT`.

**Fix.** Usually a leaked connection (held over an `await` outside
its block) or a query that ran too long. Check `pool_stats()`,
`EXPLAIN` the slow query, and consider raising `MAX_POOL_SIZE` if
you actually need it.

## `RuntimeError: this event loop is already running`

**Cause.** Calling a sync ORM method from an async function — sync
methods can spin up their own loop, which collides with the running
one.

**Fix.** Use the `a*` variant. `Author.objects.all()` →
`Author.objects.all()` is fine to construct, but materialize with
`async for` or `await Author.objects.all()`.

## `MultipleObjectsReturned: get() returned more than one Author`

**Cause.** Your filter criteria match more than one row.

**Fix.** Either make the lookup unique (filter on `pk` or a unique
column), use `.filter(...).first()`, or `.get_or_none(...)` if you
expect zero or one.

## `dbcheck` reports drift but the migration looks applied

**Cause.** Either the migration was applied to a different alias
than the one you're checking, or someone hand-edited the table.

**Fix.** Compare `dorm showmigrations <app> --settings=...` with
the affected environment. Run `dbcheck --settings=...` against the
exact alias to confirm. If hand-edited, write a `RunSQL` migration
to encode the diff.

## Pytest hangs forever with `-n 4`

**Cause.** pytest-asyncio creates a fresh event loop per test by
default; with xdist this stacks up dangling pools.

**Fix.** In `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

## My endpoint runs N queries instead of 1

**Cause.** A descriptor read inside a loop hits the DB once per row.
Common shapes:

* `for author in Author.objects.all(): print(author.publisher.name)`
  — N selects on `publishers`. Fix with `select_related("publisher")`.
* `for art in Article.objects.all(): list(art.tags.all())` — N
  selects on the through table. Fix with `prefetch_related("tags")`.

**How to confirm.** Wrap the suspect block in
`dorm.contrib.nplusone.NPlusOneDetector`:

```python
from dorm.contrib.nplusone import NPlusOneDetector

with NPlusOneDetector(threshold=5):
    handler()                 # raises NPlusOneError if any SQL template
                              # runs more than 5 times
```

The error message includes the parameter-stripped SQL template that
tripped the threshold, so you can grep your code for the offender.
For tests, use the `assert_no_nplusone()` helper — it raises an
`AssertionError` so pytest renders it like a regular failure.

For staging-style auditing without failing fast, build the detector
with `raise_on_detect=False` and read `detector.findings` /
`detector.report()` after the block.

## `EmailField` accepts garbage

**Cause.** This was a real bug pre-2.0. If you still see it, you're
on an old version.

**Fix.** Upgrade to djanorm ≥ 2.0. From 2.0 onwards, validation
runs in `to_python` so both `Author(email="x")` and
`obj.email = "x"` raise.

## Rolled-back migration leaves an orphan table

**Cause.** A `RunPython` step has no `reverse_code` so dorm couldn't
roll it back.

**Fix.** Always pass `reverse_code=` to `RunPython`. Use
`RunPython.noop` if there's genuinely nothing to undo at the data
level (the schema part is reversed by the schema operations on
either side).

## Migrations in a long-lived branch don't apply

**Cause.** Numbering collision: both branches added `0017_*`. The
recorder applies the first one it sees and refuses the rest.

**Fix.** Renumber your branch's migrations after merging main.
`dorm makemigrations --name <suffix>` regenerates the file with the
next available number.

## `select_related` ran a separate query for every row

**Cause.** You called it with no arguments. Bare `select_related()`
joins every FK on the model, which can be huge or even invalid if
the FK target is missing fields.

**Fix.** Always specify which FKs to follow:
`Book.objects.select_related("author", "publisher")`.

## Async tests pass locally, fail in CI

**Cause.** Almost always a pool that wasn't drained between tests.
Run `await close_all_async()` in a session-scoped fixture's
teardown, and use a session-scoped event loop.

**Fix.** See [Production deployment / Async event-loop sharing](production.md#async-event-loop-sharing).

## `IntegrityError` on `bulk_create`

**Cause.** A duplicate violates a `UNIQUE` constraint. Postgres
aborts the whole transaction; the entire batch fails.

**Fix.** Pre-filter duplicates in Python (see [Cookbook](cookbook.md#bulk-insert-with-deduplication))
or push the dedup into the DB with `ON CONFLICT DO NOTHING` via
`RunSQL` or `get_connection().execute(...)`.

## "Migration runs forever" on a big table

**Cause.** `ALTER TABLE ADD COLUMN ... NOT NULL DEFAULT '...'` on
PostgreSQL ≤ 10 rewrites the whole table.

**Fix.** Split it into three migrations: add nullable, backfill in
chunks, set NOT NULL. On PG 11+, adding a column with a
non-volatile default is metadata-only — dorm uses this when it
can.

## Where to ask for more help

- Open an issue on
  [GitHub](https://github.com/rroblf01/d-orm/issues) with the
  full traceback, the `DATABASES` block (with secrets redacted),
  and the version (`dorm --version`).
- For migrations issues, attach the output of `dorm showmigrations`
  and `dorm dbcheck`.
