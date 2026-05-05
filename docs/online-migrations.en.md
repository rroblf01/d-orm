# Online migrations (zero-downtime)

End-to-end recipe for adding a `NOT NULL` column with a default to
a large table **without rewriting it** and without downtime.

Added in **4.0**.

## The problem

```python
operations = [
    AddField(
        "Order",
        "currency",
        dorm.CharField(max_length=3, null=False, default="USD"),
    ),
]
```

On PostgreSQL ≤ 10 this rewrites **the entire table** (`ALTER TABLE
... NOT NULL DEFAULT 'USD'`). A 50M-row table takes hours; during
that window every write blocks on the `ACCESS EXCLUSIVE` lock.
Down.

PG 11+ optimises if the default is non-volatile — but a
Python-computed default or an `ALTER` changing the type still
rewrites.

## The recipe

Three operations, each in its own migration (or three steps in the
same migration):

```python
from dorm.migrations.operations import (
    AddFieldOnline, BackfillBatch, SetNotNullOnline,
)

operations = [
    # Step 1 — column nullable, no rewrite.
    AddFieldOnline(
        "Order",
        "currency",
        dorm.CharField(max_length=3, null=False, default="USD"),
    ),
    # Step 2 — chunked backfill by PK range.
    BackfillBatch(
        table="orders",
        update_sql=(
            'UPDATE "orders" SET "currency" = \'USD\' '
            'WHERE "id" BETWEEN %s AND %s '
            'AND "currency" IS NULL'
        ),
        pk_column="id",
        batch_size=10_000,
        sleep_seconds=0.05,    # throttle pressure on primary
    ),
    # Step 3 — promote to NOT NULL without rewrite (PG ≥ 12).
    SetNotNullOnline("Order", "currency"),
]
```

## What each step does

### `AddFieldOnline`

```sql
-- On PostgreSQL: metadata-only, instantaneous.
ALTER TABLE "orders" ADD COLUMN "currency" VARCHAR(3) NULL;
```

The field is declared `NOT NULL` on the model, but the op forces
nullable temporarily. No rewrite.

Optional `set_not_null_now=True`: if the table is small (< 1000
rows) and the default is safe, runs steps 2+3 inline. For large
tables leave it default (`False`).

### `BackfillBatch`

```sql
-- Loop over PK ranges, each range in its own tx:
UPDATE "orders" SET "currency" = 'USD'
WHERE "id" BETWEEN 1 AND 10000 AND "currency" IS NULL;
COMMIT;
-- ... next batch ...
```

Each batch:
- Takes `batch_size` rows (default 10k).
- Dedicated transaction.
- Row-level lock for the duration of the UPDATE.
- `sleep_seconds` between batches to avoid I/O saturation.

Parameters:
- `batch_size` — lower = shorter locks but more commit overhead.
  10k is a reasonable starting point.
- `sleep_seconds` — pause between batches. 0.05s typical for
  primaries serving live traffic. 0 if the table is frozen.
- `max_batches` — cut the migration after N batches (testing /
  incremental rollout).

### `SetNotNullOnline`

On PG ≥ 12 the trick is:

```sql
-- 1. CHECK NOT VALID — instant adopt (no row scan).
ALTER TABLE "orders"
  ADD CONSTRAINT chk_orders_currency_notnull
  CHECK ("currency" IS NOT NULL) NOT VALID;

-- 2. VALIDATE — scans with SHARE UPDATE EXCLUSIVE lock,
--    doesn't block readers/writers.
ALTER TABLE "orders" VALIDATE CONSTRAINT chk_orders_currency_notnull;

-- 3. SET NOT NULL — metadata-only now that the validated CHECK
--    proves no row violates the constraint.
ALTER TABLE "orders" ALTER COLUMN "currency" SET NOT NULL;

-- 4. Drop the redundant CHECK.
ALTER TABLE "orders" DROP CONSTRAINT chk_orders_currency_notnull;
```

PG ≤ 11 lacks the step-3 optimisation — the op falls back to an
`ALTER COLUMN SET NOT NULL` that does rewrite. If your target is
11, consider leaving the column nullable.

## When to split into 3 migrations

If you'll deploy the application code and the migration in
separate releases, split:

1. **Release N** — deploy `AddFieldOnline`. The column exists,
   nullable; old code doesn't read it; new code isn't deployed yet.
2. **Backfill batch job** — run `BackfillBatch` as a migration
   or standalone script outside the release window. May take
   hours; the app keeps working because the field is nullable.
3. **Release N+1** — deploy `SetNotNullOnline` plus any code that
   relies on the NOT NULL. If the backfill didn't finish before
   this release, the migration fails (visible and early).

## Caveat on `BackfillBatch.update_sql`

You write the SQL — use `%s` (PG) or `?` (SQLite) placeholders per
backend, and always include:

- `BETWEEN %s AND %s` for the PK range (both `%s` are bound by
  the op).
- `WHERE ... IS NULL` for idempotence (rerunning the job doesn't
  duplicate work).

```python
update_sql=(
    'UPDATE "orders" '
    'SET "currency" = "billing_country_currency" '   # some computation
    'WHERE "id" BETWEEN %s AND %s '
    'AND "currency" IS NULL'                          # idempotent
),
```

## Backend caveats

- **PostgreSQL ≥ 12**: full recipe, no rewrite.
- **PostgreSQL 10–11**: `SetNotNullOnline` falls back to
  `ALTER COLUMN SET NOT NULL` which does rewrite. Consider
  leaving nullable.
- **SQLite**: `AddFieldOnline` always adds nullable (the only
  form of `ADD COLUMN NOT NULL` requires a DEFAULT). Backfill
  works. `SetNotNullOnline` falls back to a rewrite. For large
  tables, consider a `RunSQL` with the `CREATE TABLE ... AS
  SELECT; DROP; RENAME` recipe.
- **MySQL**: DDL is not transactional. Each step commit-or-die.
  Plan rollback manually.
- **DuckDB**: recipe works, no SAVEPOINT. The table rewrites on
  `SET NOT NULL` because of the columnar architecture — but
  rewriting is cheap on DuckDB.

## Tests

For unit tests use the pattern shown in
`tests/test_online_migrations.py`. Each op accepts a `_State`
mock, so you don't need the full migration runner.

## More

- [Migrations](migrations.md) — base ops + new ops
- [`dorm diff`](cli.md#dorm-diff-40) — post-deploy CI gate
- [Advanced](advanced.md) — companion PG features
