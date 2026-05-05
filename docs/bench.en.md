# Comparative benchmark

Side-by-side comparison of **djanorm**, **Django ORM**,
**SQLAlchemy 2.0** and **Tortoise ORM** on the same five scenarios —
all against in-process SQLite to isolate ORM overhead from network /
disk cost.

## Reproduce

```bash
uv pip install django sqlalchemy tortoise-orm aiosqlite
uv run python -m bench.compare --runs 5 --ops 1000
```

ORMs that aren't installed are reported as `skipped:` and don't break
the run — output shows only the available ones.

Flags:

- `--runs N` — repeats per scenario (default 3); reports the median
  across repeats.
- `--ops N` — operations per repeat (default 200).
- `--orms dorm django sqlalchemy tortoise` — subset to measure.
- `--json` — JSON output instead of the fixed-width table.

## Scenarios

| Scenario | What it measures |
|----------|------------------|
| `insert_one` | `Model.objects.create(...)` repeated N times (each its own commit) |
| `bulk_insert` | `bulk_create([...N...])` in one call |
| `get_by_pk` | `Model.objects.get(pk=…)` point-by-point against N pre-existing rows |
| `filter_count` | `Model.objects.filter(active=True).count()` repeated N times |
| `list_first_n` | `list(Model.objects.all()[:N/10])` repeated 10 times |

## Results

Environment:

- Python 3.14.4, Linux x86_64, in-process SQLite
- djanorm 4.0.0, Django 6.0.4, SQLAlchemy 2.0.49, Tortoise ORM 1.1.7
- 5 repeats × 1000 operations per scenario; values reported are the
  **median microseconds per operation**.

| Scenario | dorm | django | sqlalchemy | tortoise |
|---|---|---|---|---|
| `bulk_insert` | **1.5 µs/op** | 8.0 µs/op | 23.8 µs/op | 2.6 µs/op |
| `list_first_n` | **3.5 µs/op** | 4.4 µs/op | 5.5 µs/op | 6.9 µs/op |
| `filter_count` | **89.4 µs/op** | 243.6 µs/op | 202.0 µs/op | 204.2 µs/op |
| `get_by_pk` | **62.2 µs/op** | 182.4 µs/op | 172.4 µs/op | 157.4 µs/op |
| `insert_one` | 117.7 µs/op | 175.2 µs/op | 262.8 µs/op | **86.6 µs/op** |

dorm wins 4/5 scenarios. Tortoise wins `insert_one` (individual
commits) by a small margin; the order of magnitude matches.
SQLAlchemy 2.0 is the slowest on unit and bulk writes; Django sits
in the middle.

### By category

- **Bulk inserts** (`bulk_insert`): dorm ~6× faster than Django, ~15×
  faster than SQLAlchemy. The win comes from how each ORM groups
  `INSERT ... VALUES (…), (…), …`: dorm emits a single statement
  with all placeholders; SQLAlchemy's `add_all + commit` defaults to
  N statements unless you explicitly opt into `executemany`.
- **Indexed reads** (`get_by_pk`, `filter_count`): dorm ~2-3× faster
  than the rest. The win concentrates in the Python cost of SQL
  compilation — dorm caches the compiled shape of repeated queries
  (`@functools.lru_cache` on `_to_pyformat`), Django regenerates
  the plan every call.
- **Unit inserts** (`insert_one`): Tortoise wins by grouping
  `INSERT`s on a single cursor without per-call connection re-
  checkout. dorm trails by ~30 % thanks to its conservative
  open/close-autocommit-transaction-per-`create()` policy.
- **QuerySet iteration** (`list_first_n`): technical tie — all four
  are dominated by SQLite fetch cost, not ORM cost.

### Caveats

- The chart reflects **Python framework cost**. In production,
  network latency to the PG/MySQL server eclipses what we measure
  here by 10-100×. The metric matters for hot loops that issue
  thousands of queries per second (cron jobs, ETL pipelines,
  dashboards).
- Numbers fluctuate ±5-10 % between runs due to scheduler jitter.
  The runner also reports `best_seconds_per_op` in `--json` so you
  can discard outliers.
- SQLAlchemy 2.0 exposes a Core layer that's significantly faster
  than the Session/ORM tier measured here — the comparison stages
  the "high-level ORM" scenario because that's the direct equivalent
  to `Model.objects`.
- Django runs in "dynamic model, no app" mode — minimum
  INSTALLED_APPS. A real app adds signal-dispatch overhead that
  doesn't appear here.

## Extending

Add a scenario by writing a function in every `_run_<orm>` block
and adding it to the returned dict. Stable signature: `fn(ops: int)
-> float` (elapsed seconds). Use `_measure(fn, ops, runs)` for the
median + best aggregation.

To add a new ORM, write `_run_<orm>(ops, runs) -> dict` and register
it in `_RUNNERS` at the bottom of the module.
