# djanorm benchmark suite

Standalone microbenchmarks for the dorm ORM. Comparative numbers
against Django ORM, SQLAlchemy or Tortoise live in
`bench_compare/` (each as an opt-in extra so installing dorm
itself doesn't pull a competitor's dependencies).

## Run

```bash
# In a venv with the dev extras + a SQLite-only run:
python -m bench.run --backend sqlite --runs 5

# PostgreSQL via testcontainers:
python -m bench.run --backend postgres --runs 5
```

The script uses the stdlib `timeit` module; numbers are wall-clock
medians across `--runs` iterations of `--ops` operations each. Output
is a JSON blob suitable for committing into `bench/results/` and
charting later.

## What we measure

| Op                                     | Why it matters                          |
|----------------------------------------|-----------------------------------------|
| `Model.objects.create()`               | Insert + reselect for the new PK        |
| `Model.objects.bulk_create(N)`         | Batched insert path                     |
| `Model.objects.get(pk=…)`              | Single-row hot read                     |
| `Model.objects.filter(...).count()`    | Aggregate without materialisation       |
| `list(Model.objects.all()[:N])`        | Materialise & hydrate a list            |
| `await aget`, `await acreate`          | Async parity                            |

## Adding a new benchmark

Each scenario lives as a `bench/scenarios/<name>.py` module with
two functions:

```python
def setup(conn):
    """Create / migrate any tables. Called once."""

def run(n: int):
    """Execute the operation `n` times. Called many times by the
    runner; should not print anything."""
```

The runner imports them lazily, so unused scenarios cost nothing.

## Compare-vs-others

Comparison scripts live under `bench/compare/`. Each picks a single
competitor and pins its version in `bench/compare/<name>/requirements.txt`
so re-running the same numbers is reproducible. The dorm runner does
not import competitor frameworks itself — keep the dorm benchmark
result honest by running the competitor in a separate venv.
