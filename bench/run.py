"""Tiny benchmark runner — no third-party deps, stdlib only.

Usage::

    python -m bench.run --backend sqlite --runs 5 --ops 1000

Prints a JSON blob of ``{scenario: median_seconds_per_op}`` for the
chosen backend. Commit the result into ``bench/results/`` for
historical tracking; chart from there.

Why a custom runner instead of pytest-benchmark / asv:

- We want zero competitor / dev dependencies on the dorm side so
  the published numbers reflect a clean install (only ``djanorm[…]``).
- The set of scenarios is small and stable — a generic runner would
  be more code than the suite itself.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import tempfile

import dorm


def _configure(backend: str) -> None:
    if backend == "sqlite":
        path = tempfile.mktemp(suffix=".db")
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": path}},
            INSTALLED_APPS=["bench.scenarios"],
        )
        return
    if backend == "postgres":
        # Reads DSN bits from env so the runner stays usable in CI
        # without inventing yet another flag forest.
        import os

        dorm.configure(
            DATABASES={
                "default": {
                    "ENGINE": "postgresql",
                    "NAME": os.environ.get("DORM_BENCH_PG_DB", "bench"),
                    "USER": os.environ.get("DORM_BENCH_PG_USER", "postgres"),
                    "PASSWORD": os.environ.get("DORM_BENCH_PG_PASSWORD", "postgres"),
                    "HOST": os.environ.get("DORM_BENCH_PG_HOST", "localhost"),
                    "PORT": int(os.environ.get("DORM_BENCH_PG_PORT", "5432")),
                }
            },
            INSTALLED_APPS=["bench.scenarios"],
        )
        return
    raise SystemExit(f"unknown backend {backend!r}")


def _setup_tables() -> None:
    """Drop + create the tiny scenario table. Plain DDL — we don't
    want to depend on the migration tooling for a microbenchmark."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script("DROP TABLE IF EXISTS bench_widget")
    if getattr(conn, "vendor", "sqlite") == "sqlite":
        conn.execute_script(
            "CREATE TABLE bench_widget ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " name TEXT NOT NULL,"
            " price INTEGER NOT NULL,"
            " active INTEGER NOT NULL"
            ")"
        )
    else:
        conn.execute_script(
            "CREATE TABLE bench_widget ("
            " id SERIAL PRIMARY KEY,"
            ' name TEXT NOT NULL,'
            ' price INTEGER NOT NULL,'
            ' active BOOLEAN NOT NULL'
            ")"
        )


# ── Scenario module ──────────────────────────────────────────────────────────
#
# Defined inline so the runner is self-contained. New scenarios should
# move to ``bench/scenarios/<name>.py`` once we add a second one.

class Widget(dorm.Model):
    name = dorm.CharField(max_length=80)
    price = dorm.IntegerField()
    active = dorm.BooleanField(default=True)

    class Meta:
        app_label = "bench"
        db_table = "bench_widget"


# ── Runner ────────────────────────────────────────────────────────────────────


def _bench_create(ops: int) -> float:
    start = time.perf_counter()
    for i in range(ops):
        Widget.objects.create(name=f"w{i}", price=i, active=True)
    return time.perf_counter() - start


def _bench_get(ops: int) -> float:
    pks = [w.pk for w in Widget.objects.all()[:ops]]
    start = time.perf_counter()
    for pk in pks:
        Widget.objects.get(pk=pk)
    return time.perf_counter() - start


def _bench_filter_count(ops: int) -> float:
    start = time.perf_counter()
    for _ in range(ops):
        Widget.objects.filter(active=True).count()
    return time.perf_counter() - start


def _bench_bulk_create(ops: int) -> float:
    objs = [Widget(name=f"b{i}", price=i, active=True) for i in range(ops)]
    start = time.perf_counter()
    Widget.objects.bulk_create(objs)
    return time.perf_counter() - start


def _bench_list_first_n(ops: int) -> float:
    n = max(1, ops // 10)
    start = time.perf_counter()
    for _ in range(10):
        list(Widget.objects.all()[:n])
    return time.perf_counter() - start


SCENARIOS = {
    "create": _bench_create,
    "bulk_create": _bench_bulk_create,
    "get": _bench_get,
    "filter_count": _bench_filter_count,
    "list_first_n": _bench_list_first_n,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench.run")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="DATABASES backend (default: sqlite).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="How many times to repeat each scenario (default: 3).",
    )
    parser.add_argument(
        "--ops",
        type=int,
        default=200,
        help="How many operations per run (default: 200).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to write the JSON result (default: stdout).",
    )
    args = parser.parse_args(argv)

    _configure(args.backend)
    _setup_tables()

    results: dict[str, list[float]] = {name: [] for name in SCENARIOS}
    for _run_idx in range(args.runs):
        # Reset the table between runs so each scenario starts from a
        # known cardinality.
        from dorm.db.connection import get_connection
        get_connection().execute_script("DELETE FROM bench_widget")
        # Seed for the read-heavy scenarios.
        Widget.objects.bulk_create(
            [Widget(name=f"seed{i}", price=i, active=True) for i in range(args.ops)]
        )
        for name, fn in SCENARIOS.items():
            elapsed = fn(args.ops)
            results[name].append(elapsed / args.ops)

    summary = {
        "backend": args.backend,
        "runs": args.runs,
        "ops": args.ops,
        "median_seconds_per_op": {
            name: statistics.median(samples) for name, samples in results.items()
        },
        "best_seconds_per_op": {
            name: min(samples) for name, samples in results.items()
        },
    }
    blob = json.dumps(summary, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(blob)
    else:
        print(blob)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
