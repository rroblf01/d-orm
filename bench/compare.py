"""Comparative micro-benchmark: dorm vs Django ORM vs SQLAlchemy
vs Tortoise ORM.

Each ORM is exercised through an equivalent set of scenarios:

- ``insert_one`` — create a single row.
- ``bulk_insert`` — create N rows in one batch.
- ``get_by_pk`` — fetch one row by primary key.
- ``filter_count`` — issue ``COUNT(*)`` with a WHERE.
- ``list_first_n`` — paginated read.

Run only the ORMs that are importable in the active environment;
missing ones are reported as ``"skipped: <ImportError>"`` instead of
crashing the whole suite. The deliberate goal is to make
``python -m bench.compare`` a single command that prints a
side-by-side table any reader can reproduce.

Usage::

    python -m bench.compare --backend sqlite --runs 3 --ops 1000

The runner uses an in-process SQLite database for every ORM so the
comparison reflects ORM overhead, not network latency.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
import os
from contextlib import contextmanager
from typing import Callable


def _new_sqlite_path() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _measure(fn: Callable[[int], float], ops: int, runs: int) -> dict:
    samples: list[float] = []
    for _ in range(runs):
        elapsed = fn(ops)
        samples.append(elapsed / ops)
    return {
        "median_s_per_op": statistics.median(samples),
        "best_s_per_op": min(samples),
    }


# ── dorm ─────────────────────────────────────────────────────────────────────


def _run_dorm(ops: int, runs: int) -> dict:
    import dorm

    path = _new_sqlite_path()
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": path}},
        INSTALLED_APPS=["bench.compare"],
    )
    from dorm.db.connection import get_connection, reset_connections

    conn = get_connection()
    conn.execute_script("DROP TABLE IF EXISTS bench_widget")
    conn.execute_script(
        "CREATE TABLE bench_widget (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " name TEXT NOT NULL, price INTEGER NOT NULL, active INTEGER NOT NULL)"
    )

    class Widget(dorm.Model):
        name = dorm.CharField(max_length=80)
        price = dorm.IntegerField()
        active = dorm.BooleanField(default=True)

        class Meta:
            app_label = "bench_compare_dorm"
            db_table = "bench_widget"

    def _seed(n: int) -> None:
        Widget.objects.bulk_create(
            [Widget(name=f"s{i}", price=i, active=True) for i in range(n)]
        )

    def insert_one(n: int) -> float:
        conn.execute_script("DELETE FROM bench_widget")
        start = time.perf_counter()
        for i in range(n):
            Widget.objects.create(name=f"x{i}", price=i, active=True)
        return time.perf_counter() - start

    def bulk_insert(n: int) -> float:
        conn.execute_script("DELETE FROM bench_widget")
        objs = [Widget(name=f"b{i}", price=i, active=True) for i in range(n)]
        start = time.perf_counter()
        Widget.objects.bulk_create(objs)
        return time.perf_counter() - start

    def get_by_pk(n: int) -> float:
        conn.execute_script("DELETE FROM bench_widget")
        _seed(n)
        pks = [w.pk for w in Widget.objects.all()[:n]]
        start = time.perf_counter()
        for pk in pks:
            Widget.objects.get(pk=pk)
        return time.perf_counter() - start

    def filter_count(n: int) -> float:
        start = time.perf_counter()
        for _ in range(n):
            Widget.objects.filter(active=True).count()
        return time.perf_counter() - start

    def list_first_n(n: int) -> float:
        m = max(1, n // 10)
        start = time.perf_counter()
        for _ in range(10):
            list(Widget.objects.all()[:m])
        return time.perf_counter() - start

    out = {
        "insert_one": _measure(insert_one, ops, runs),
        "bulk_insert": _measure(bulk_insert, ops, runs),
        "get_by_pk": _measure(get_by_pk, ops, runs),
        "filter_count": _measure(filter_count, ops, runs),
        "list_first_n": _measure(list_first_n, ops, runs),
    }
    reset_connections()
    return out


# ── Django ───────────────────────────────────────────────────────────────────


def _run_django(ops: int, runs: int) -> dict:
    import django
    from django.apps import apps as django_apps
    from django.conf import settings as dj_settings

    path = _new_sqlite_path()
    if not dj_settings.configured:
        dj_settings.configure(
            DATABASES={
                "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": path}
            },
            INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
            USE_TZ=False,
        )
        django.setup()

    from django.db import connection, models

    # Piggyback on the ``auth`` app_label so Django accepts the model
    # without us having to ship a real app package on disk.
    class Widget(models.Model):
        name = models.CharField(max_length=80)
        price = models.IntegerField()
        active = models.BooleanField(default=True)

        class Meta:
            app_label = "auth"
            db_table = "bench_widget"

    # Drop & re-create cleanly between bench runs (the apps registry
    # caches the model after the first import).
    with connection.schema_editor() as editor:
        try:
            editor.delete_model(Widget)
        except Exception:
            pass
        editor.create_model(Widget)
    _ = django_apps  # silence unused-import linters; needed for setup()

    def _seed(n: int) -> None:
        Widget.objects.bulk_create(
            [Widget(name=f"s{i}", price=i, active=True) for i in range(n)]
        )

    def insert_one(n: int) -> float:
        Widget.objects.all().delete()
        start = time.perf_counter()
        for i in range(n):
            Widget.objects.create(name=f"x{i}", price=i, active=True)
        return time.perf_counter() - start

    def bulk_insert(n: int) -> float:
        Widget.objects.all().delete()
        objs = [Widget(name=f"b{i}", price=i, active=True) for i in range(n)]
        start = time.perf_counter()
        Widget.objects.bulk_create(objs)
        return time.perf_counter() - start

    def get_by_pk(n: int) -> float:
        Widget.objects.all().delete()
        _seed(n)
        pks = list(Widget.objects.values_list("pk", flat=True)[:n])
        start = time.perf_counter()
        for pk in pks:
            Widget.objects.get(pk=pk)
        return time.perf_counter() - start

    def filter_count(n: int) -> float:
        start = time.perf_counter()
        for _ in range(n):
            Widget.objects.filter(active=True).count()
        return time.perf_counter() - start

    def list_first_n(n: int) -> float:
        m = max(1, n // 10)
        start = time.perf_counter()
        for _ in range(10):
            list(Widget.objects.all()[:m])
        return time.perf_counter() - start

    return {
        "insert_one": _measure(insert_one, ops, runs),
        "bulk_insert": _measure(bulk_insert, ops, runs),
        "get_by_pk": _measure(get_by_pk, ops, runs),
        "filter_count": _measure(filter_count, ops, runs),
        "list_first_n": _measure(list_first_n, ops, runs),
    }


# ── SQLAlchemy 2.0 ───────────────────────────────────────────────────────────


def _run_sqlalchemy(ops: int, runs: int) -> dict:
    from sqlalchemy import (
        Boolean,
        Column,
        Integer,
        String,
        create_engine,
        select,
        func,
    )
    from sqlalchemy.orm import DeclarativeBase, Session

    path = _new_sqlite_path()
    engine = create_engine(f"sqlite:///{path}", future=True)

    class Base(DeclarativeBase):
        pass

    class Widget(Base):
        __tablename__ = "bench_widget"
        id = Column(Integer, primary_key=True)
        name = Column(String(80), nullable=False)
        price = Column(Integer, nullable=False)
        active = Column(Boolean, nullable=False)

    Base.metadata.create_all(engine)

    @contextmanager
    def _session():
        with Session(engine) as s:
            yield s

    def _wipe():
        with _session() as s:
            s.execute(Widget.__table__.delete())
            s.commit()

    def _seed(n: int) -> None:
        with _session() as s:
            s.add_all(
                [Widget(name=f"s{i}", price=i, active=True) for i in range(n)]
            )
            s.commit()

    def insert_one(n: int) -> float:
        _wipe()
        start = time.perf_counter()
        with _session() as s:
            for i in range(n):
                s.add(Widget(name=f"x{i}", price=i, active=True))
                s.commit()
        return time.perf_counter() - start

    def bulk_insert(n: int) -> float:
        _wipe()
        objs = [Widget(name=f"b{i}", price=i, active=True) for i in range(n)]
        start = time.perf_counter()
        with _session() as s:
            s.add_all(objs)
            s.commit()
        return time.perf_counter() - start

    def get_by_pk(n: int) -> float:
        _wipe()
        _seed(n)
        with _session() as s:
            pks = [r[0] for r in s.execute(select(Widget.id).limit(n)).all()]
        start = time.perf_counter()
        with _session() as s:
            for pk in pks:
                s.get(Widget, pk)
        return time.perf_counter() - start

    def filter_count(n: int) -> float:
        start = time.perf_counter()
        with _session() as s:
            for _ in range(n):
                s.execute(
                    select(func.count())
                    .select_from(Widget)
                    .where(Widget.active.is_(True))
                ).scalar_one()
        return time.perf_counter() - start

    def list_first_n(n: int) -> float:
        m = max(1, n // 10)
        start = time.perf_counter()
        with _session() as s:
            for _ in range(10):
                list(s.execute(select(Widget).limit(m)).scalars())
        return time.perf_counter() - start

    return {
        "insert_one": _measure(insert_one, ops, runs),
        "bulk_insert": _measure(bulk_insert, ops, runs),
        "get_by_pk": _measure(get_by_pk, ops, runs),
        "filter_count": _measure(filter_count, ops, runs),
        "list_first_n": _measure(list_first_n, ops, runs),
    }


# ── Tortoise (async) ─────────────────────────────────────────────────────────
#
# Tortoise discovers models by importing a module and walking the names,
# so the model must live at module scope. We guard the import behind a
# ``try`` so the rest of the bench runner stays usable when Tortoise
# isn't installed.

try:
    from tortoise import fields as _tf, models as _tm

    class _TortoiseWidget(_tm.Model):
        id = _tf.IntField(pk=True)
        name = _tf.CharField(max_length=80)
        price = _tf.IntField()
        active = _tf.BooleanField(default=True)

        class Meta:
            table = "bench_widget"
except ImportError:  # pragma: no cover — handled by the runner
    pass


def _run_tortoise(ops: int, runs: int) -> dict:
    import asyncio

    from tortoise import Tortoise

    # Tortoise discovers models by importing a module path and walking
    # the symbols. The model must therefore live at module scope, not
    # inside this function — see ``_TortoiseWidget`` below.
    from bench.compare import _TortoiseWidget as Widget

    path = _new_sqlite_path()

    async def _setup():
        # Silence ``RuntimeWarning: Module "bench.compare" has no
        # models`` — Tortoise scans the module for ``Model`` subclasses
        # at import time but our ``_TortoiseWidget`` is bound inside
        # the function scope's import. Filter the warning to keep the
        # bench output clean; functionally Tortoise still picks up
        # the model via the explicit name list below.
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message='Module "bench.compare" has no models',
                category=RuntimeWarning,
            )
            await Tortoise.init(
                db_url=f"sqlite:///{path}",
                modules={"models": ["bench.compare"]},
            )
        await Tortoise.generate_schemas()

    async def _wipe():
        await Widget.all().delete()

    async def _seed(n: int):
        await Widget.bulk_create(
            [Widget(name=f"s{i}", price=i, active=True) for i in range(n)]
        )

    async def insert_one(n: int) -> float:
        await _wipe()
        start = time.perf_counter()
        for i in range(n):
            await Widget.create(name=f"x{i}", price=i, active=True)
        return time.perf_counter() - start

    async def bulk_insert(n: int) -> float:
        await _wipe()
        objs = [Widget(name=f"b{i}", price=i, active=True) for i in range(n)]
        start = time.perf_counter()
        await Widget.bulk_create(objs)
        return time.perf_counter() - start

    async def get_by_pk(n: int) -> float:
        await _wipe()
        await _seed(n)
        pks = [w.id for w in await Widget.all().limit(n)]
        start = time.perf_counter()
        for pk in pks:
            await Widget.get(id=pk)
        return time.perf_counter() - start

    async def filter_count(n: int) -> float:
        start = time.perf_counter()
        for _ in range(n):
            await Widget.filter(active=True).count()
        return time.perf_counter() - start

    async def list_first_n(n: int) -> float:
        m = max(1, n // 10)
        start = time.perf_counter()
        for _ in range(10):
            list(await Widget.all().limit(m))
        return time.perf_counter() - start

    async def _all():
        await _setup()
        try:
            return {
                "insert_one": _measure(
                    lambda n: asyncio.get_event_loop().run_until_complete(
                        insert_one(n)
                    )
                    if False  # placeholder; handled below
                    else 0.0,
                    ops,
                    runs,
                ),
            }
        finally:
            await Tortoise.close_connections()

    # Tortoise's async-only API doesn't fit ``_measure`` cleanly because
    # ``_measure`` calls the function ``runs`` times via a sync loop.
    # Build a per-scenario async runner instead.
    async def _bench_all() -> dict:
        await _setup()
        try:
            out = {}
            for name, fn in [
                ("insert_one", insert_one),
                ("bulk_insert", bulk_insert),
                ("get_by_pk", get_by_pk),
                ("filter_count", filter_count),
                ("list_first_n", list_first_n),
            ]:
                samples = []
                for _ in range(runs):
                    elapsed = await fn(ops)
                    samples.append(elapsed / ops)
                out[name] = {
                    "median_s_per_op": statistics.median(samples),
                    "best_s_per_op": min(samples),
                }
            return out
        finally:
            await Tortoise.close_connections()

    return asyncio.run(_bench_all())


# ── Driver ───────────────────────────────────────────────────────────────────


_RUNNERS = {
    "dorm": _run_dorm,
    "django": _run_django,
    "sqlalchemy": _run_sqlalchemy,
    "tortoise": _run_tortoise,
}


def _format_table(results: dict) -> str:
    """Render the comparison as a fixed-width table for stdout."""
    orms = [k for k in results if isinstance(results[k], dict)]
    if not orms:
        return "(no ORM produced results)"
    scenarios = sorted(
        {sc for orm in orms for sc in results[orm].keys()}
    )
    header = "scenario".ljust(15) + "".join(o.ljust(18) for o in orms)
    lines = [header, "-" * len(header)]
    for sc in scenarios:
        row = sc.ljust(15)
        for orm in orms:
            v = results[orm].get(sc, {}).get("median_s_per_op")
            cell = f"{v * 1e6:>10.1f} µs/op" if v is not None else "-"
            row += cell.ljust(18)
        lines.append(row)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench.compare")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--ops", type=int, default=200)
    parser.add_argument(
        "--orms",
        nargs="+",
        default=list(_RUNNERS.keys()),
        choices=list(_RUNNERS.keys()),
        help="Subset of ORMs to benchmark (default: all importable).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of a fixed-width table.",
    )
    args = parser.parse_args(argv)

    results: dict = {}
    for orm in args.orms:
        try:
            results[orm] = _RUNNERS[orm](args.ops, args.runs)
        except ImportError as e:
            results[orm] = f"skipped: {e}"
        except Exception as e:
            results[orm] = f"error: {type(e).__name__}: {e}"

    if args.json:
        print(json.dumps({"ops": args.ops, "runs": args.runs, "results": results}, indent=2))
    else:
        print(_format_table(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
