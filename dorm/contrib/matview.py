"""Materialized view helpers (PostgreSQL).

PostgreSQL has supported materialised views since 9.3 (with
``REFRESH MATERIALIZED VIEW CONCURRENTLY`` added in 9.4). The
feature is handy whenever a heavy aggregate / join query needs to
respond in milliseconds and the user can tolerate a freshness lag —
think dashboards, leaderboards, monthly reports.

This module wraps the lifecycle SQL so callers don't have to
reach for ``connection.execute_script("CREATE MATERIALIZED VIEW
...")`` with hand-rolled identifier quoting:

- :func:`create_matview` / :func:`drop_matview`
- :func:`refresh_matview` (optionally ``CONCURRENTLY``)
- :func:`schedule_refresh` — registers the view with the
  :mod:`dorm.contrib.tasks` scheduler so a cron-like worker keeps
  it warm.

Async equivalents are provided where the underlying driver
supports them.

PostgreSQL-only on purpose: SQLite and MySQL don't ship a
``MATERIALIZED VIEW`` primitive. Calling the helpers against a
non-PG alias raises :class:`NotImplementedError` rather than
silently emitting SQL that the other backends reject with an
unhelpful syntax error.
"""
from __future__ import annotations

import logging
import re
from typing import Any

_log = logging.getLogger("dorm.contrib.matview")


_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    """Quote an SQL identifier, rejecting anything that isn't a
    plain ``[A-Za-z_][A-Za-z0-9_]*``.

    Identifier slots can't go through psycopg's parameter binding —
    we'd splice ``%s`` into the SQL grammar position that demands a
    literal. The allowlist closes the door on accidental SQL
    injection from a config-driven view name."""
    if not isinstance(name, str) or not _VALID_IDENT.match(name):
        raise ValueError(
            f"matview: invalid identifier {name!r}. Must match "
            r"[A-Za-z_][A-Za-z0-9_]*"
        )
    return f'"{name}"'


def _require_pg(conn: Any) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "dorm.contrib.matview is PostgreSQL-only — other backends "
            "do not ship a MATERIALIZED VIEW primitive."
        )


def create_matview(
    name: str,
    select_sql: str,
    *,
    using: str = "default",
    with_data: bool = True,
    if_not_exists: bool = False,
) -> None:
    """Run ``CREATE MATERIALIZED VIEW <name> AS <select_sql>``.

    Args:
        name: view identifier (validated against the SQL allowlist).
        select_sql: the SELECT body. Passed verbatim — bind any
            parameters into the string upstream if the SELECT
            depends on caller-supplied values. The helper does
            **not** parameter-bind because PG rejects ``%s`` in
            DDL positions.
        using: database alias.
        with_data: whether to populate the view immediately. Pass
            ``False`` to skip the initial scan (you must run
            :func:`refresh_matview` before the view returns rows).
        if_not_exists: emit ``IF NOT EXISTS`` so repeat invocations
            on a fresh DB are idempotent.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    ident = _quote_ident(name)
    ine = "IF NOT EXISTS " if if_not_exists else ""
    suffix = "WITH DATA" if with_data else "WITH NO DATA"
    conn.execute_script(
        f"CREATE MATERIALIZED VIEW {ine}{ident} AS {select_sql} {suffix}"
    )


def drop_matview(
    name: str,
    *,
    using: str = "default",
    if_exists: bool = True,
    cascade: bool = False,
) -> None:
    """Run ``DROP MATERIALIZED VIEW``. ``if_exists`` defaults to
    True so a teardown helper can run unconditionally; ``cascade``
    propagates to dependent views / triggers."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    ident = _quote_ident(name)
    ie = "IF EXISTS " if if_exists else ""
    cas = " CASCADE" if cascade else ""
    conn.execute_script(f"DROP MATERIALIZED VIEW {ie}{ident}{cas}")


def refresh_matview(
    name: str,
    *,
    using: str = "default",
    concurrently: bool = False,
) -> None:
    """Run ``REFRESH MATERIALIZED VIEW [CONCURRENTLY] <name>``.

    ``concurrently=True`` lets readers keep querying the view while
    the refresh runs — required for hot dashboards. The view must
    have at least one unique index for ``CONCURRENTLY`` to work
    (PG raises ``ERROR: cannot refresh materialized view
    "<name>" concurrently`` otherwise).
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    ident = _quote_ident(name)
    keyword = "CONCURRENTLY " if concurrently else ""
    conn.execute_script(f"REFRESH MATERIALIZED VIEW {keyword}{ident}")


async def arefresh_matview(
    name: str,
    *,
    using: str = "default",
    concurrently: bool = False,
) -> None:
    """Async counterpart of :func:`refresh_matview`."""
    from ..db.connection import get_async_connection

    conn = get_async_connection(using)
    _require_pg(conn)
    ident = _quote_ident(name)
    keyword = "CONCURRENTLY " if concurrently else ""
    await conn.execute_write(
        f"REFRESH MATERIALIZED VIEW {keyword}{ident}"
    )


def list_matviews(*, using: str = "default", schema: str = "public") -> list[str]:
    """Return the materialised view names visible to the current
    role under *schema*.

    Reads :pg:catalog:`pg_matviews` directly rather than the
    information_schema view — the latter doesn't list materialised
    views (they're technically not in the SQL standard catalog
    yet) and would silently return an empty list.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    rows = conn.execute(
        "SELECT matviewname FROM pg_matviews WHERE schemaname = %s "
        "ORDER BY matviewname",
        [schema],
    )
    return [r["matviewname"] for r in rows]


def matview_refresh_task(
    name: str,
    *,
    using: str = "default",
    concurrently: bool = True,
):
    """Return a zero-arg callable that refreshes *name*. Designed
    to plug into the :mod:`dorm.contrib.tasks` decorator::

        from dorm.contrib.matview import matview_refresh_task
        from dorm.contrib.tasks import task, TaskQueue

        q = TaskQueue(name="cron")

        @task(q, name="dashboards.refresh", cron="*/5 * * * *")
        def _refresh():
            matview_refresh_task("dashboard_v")()

    Keeping the helper as a thin closure (instead of a full
    scheduler registration) avoids re-implementing scheduling
    logic that lives in :mod:`dorm.contrib.tasks` already."""

    def _job() -> None:
        refresh_matview(name, using=using, concurrently=concurrently)
        _log.info("matview %r refreshed", name)

    return _job


__all__ = [
    "create_matview",
    "drop_matview",
    "refresh_matview",
    "arefresh_matview",
    "list_matviews",
    "matview_refresh_task",
]
