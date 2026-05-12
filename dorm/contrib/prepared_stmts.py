"""Runtime control + introspection for PostgreSQL prepared statements.

dorm's PG backend already plumbs ``PREPARE_THRESHOLD`` from the
``DATABASES`` settings dict through to psycopg, so the *initial*
threshold is configurable at process start. This module exposes the
matching *runtime* surface — three jobs the existing settings hook
doesn't cover:

1. :func:`set_threshold` — change the threshold without rebuilding
   ``DATABASES``. Useful for an ops console or a feature-flag
   rollout that wants to experiment with prepared-statement
   caching live.
2. :func:`active_prepared` — list every prepared statement on the
   *current* PG backend connection via ``pg_prepared_statements``.
   The view is connection-scoped, so the result reflects what
   psycopg has cached on whichever pool member served the query.
3. :func:`deallocate_all` — issues ``DEALLOCATE ALL`` to drop every
   server-side prepared statement on the active connection. Handy
   when a long-running connection accumulates stale statements
   after a schema change.

All three are PostgreSQL-only. Calling them against another vendor
raises :class:`NotImplementedError` so misuse is loud rather than
silent.
"""
from __future__ import annotations

import logging
from typing import Any

import dorm
from ..conf import settings

_log = logging.getLogger("dorm.contrib.prepared_stmts")


def _require_pg(conn: Any) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "dorm.contrib.prepared_stmts is PostgreSQL-only — the "
            "underlying ``PREPARE`` mechanism has no portable counterpart."
        )


def set_threshold(threshold: int | None, *, alias: str = "default") -> None:
    """Update the ``PREPARE_THRESHOLD`` for *alias* at runtime.

    Pass an integer to enable caching after *threshold* executions
    of the same SQL shape (psycopg's default is 5). Pass ``None``
    to disable prepared-statement caching entirely. The change
    rebuilds the connection pool so existing checked-out
    connections finish their work without disruption; the next
    pool checkout uses the new threshold.

    Side-effect: this re-invokes :func:`dorm.configure` to thread
    the new value through to the backend wrapper. Other DATABASES
    settings on *alias* are preserved.
    """
    current = settings.DATABASES.get(alias)
    if current is None:
        raise KeyError(
            f"set_threshold: alias {alias!r} not in DATABASES — "
            "configure the alias before tuning prepared statements."
        )
    if (current.get("ENGINE") or "").lower() not in (
        "postgresql",
        "postgres",
    ):
        raise NotImplementedError(
            f"alias {alias!r} ENGINE is {current.get('ENGINE')!r} — "
            "prepared-statement tuning is PostgreSQL-only."
        )
    new_cfg = dict(current)
    new_cfg["PREPARE_THRESHOLD"] = threshold
    merged = {**settings.DATABASES, alias: new_cfg}
    # ``configure(DATABASES=...)`` also runs ``reset_connections`` so
    # the next ``get_connection(alias)`` builds a fresh wrapper with
    # the updated threshold.
    dorm.configure(DATABASES=merged)
    _log.info(
        "prepared statements threshold set to %r on alias %r",
        threshold,
        alias,
    )


def active_prepared(*, alias: str = "default") -> list[dict[str, Any]]:
    """Return the rows of ``pg_prepared_statements`` on *alias*.

    Each row carries ``name`` / ``statement`` / ``prepare_time`` /
    ``parameter_types`` / ``from_sql``. The view is filtered to the
    current backend session — multi-connection pools may need to
    call this from a known session if reproducibility matters.
    """
    from ..db.connection import get_connection

    conn = get_connection(alias)
    _require_pg(conn)
    rows = conn.execute(
        "SELECT name, statement, prepare_time, parameter_types, from_sql "
        "FROM pg_prepared_statements ORDER BY name"
    )
    return list(rows)


def deallocate_all(*, alias: str = "default") -> None:
    """Run ``DEALLOCATE ALL`` on the active connection for *alias*.

    Useful after a migration that altered a column type — psycopg's
    cached plan references the old type and the next execution
    raises ``cached plan must not change result type``. Calling
    :func:`deallocate_all` from the post-migration hook clears the
    cache without bouncing the pool."""
    from ..db.connection import get_connection

    conn = get_connection(alias)
    _require_pg(conn)
    conn.execute_script("DEALLOCATE ALL")
    _log.info("deallocated all prepared statements on alias %r", alias)


__all__ = ["set_threshold", "active_prepared", "deallocate_all"]
