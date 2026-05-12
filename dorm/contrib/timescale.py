"""TimescaleDB helpers.

`TimescaleDB <https://www.timescale.com/>`_ is a PostgreSQL
extension that turns a regular table into a *hypertable* —
internally partitioned by a time column, with built-in retention,
compression and continuous-aggregate primitives. This module
wraps the lifecycle SQL so dorm callers don't have to remember
the exact argument shape of ``create_hypertable()``,
``add_retention_policy()`` and friends.

The extension is loaded into a database with::

    CREATE EXTENSION IF NOT EXISTS timescaledb;

— after which the helpers below operate on existing tables /
hypertables. The helpers do not run the ``CREATE EXTENSION`` for
you because that statement requires a privileged role and is
usually committed via migration tooling.

PostgreSQL-only — calling the helpers against another vendor
raises :class:`NotImplementedError`. The extension itself is
optional; helpers detect absence via a clear runtime error rather
than silently producing invalid SQL.
"""
from __future__ import annotations

import logging
import re
from typing import Any

_log = logging.getLogger("dorm.contrib.timescale")


_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_INTERVAL = re.compile(
    r"^\d+\s*(microseconds?|milliseconds?|seconds?|minutes?|"
    r"hours?|days?|weeks?|months?|years?)$",
    re.IGNORECASE,
)


def _validate_unquoted_ident(name: str) -> str:
    """Validate an identifier and return it unquoted.

    Used in slots where the SQL grammar wants a ``NAME`` parameter
    (column names inside ``create_hypertable(...)``) — wrapping in
    double quotes would change semantics from "look up column ``ts``"
    to "look up column literally named ``\"ts\"``".
    """
    if not isinstance(name, str) or not _VALID_IDENT.match(name):
        raise ValueError(
            f"timescale: invalid identifier {name!r} — must match "
            r"[A-Za-z_][A-Za-z0-9_]*"
        )
    return name


def _quote_ident(name: str) -> str:
    """Return a double-quoted SQL identifier, validated against the
    allowlist. Use this when the slot expects an *identifier* (table
    name in DDL); use :func:`_validate_unquoted_ident` when the slot
    expects a ``NAME`` text parameter (column name inside a function
    call)."""
    return f'"{_validate_unquoted_ident(name)}"'


def _validate_interval(spec: str) -> str:
    """Reject anything that doesn't look like a TimescaleDB interval.

    The extension accepts standard PG interval syntax + a few
    bespoke shortcuts; the allowlist below covers every shape the
    helpers' callers actually emit and rejects free-form SQL that
    could splice quotes / semicolons into the DDL slot."""
    if not isinstance(spec, str) or not _VALID_INTERVAL.match(spec.strip()):
        raise ValueError(
            f"timescale: invalid interval {spec!r}. Expected '<n> <unit>' "
            "with unit in {seconds, minutes, hours, days, weeks, months, "
            "years} (singular or plural)."
        )
    return spec.strip()


def _require_pg(conn: Any) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "dorm.contrib.timescale is PostgreSQL + TimescaleDB-only — "
            "the helpers depend on the ``timescaledb`` extension which "
            "ships exclusively for PG."
        )


def create_hypertable(
    table: str,
    time_column: str,
    *,
    chunk_time_interval: str = "1 day",
    if_not_exists: bool = True,
    using: str = "default",
) -> None:
    """Convert *table* into a hypertable partitioned by *time_column*.

    Args:
        table: identifier of the source table (already created via
            normal dorm migrations).
        time_column: name of the timestamp column used for
            partitioning.
        chunk_time_interval: PG interval string controlling the
            partition width. Smaller intervals give finer-grained
            retention + better insert locality but more chunks
            overall. Default ``"1 day"`` matches the TimescaleDB
            recommendation for second-resolution event streams.
        if_not_exists: pass ``if_not_exists => TRUE`` so the
            helper is safe to call from idempotent migrations.
        using: database alias.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    # ``table`` is validated against the identifier allowlist + then
    # quoted; PG's ``regclass`` cast resolves the quoted form to the
    # underlying table OID. ``time_column`` flows into the helper's
    # ``NAME`` parameter and must be passed *unquoted* — wrapping it
    # in double quotes would make TimescaleDB look for a column
    # literally named ``"ts"`` and raise "column does not exist".
    tbl_ident = _quote_ident(table)
    col_name = _validate_unquoted_ident(time_column)
    interval = _validate_interval(chunk_time_interval)
    sql = (
        "SELECT create_hypertable(%s, %s, "
        f"chunk_time_interval => INTERVAL '{interval}', "
        f"if_not_exists => {str(if_not_exists).upper()})"
    )
    conn.execute(sql, [tbl_ident, col_name])


def add_retention_policy(
    table: str,
    *,
    drop_after: str,
    if_not_exists: bool = True,
    using: str = "default",
) -> None:
    """Schedule TimescaleDB's background retention job that drops
    chunks older than *drop_after* relative to the latest row.

    Returns the job id Timescale assigns is not captured — the
    job survives across restarts; callers that need to introspect
    it can query ``timescaledb_information.jobs``.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    tbl_ident = _quote_ident(table)
    interval = _validate_interval(drop_after)
    sql = (
        "SELECT add_retention_policy(%s, "
        f"INTERVAL '{interval}', "
        f"if_not_exists => {str(if_not_exists).upper()})"
    )
    conn.execute(sql, [tbl_ident])


def remove_retention_policy(
    table: str,
    *,
    if_exists: bool = True,
    using: str = "default",
) -> None:
    """Cancel a previously-installed retention policy on *table*."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    tbl_ident = _quote_ident(table)
    sql = (
        "SELECT remove_retention_policy(%s, "
        f"if_exists => {str(if_exists).upper()})"
    )
    conn.execute(sql, [tbl_ident])


def add_compression_policy(
    table: str,
    *,
    compress_after: str,
    if_not_exists: bool = True,
    using: str = "default",
) -> None:
    """Schedule the background compression job that compresses
    chunks older than *compress_after*."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    tbl_ident = _quote_ident(table)
    interval = _validate_interval(compress_after)
    sql = (
        "SELECT add_compression_policy(%s, "
        f"INTERVAL '{interval}', "
        f"if_not_exists => {str(if_not_exists).upper()})"
    )
    conn.execute(sql, [tbl_ident])


def hypertables(*, using: str = "default") -> list[str]:
    """Return the names of every hypertable in the current DB.

    Reads ``timescaledb_information.hypertables`` — the view that
    Timescale exposes for introspection. Returns an empty list
    when the extension isn't installed (the catalog view is
    missing → :class:`RuntimeError` from the driver — surfaced
    here as an empty list with a clear log warning so monitoring
    scripts can detect "extension not installed" without
    differentiating exception shapes)."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_pg(conn)
    try:
        rows = conn.execute(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "ORDER BY hypertable_name"
        )
    except Exception as exc:
        _log.warning(
            "hypertables(): catalog read failed — timescaledb extension "
            "may not be installed: %s",
            exc,
        )
        return []
    return [r["hypertable_name"] for r in rows]


__all__ = [
    "create_hypertable",
    "add_retention_policy",
    "remove_retention_policy",
    "add_compression_policy",
    "hypertables",
]
