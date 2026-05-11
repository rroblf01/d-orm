"""CockroachDB backend — distributed SQL on top of the PostgreSQL wire
protocol.

CockroachDB exposes the same wire protocol as PostgreSQL, so we reuse
``psycopg`` and inherit from
:class:`~dorm.db.backends.postgresql.PostgreSQLDatabaseWrapper` /
:class:`~dorm.db.backends.postgresql.PostgreSQLAsyncDatabaseWrapper`.
The differences are small but real:

- ``vendor`` stays ``"postgresql"`` so every PG-only code path in the
  ORM (``CreatePGEnum``, ``CreatePartitionedTable``, ``copy_from``,
  ``execute_streaming``, …) keeps working unchanged. We expose a
  separate ``dialect = "cockroachdb"`` attribute for the few cases
  where divergence matters (e.g. ``SAVEPOINT`` semantics, which Cockroach
  uses internally for retry but treats as no-op for nested user
  transactions).
- ``SERIALIZABLE`` is the only isolation level Cockroach supports for
  general workloads, so concurrent writes occasionally surface SQLSTATE
  ``40001`` (serialization failure). The recommended pattern is to
  wrap a transaction in a retry loop — use
  :func:`dorm.contrib.cockroach.retry_on_serialization` for the helper.
- Default port is ``26257`` (CockroachDB), not 5432.

Configuration::

    DATABASES = {
        "default": {
            "ENGINE": "cockroachdb",
            "NAME": "defaultdb",
            "HOST": "localhost",
            "PORT": 26257,
            "USER": "root",
            "PASSWORD": "",
            "OPTIONS": {"sslmode": "disable"},
        }
    }

Install with ``pip install 'djanorm[postgresql]'`` — Cockroach reuses
the psycopg extra.
"""
from __future__ import annotations

from typing import Any

from .postgresql import (
    PostgreSQLAsyncDatabaseWrapper,
    PostgreSQLDatabaseWrapper,
)


def _cockroach_default_port(settings: dict[str, Any]) -> dict[str, Any]:
    """Patch *settings* in-place so the default port becomes 26257 when
    the caller didn't supply one. PG's parent ``_build_dsn`` defaults to
    5432, which would silently fail against a Cockroach cluster."""
    if "PORT" not in settings:
        settings = {**settings, "PORT": 26257}
    return settings


class CockroachDBDatabaseWrapper(PostgreSQLDatabaseWrapper):
    """Synchronous Cockroach wrapper. Inherits the full psycopg-based
    PostgreSQL pipeline; the only override is the default port."""

    # Stays ``"postgresql"`` so every ``vendor == "postgresql"`` branch
    # in the ORM (DDL, COPY, materialised views, partitioning, …) is
    # picked up transparently. The dialect attribute below is the
    # opt-in escape hatch for Cockroach-specific divergence.
    vendor = "postgresql"
    dialect = "cockroachdb"

    def __init__(self, settings: dict[str, Any]) -> None:
        super().__init__(_cockroach_default_port(settings))


class CockroachDBAsyncDatabaseWrapper(PostgreSQLAsyncDatabaseWrapper):
    """Async Cockroach wrapper. Same inheritance story as the sync class."""

    vendor = "postgresql"
    dialect = "cockroachdb"

    def __init__(self, settings: dict[str, Any]) -> None:
        super().__init__(_cockroach_default_port(settings))
