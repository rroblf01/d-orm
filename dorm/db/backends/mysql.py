"""MySQL / MariaDB backend — scaffold only.

The full implementation lands in v3.2. This module ships the
``ENGINE = "mysql"`` route + URL parser entry so projects can pin
on a future-compatible config string today, and surface a clear
error pointing at the roadmap if they try to actually connect.

Why scaffold instead of leaving the engine unrecognised: makes the
URL form (``mysql://...``) parseable by ``parse_database_url`` and
keeps doctor / dbcheck commands graceful (they recognise the
engine name, just refuse to operate).

When the full implementation lands, this module gets the same
shape as ``dorm/db/backends/postgresql.py`` — sync wrapper, async
wrapper, ``execute`` / ``execute_write`` / ``execute_insert`` /
``execute_streaming``, retry-on-transient hook, atomic context.
Dialect bits (``ON DUPLICATE KEY UPDATE``, backtick identifiers,
no transactional DDL, ``RETURNING`` only on MariaDB) get
documented inline next to the path that emits them.
"""

from __future__ import annotations

from typing import Any

from ...exceptions import ImproperlyConfigured

_NOT_IMPLEMENTED_MESSAGE = (
    "The MySQL / MariaDB backend is not implemented yet (slated for "
    "v3.2). Track progress on the project tracker, or use the "
    "PostgreSQL / SQLite / libsql backends in the meantime."
)


class MySQLDatabaseWrapper:
    """Sync wrapper. Refuses to operate; routes a clear error."""

    vendor = "mysql"

    def __init__(self, settings: dict[str, Any], alias: str = "default") -> None:
        self.settings = settings
        self.alias = alias
        raise ImproperlyConfigured(_NOT_IMPLEMENTED_MESSAGE)


class MySQLAsyncDatabaseWrapper:
    """Async counterpart. Same scaffold semantics."""

    vendor = "mysql"

    def __init__(self, settings: dict[str, Any], alias: str = "default") -> None:
        self.settings = settings
        self.alias = alias
        raise ImproperlyConfigured(_NOT_IMPLEMENTED_MESSAGE)


__all__ = ["MySQLDatabaseWrapper", "MySQLAsyncDatabaseWrapper"]
