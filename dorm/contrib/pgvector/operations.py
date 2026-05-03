"""Migration operations for vector-search setup.

Two backends, two extensions:

* **PostgreSQL** runs ``CREATE EXTENSION IF NOT EXISTS "vector"``
  on apply — pgvector lives server-side in the database, so the
  migration is the single source of truth.
* **SQLite** loads the ``sqlite-vec`` extension into the active
  connection. Unlike pgvector, sqlite-vec is a client-side loadable
  extension: it has to be re-loaded on every new connection.
  :class:`VectorExtension` calls :func:`load_sqlite_vec_extension`
  during the apply step, *and* registers a per-connection hook on
  the wrapper so subsequent connections (re-opens, new threads)
  load it too.
"""

from __future__ import annotations

import logging
from typing import Any

from ...migrations.operations import Operation


_logger = logging.getLogger("dorm.contrib.pgvector")


def load_sqlite_vec_extension(raw_connection: Any) -> None:
    """Load the sqlite-vec extension on a raw ``sqlite3.Connection``.

    The user must have installed the ``sqlite-vec`` PyPI package —
    that's the cleanest way to ship the compiled extension across
    platforms (the package bundles the per-OS shared object and
    exposes the path).

    Raises:
        ImportError: ``sqlite-vec`` not installed.
        sqlite3.OperationalError: ``enable_load_extension`` is
            disabled in the active Python build (rare, but Ubuntu's
            apt-installed Python historically shipped without it).

    Idempotent — sqlite-vec's loader is safe to call multiple times
    on the same connection; reloading skips re-registering the
    functions.
    """
    try:
        import sqlite_vec
    except ImportError as exc:  # pragma: no cover — exercised by integration tests
        raise ImportError(
            "VectorExtension on SQLite requires the ``sqlite-vec`` "
            "Python package. Install with: "
            "``pip install 'djanorm[sqlite,pgvector]'`` or just "
            "``pip install sqlite-vec``."
        ) from exc
    raw_connection.enable_load_extension(True)
    try:
        sqlite_vec.load(raw_connection)
    finally:
        # ``enable_load_extension`` is sticky on the connection —
        # turn it off so user code can't accidentally load arbitrary
        # extensions later. The functions sqlite-vec registered
        # remain available regardless.
        raw_connection.enable_load_extension(False)


class VectorExtension(Operation):
    """Migration operation that enables vector search on the target
    DB — pgvector on PostgreSQL, sqlite-vec on SQLite.

    Runs idempotently:

    * **PostgreSQL** — ``CREATE EXTENSION IF NOT EXISTS "vector"``
      forwards, ``DROP EXTENSION IF EXISTS "vector"`` backwards.
      The extension persists on the server.
    * **SQLite** — loads sqlite-vec into the migration's
      connection AND registers a hook on the wrapper so every
      *future* connection (re-opens, new threads, new processes
      after restart) loads it automatically. The hook key
      (``_vec_extension_enabled``) is a wrapper attribute, not a
      DB row, so a process restart needs to hit the hook again —
      either by re-running the migration, or by importing
      :func:`load_sqlite_vec_extension` from app startup. The
      generated migration file is the recommended trigger because
      it lives in source control next to the model.

    Typical layout::

        # 0001_enable_pgvector.py
        from dorm.contrib.pgvector import VectorExtension
        operations = [VectorExtension()]

    Generate with ``dorm makemigrations --enable-pgvector <app>``.
    """

    def __init__(self) -> None:
        pass

    def state_forwards(self, app_label: str, state: Any) -> None:
        pass

    def database_forwards(
        self,
        app_label: str,
        connection: Any,
        from_state: Any,
        to_state: Any,
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            connection.execute_script('CREATE EXTENSION IF NOT EXISTS "vector"')
            return
        if vendor == "sqlite":
            # 1. Mark the wrapper so subsequent ``_new_connection``
            #    calls re-load the extension. The wrapper checks
            #    this flag in its own connect path (added below).
            setattr(connection, "_vec_extension_enabled", True)
            # 2. Load it on the *current* connection too so the
            #    migration runner can use vector functions in
            #    follow-up data-migration steps if it wants.
            raw = connection.get_connection()
            load_sqlite_vec_extension(raw)
            return
        if vendor in ("libsql", "mysql", "mariadb"):
            # libsql, MariaDB 11.7+ and MySQL 9.0+ ship vector
            # functions natively — no extension to load. The
            # operation is a no-op so the same migration file
            # works across every backend.
            return
        _logger.warning(
            "VectorExtension: unknown backend %r — skipped.", vendor
        )

    def database_backwards(
        self,
        app_label: str,
        connection: Any,
        from_state: Any,
        to_state: Any,
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            connection.execute_script('DROP EXTENSION IF EXISTS "vector"')
            return
        if vendor == "sqlite":
            # SQLite extensions don't have a "drop" equivalent — the
            # functions live in the loaded shared library and are
            # gone when the connection closes. Just clear the flag
            # so future connections don't auto-load.
            if hasattr(connection, "_vec_extension_enabled"):
                connection._vec_extension_enabled = False
            return
        # libsql / mysql / mariadb — vector support is built into
        # the engine, nothing to undo.

    def describe(self) -> str:
        return "Enable vector-search extension (pgvector / sqlite-vec)"

    def __repr__(self) -> str:
        return "VectorExtension()"
