"""Async-friendly façade over :class:`MigrationExecutor`.

Targets fully-async stacks (Lambda, edge runtimes, FastAPI startup
hooks) where calling the sync executor directly would block the
event loop on disk I/O + DB round-trips. The wrapper offloads each
operation to :func:`asyncio.to_thread`, so the migration logic stays
in one place — the sync executor — and the async surface is a thin
adapter.

This is *not* a re-implementation of the migration internals: every
``await`` round-trips through a worker thread that drives the same
sync executor against the same sync connection. That keeps the
correctness guarantees (advisory lock, dry-run capture, recorder
semantics) identical to the blocking path. The trade-off — one
thread hop per migrate() call — is negligible against the cost of
the migration itself.

Usage::

    from dorm.db.connection import get_connection
    from dorm.migrations.aexecutor import AsyncMigrationExecutor

    async def run_startup_migrations() -> None:
        executor = AsyncMigrationExecutor(get_connection())
        await executor.amigrate("blog", "blog/migrations")
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from .executor import MigrationExecutor


class AsyncMigrationExecutor:
    """Async wrapper around :class:`MigrationExecutor`.

    Each method delegates to the synchronous counterpart via
    :func:`asyncio.to_thread`, so a coroutine awaiting
    :meth:`amigrate` doesn't block its event loop. The underlying
    connection is still the sync wrapper — the worker thread runs
    real DB I/O.
    """

    def __init__(self, connection: Any, verbosity: int = 1) -> None:
        self._inner = MigrationExecutor(connection, verbosity=verbosity)

    @property
    def loader(self):
        return self._inner.loader

    @property
    def recorder(self):
        return self._inner.recorder

    @property
    def connection(self):
        return self._inner.connection

    async def amigrate(
        self,
        app_label: str,
        migrations_dir: str | Path,
        dry_run: bool = False,
        fake: bool = False,
        fake_initial: bool = False,
    ) -> list[tuple[str, list]] | None:
        """Async counterpart of :meth:`MigrationExecutor.migrate`."""
        return await asyncio.to_thread(
            self._inner.migrate,
            app_label,
            migrations_dir,
            dry_run,
            fake,
            fake_initial,
        )

    async def arollback(
        self, app_label: str, migrations_dir: str | Path, target: str
    ) -> None:
        """Async counterpart of :meth:`MigrationExecutor.rollback`."""
        await asyncio.to_thread(
            self._inner.rollback, app_label, migrations_dir, target
        )

    async def amigrate_to(
        self, app_label: str, migrations_dir: str | Path, target: str
    ) -> None:
        """Async counterpart of :meth:`MigrationExecutor.migrate_to`."""
        await asyncio.to_thread(
            self._inner.migrate_to, app_label, migrations_dir, target
        )

    async def ashow_migrations(
        self, app_label: str, migrations_dir: str | Path
    ) -> None:
        """Async counterpart of :meth:`MigrationExecutor.show_migrations`.
        Only useful for symmetry — the underlying call just prints to
        stdout, so it doesn't actually need to be async, but ships here
        so callers can keep their migration plumbing all-async."""
        await asyncio.to_thread(
            self._inner.show_migrations, app_label, migrations_dir
        )


__all__ = ["AsyncMigrationExecutor"]
