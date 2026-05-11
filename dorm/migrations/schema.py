"""Low-level schema editor — DDL helpers for ad-hoc work outside a
migration file.

Migrations remain the canonical way to evolve a schema (file-based
history, recorder, dry-run, lint, autodetector). The :class:`SchemaEditor`
exposes the same DDL machinery for tooling that needs to apply
schema changes imperatively — REPL exploration, fixture loaders,
test harnesses that bootstrap an ad-hoc table, schema-drift repair
jobs, ContentType backfills.

Usage::

    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor


    with SchemaEditor(get_connection()) as se:
        se.create_model(Article)
        se.add_field(Article, "summary", dorm.TextField(null=True))


    # Or via the connection's helper for symmetry with Django:
    with get_connection().schema_editor() as se:
        se.delete_model(LegacyTable)

The editor is a thin façade over the same migration operations the
migration executor uses, so behaviour is identical to running the
equivalent op inside a ``RunPython`` step.
"""

from __future__ import annotations

from typing import Any


class SchemaEditor:
    """Imperative DDL helper. Wraps :mod:`dorm.migrations.operations`
    so the ad-hoc path and the migration path produce the same SQL.

    The editor is a context manager — entering / exiting are no-ops
    for now (they exist so future refactors can attach hooks like
    a single transaction wrapping every operation, or batched DDL
    on databases that support it).
    """

    def __init__(self, connection: Any, *, atomic: bool = False) -> None:
        """*atomic*: when True, wrap every DDL call in a top-level
        ``connection.atomic()``. Use sparingly — DDL on MySQL is
        non-transactional regardless, and PG's ``CREATE INDEX
        CONCURRENTLY`` rejects an enclosing transaction. The default
        leaves transaction control to the caller."""
        self.connection = connection
        self._atomic = atomic
        self._atomic_ctx = None

    def __enter__(self) -> "SchemaEditor":
        if self._atomic:
            self._atomic_ctx = self.connection.atomic()
            self._atomic_ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        ctx = self._atomic_ctx
        self._atomic_ctx = None
        if ctx is not None:
            return bool(ctx.__exit__(exc_type, exc, tb))
        return False

    # ── DDL primitives ───────────────────────────────────────────────────────

    def create_model(self, model_cls: type) -> None:
        """Issue ``CREATE TABLE`` for *model_cls* (and its M2M
        junction tables, if any). Equivalent to a single
        :class:`CreateModel` migration op."""
        from .operations import CreateModel
        from .state import ProjectState

        meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        fields = [(f.name, f) for f in meta.fields]
        options = {"db_table": meta.db_table}
        if getattr(meta, "db_table_comment", "") or "":
            options["db_table_comment"] = meta.db_table_comment
        op = CreateModel(name=model_cls.__name__, fields=fields, options=options)
        op.database_forwards(meta.app_label, self.connection, ProjectState(), ProjectState())

    def delete_model(self, model_cls: type) -> None:
        """``DROP TABLE``."""
        from .operations import DeleteModel
        from .state import ProjectState

        meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        op = DeleteModel(name=model_cls.__name__)
        # ``DeleteModel`` reads the table name from ``from_state.options``;
        # populate just enough for the op to find it.
        state = ProjectState()
        state.add_model(
            meta.app_label,
            model_cls.__name__,
            {f.name: f for f in meta.fields},
            {"db_table": meta.db_table},
        )
        op.database_forwards(
            meta.app_label, self.connection, state, ProjectState()
        )

    def add_field(self, model_cls: type, name: str, field: Any) -> None:
        """``ALTER TABLE ... ADD COLUMN``."""
        from .operations import AddField
        from .state import ProjectState

        meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        op = AddField(model_name=model_cls.__name__, name=name, field=field)
        from_state = ProjectState()
        from_state.add_model(
            meta.app_label,
            model_cls.__name__,
            {f.name: f for f in meta.fields},
            {"db_table": meta.db_table},
        )
        to_state = from_state.clone()
        op.database_forwards(meta.app_label, self.connection, from_state, to_state)

    def remove_field(self, model_cls: type, name: str) -> None:
        """``ALTER TABLE ... DROP COLUMN``."""
        from .operations import RemoveField
        from .state import ProjectState

        meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        op = RemoveField(model_name=model_cls.__name__, name=name)
        from_state = ProjectState()
        from_state.add_model(
            meta.app_label,
            model_cls.__name__,
            {f.name: f for f in meta.fields},
            {"db_table": meta.db_table},
        )
        op.database_forwards(meta.app_label, self.connection, from_state, ProjectState())

    def alter_field(self, model_cls: type, name: str, field: Any) -> None:
        """``ALTER TABLE ... ALTER COLUMN`` (vendor-specific syntax —
        SQLite recreates the table; PG / MySQL do real ALTERs)."""
        from .operations import AlterField
        from .state import ProjectState

        meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        op = AlterField(model_name=model_cls.__name__, name=name, field=field)
        from_state = ProjectState()
        from_state.add_model(
            meta.app_label,
            model_cls.__name__,
            {f.name: f for f in meta.fields},
            {"db_table": meta.db_table},
        )
        to_state = from_state.clone()
        op.database_forwards(meta.app_label, self.connection, from_state, to_state)

    def execute(self, sql: str, params: list | None = None) -> None:
        """Escape hatch — run arbitrary DDL through the connection.
        Use when none of the helpers fit (creating a custom function,
        a vendor-specific extension, …). No state tracking; the
        caller is responsible for keeping the migration history
        consistent if they care about it."""
        if params is None:
            self.connection.execute_script(sql)
        else:
            self.connection.execute_write(sql, params)


class AsyncSchemaEditor:
    """Async façade over :class:`SchemaEditor`.

    Each DDL helper offloads the synchronous editor's work to
    :func:`asyncio.to_thread`, mirroring the
    :class:`~dorm.migrations.aexecutor.AsyncMigrationExecutor` design.
    The connection passed in is the *sync* wrapper — the worker thread
    drives real DB I/O against it. Use this from async startup hooks
    (FastAPI, Litestar, edge runtimes) where the sync editor would
    block the event loop.

    The context-manager protocol is async (``async with`` /
    ``__aenter__`` / ``__aexit__``); ``atomic=True`` runs every DDL
    call inside a single ``connection.atomic()`` block, opened on the
    worker thread.
    """

    def __init__(self, connection: Any, *, atomic: bool = False) -> None:
        self._inner = SchemaEditor(connection, atomic=atomic)

    @property
    def connection(self) -> Any:
        return self._inner.connection

    async def __aenter__(self) -> "AsyncSchemaEditor":
        import asyncio

        await asyncio.to_thread(self._inner.__enter__)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        import asyncio

        return await asyncio.to_thread(self._inner.__exit__, exc_type, exc, tb)

    async def acreate_model(self, model_cls: type) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.create_model, model_cls)

    async def adelete_model(self, model_cls: type) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.delete_model, model_cls)

    async def aadd_field(self, model_cls: type, name: str, field: Any) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.add_field, model_cls, name, field)

    async def aremove_field(self, model_cls: type, name: str) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.remove_field, model_cls, name)

    async def aalter_field(
        self, model_cls: type, name: str, field: Any
    ) -> None:
        import asyncio

        await asyncio.to_thread(
            self._inner.alter_field, model_cls, name, field
        )

    async def aexecute(self, sql: str, params: list | None = None) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.execute, sql, params)


__all__ = ["SchemaEditor", "AsyncSchemaEditor"]
