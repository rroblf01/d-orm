"""Schema-per-tenant routing — scaffold for v3.0 (full impl v3.1).

Switches PostgreSQL ``search_path`` per request / context so the
same model classes route to a tenant-specific schema. The shape
below is the public API; the full implementation (per-tenant
migration runner, schema bootstrap helpers, signal-driven
schema-create-on-tenant-add) lands in v3.1.

Today this module ships:

- ``TenantContext`` context manager that switches ``search_path``
  on the active connection.
- ``current_tenant()`` lookup for routers.
- ``register_tenant(name)`` for the future migration runner.

Usage from a FastAPI / Starlette middleware::

    from dorm.contrib.tenants import TenantContext

    @app.middleware("http")
    async def tenant_middleware(request, call_next):
        tenant = resolve_tenant_from_host(request)
        async with TenantContext(tenant):
            return await call_next(request)

Each tenant ends up reading / writing against its own schema; the
public schema (``public``) keeps shared / cross-tenant tables.
"""

from __future__ import annotations

import contextvars
import re
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Iterator

# Schema name → must be a SQL identifier; anything else gets
# rejected to prevent ``search_path`` injection. Mirrors the
# ``_validate_identifier`` shape used in conf.py.
_SAFE_SCHEMA = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Per-task active tenant. ``None`` means "use the default
# search_path" (typically ``"public"``).
_active_tenant: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "dorm_active_tenant", default=None
)

# Set of registered tenants — used by the future per-tenant
# migration runner to know which schemas to ``CREATE SCHEMA`` and
# migrate. The runtime tenant context manager doesn't consult this
# set (callers may have tenants whose schema already exists in the
# DB but isn't tracked here).
_registered_tenants: set[str] = set()


def _validate_schema_name(name: str) -> None:
    if not isinstance(name, str) or not _SAFE_SCHEMA.match(name):
        raise ValueError(
            f"Tenant schema name must match {_SAFE_SCHEMA.pattern!r}; "
            f"got {name!r}. Schema names are spliced into "
            "``SET search_path`` and cannot be parameterised."
        )


def current_tenant() -> str | None:
    """Return the active tenant name on the current task / thread,
    or ``None`` if no ``TenantContext`` is in scope."""
    return _active_tenant.get()


def register_tenant(name: str) -> None:
    """Add *name* to the in-process tenant registry. The migration
    runner (v3.1) reads this list to know which schemas to migrate.
    Idempotent."""
    _validate_schema_name(name)
    _registered_tenants.add(name)


def registered_tenants() -> set[str]:
    """Return a copy of the registered tenant set."""
    return set(_registered_tenants)


def _set_search_path(schema: str | None, *, using: str = "default") -> None:
    """Apply ``SET search_path = schema, public`` to the live
    connection. ``None`` reverts to ``public``."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        # Multi-tenant via search_path is PG-specific. Other
        # backends would need a different approach (per-tenant
        # database, per-tenant prefix in a shared table, …); refuse
        # loudly so the user doesn't think it worked.
        raise NotImplementedError(
            "TenantContext only supports the PostgreSQL backend. "
            "Other backends need application-level routing."
        )
    if schema is None:
        conn.execute_script("SET search_path = public")
        return
    _validate_schema_name(schema)
    conn.execute_script(f"SET search_path = {schema}, public")


@contextmanager
def TenantContext(name: str, *, using: str = "default") -> Iterator[str]:
    """Switch the connection's ``search_path`` to *name* for the
    duration of the block. Yields the schema name back so callers
    that want to log / pass it can do so without re-deriving."""
    _validate_schema_name(name)
    previous = _active_tenant.get()
    token = _active_tenant.set(name)
    try:
        _set_search_path(name, using=using)
        yield name
    finally:
        _active_tenant.reset(token)
        # Restore the OUTER tenant — supports nested contexts (rare
        # but legal, e.g. cross-tenant aggregation jobs).
        _set_search_path(previous, using=using)


@asynccontextmanager
async def aTenantContext(name: str, *, using: str = "default") -> AsyncIterator[str]:
    """Async counterpart of :class:`TenantContext`. Holds the
    search_path swap inside an asyncio task so concurrent requests
    each get their own tenant routing."""
    _validate_schema_name(name)
    previous = _active_tenant.get()
    token = _active_tenant.set(name)
    try:
        # ``_set_search_path`` runs sync DDL through the connection
        # wrapper; on the async path we want it to flow through the
        # async backend. The PG async wrapper exposes a
        # ``execute_script`` that's a coroutine — call it directly.
        from ..db.connection import get_async_connection

        conn: Any = get_async_connection(using)
        if getattr(conn, "vendor", "sqlite") != "postgresql":
            raise NotImplementedError(
                "aTenantContext only supports the PostgreSQL backend."
            )
        await conn.execute_script(f"SET search_path = {name}, public")
        yield name
    finally:
        _active_tenant.reset(token)
        from ..db.connection import get_async_connection

        conn = get_async_connection(using)
        if getattr(conn, "vendor", "sqlite") == "postgresql":
            target = previous if previous is not None else "public"
            if previous is not None:
                _validate_schema_name(previous)
            await conn.execute_script(f"SET search_path = {target}, public")


def ensure_schema(name: str, *, using: str = "default") -> None:
    """Run ``CREATE SCHEMA IF NOT EXISTS <name>`` against *using*.

    Idempotent. PG-only — the migration runner needs a place to put
    the tenant's tables before it can flip search_path and apply the
    DDL. Refuses loudly on non-PG backends because there's no portable
    equivalent (sqlite has no schemas; mysql conflates schema with
    database).
    """
    from ..db.connection import get_connection

    _validate_schema_name(name)
    conn = get_connection(using)
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        raise NotImplementedError(
            "ensure_schema() only supports the PostgreSQL backend. "
            "Other backends need application-level routing."
        )
    conn.execute_script(f"CREATE SCHEMA IF NOT EXISTS {name}")


def migrate_tenant(
    name: str,
    *,
    using: str = "default",
    verbosity: int = 1,
    apps: list[str] | None = None,
) -> None:
    """Apply migrations for one tenant.

    Steps:
      1. ``CREATE SCHEMA IF NOT EXISTS <name>``.
      2. Switch ``search_path`` so DDL lands in the tenant's schema.
      3. Run :class:`MigrationExecutor.migrate` for each INSTALLED_APPS
         entry (or the subset passed via *apps*).
      4. Restore the previous ``search_path``.

    The recorder table itself lives in the tenant's schema — each
    tenant tracks its own migration application state independently.
    Bootstrapping a fresh tenant therefore re-runs every migration
    from the top.
    """
    from ..conf import settings
    from ..db.connection import get_connection
    from ..migrations.executor import MigrationExecutor

    ensure_schema(name, using=using)
    installed_apps: list[str] = list(getattr(settings, "INSTALLED_APPS", []))
    target_apps = apps if apps is not None else installed_apps

    from ..transaction import atomic

    conn = get_connection(using)
    # Pin a single connection for the whole tenant migration. Without
    # the surrounding ``atomic()`` block, the PG pool checks out a
    # fresh connection per ``execute_script`` call — and the
    # ``SET search_path`` we ran from ``TenantContext`` would only
    # stick to that one connection, while the migration's DDL would
    # land on a *different* checkout that still points at ``public``.
    with atomic(using=using):
        with TenantContext(name, using=using):
            executor = MigrationExecutor(conn, verbosity=verbosity)
            for raw_app in target_apps:
                mig_dir = _resolve_migrations_dir(raw_app)
                if mig_dir is None:
                    continue
                app_label = _resolve_app_label(raw_app)
                executor.migrate(app_label, mig_dir)


def migrate_all_tenants(
    *,
    using: str = "default",
    verbosity: int = 1,
    apps: list[str] | None = None,
) -> dict[str, str]:
    """Migrate every tenant in :func:`registered_tenants`.

    Returns ``{tenant_name: "ok" | "error: …"}`` so a CI step can
    surface partial failures without aborting the whole batch.
    Caller decides whether to fail loud or aggregate — neither is
    universally right (a partial deploy can be desired during a slow
    rollout, but a CI gate wants strict).
    """
    results: dict[str, str] = {}
    for tenant in sorted(registered_tenants()):
        try:
            migrate_tenant(
                tenant, using=using, verbosity=verbosity, apps=apps
            )
            results[tenant] = "ok"
        except Exception as exc:  # noqa: BLE001 — partial failures must propagate to summary
            results[tenant] = f"error: {exc!r}"
    return results


def _resolve_migrations_dir(installed_app: str):
    """Translate an INSTALLED_APPS entry to its on-disk migrations
    directory, or ``None`` if the app has no migrations folder.

    Uses the same resolution logic as the main CLI so per-tenant
    migrations and global ``dorm migrate`` agree on which files to
    run.
    """
    import importlib
    from pathlib import Path

    try:
        mod = importlib.import_module(installed_app)
        if mod.__file__ is None:
            return None
        base = Path(mod.__file__).parent
    except Exception:
        return None
    mig_dir = base / "migrations"
    return mig_dir if mig_dir.exists() else None


def _resolve_app_label(installed_app: str) -> str:
    """Mirror ``dorm.cli._resolve_app_label`` so per-tenant runs and
    global ``dorm migrate`` use the same label resolution rules."""
    from ..models import _model_registry

    candidates: set[str] = set()
    for key, model_cls in _model_registry.items():
        if "." in key:
            continue
        mod = getattr(model_cls, "__module__", "")
        if mod == installed_app or mod.startswith(installed_app + "."):
            label = getattr(model_cls._meta, "app_label", "") or ""
            if label:
                candidates.add(label)
    if len(candidates) == 1:
        return next(iter(candidates))
    return installed_app


__all__ = [
    "TenantContext",
    "aTenantContext",
    "current_tenant",
    "ensure_schema",
    "migrate_all_tenants",
    "migrate_tenant",
    "register_tenant",
    "registered_tenants",
]
