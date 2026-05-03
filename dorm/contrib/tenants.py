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


__all__ = [
    "TenantContext",
    "aTenantContext",
    "current_tenant",
    "register_tenant",
    "registered_tenants",
]
