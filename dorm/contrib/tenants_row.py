"""Row-level multi-tenancy.

Two flavours of multi-tenant data live side by side in a single
database:

- **Schema-level** (``dorm.contrib.tenants``) — each tenant gets its
  own PostgreSQL schema. Heavyweight (one schema per tenant means
  more migrations to run) but isolation is hard.
- **Row-level** — every tenant-scoped table carries a ``tenant_id``
  column. Every query is implicitly filtered by the active tenant.
  Lighter weight, works on any backend (SQLite / MySQL / PG /
  libsql), but isolation depends entirely on the application
  always remembering the filter.

This module provides the row-level path. The contract:

1. Subclass :class:`TenantModel` instead of :class:`dorm.Model` —
   it ships a default ``tenant_id`` :class:`~dorm.fields.CharField`
   plus a manager that scopes every queryset to the active tenant.
2. Wrap request / job processing in :func:`current_tenant` so the
   manager knows which tenant value to use.
3. Writes auto-fill ``tenant_id`` from the active tenant; reads
   filter on it.

Bypass paths exist for back-office tasks (cross-tenant reports,
admin dashboards): :attr:`TenantModel.unscoped` returns a queryset
that does **not** filter by tenant. The escape hatch is explicit on
purpose — every call site that uses it surfaces in code review.
"""

from __future__ import annotations

import contextlib
import contextvars
from typing import Any

from .. import fields
from ..manager import Manager
from ..models import Model


class NoActiveTenantError(RuntimeError):
    """Raised when a query against a tenant-scoped model runs
    without an active tenant.

    Silent fallback to "no filter" would leak rows across tenants —
    the worst kind of multi-tenant bug. The explicit failure forces
    every call site to either set a tenant or use
    :attr:`TenantModel.unscoped` deliberately."""


_ACTIVE_TENANT: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "dorm_active_tenant", default=None
)


@contextlib.contextmanager
def current_tenant(tenant_id: Any):
    """Pin *tenant_id* as the active tenant for the surrounding
    block.

    Per-task (asyncio) and per-thread context — does not bleed
    between requests. Nested calls stack: the inner pin wins for
    its own scope, the outer pin is restored on exit.
    """
    if tenant_id is None:
        raise ValueError("current_tenant(): tenant_id must not be None")
    token = _ACTIVE_TENANT.set(tenant_id)
    try:
        yield tenant_id
    finally:
        _ACTIVE_TENANT.reset(token)


def get_active_tenant() -> Any | None:
    """Return the currently-pinned tenant id, or ``None``."""
    return _ACTIVE_TENANT.get()


class TenantManager(Manager):
    """Manager that filters every queryset by the active tenant.

    On each call to :meth:`get_queryset` we read the active tenant
    from the contextvar and inject ``filter(<tenant_field>=...)``.
    When no tenant is active the manager raises
    :class:`NoActiveTenantError` rather than silently returning rows
    from every tenant.

    The column name used for the filter is read from the model's
    ``Meta.tenant_field`` if present (default ``"tenant_id"``), so
    user code that overrides the column doesn't have to subclass the
    manager too.
    """

    # Class-level fallback — overridable on the instance or via
    # ``Meta.tenant_field``. The instance / Meta wins at call time.
    tenant_field: str = "tenant_id"

    def _resolved_field(self) -> str:
        if self.model is not None:
            meta_field = getattr(self.model._meta, "tenant_field", None)
            if isinstance(meta_field, str) and meta_field:
                return meta_field
        return self.tenant_field

    def get_queryset(self):
        qs = super().get_queryset()
        tenant = _ACTIVE_TENANT.get()
        if tenant is None:
            model_name = (
                self.model.__name__ if self.model is not None else "<model>"
            )
            raise NoActiveTenantError(
                f"No active tenant — wrap the call in "
                f"`with current_tenant(<tenant_id>):` or use "
                f"`{model_name}.unscoped` for a deliberate "
                f"cross-tenant query."
            )
        return qs.filter(**{self._resolved_field(): tenant})


class _UnscopedManager(Manager):
    """Escape-hatch manager that does NOT filter by tenant.

    Bound at ``TenantModel.unscoped`` so admin / cross-tenant call
    sites are textually obvious in the diff."""


def make_async_tenant_manager() -> type:
    """Build an ``AsyncOnlyManager`` + ``TenantManager`` composite.

    Use when a model needs **both** strict async-only semantics
    (sync calls raise ``AsyncOnlyError``) **and** row-level tenant
    scoping. The plain ``class Foo(TenantModel, AsyncModel)`` MRO
    picks the first manager it finds (TenantManager) and silently
    drops AsyncOnlyManager's enforcement, so the helper here gives
    callers a single explicit class that mixes both behaviours.

    Example::

        from dorm.contrib.asyncmodel import AsyncModel
        from dorm.contrib.tenants_row import (
            TenantModel, make_async_tenant_manager,
        )

        class Order(TenantModel, AsyncModel):
            title = dorm.CharField(max_length=200)

            objects = make_async_tenant_manager()()
    """
    from .asyncmodel import AsyncOnlyManager

    class AsyncTenantManager(TenantManager, AsyncOnlyManager):
        """TenantManager that also rejects sync ORM calls.

        Resolution order: AsyncOnlyManager's ``__getattribute__``
        runs first (most-derived MRO entry), so sync method names
        in ``_SYNC_FORBIDDEN`` raise before the tenant filter ever
        runs. Async paths fall through to ``TenantManager``'s
        ``get_queryset`` which scopes the queryset to the active
        tenant.
        """

    return AsyncTenantManager


class TenantModel(Model):
    """Base class for row-level tenant-scoped models.

    Adds:

    - ``tenant_id`` :class:`~dorm.fields.CharField` (override by
      redeclaring in the subclass with a different field type if
      you prefer ``UUIDField`` or ``IntegerField``).
    - ``objects`` — :class:`TenantManager`, scoped reads/writes.
    - ``unscoped`` — :class:`_UnscopedManager`, the escape hatch.
    - :meth:`save` / :meth:`asave` auto-fill ``tenant_id`` from the
      active tenant when the field is unset.

    Override ``Meta.tenant_field`` to change the column name (e.g.
    ``"org_id"`` if your domain uses orgs not tenants).
    """

    tenant_id = fields.CharField(max_length=64, db_index=True)

    objects = TenantManager()
    unscoped = _UnscopedManager()

    class Meta:
        abstract = True

    def _autofill_tenant(self) -> None:
        # Honour ``Meta.tenant_field`` so subclasses that renamed the
        # column to ``org_id`` / ``account_id`` don't have to re-
        # implement save / asave. Default ``"tenant_id"`` matches the
        # built-in field declared above.
        field_name = getattr(self._meta, "tenant_field", "tenant_id") or "tenant_id"
        if getattr(self, field_name, None):
            return
        tenant = _ACTIVE_TENANT.get()
        if tenant is None:
            raise NoActiveTenantError(
                f"{type(self).__name__}.save() called without an "
                f"active tenant. Wrap the call in "
                f"`with current_tenant(<tenant_id>):`."
            )
        setattr(self, field_name, tenant)

    def save(self, *args: Any, **kwargs: Any) -> None:
        self._autofill_tenant()
        return super().save(*args, **kwargs)

    async def asave(self, *args: Any, **kwargs: Any) -> None:
        self._autofill_tenant()
        return await super().asave(*args, **kwargs)


__all__ = [
    "NoActiveTenantError",
    "TenantManager",
    "TenantModel",
    "current_tenant",
    "get_active_tenant",
    "make_async_tenant_manager",
]
