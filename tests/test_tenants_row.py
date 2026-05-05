"""Tests for ``dorm.contrib.tenants_row``."""

from __future__ import annotations

import pytest

import dorm
from dorm.contrib.tenants_row import (
    NoActiveTenantError,
    TenantModel,
    current_tenant,
    get_active_tenant,
)
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _Note(TenantModel):
    title = dorm.CharField(max_length=100)

    class Meta:
        db_table = "tenant_notes"
        app_label = "tests"


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "tenant_notes"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Note._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "tenant_notes" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "tenant_notes"{cascade}')


def test_get_active_tenant_returns_none_outside_block():
    assert get_active_tenant() is None


def test_current_tenant_pins_value():
    with current_tenant("acme"):
        assert get_active_tenant() == "acme"
    assert get_active_tenant() is None


def test_current_tenant_nested():
    with current_tenant("a"):
        with current_tenant("b"):
            assert get_active_tenant() == "b"
        assert get_active_tenant() == "a"


def test_current_tenant_rejects_none():
    with pytest.raises(ValueError):
        with current_tenant(None):
            pass


def test_save_without_tenant_raises():
    with pytest.raises(NoActiveTenantError):
        _Note(title="x").save()


def test_save_autofills_tenant():
    with current_tenant("acme"):
        n = _Note(title="x")
        n.save()
        assert n.tenant_id == "acme"


def test_query_scoped_to_active_tenant():
    with current_tenant("acme"):
        _Note.objects.create(title="acme-1")
        _Note.objects.create(title="acme-2")
    with current_tenant("globex"):
        _Note.objects.create(title="globex-1")

    with current_tenant("acme"):
        rows = list(_Note.objects.all())
        assert {r.title for r in rows} == {"acme-1", "acme-2"}

    with current_tenant("globex"):
        rows = list(_Note.objects.all())
        assert {r.title for r in rows} == {"globex-1"}


def test_query_without_tenant_raises():
    with pytest.raises(NoActiveTenantError):
        list(_Note.objects.all())


def test_unscoped_returns_every_tenant():
    with current_tenant("a"):
        _Note.objects.create(title="a-1")
    with current_tenant("b"):
        _Note.objects.create(title="b-1")

    rows = list(_Note.unscoped.all())
    assert {r.title for r in rows} == {"a-1", "b-1"}


def test_explicit_tenant_id_not_overwritten():
    with current_tenant("acme"):
        n = _Note(title="x", tenant_id="explicit")
        n.save()
        assert n.tenant_id == "explicit"


@pytest.mark.asyncio
async def test_asave_autofills_tenant():
    with current_tenant("acme"):
        n = _Note(title="async-x")
        await n.asave()
        assert n.tenant_id == "acme"


@pytest.mark.asyncio
async def test_asave_without_tenant_raises():
    with pytest.raises(NoActiveTenantError):
        await _Note(title="x").asave()
