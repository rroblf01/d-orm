"""Tests for PostgreSQL Row-Level Security migration operations.

We exercise the DDL emitter via a fake connection that records every
``execute_script`` call. The ops are PG-only so we don't need a live
PG cluster — the goal is to lock in the SQL shape, the no-op behaviour
on non-PG backends, and reversibility semantics.
"""
from __future__ import annotations

import pytest

from dorm.migrations.operations import (
    AlterPolicy,
    CreatePolicy,
    DropPolicy,
    EnableRowLevelSecurity,
    ForceRowLevelSecurity,
)


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)


# ── EnableRowLevelSecurity / DisableRowLevelSecurity ─────────────────────────


class TestEnableRowLevelSecurity:
    def test_emits_alter_enable_on_postgres(self):
        op = EnableRowLevelSecurity("articles")
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == [
            'ALTER TABLE "articles" ENABLE ROW LEVEL SECURITY'
        ]

    def test_reverse_emits_disable(self):
        op = EnableRowLevelSecurity("articles")
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        assert conn.scripts == [
            'ALTER TABLE "articles" DISABLE ROW LEVEL SECURITY'
        ]

    def test_noop_on_sqlite(self):
        op = EnableRowLevelSecurity("articles")
        conn = _FakeConn(vendor="sqlite")
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == []

    def test_describe(self):
        assert "Enable RLS" in EnableRowLevelSecurity("x").describe()


class TestForceRowLevelSecurity:
    def test_force_emits_alter_force(self):
        op = ForceRowLevelSecurity("articles")
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == [
            'ALTER TABLE "articles" FORCE ROW LEVEL SECURITY'
        ]

    def test_force_reverse_emits_no_force(self):
        op = ForceRowLevelSecurity("articles")
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        assert conn.scripts == [
            'ALTER TABLE "articles" NO FORCE ROW LEVEL SECURITY'
        ]


# ── CreatePolicy ─────────────────────────────────────────────────────────────


class TestCreatePolicy:
    def test_basic_select_policy(self):
        op = CreatePolicy(
            "p_owner_select",
            "articles",
            command="SELECT",
            using="owner_id = current_setting('app.user_id')::int",
        )
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == [
            'CREATE POLICY "p_owner_select" ON "articles" FOR SELECT '
            "USING (owner_id = current_setting('app.user_id')::int)"
        ]

    def test_insert_policy_with_check(self):
        op = CreatePolicy(
            "p_owner_insert",
            "articles",
            command="INSERT",
            check="owner_id = current_setting('app.user_id')::int",
        )
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert "FOR INSERT" in conn.scripts[0]
        assert "WITH CHECK" in conn.scripts[0]

    def test_restrictive_policy(self):
        op = CreatePolicy(
            "p_block_delete",
            "articles",
            command="DELETE",
            using="false",
            permissive=False,
        )
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert "AS RESTRICTIVE" in conn.scripts[0]

    def test_roles_clause(self):
        op = CreatePolicy(
            "p_tenant",
            "articles",
            command="ALL",
            roles=["app_user", "app_admin"],
            using="tenant_id = current_setting('app.tenant_id')::int",
        )
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert 'TO "app_user", "app_admin"' in conn.scripts[0]

    def test_select_without_using_rejected(self):
        with pytest.raises(ValueError, match="requires a 'using'"):
            CreatePolicy("p", "t", command="SELECT")

    def test_insert_without_check_rejected(self):
        with pytest.raises(ValueError, match="requires a 'check'"):
            CreatePolicy("p", "t", command="INSERT")

    def test_invalid_command_rejected(self):
        with pytest.raises(ValueError, match="command"):
            CreatePolicy(
                "p", "t", command="TRUNCATE", using="true"
            )

    def test_reverse_drops_policy(self):
        op = CreatePolicy(
            "p_owner_select",
            "articles",
            command="SELECT",
            using="true",
        )
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        assert conn.scripts == [
            'DROP POLICY IF EXISTS "p_owner_select" ON "articles"'
        ]

    def test_noop_on_sqlite(self):
        op = CreatePolicy(
            "p_owner_select", "articles", command="SELECT", using="true"
        )
        conn = _FakeConn(vendor="sqlite")
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == []


# ── DropPolicy ───────────────────────────────────────────────────────────────


class TestDropPolicy:
    def test_drop_emits_if_exists(self):
        op = DropPolicy("p", "articles")
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == [
            'DROP POLICY IF EXISTS "p" ON "articles"'
        ]

    def test_irreversible_by_default(self):
        op = DropPolicy("p", "articles")
        assert not op.reversible
        with pytest.raises(NotImplementedError):
            op.database_backwards("app", _FakeConn(), None, None)

    def test_reversible_when_reverse_args_supplied(self):
        op = DropPolicy(
            "p",
            "articles",
            reverse_command="SELECT",
            reverse_using="true",
        )
        assert op.reversible
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        assert "CREATE POLICY" in conn.scripts[0]


# ── AlterPolicy ──────────────────────────────────────────────────────────────


class TestAlterPolicy:
    def test_alter_using_only(self):
        op = AlterPolicy("p", "articles", using="owner_id = 1")
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert conn.scripts == [
            'ALTER POLICY "p" ON "articles" USING (owner_id = 1)'
        ]

    def test_alter_requires_at_least_one_change(self):
        with pytest.raises(ValueError, match="at least one"):
            AlterPolicy("p", "articles")

    def test_alter_irreversible_without_previous(self):
        op = AlterPolicy("p", "articles", using="true")
        assert not op.reversible
        with pytest.raises(NotImplementedError):
            op.database_backwards("app", _FakeConn(), None, None)

    def test_alter_reverse_uses_previous_values(self):
        op = AlterPolicy(
            "p",
            "articles",
            using="new_predicate",
            previous_using="old_predicate",
        )
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        assert "USING (old_predicate)" in conn.scripts[0]

    def test_alter_roles_to_public_when_empty(self):
        op = AlterPolicy("p", "articles", roles=[])
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        assert "TO PUBLIC" in conn.scripts[0]
