"""Tests for AlterColumnTypeOnline migration op."""
from __future__ import annotations

import pytest

from dorm.migrations.operations import AlterColumnTypeOnline


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)

    def atomic(self):
        return _NoopAtomic()


class _NoopAtomic:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _State:
    def __init__(self):
        self.models = {
            "app.user": {"options": {"db_table": "user"}, "fields": {}}
        }


class TestAlterColumnTypeOnline:
    def test_emits_alter_with_lock_timeout(self):
        op = AlterColumnTypeOnline("User", "age", "BIGINT")
        conn = _FakeConn()
        op.database_forwards("app", conn, _State(), _State())
        # Three statements: SET LOCAL, ALTER TABLE
        assert "SET LOCAL lock_timeout" in conn.scripts[0]
        assert "5s" in conn.scripts[0]
        assert 'ALTER TABLE "user"' in conn.scripts[1]
        assert "TYPE BIGINT" in conn.scripts[1]
        assert "USING" in conn.scripts[1]

    def test_custom_lock_timeout(self):
        op = AlterColumnTypeOnline("User", "age", "BIGINT", lock_timeout="500ms")
        conn = _FakeConn()
        op.database_forwards("app", conn, _State(), _State())
        assert "500ms" in conn.scripts[0]

    def test_custom_using_clause(self):
        op = AlterColumnTypeOnline(
            "User", "age", "TEXT", using='to_char("age", \'999\')'
        )
        conn = _FakeConn()
        op.database_forwards("app", conn, _State(), _State())
        assert 'to_char("age"' in conn.scripts[1]

    def test_non_pg_rejected(self):
        op = AlterColumnTypeOnline("User", "age", "BIGINT")
        conn = _FakeConn(vendor="sqlite")
        with pytest.raises(NotImplementedError):
            op.database_forwards("app", conn, _State(), _State())

    def test_irreversible_without_old_type(self):
        op = AlterColumnTypeOnline("User", "age", "BIGINT")
        assert op.reversible is False
        with pytest.raises(NotImplementedError):
            op.database_backwards("app", _FakeConn(), _State(), _State())

    def test_reversible_with_old_type(self):
        op = AlterColumnTypeOnline(
            "User", "age", "BIGINT", old_type="INTEGER"
        )
        assert op.reversible is True
        conn = _FakeConn()
        op.database_backwards("app", conn, _State(), _State())
        assert "TYPE INTEGER" in conn.scripts[1]

    def test_empty_new_type_rejected(self):
        with pytest.raises(ValueError, match="new_type"):
            AlterColumnTypeOnline("User", "age", "")

    def test_describe(self):
        d = AlterColumnTypeOnline("User", "age", "BIGINT").describe()
        assert "User.age" in d
        assert "BIGINT" in d
