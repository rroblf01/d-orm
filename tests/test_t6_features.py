"""Tier-6 (sugar) features added in v4.2."""
from __future__ import annotations

import pytest


# ── F.apply / Func.apply chainable ──────────────────────────────────────────


class TestFApply:
    def test_f_apply_wraps_in_func(self):
        from dorm.expressions import F
        from dorm.functions import Lower

        wrapped = F("name").apply(Lower)
        assert isinstance(wrapped, Lower)
        assert wrapped.expressions[0].name == "name"

    def test_chainable_apply(self):
        from dorm.expressions import F
        from dorm.functions import Lower, Upper

        wrapped = F("name").apply(Lower).apply(Upper)
        assert isinstance(wrapped, Upper)
        assert isinstance(wrapped.expressions[0], Lower)

    def test_apply_with_extra_args(self):
        from dorm.expressions import F
        from dorm.functions import Substr

        wrapped = F("name").apply(Substr, 1, 4)
        assert isinstance(wrapped, Substr)
        assert wrapped.expressions[1] == 1
        assert wrapped.expressions[2] == 4


# ── QuerySet.lookup shortcut ────────────────────────────────────────────────


class TestQuerySetLookup:
    def test_lookup_returns_subquery(self):
        import dorm
        from dorm.expressions import Subquery

        class _L(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        sub = _L.objects.all().lookup()
        assert isinstance(sub, Subquery)

    def test_lookup_with_column_projects(self):
        import dorm
        from dorm.expressions import Subquery

        class _LP(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        sub = _LP.objects.all().lookup("name")
        assert isinstance(sub, Subquery)


# ── Manager.union_with ──────────────────────────────────────────────────────


class TestUnionWith:
    def test_invalid_source_rejected(self):
        import dorm

        class _U(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with pytest.raises(TypeError, match="QuerySet"):
            _U.objects.union_with(42)

    def test_managers_compose(self):
        import dorm

        class _Source(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        class _Other(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        combined = _Source.objects.union_with(_Other.objects, all=True)
        assert combined is not None


# ── SQL allow-list ──────────────────────────────────────────────────────────


class TestSQLAllowList:
    @pytest.fixture(autouse=True)
    def _cleanup(self):
        from dorm.contrib import sql_allowlist

        yield
        sql_allowlist.uninstall()

    def test_install_blocks_unknown_query(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT * FROM allowed_table"],
            allow_ddl=False,
            raise_on_violation=True,
        )
        with pytest.raises(sql_allowlist.SQLNotAllowedError):
            signals.pre_query.send(
                sender="sqlite",
                sql="SELECT * FROM forbidden_table",
                params=None,
            )

    def test_allowed_template_passes(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT * FROM allowed_table WHERE id = 1"],
            allow_ddl=False,
        )
        # Same template (literal value differs) — must pass.
        signals.pre_query.send(
            sender="sqlite",
            sql="SELECT * FROM allowed_table WHERE id = 42",
            params=None,
        )

    def test_ddl_bypass_default(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT 1"], allow_ddl=True, raise_on_violation=True
        )
        # DDL bypasses the allow-list when allow_ddl=True (default).
        signals.pre_query.send(
            sender="sqlite", sql="CREATE TABLE x (id INTEGER)", params=None
        )

    def test_ddl_enforced_when_disabled(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT 1"], allow_ddl=False, raise_on_violation=True
        )
        with pytest.raises(sql_allowlist.SQLNotAllowedError):
            signals.pre_query.send(
                sender="sqlite",
                sql="CREATE TABLE x (id INTEGER)",
                params=None,
            )

    def test_log_only_mode_records_violations(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT 1"], raise_on_violation=False, allow_ddl=False
        )
        signals.pre_query.send(
            sender="sqlite", sql="SELECT bogus", params=None
        )
        rejected = sql_allowlist.rejected_templates()
        assert rejected and "bogus" in rejected[0]

    def test_uninstall_disables(self):
        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(["SELECT 1"], allow_ddl=False)
        sql_allowlist.uninstall()
        # No exception even though SQL doesn't match.
        signals.pre_query.send(
            sender="sqlite", sql="SELECT anything", params=None
        )

    def test_dump_and_reload(self, tmp_path):
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT name FROM t WHERE id = 1", "DELETE FROM logs WHERE day < 1"],
            raise_on_violation=False,
            allow_ddl=False,
        )
        path = tmp_path / "allow.json"
        sql_allowlist.dump_captured(str(path))
        assert path.exists()

        # Wipe + reload.
        sql_allowlist.uninstall()
        loaded = sql_allowlist.load_from_file(
            str(path), raise_on_violation=False, allow_ddl=False
        )
        assert loaded == 2
        templates = set(sql_allowlist.allowed_templates())
        assert any("SELECT" in t for t in templates)
        assert any("DELETE" in t for t in templates)

    def test_dump_with_rejected_section(self, tmp_path):
        import json

        from dorm import signals
        from dorm.contrib import sql_allowlist

        sql_allowlist.install(
            ["SELECT 1"], raise_on_violation=False, allow_ddl=False
        )
        signals.pre_query.send(
            sender="sqlite", sql="SELECT bogus", params=None
        )
        path = tmp_path / "audit.json"
        sql_allowlist.dump_captured(str(path))
        payload = json.loads(path.read_text())
        assert "allowed" in payload and "rejected" in payload
        assert any("bogus" in t for t in payload["rejected"])
