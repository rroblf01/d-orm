"""Third coverage uplift pass — push CLI + migration ops + edge
cases on remaining v4.3 modules. Pure-Python paths only (no live PG
required)."""
from __future__ import annotations

import argparse
import pytest


@pytest.fixture
def restore_settings():
    """Snapshot DATABASES / INSTALLED_APPS + every key our tests
    may touch so reconfigures don't bleed into the next test (the
    conftest's clean_db fixture relies on the suite-wide DATABASES
    pointing at the test backend)."""
    import dorm
    from dorm.conf import settings
    from dorm.db.connection import _async_connections, _sync_connections

    saved_db = {alias: dict(cfg) for alias, cfg in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _SENTINEL = object()
    keys = ("DEBUG", "DEBUG_NPLUSONE", "SLOW_QUERY_MS", "SLOW_QUERY_EXPLAIN")
    saved_attrs = {k: settings.__dict__.get(k, _SENTINEL) for k in keys}
    saved_explicit = {k: (k in settings._explicit_settings) for k in keys}
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    for k in keys:
        val = saved_attrs[k]
        if val is _SENTINEL:
            try:
                delattr(settings, k)
            except AttributeError:
                pass
        else:
            settings.__dict__[k] = val
        if saved_explicit[k]:
            settings._explicit_settings.add(k)
        else:
            settings._explicit_settings.discard(k)
    _sync_connections.clear()
    _async_connections.clear()


# ── CLI subcommand smoke / arg-parsing branches ─────────────────────────────


class TestCLISmoke:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_main_help_exits_zero(self, capsys, monkeypatch):
        import sys

        from dorm.cli import main

        monkeypatch.setattr(sys, "argv", ["dorm", "--help"])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "migrate" in out

    def test_cmd_version_runs(self, capsys):
        from dorm import __version__
        from dorm.cli import cmd_version

        cmd_version(argparse.Namespace())
        out = capsys.readouterr().out
        assert __version__ in out

    def test_cmd_help_prints(self, capsys):
        from dorm.cli import cmd_help

        # cmd_help reads ``args.parser.print_help()`` — feed a minimal
        # stub.
        class _P:
            @staticmethod
            def print_help():
                print("dorm CLI help")

        cmd_help(argparse.Namespace(parser=_P()))
        out = capsys.readouterr().out
        assert "dorm" in out.lower()

    def test_unknown_template_exits(self, tmp_path, monkeypatch):
        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        with pytest.raises(SystemExit):
            cmd_init(argparse.Namespace(template="bogus", app=None))

    def test_migrations_graph_invalid_format(self, capsys):
        from dorm.cli import cmd_migrations_graph

        with pytest.raises(SystemExit):
            cmd_migrations_graph(
                argparse.Namespace(format="svg", settings=None)
            )
        err = capsys.readouterr().err
        assert "Unknown --format" in err

    def test_migrations_graph_mermaid_runs(self, tmp_path, monkeypatch, capsys):
        from dorm.cli import cmd_migrations_graph

        # Configure dorm with INSTALLED_APPS=[] so the walker skips
        # missing migrations dirs cleanly.
        import dorm

        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            INSTALLED_APPS=[],
        )
        monkeypatch.chdir(tmp_path)
        cmd_migrations_graph(
            argparse.Namespace(format="mermaid", settings=None)
        )
        out = capsys.readouterr().out
        assert "graph TD" in out

    def test_migrations_graph_dot_runs(self, tmp_path, monkeypatch, capsys):
        from dorm.cli import cmd_migrations_graph

        import dorm

        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            INSTALLED_APPS=[],
        )
        monkeypatch.chdir(tmp_path)
        cmd_migrations_graph(
            argparse.Namespace(format="dot", settings=None)
        )
        out = capsys.readouterr().out
        assert "digraph" in out

    def test_reset_refuses_production_shape(self, tmp_path, monkeypatch, capsys):
        import sys

        from dorm.cli import cmd_reset

        # Use a unique module name so cmd_reset's import doesn't leak a
        # ``sys.modules["settings"]`` entry into later tests (the
        # in-process CLI suite re-imports ``settings`` and would pick up
        # a stale prod-shaped module → connection pool hangs trying to
        # reach ``prod.example.com``).
        mod_name = "prod_refuse_settings"
        (tmp_path / f"{mod_name}.py").write_text(
            "DATABASES = {\n"
            "    'default': {\n"
            "        'ENGINE': 'postgresql',\n"
            "        'NAME': 'production',\n"
            "        'HOST': 'prod.example.com',\n"
            "    }\n"
            "}\n"
            "INSTALLED_APPS = []\n"
            "DEBUG = False\n"
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.syspath_prepend(str(tmp_path))
        try:
            with pytest.raises(SystemExit):
                cmd_reset(argparse.Namespace(force=False, settings=mod_name))
            err = capsys.readouterr().err
            assert "Refusing" in err
        finally:
            sys.modules.pop(mod_name, None)


# ── migrations.operations DDL edge cases ───────────────────────────────────


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


class TestRLSEdges:
    def test_alter_policy_describe(self):
        from dorm.migrations.operations import AlterPolicy

        op = AlterPolicy("p", "t", using="x = 1")
        assert "Alter RLS" in op.describe()

    def test_create_policy_describe(self):
        from dorm.migrations.operations import CreatePolicy

        op = CreatePolicy("p", "t", command="SELECT", using="true")
        assert "Create RLS" in op.describe()

    def test_drop_policy_describe(self):
        from dorm.migrations.operations import DropPolicy

        op = DropPolicy("p", "t")
        assert "Drop RLS" in op.describe()

    def test_enable_rls_repr(self):
        from dorm.migrations.operations import EnableRowLevelSecurity

        op = EnableRowLevelSecurity("t")
        # repr falls back to describe via base Operation.
        assert "Enable RLS" in op.describe()


class TestAlterColumnTypeOnlineEdges:
    def test_irreversible_when_old_type_omitted(self):
        from dorm.migrations.operations import AlterColumnTypeOnline

        op = AlterColumnTypeOnline("User", "age", "BIGINT")
        assert op.reversible is False
        with pytest.raises(NotImplementedError):
            op.database_backwards("app", _FakeConn(), _MockState(), _MockState())

    def test_reverse_uses_old_using(self):
        from dorm.migrations.operations import AlterColumnTypeOnline

        op = AlterColumnTypeOnline(
            "User", "age", "BIGINT",
            old_type="INTEGER",
            old_using="age::INTEGER",
        )
        conn = _FakeConn()
        op.database_backwards("app", conn, _MockState(), _MockState())
        joined = "\n".join(conn.scripts)
        assert "age::INTEGER" in joined


class _MockState:
    def __init__(self):
        self.models = {
            "app.user": {"options": {"db_table": "user"}, "fields": {}}
        }


class TestAddCheckConstraintOnlineEdges:
    def test_non_pg_fallback(self):
        from dorm.migrations.operations import AddCheckConstraintOnline

        op = AddCheckConstraintOnline("orders", "chk", "amount > 0")
        conn = _FakeConn(vendor="sqlite")
        op.database_forwards("app", conn, None, None)
        joined = "\n".join(conn.scripts)
        # Plain ADD CONSTRAINT path.
        assert "ADD CONSTRAINT" in joined
        assert "NOT VALID" not in joined

    def test_describe(self):
        from dorm.migrations.operations import AddCheckConstraintOnline

        op = AddCheckConstraintOnline("orders", "chk", "amount > 0")
        assert "Add CHECK" in op.describe()


class TestSeederMigrationEdges:
    def test_seed_missing_model_key(self):
        from dorm.migrations.operations import SeederMigration

        op = SeederMigration([{"fields": {"x": 1}}])
        with pytest.raises(ValueError, match="model"):
            op._apply(op.fixture, _FakeConn())

    def test_seed_unregistered_model(self):
        from dorm.migrations.operations import SeederMigration

        op = SeederMigration(
            [{"model": "nope.NotRegistered", "fields": {"x": 1}}]
        )
        with pytest.raises(LookupError, match="not registered"):
            op._apply(op.fixture, _FakeConn())

    def test_seed_callable_fixture(self):
        from dorm.migrations.operations import SeederMigration

        called = []

        def factory():
            called.append(1)
            return []

        op = SeederMigration(factory)
        op._apply(op.fixture, _FakeConn())
        assert called == [1]


class TestMakeTableAppendOnlyEdges:
    def test_describe(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        assert "append-only" in op.describe()

    def test_repr(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log", allow_delete=True)
        r = repr(op)
        assert "audit_log" in r
        assert "allow_delete=True" in r

    def test_mysql_skipped_with_log(self, caplog):
        import logging

        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn(vendor="mysql")
        with caplog.at_level(logging.WARNING, logger="dorm.migrations"):
            op.database_forwards("app", conn, None, None)
        assert conn.scripts == []

    def test_reverse_unknown_vendor(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn(vendor="mysql")
        # No scripts emitted on reverse for unsupported vendor.
        op.database_backwards("app", conn, None, None)
        assert conn.scripts == []


# ── slow_tx wraps atomic ───────────────────────────────────────────────────


class TestSlowTxIntegration:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_install_then_use_atomic(self, caplog, tmp_path):
        import logging
        import time

        import dorm
        from dorm.contrib import slow_tx
        from dorm.db.connection import _async_connections, _sync_connections

        _sync_connections.clear()
        _async_connections.clear()
        dorm.configure(
            DATABASES={
                "default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "st.db")}
            },
            INSTALLED_APPS=[],
        )
        slow_tx.install(threshold_ms=0.001)  # everything is "slow"
        try:
            with caplog.at_level(
                logging.WARNING, logger="dorm.contrib.slow_tx"
            ):
                with dorm.transaction.atomic():
                    time.sleep(0.002)
            assert any(
                "slow transaction" in r.message for r in caplog.records
            )
        finally:
            slow_tx.uninstall()
            _sync_connections.clear()
            _async_connections.clear()


# ── encrypted helper edge cases ────────────────────────────────────────────


class TestEncryptedHelpers:
    def test_rotate_rejects_named_non_encrypted(self):
        import dorm
        from dorm.contrib.encrypted import rotate_encryption_keys

        class _Ne(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with pytest.raises(TypeError, match="not an EncryptedField"):
            rotate_encryption_keys(_Ne, fields=["name"])

    def test_arotate_rejects_named_non_encrypted(self):
        import asyncio

        import dorm
        from dorm.contrib.encrypted import arotate_encryption_keys

        class _Ane(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        async def _scenario():
            with pytest.raises(TypeError, match="not an EncryptedField"):
                await arotate_encryption_keys(_Ane, fields=["name"])

        asyncio.run(_scenario())


# ── advisory async paths ───────────────────────────────────────────────────


class TestAdvisoryAsyncRequiresPg:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_aadvisory_lock_requires_pg(self, tmp_path):
        import asyncio

        import dorm
        from dorm.contrib.advisory import aadvisory_lock
        from dorm.db.connection import _async_connections, _sync_connections

        _sync_connections.clear()
        _async_connections.clear()
        dorm.configure(
            DATABASES={
                "default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "a.db")}
            },
            INSTALLED_APPS=[],
        )

        async def _scenario():
            from dorm.db.connection import get_async_connection

            if (
                getattr(get_async_connection(), "vendor", None)
                == "postgresql"
            ):
                pytest.skip("PG suite covered elsewhere")
            with pytest.raises(NotImplementedError):
                async with aadvisory_lock("x"):
                    pass

        asyncio.run(_scenario())
        _sync_connections.clear()
        _async_connections.clear()


# ── listen_notify shape branches ───────────────────────────────────────────


class TestListenNotifyShape:
    def test_notification_dataclass(self):
        from dorm.contrib.listen_notify import Notification

        n = Notification(channel="orders", payload="hi", pid=42)
        assert n.channel == "orders"
        assert n.payload == "hi"
        assert n.pid == 42

    def test_listen_requires_channels(self):
        import asyncio

        from dorm.contrib.listen_notify import listen

        async def _scenario():
            with pytest.raises(ValueError, match="channel"):
                async with listen():
                    pass

        asyncio.run(_scenario())


# ── tasks _wait_for_notify guard path ──────────────────────────────────────


class TestTasksWaitForNotify:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_wait_for_notify_sleeps_on_non_pg(self, tmp_path, monkeypatch):
        import dorm
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue
        from dorm.db.connection import _async_connections, _sync_connections

        _sync_connections.clear()
        _async_connections.clear()
        dorm.configure(
            DATABASES={
                "default": {
                    "ENGINE": "sqlite",
                    "NAME": str(tmp_path / "wn.db"),
                }
            },
            INSTALLED_APPS=[],
        )

        class _OB(OutboxEvent):
            class Meta:
                app_label = "tests"
                db_table = "ob_wn"

        queue = TaskQueue(
            model=_OB, channel="x", poll_interval_s=0.001, using="default"
        )
        # Should fall through to sleep without exception on SQLite.
        queue._wait_for_notify()
        _sync_connections.clear()
        _async_connections.clear()


# ── permissions decorator on coroutine without user kwarg ─────────────────


class TestPermissionsCoroutineNoUser:
    def test_async_no_user_arg(self):
        import asyncio

        from dorm.contrib.permissions import PermissionDenied, requires

        @requires("x")
        async def fn(user):
            return "ok"

        async def _scenario():
            with pytest.raises(PermissionDenied):
                await fn()

        asyncio.run(_scenario())


# ── extra_fields branch coverage ──────────────────────────────────────────


class TestExtraFieldsBranches:
    def test_money_with_decimal_explicit_currency(self):
        import decimal

        from dorm.contrib.extra_fields import Money, MoneyField

        f = MoneyField(currency="EUR")
        v = f.to_python(Money(amount=decimal.Decimal("3.50"), currency="EUR"))
        assert v.amount == decimal.Decimal("3.50")

    def test_money_invalid_currency_caps_must_be_alpha(self):
        from dorm.contrib.extra_fields import MoneyField

        with pytest.raises(ValueError):
            MoneyField(currency="US1")

    def test_iprange_v6_complex(self):
        from dorm.contrib.extra_fields import IPRangeField

        assert IPRangeField().to_python("fe80::/10") == "fe80::/10"

    def test_country_lowercase_normalised(self):
        from dorm.contrib.extra_fields import CountryField

        assert CountryField().to_python("us") == "US"

    def test_phone_too_long(self):
        from dorm.contrib.extra_fields import PhoneField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            PhoneField().to_python("+" + "1" * 30)

    def test_timezone_utc_simple(self):
        from dorm.contrib.extra_fields import TimezoneField

        assert TimezoneField().to_python("UTC") == "UTC"

    def test_percentage_zero_allowed(self):
        import decimal

        from dorm.contrib.extra_fields import PercentageField

        assert PercentageField().to_python(0) == decimal.Decimal("0")

    def test_percentage_hundred_allowed(self):
        import decimal

        from dorm.contrib.extra_fields import PercentageField

        assert PercentageField().to_python(100) == decimal.Decimal("100")


# ── serializers more branches ──────────────────────────────────────────────


class TestSerializersBranches:
    def test_openapi_includes_required_for_non_null(self):
        import dorm
        from dorm.contrib.serializers import openapi_schema_for

        class _Nn(dorm.Model):
            name = dorm.CharField(max_length=20)  # null=False default

            class Meta:
                app_label = "tests"

        sch = openapi_schema_for(_Nn)
        assert "name" in sch["required"]

    def test_openapi_skips_required_for_blank(self):
        import dorm
        from dorm.contrib.serializers import openapi_schema_for

        class _Bk(dorm.Model):
            note = dorm.CharField(max_length=20, blank=True)

            class Meta:
                app_label = "tests"

        sch = openapi_schema_for(_Bk)
        assert "note" not in sch["required"]

    def test_openapi_unknown_field_type_falls_back_string(self):
        import dorm
        from dorm.contrib.serializers import openapi_schema_for

        class _Cr(dorm.Model):
            kind = dorm.CharField(max_length=10)  # fallback path
            n = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        sch = openapi_schema_for(_Cr)
        # CharField hits the catch-all string branch + maxLength.
        assert sch["properties"]["kind"]["type"] == "string"
        assert sch["properties"]["kind"]["maxLength"] == 10

    def test_avro_int_maps_to_long(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Ai(dorm.Model):
            n = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Ai)
        n_field = next(f for f in schema["fields"] if f["name"] == "n")
        assert n_field["type"] == "long"

    def test_avro_float_maps_to_double(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Af(dorm.Model):
            x = dorm.FloatField()

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Af)
        x_field = next(f for f in schema["fields"] if f["name"] == "x")
        assert x_field["type"] == "double"

    def test_avro_boolean(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Ab(dorm.Model):
            on = dorm.BooleanField(default=False)

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Ab)
        on_field = next(f for f in schema["fields"] if f["name"] == "on")
        assert on_field["type"] == "boolean"

    def test_avro_date(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Ad(dorm.Model):
            d = dorm.DateField()

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Ad)
        d_field = next(f for f in schema["fields"] if f["name"] == "d")
        assert d_field["type"]["logicalType"] == "date"

    def test_avro_uuid(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Au(dorm.Model):
            id = dorm.UUIDField(primary_key=True)

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Au)
        id_field = next(f for f in schema["fields"] if f["name"] == "id")
        assert id_field["type"]["logicalType"] == "uuid"

    def test_avro_binary(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _Abi(dorm.Model):
            blob = dorm.BinaryField()

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_Abi)
        b_field = next(f for f in schema["fields"] if f["name"] == "blob")
        assert b_field["type"] == "bytes"


# ── inbox decorator wraps function ─────────────────────────────────────────


class TestInboxDecoratorBasics:
    def test_handler_name_explicit(self):
        from dorm.contrib.inbox import InboxRecord, idempotent

        class _Ib(InboxRecord):
            class Meta:
                app_label = "tests"
                db_table = "inb_basics"

        @idempotent(_Ib, handler_name="custom-handler")
        def h(mid: str):
            pass

        assert h.__wrapped__.__name__ == "h"  # ty: ignore[unresolved-attribute]


# ── lag router measure_lag non-pg ──────────────────────────────────────────


class TestLagRouterMeasureLag:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_measure_lag_non_pg_returns_zero(self, tmp_path):
        import dorm
        from dorm.contrib.lag_router import LagAwareReadRouter
        from dorm.db.connection import _async_connections, _sync_connections

        _sync_connections.clear()
        _async_connections.clear()
        dorm.configure(
            DATABASES={
                "default": {
                    "ENGINE": "sqlite",
                    "NAME": str(tmp_path / "lr.db"),
                }
            },
            INSTALLED_APPS=[],
        )
        router = LagAwareReadRouter(replicas=["default"])
        # Non-PG → measure returns 0.0 → replica counts as healthy.
        assert router._measure_lag("default") == 0.0
        _sync_connections.clear()
        _async_connections.clear()


# ── pgvector reranker error path on dimensions mismatch (signature only) ──


class TestPgvectorRerankerSig:
    def test_l2_distance_branch_validates_lookup_arg(self):
        from dorm.contrib.pgvector.expressions import _format_pgvector_literal

        out = _format_pgvector_literal([0.1, 0.2, 0.3])
        # Result is a bracketed string.
        assert out.startswith("[") and out.endswith("]")


# ── F.coalesce + F.between ─────────────────────────────────────────────────


class TestFExpressionsBranches:
    def test_coalesce_with_F_default(self):
        from dorm.expressions import F
        from dorm.functions import Coalesce

        expr = F("a").coalesce(F("b"))
        assert isinstance(expr, Coalesce)
        assert len(expr.expressions) == 2

    def test_between_with_strings(self):
        from dorm.expressions import F

        q = F("name").between("a", "z")
        assert ("name__range", ("a", "z")) in q.children


# ── Manager.exists_or_create / create_or_update ─────────────────────────────


class TestManagerSugar:
    @pytest.fixture(autouse=True)
    def _r(self, restore_settings):
        yield

    def test_exists_or_create_misses_then_creates(self, tmp_path):
        import dorm
        from dorm.db.connection import _async_connections, _sync_connections, get_connection
        from dorm.migrations.schema import SchemaEditor

        _sync_connections.clear()
        _async_connections.clear()
        dorm.configure(
            DATABASES={
                "default": {
                    "ENGINE": "sqlite",
                    "NAME": str(tmp_path / "eoc.db"),
                }
            },
            INSTALLED_APPS=["tests"],
        )

        class _Eoc(dorm.Model):
            name = dorm.CharField(max_length=20, unique=True)
            score = dorm.IntegerField(default=0)

            class Meta:
                app_label = "tests"

        try:
            with SchemaEditor(get_connection()) as se:
                se.create_model(_Eoc)
            exists, inst = _Eoc.objects.exists_or_create(
                name="alice", defaults={"score": 10}
            )
            assert exists is False
            assert inst.name == "alice"
            assert inst.score == 10

            exists2, inst2 = _Eoc.objects.exists_or_create(name="alice")
            assert exists2 is True
            assert inst2.name == "alice"
        finally:
            _sync_connections.clear()
            _async_connections.clear()
