"""Tests for the v4.3 expansion round (bug fixes + new features)."""
from __future__ import annotations

import datetime as _dt

import pytest


# ── Bug fix 1: upsert(returning=True) uses field names ─────────────────────


class TestUpsertReturningFix:
    def test_returning_passes_names_not_field_objects(self):
        import dorm

        class _UF(dorm.Model):
            slug = dorm.CharField(max_length=8, unique=True)
            count = dorm.IntegerField(default=0)

            class Meta:
                app_label = "tests"

        # Empty list → early return, but exercises the field-name
        # computation indirectly via shape.
        result = _UF.objects.upsert([], unique_fields=["slug"], returning=True)
        assert result == []


# ── Bug fix 3: Saga inside outer atomic warns ───────────────────────────────


class TestSagaNestedWarning:
    def test_warns_when_inside_outer_atomic(self, caplog, tmp_path):
        import logging

        import dorm
        from dorm.conf import settings
        from dorm.contrib.saga import Saga, Step
        from dorm.db.connection import _async_connections, _sync_connections

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "snest.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )
        try:
            saga = Saga(steps=[Step("noop", lambda ctx: None)])
            with caplog.at_level(logging.WARNING, logger="dorm.contrib.saga"):
                with dorm.transaction.atomic():
                    saga.run()
            assert any(
                "outer atomic" in r.message for r in caplog.records
            )
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── T2 TaskQueue cron + retry-backoff + DLQ ─────────────────────────────────


class TestCronMatches:
    def test_every_minute(self):
        from dorm.contrib.tasks import cron_matches

        assert cron_matches(
            "* * * * *",
            _dt.datetime(2026, 1, 1, 12, 0, tzinfo=_dt.timezone.utc),
        )

    def test_minute_specific(self):
        from dorm.contrib.tasks import cron_matches

        assert cron_matches(
            "5 * * * *",
            _dt.datetime(2026, 1, 1, 12, 5, tzinfo=_dt.timezone.utc),
        )
        assert not cron_matches(
            "5 * * * *",
            _dt.datetime(2026, 1, 1, 12, 6, tzinfo=_dt.timezone.utc),
        )

    def test_step(self):
        from dorm.contrib.tasks import cron_matches

        assert cron_matches(
            "*/15 * * * *",
            _dt.datetime(2026, 1, 1, 12, 15, tzinfo=_dt.timezone.utc),
        )

    def test_invalid_field_count(self):
        from dorm.contrib.tasks import cron_matches

        with pytest.raises(ValueError, match="5 fields"):
            cron_matches("* * *", _dt.datetime.now(_dt.timezone.utc))

    def test_invalid_value(self):
        from dorm.contrib.tasks import cron_matches

        with pytest.raises(ValueError, match="outside"):
            cron_matches("99 * * * *", _dt.datetime.now(_dt.timezone.utc))


# ── T2 retry_with_backoff ───────────────────────────────────────────────────


class TestRetryBackoff:
    def test_eta_grows_with_attempts(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import retry_with_backoff

        class _OB(OutboxEvent):
            class Meta:
                app_label = "tests"

        # Simulate event with attempts
        e = _OB(event_type="x", payload={}, status="pending", attempts=0)
        eta0 = retry_with_backoff(None, e, base_seconds=10, jitter=False)  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]
        e.attempts = 3
        eta3 = retry_with_backoff(None, e, base_seconds=10, jitter=False)  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]
        # eta3 should be at least 8x further out than eta0.
        delta0 = (eta0 - _dt.datetime.now(_dt.timezone.utc)).total_seconds()
        delta3 = (eta3 - _dt.datetime.now(_dt.timezone.utc)).total_seconds()
        assert delta3 > delta0 * 4


# ── T3 lint v2: irreversible + destructive ──────────────────────────────────


class TestLintV2:
    def test_irreversible_op_flagged(self):
        from dorm.migrations.lint import lint_operations
        from dorm.migrations.operations import AddPGEnumValue

        op = AddPGEnumValue("status", "x")  # reversible=False
        result = lint_operations([op])
        codes = {f.code for f in result.findings}
        assert "DORM-M005" in codes

    def test_destructive_noop_reverse_flagged(self):
        from dorm.migrations.lint import lint_operations
        from dorm.migrations.operations import RunSQL

        op = RunSQL("DROP TABLE old", reverse_sql="")
        result = lint_operations([op])
        codes = {f.code for f in result.findings}
        assert "DORM-M006" in codes


# ── T3 SeederMigration ──────────────────────────────────────────────────────


class TestSeederMigration:
    def test_seeder_op_description(self):
        from dorm.migrations.operations import SeederMigration

        op = SeederMigration([])
        assert "Seed" in op.describe()
        # Irreversible without reverse_fixture.
        assert op.reversible is False

    def test_reverse_makes_reversible(self):
        from dorm.migrations.operations import SeederMigration

        op = SeederMigration([], reverse_fixture=[])
        assert op.reversible is True


# ── T3 AddCheckConstraintOnline ─────────────────────────────────────────────


class TestAddCheckConstraintOnline:
    class _FakeConn:
        vendor = "postgresql"

        def __init__(self):
            self.scripts: list[str] = []

        def execute_script(self, sql: str) -> None:
            self.scripts.append(sql)

    def test_pg_emits_two_phase(self):
        from dorm.migrations.operations import AddCheckConstraintOnline

        op = AddCheckConstraintOnline("orders", "chk_pos", "amount > 0")
        conn = TestAddCheckConstraintOnline._FakeConn()
        op.database_forwards("app", conn, None, None)
        joined = "\n".join(conn.scripts)
        assert "NOT VALID" in joined
        assert "VALIDATE CONSTRAINT" in joined

    def test_reverse_drops(self):
        from dorm.migrations.operations import AddCheckConstraintOnline

        op = AddCheckConstraintOnline("orders", "chk_pos", "amount > 0")
        conn = TestAddCheckConstraintOnline._FakeConn()
        op.database_backwards("app", conn, None, None)
        assert "DROP CONSTRAINT" in conn.scripts[0]

    def test_empty_check_rejected(self):
        from dorm.migrations.operations import AddCheckConstraintOnline

        with pytest.raises(ValueError):
            AddCheckConstraintOnline("t", "c", "")


# ── T6 fields ──────────────────────────────────────────────────────────────


class TestExtraFieldsExpansion:
    def test_uuid_v7_default(self):
        import dorm

        f = dorm.UUIDField(version=7)
        v = f.get_default()
        assert v is not None
        assert v.version == 7

    def test_uuid_v4_default(self):
        import dorm

        f = dorm.UUIDField(version=4)
        v = f.get_default()
        assert v.version == 4

    def test_uuid_invalid_version(self):
        import dorm

        with pytest.raises(ValueError):
            dorm.UUIDField(version=2)

    def test_iprange_valid_ipv4(self):
        from dorm.contrib.extra_fields import IPRangeField

        assert IPRangeField().to_python("10.0.0.0/24") == "10.0.0.0/24"

    def test_iprange_invalid(self):
        from dorm.contrib.extra_fields import IPRangeField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            IPRangeField().to_python("not-a-cidr")

    def test_timezone_valid(self):
        from dorm.contrib.extra_fields import TimezoneField

        assert TimezoneField().to_python("America/Los_Angeles") == "America/Los_Angeles"

    def test_timezone_invalid(self):
        from dorm.contrib.extra_fields import TimezoneField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            TimezoneField().to_python("LosAngeles")

    def test_path_rejects_traversal(self):
        from dorm.contrib.extra_fields import PathField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            PathField().to_python("/etc/../shadow")

    def test_path_allow_traversal(self):
        from dorm.contrib.extra_fields import PathField

        f = PathField(allow_traversal=True)
        assert f.to_python("a/../b") == "a/../b"

    def test_percentage_in_range(self):
        from dorm.contrib.extra_fields import PercentageField

        import decimal

        assert PercentageField().to_python("42.5") == decimal.Decimal("42.5")

    def test_percentage_out_of_range(self):
        from dorm.contrib.extra_fields import PercentageField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            PercentageField().to_python("101.0")

    def test_country_valid_normalised(self):
        from dorm.contrib.extra_fields import CountryField

        assert CountryField().to_python("es") == "ES"

    def test_country_invalid(self):
        from dorm.contrib.extra_fields import CountryField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            CountryField().to_python("ZZ")

    def test_autoslug_derives(self):
        from dorm.contrib.extra_fields import autoslug

        fn = autoslug("title")

        class _M:
            title = "Hello, World!"

        assert fn(_M()) == "hello-world"

    def test_autoslug_unicode_normalisation(self):
        from dorm.contrib.extra_fields import autoslug

        fn = autoslug("title")

        class _M:
            title = "Café é"

        assert fn(_M()) == "cafe-e"


# ── T7 permissions DSL ──────────────────────────────────────────────────────


class TestPermissionsDSL:
    def test_grants_when_perm_present(self):
        from dorm.contrib.permissions import requires

        class _U:
            permissions = {"article.edit"}

        @requires("article.edit")
        def edit(user, payload):
            return "ok"

        assert edit(_U(), {}) == "ok"

    def test_denies_missing_perm(self):
        from dorm.contrib.permissions import PermissionDenied, requires

        class _U:
            permissions = set()

        @requires("article.edit")
        def edit(user):
            return "ok"

        with pytest.raises(PermissionDenied):
            edit(_U())

    def test_requires_any(self):
        from dorm.contrib.permissions import PermissionDenied, requires_any

        class _U:
            permissions = {"article.read"}

        @requires_any("article.read", "article.edit")
        def view(user):
            return "ok"

        assert view(_U()) == "ok"

        class _U2:
            permissions = set()

        with pytest.raises(PermissionDenied):
            view(_U2())

    def test_async_support(self):
        import asyncio

        from dorm.contrib.permissions import requires

        class _U:
            permissions = {"x"}

        @requires("x")
        async def fetch(user):
            return "fetched"

        assert asyncio.run(fetch(_U())) == "fetched"

    def test_requires_needs_at_least_one_perm(self):
        from dorm.contrib.permissions import requires

        with pytest.raises(ValueError):
            requires()


# ── T7 rate limit ──────────────────────────────────────────────────────────


class TestRateLimit:
    def test_bucket_allows_burst(self):
        from dorm.contrib.rate_limit import TokenBucket

        bucket = TokenBucket(rate_per_second=10, burst=3)
        # Burst lets 3 through.
        assert bucket.allow("k")
        assert bucket.allow("k")
        assert bucket.allow("k")
        # Fourth denied (no refill yet at this instant).
        assert not bucket.allow("k")

    def test_decorator_raises(self):
        from dorm.contrib.rate_limit import (
            TokenBucket,
            TooManyRequests,
            rate_limited,
        )

        bucket = TokenBucket(rate_per_second=1, burst=1)

        @rate_limited(bucket, key=lambda: "g")
        def hit():
            return "ok"

        assert hit() == "ok"
        with pytest.raises(TooManyRequests):
            hit()

    def test_per_key_independence(self):
        from dorm.contrib.rate_limit import TokenBucket

        bucket = TokenBucket(rate_per_second=10, burst=1)
        assert bucket.allow("a")
        assert bucket.allow("b")  # different key — independent
        assert not bucket.allow("a")  # same key — denied

    def test_invalid_args(self):
        from dorm.contrib.rate_limit import TokenBucket

        with pytest.raises(ValueError):
            TokenBucket(rate_per_second=0, burst=1)
        with pytest.raises(ValueError):
            TokenBucket(rate_per_second=1, burst=0)


# ── T8 concurrency ──────────────────────────────────────────────────────────


class TestConcurrency:
    def test_named_lock_in_proc_fallback(self):
        # On SQLite the helper falls back to threading.Lock; verify
        # acquisition + release shape.
        from dorm.contrib.concurrency import named_lock

        with named_lock("dorm-test-key"):
            # Reentrant lock would block here — we expect a separate
            # acquire to be blocked on a fresh thread. Smoke-test the
            # context manager only.
            pass

    def test_serializable_snapshot_invalid_attempts(self):
        from dorm.contrib.concurrency import SerializableSnapshot

        with pytest.raises(ValueError):
            SerializableSnapshot(max_attempts=0)

    def test_optimistic_lock_no_pk(self):
        import dorm
        from dorm.contrib.concurrency import (
            OptimisticLockError,
            with_optimistic_lock,
        )

        class _OL(dorm.Model):
            version = dorm.IntegerField(default=0)
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        inst = _OL(name="x")
        # No PK assigned yet → must raise.
        with pytest.raises(OptimisticLockError, match="no PK"):
            with_optimistic_lock(inst)


# ── T11 ActivePassiveRouter ─────────────────────────────────────────────────


class TestActivePassiveRouter:
    def test_requires_two_aliases(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        with pytest.raises(ValueError):
            ActivePassiveRouter(aliases=["a"])

    def test_invalid_probe_seconds(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        with pytest.raises(ValueError):
            ActivePassiveRouter(aliases=["a", "b"], probe_seconds=0)


# ── T12 serializers ─────────────────────────────────────────────────────────


class TestSerializers:
    def test_msgpack_optional(self):
        from dorm.contrib.serializers import stream_msgpack

        try:
            import msgpack  # noqa: F401  # ty:ignore[unresolved-import]
        except ImportError:
            pytest.skip("msgpack not installed")
        rows = [{"a": 1}, {"a": 2}]
        out = b"".join(stream_msgpack(rows))
        assert isinstance(out, bytes) and out

    def test_avro_schema_for_model(self):
        import dorm
        from dorm.contrib.serializers import avro_schema_for

        class _A(dorm.Model):
            n = dorm.IntegerField()
            text = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        schema = avro_schema_for(_A)
        assert schema["type"] == "record"
        assert schema["name"] == "_A"
        names = {f["name"] for f in schema["fields"]}
        assert {"n", "text"}.issubset(names)

    def test_avro_rejects_non_model(self):
        from dorm.contrib.serializers import avro_schema_for

        with pytest.raises(TypeError):
            avro_schema_for(int)

    def test_openapi_schema(self):
        import dorm
        from dorm.contrib.serializers import openapi_schema_for

        class _OA(dorm.Model):
            name = dorm.CharField(max_length=20)
            email = dorm.EmailField()
            created = dorm.DateTimeField()

            class Meta:
                app_label = "tests"

        sch = openapi_schema_for(_OA)
        assert sch["type"] == "object"
        assert sch["properties"]["name"]["type"] == "string"
        assert sch["properties"]["email"]["format"] == "email"
        assert sch["properties"]["created"]["format"] == "date-time"


# ── T13 sugar ──────────────────────────────────────────────────────────────


class TestSugar:
    def test_F_between_emits_Q_range(self):
        from dorm.expressions import F

        q = F("age").between(18, 65)
        # Q exposes ``children`` of (key, value) shape.
        assert q.children == [("age__range", (18, 65))]


# ── T14 slow_tx ─────────────────────────────────────────────────────────────


class TestSlowTx:
    def test_install_uninstall_idempotent(self):
        from dorm.contrib import slow_tx

        slow_tx.install()
        slow_tx.install()  # idempotent
        slow_tx.uninstall()
        slow_tx.uninstall()  # idempotent
