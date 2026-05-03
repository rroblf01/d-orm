"""Tests for v3.1 Django-parity additions.

Single test file because most of these are small, focused checks
that don't need their own fixture machinery. Grouped by topic.
"""

from __future__ import annotations

import datetime as _dt
import time as _time
from contextlib import contextmanager

import pytest

import dorm


# ──────────────────────────────────────────────────────────────────────────────
# T1-1 USE_TZ + tz-aware datetimes
# ──────────────────────────────────────────────────────────────────────────────


@contextmanager
def _use_tz_on():
    prev = getattr(dorm.conf.settings, "USE_TZ", False)
    dorm.configure(USE_TZ=True)
    try:
        yield
    finally:
        dorm.configure(USE_TZ=prev)


def test_datetimefield_make_aware_attaches_settings_timezone():
    from dorm.fields import DateTimeField

    f = DateTimeField()
    naive = _dt.datetime(2026, 1, 1, 12, 0)

    # USE_TZ off (default) — naive stays naive.
    assert f.to_python(naive).tzinfo is None

    with _use_tz_on():
        aware = f.to_python(naive)
        assert aware.tzinfo is not None


def test_datetimefield_get_db_prep_value_normalises_to_utc():
    from dorm.fields import DateTimeField

    f = DateTimeField()
    aware = _dt.datetime(2026, 1, 1, 12, 0, tzinfo=_dt.timezone(_dt.timedelta(hours=5)))
    with _use_tz_on():
        out = f.get_db_prep_value(aware)
        # UTC normalisation: 12:00+05:00 → 07:00 UTC.
        parsed = _dt.datetime.fromisoformat(out)
        assert parsed.tzinfo is not None
        assert parsed.astimezone(_dt.timezone.utc).hour == 7


def test_datetimefield_db_type_postgres_uses_timestamptz_when_use_tz():
    from dorm.fields import DateTimeField

    class _PGConn:
        vendor = "postgresql"

    f = DateTimeField()
    assert f.db_type(_PGConn()) == "TIMESTAMP"
    with _use_tz_on():
        assert f.db_type(_PGConn()) == "TIMESTAMP WITH TIME ZONE"


def test_datetimefield_from_db_value_returns_utc_aware_when_use_tz():
    from dorm.fields import DateTimeField

    f = DateTimeField()
    naive_iso = "2026-03-15T08:00:00"
    with _use_tz_on():
        out = f.from_db_value(naive_iso)
        assert out.tzinfo is not None
        # Naive ISO assumed UTC.
        assert out.utcoffset() == _dt.timedelta(0)


# ──────────────────────────────────────────────────────────────────────────────
# T1-2 Meta.proxy
# ──────────────────────────────────────────────────────────────────────────────


def test_meta_proxy_shares_parent_table():
    """A proxy model's ``db_table`` resolves to the parent's table,
    NOT a new one derived from the proxy's class name."""
    class Concrete(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "v3_1_proxy"
            db_table = "v3_1_proxy_concrete"

    class ProxyConcrete(Concrete):
        class Meta:
            app_label = "v3_1_proxy"
            proxy = True

    assert ProxyConcrete._meta.db_table == "v3_1_proxy_concrete"
    assert ProxyConcrete._meta.proxy is True
    assert ProxyConcrete._meta.concrete_model is Concrete


def test_meta_proxy_skipped_in_project_state():
    class Real(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "v3_1_proxy_state"
            db_table = "v3_1_proxy_state_real"

    class _ProxyReal(Real):
        class Meta:
            app_label = "v3_1_proxy_state"
            proxy = True

    from dorm.migrations.state import ProjectState

    state = ProjectState.from_apps(app_label="v3_1_proxy_state")
    assert "v3_1_proxy_state.real" in state.models
    assert "v3_1_proxy_state._proxyreal" not in state.models


# ──────────────────────────────────────────────────────────────────────────────
# T1-3 dates() / datetimes()
# ──────────────────────────────────────────────────────────────────────────────


def test_dates_kind_validation():
    from tests.models import Article

    with pytest.raises(ValueError, match="kind must be"):
        Article.objects.dates("title", "century")


def test_dates_order_validation():
    from tests.models import Article

    with pytest.raises(ValueError, match="ASC.*DESC"):
        Article.objects.dates("title", "day", order="weird")


def test_datetimes_kind_validation():
    from tests.models import Article

    with pytest.raises(ValueError, match="kind must be"):
        Article.objects.datetimes("title", "millisecond")


def test_dates_python_truncation_works_on_real_dates():
    """Python-side truncation: feed three different dates from the
    same month, expect one entry."""
    from dorm.queryset import QuerySet

    qs = QuerySet.__new__(QuerySet)

    # Stub ``values_list`` to feed canned values without hitting DB.
    qs.values_list = lambda field, flat=False: [
        _dt.date(2026, 3, 5),
        _dt.date(2026, 3, 18),
        _dt.date(2026, 5, 1),
    ]

    months = qs.dates("created_at", "month")
    assert months == [_dt.date(2026, 3, 1), _dt.date(2026, 5, 1)]


def test_datetimes_python_truncation():
    from dorm.queryset import QuerySet

    qs = QuerySet.__new__(QuerySet)
    qs.values_list = lambda field, flat=False: [
        _dt.datetime(2026, 3, 5, 12, 30, 45),
        _dt.datetime(2026, 3, 5, 12, 31, 0),
        _dt.datetime(2026, 3, 6, 0, 0, 0),
    ]
    out = qs.datetimes("ts", "hour")
    # Two distinct hours: 2026-03-05 12:00 and 2026-03-06 00:00.
    assert len(out) == 2


# ──────────────────────────────────────────────────────────────────────────────
# T1-4 migrate --fake / --fake-initial
# ──────────────────────────────────────────────────────────────────────────────


def test_migrate_fake_records_without_running(tmp_path):
    """``migrate(... fake=True)`` records every pending migration as
    applied without touching the schema. Pinning behaviour with a
    minimal in-memory sqlite + fake migration module on disk."""
    import sys
    import textwrap
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor

    # Build a one-migration package on disk.
    app_dir = tmp_path / "fakeapp"
    app_dir.mkdir()
    (app_dir / "__init__.py").write_text("")
    mig_dir = app_dir / "migrations"
    mig_dir.mkdir()
    (mig_dir / "__init__.py").write_text("")
    (mig_dir / "0001_initial.py").write_text(textwrap.dedent("""
        from dorm.migrations import operations as ops
        from dorm.fields import IntegerField

        class Migration:
            dependencies = []
            operations = [
                ops.CreateModel(
                    name='_FakedTable',
                    fields=[('id', IntegerField(primary_key=True))],
                    options={'db_table': '_v3_1_fake_table'},
                ),
            ]
    """))

    sys.path.insert(0, str(tmp_path))
    try:
        # Use a fresh in-memory sqlite — the conftest's
        # configure_dorm fixture already pointed at one, but we
        # want a clean recorder.
        conn = get_connection()
        executor = MigrationExecutor(conn, verbosity=0)
        executor.migrate("fakeapp", str(mig_dir), fake=True)

        # Migration recorded as applied.
        executor.loader.load_applied(executor.recorder)
        assert ("fakeapp", "0001_initial") in executor.loader.applied

        # Yet the table was NOT created — schema untouched.
        try:
            list(conn.execute("SELECT 1 FROM _v3_1_fake_table"))
            had_table = True
        except Exception:
            had_table = False
        assert not had_table, "fake=True must NOT execute DDL"
    finally:
        sys.path.remove(str(tmp_path))
        for k in list(sys.modules):
            if k == "fakeapp" or k.startswith("fakeapp."):
                sys.modules.pop(k, None)
        # Clean recorder.
        try:
            conn.execute_script("DELETE FROM dorm_migrations WHERE app = 'fakeapp'")
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# T1-5 JSONField / ArrayField PG operator names
# ──────────────────────────────────────────────────────────────────────────────


def test_jsonfield_django_lookup_aliases_present():
    from dorm.lookups import LOOKUPS

    for name in (
        "contained_by", "has_key", "has_keys",
        "has_any_keys", "overlap", "len",
    ):
        assert name in LOOKUPS, f"missing Django-style lookup {name!r}"
    # Spot-check the SQL templates emit the right operator.
    assert LOOKUPS["contained_by"][0] == "{col} <@ %s"
    assert LOOKUPS["has_key"][0] == "{col} ? %s"
    assert LOOKUPS["has_keys"][0] == "{col} ?& %s"
    assert LOOKUPS["has_any_keys"][0] == "{col} ?| %s"
    assert LOOKUPS["overlap"][0] == "{col} && %s"


# ──────────────────────────────────────────────────────────────────────────────
# T1-6 Aggregate filter= argument (already implemented — pin it)
# ──────────────────────────────────────────────────────────────────────────────


def test_aggregate_filter_argument_round_trip():
    from dorm import Count, Q
    from tests.models import Author

    Author.objects.create(name="A", age=18, is_active=True)
    Author.objects.create(name="B", age=20, is_active=False)
    Author.objects.create(name="C", age=22, is_active=True)
    out = Author.objects.aggregate(
        active=Count("pk", filter=Q(is_active=True)),
        inactive=Count("pk", filter=Q(is_active=False)),
    )
    assert out["active"] == 2
    assert out["inactive"] == 1


# ──────────────────────────────────────────────────────────────────────────────
# T1-7 Field.deconstruct()
# ──────────────────────────────────────────────────────────────────────────────


def test_field_deconstruct_returns_tuple_shape():
    from dorm.fields import CharField

    f = CharField(max_length=200, null=True)
    out = f.deconstruct()
    name, path, args, kwargs = out
    assert path == "dorm.fields.CharField"
    assert args == []
    assert kwargs.get("max_length") == 200
    assert kwargs.get("null") is True


def test_field_deconstruct_omits_default_values():
    from dorm.fields import IntegerField

    f = IntegerField()
    _, _, _, kwargs = f.deconstruct()
    # No non-default kwargs — should be empty.
    assert kwargs == {}


def test_field_deconstruct_round_trip():
    """Reconstructing the class with the deconstructed kwargs should
    yield a field with the same configuration."""
    from importlib import import_module
    from dorm.fields import CharField

    original = CharField(max_length=80, unique=True, null=True)
    name, path, args, kwargs = original.deconstruct()
    mod_path, _, cls_name = path.rpartition(".")
    cls = getattr(import_module(mod_path), cls_name)
    rebuilt = cls(*args, **kwargs)
    assert rebuilt.max_length == 80
    assert rebuilt.unique is True
    assert rebuilt.null is True


# ──────────────────────────────────────────────────────────────────────────────
# T2-8 contrib.auth.tokens
# ──────────────────────────────────────────────────────────────────────────────


@contextmanager
def _signing_key_set(key: str = "test-signing-key-v3.1"):
    prev = getattr(dorm.conf.settings, "SECRET_KEY", "")
    dorm.configure(SECRET_KEY=key)
    try:
        yield
    finally:
        dorm.configure(SECRET_KEY=prev)


class _FakeUser:
    """Minimal user shape — token generator only reads ``pk`` /
    ``password`` / ``last_login`` / ``email``."""

    def __init__(self, pk=1, password="pw1", last_login=None, email="x@y.com"):
        self.pk = pk
        self.password = password
        self.last_login = last_login
        self.email = email


def test_token_make_check_round_trip():
    from dorm.contrib.auth.tokens import default_token_generator as gen

    with _signing_key_set():
        u = _FakeUser()
        token = gen.make_token(u)
        assert gen.check_token(u, token) is True


def test_token_invalidates_on_password_change():
    from dorm.contrib.auth.tokens import default_token_generator as gen

    with _signing_key_set():
        u = _FakeUser(password="old")
        token = gen.make_token(u)
        assert gen.check_token(u, token) is True
        u.password = "new"
        # Same token, different user state → reject.
        assert gen.check_token(u, token) is False


def test_token_rejects_malformed_input():
    from dorm.contrib.auth.tokens import default_token_generator as gen

    with _signing_key_set():
        u = _FakeUser()
        for bad in (None, "", "no-dash", "abc-not-base64!@#", 12345, "-only-second"):
            assert gen.check_token(u, bad) is False  # ty:ignore[invalid-argument-type]


def test_token_namespace_separation():
    from dorm.contrib.auth.tokens import PasswordResetTokenGenerator

    with _signing_key_set():
        u = _FakeUser()
        reset = PasswordResetTokenGenerator(salt_namespace="password-reset")
        verify = PasswordResetTokenGenerator(salt_namespace="email-verify")
        token = reset.make_token(u)
        # Same secret + user, different namespace → reject.
        assert verify.check_token(u, token) is False


def test_token_timeout_enforced():
    from dorm.contrib.auth.tokens import PasswordResetTokenGenerator

    with _signing_key_set():
        u = _FakeUser()
        gen = PasswordResetTokenGenerator(timeout=0)
        token = gen.make_token(u)
        # Sleep over zero-second timeout window to make sure the
        # comparison is strictly past, not merely ≥.
        _time.sleep(1.5)
        assert gen.check_token(u, token) is False


def test_token_requires_signing_key():
    """Unset settings.SECRET_KEY must surface as a clear
    configuration error rather than minting / checking a forgeable
    token."""
    from dorm.contrib.auth.tokens import default_token_generator as gen
    from dorm.exceptions import ImproperlyConfigured

    dorm.configure(SECRET_KEY="", CACHE_SIGNING_KEY="")
    try:
        with pytest.raises(ImproperlyConfigured, match="SECRET_KEY"):
            gen.make_token(_FakeUser())
    finally:
        # Don't leak empty-key state into other tests.
        dorm.configure(SECRET_KEY="restore-after-test")


# ──────────────────────────────────────────────────────────────────────────────
# T2-9 Meta.permissions sync_permissions
# ──────────────────────────────────────────────────────────────────────────────


def test_permissions_default_verbs_emitted_per_concrete_model():
    from dorm.contrib.auth.management import _default_permissions

    perms = _default_permissions("widget", "shop")
    codenames = {c for c, _ in perms}
    assert codenames == {
        "shop.add_widget", "shop.change_widget",
        "shop.delete_widget", "shop.view_widget",
    }


# ──────────────────────────────────────────────────────────────────────────────
# T2-10 Model.from_db()
# ──────────────────────────────────────────────────────────────────────────────


def test_model_from_db_stamps_alias_on_state():
    from tests.models import Author

    inst = Author.from_db("replica", ["id", "name", "age"], [1, "Alice", 30])
    assert inst._state.adding is False  # ty:ignore[unresolved-attribute]
    assert inst._state.db == "replica"  # ty:ignore[unresolved-attribute]
    assert inst.name == "Alice"
    assert inst.age == 30


def test_model_from_db_accepts_attname_or_column_keys():
    """``field_names`` may be either column names or attnames —
    both should hydrate correctly."""
    from tests.models import Author

    inst = Author.from_db("default", ["name", "age"], ["B", 25])
    assert inst.name == "B"
    assert inst.age == 25


# ──────────────────────────────────────────────────────────────────────────────
# T2-12 transaction.savepoint API
# ──────────────────────────────────────────────────────────────────────────────


def test_savepoint_round_trip():
    from dorm import transaction
    from tests.models import Author

    with transaction.atomic():
        Author.objects.create(name="outer", age=1)
        sid = transaction.savepoint()
        Author.objects.create(name="inner", age=2)
        transaction.savepoint_rollback(sid)
        # The "inner" row is gone; "outer" survives.
        names = set(Author.objects.values_list("name", flat=True))
        assert "outer" in names
        assert "inner" not in names


def test_savepoint_commit_keeps_writes():
    from dorm import transaction
    from tests.models import Author

    with transaction.atomic():
        sid = transaction.savepoint()
        Author.objects.create(name="kept", age=42)
        transaction.savepoint_commit(sid)
        names = set(Author.objects.values_list("name", flat=True))
        assert "kept" in names


def test_savepoint_rejects_invalid_id():
    from dorm import transaction

    with pytest.raises(ValueError, match="invalid savepoint"):
        transaction.savepoint_commit("'; DROP TABLE x; --")
    with pytest.raises(ValueError, match="invalid savepoint"):
        transaction.savepoint_rollback("not-a-savepoint-id")


# ──────────────────────────────────────────────────────────────────────────────
# MySQL backend scaffold
# ──────────────────────────────────────────────────────────────────────────────


def test_mysql_backend_scaffold_raises_clear_error():
    from dorm.db.backends.mysql import MySQLDatabaseWrapper
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="not implemented yet"):
        MySQLDatabaseWrapper({"ENGINE": "mysql", "NAME": "x"})


# ──────────────────────────────────────────────────────────────────────────────
# Multi-tenant scaffold
# ──────────────────────────────────────────────────────────────────────────────


def test_tenant_schema_validation_rejects_injection():
    from dorm.contrib.tenants import _validate_schema_name

    for bad in ("foo; DROP", "1bad", "", "with space", None, 123):
        with pytest.raises((ValueError, TypeError)):
            _validate_schema_name(bad)  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]


def test_tenant_register_and_lookup():
    from dorm.contrib.tenants import (
        register_tenant,
        registered_tenants,
    )

    register_tenant("acme")
    register_tenant("globex")
    assert {"acme", "globex"} <= registered_tenants()


def test_tenant_register_rejects_invalid_names():
    from dorm.contrib.tenants import register_tenant

    with pytest.raises(ValueError):
        register_tenant("bad name")


def test_tenant_context_only_supports_postgres():
    """Non-PG backends should refuse loudly. The conftest fixture
    runs both sqlite and postgres params; we only assert on the
    sqlite case to avoid touching the real PG connection."""
    from dorm.contrib.tenants import TenantContext
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") != "sqlite":
        pytest.skip("This assertion is sqlite-specific.")

    with pytest.raises(NotImplementedError, match="PostgreSQL"):
        with TenantContext("acme_dummy"):
            pass
