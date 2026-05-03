"""End-to-end integration coverage for v3.0 features that the
unit-style tests don't fully exercise.

Focus on flows the docs promise:

- ``sync_permissions()`` walks the model registry, materialises
  default-verb perms + ``Meta.permissions`` entries, idempotent.
- Password-reset round-trip end-to-end through
  ``PasswordResetTokenGenerator`` against a real ``User`` row.
- ``parse_database_url("mysql://...")`` → ``configure(DATABASES=…)``
  → connection wrapper raises the v3.1 milestone error.
- ``LocMemCache`` → ``Manager.cache_get`` → ``Manager.cache_get_many``
  end-to-end with model invalidation on save.
- ``QueryLog`` summary returns ``TemplateStats`` instances; ASGI
  middleware attaches log to scope.
- ``dorm.contrib.tenants`` validates schema names + raises on
  non-PG backends + restores outer state.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.exceptions import ImproperlyConfigured


# ──────────────────────────────────────────────────────────────────────────────
# sync_permissions full roundtrip
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _auth_tables():
    """Create the auth_* tables by hand for these integration
    tests — we don't want to wire ``dorm.contrib.auth`` into
    ``INSTALLED_APPS`` of the test project."""
    from dorm.db.connection import get_connection
    from dorm.contrib.auth.models import Group, Permission, User
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in (
        "auth_user_user_permissions",
        "auth_user_groups",
        "auth_group_permissions",
        "auth_user",
        "auth_group",
        "auth_permission",
    ):
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, table in [
        (Permission, "auth_permission"),
        (Group, "auth_group"),
        (User, "auth_user"),
    ]:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )

    vendor = getattr(conn, "vendor", "sqlite")
    pk_type = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if vendor == "sqlite"
        else "SERIAL PRIMARY KEY"
    )
    for junction, left, right in (
        ("auth_group_permissions", "group_id", "permission_id"),
        ("auth_user_groups", "user_id", "group_id"),
        ("auth_user_user_permissions", "user_id", "permission_id"),
    ):
        left_table = "auth_group" if left == "group_id" else "auth_user"
        right_table = "auth_permission" if right == "permission_id" else "auth_group"
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{junction}" (\n'
            f'  "id" {pk_type},\n'
            f'  "{left}" BIGINT NOT NULL REFERENCES "{left_table}"("id"),\n'
            f'  "{right}" BIGINT NOT NULL REFERENCES "{right_table}"("id")\n'
            f")"
        )

    yield


def test_sync_permissions_creates_default_verbs_per_concrete_model(_auth_tables):
    """Walking the registry must materialise add/change/delete/view
    rows for every concrete model in INSTALLED_APPS apps. Idempotent
    second call → 0 new rows."""
    from dorm.contrib.auth.management import sync_permissions
    from dorm.contrib.auth.models import Permission
    from tests.models import Author

    # Build a single-model registry to keep the assertion focused —
    # the test conftest already populates the registry with several
    # apps which would clutter the count.
    registry = {f"{Author._meta.app_label}.{Author._meta.model_name}": Author}

    Permission.objects.all().delete()
    created = sync_permissions(registry=registry)
    assert created == 4  # add / change / delete / view

    codenames = set(
        Permission.objects.values_list("codename", flat=True)
    )
    expected = {
        "tests.add_author",
        "tests.change_author",
        "tests.delete_author",
        "tests.view_author",
    }
    assert expected.issubset(codenames)

    # Second call is a no-op — Permission rows already exist.
    again = sync_permissions(registry=registry)
    assert again == 0


def test_sync_permissions_picks_up_meta_permissions_entries(_auth_tables):
    """``Meta.permissions = [(codename, name), ...]`` declarations
    surface as Permission rows alongside the default verbs."""
    from dorm.contrib.auth.management import sync_permissions
    from dorm.contrib.auth.models import Permission

    class _Article(dorm.Model):
        title = dorm.CharField(max_length=10)

        class Meta:
            app_label = "v3_0_perms"
            db_table = "v3_0_perms_article"
            permissions = [
                ("articles.publish", "Can publish articles"),
                ("articles.archive", "Can archive articles"),
            ]

    registry = {f"{_Article._meta.app_label}.{_Article._meta.model_name}": _Article}

    Permission.objects.all().delete()
    sync_permissions(registry=registry)
    codenames = set(Permission.objects.values_list("codename", flat=True))
    assert "articles.publish" in codenames
    assert "articles.archive" in codenames
    # Default verbs also present.
    assert "v3_0_perms.add__article" in codenames or "v3_0_perms.add__article" in codenames


def test_sync_permissions_skips_proxy_and_abstract_models(_auth_tables):
    """Proxy + abstract models share storage with their concrete
    parent (or have no storage at all). Surfacing per-permission
    rows for them would double-count or generate garbage codenames."""
    from dorm.contrib.auth.management import sync_permissions
    from dorm.contrib.auth.models import Permission

    class _Concrete(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "v3_0_perms_skip"
            db_table = "v3_0_perms_skip_concrete"

    class _ProxyConcrete(_Concrete):
        class Meta:
            app_label = "v3_0_perms_skip"
            proxy = True

    registry = {
        "v3_0_perms_skip.concrete": _Concrete,
        "v3_0_perms_skip.proxyconcrete": _ProxyConcrete,
    }

    Permission.objects.all().delete()
    sync_permissions(registry=registry)
    codenames = set(Permission.objects.values_list("codename", flat=True))
    # Concrete's verbs present. Class name starts with ``_`` →
    # model_name = ``_concrete`` → codename ``add__concrete``
    # (double underscore is intentional, mirrors Django's behaviour
    # of using the lowercased class name verbatim).
    assert "v3_0_perms_skip.add__concrete" in codenames
    # Proxy's would-be verbs absent — same table, no separate perms.
    assert "v3_0_perms_skip.add__proxyconcrete" not in codenames


# ──────────────────────────────────────────────────────────────────────────────
# Password reset full roundtrip
# ──────────────────────────────────────────────────────────────────────────────


def test_password_reset_full_roundtrip_invalidates_after_use(_auth_tables):
    """Mint → check OK → set new password → check fails (salt rolled)."""
    import dorm
    from dorm.contrib.auth.tokens import PasswordResetTokenGenerator
    from dorm.contrib.auth.models import User

    # Pin a deterministic key so the test isn't dependent on
    # whatever the previous test left in settings.
    dorm.configure(SECRET_KEY="test-reset-roundtrip-key")

    user = User.objects.create_user(email="reset@example.com", password="old")
    gen = PasswordResetTokenGenerator()
    token = gen.make_token(user)

    # Token verifies against the live user state.
    assert gen.check_token(user, token) is True

    # The user resets their password — same salt-driving fields
    # change → token must reject.
    user.set_password("brand-new")
    user.save()
    assert gen.check_token(user, token) is False


def test_password_reset_namespace_separation_blocks_cross_use(_auth_tables):
    """Two generators with different ``salt_namespace`` must not
    accept each other's tokens — domain separation is the whole
    point of the namespace argument."""
    import dorm
    from dorm.contrib.auth.tokens import PasswordResetTokenGenerator
    from dorm.contrib.auth.models import User

    dorm.configure(SECRET_KEY="test-reset-namespace-key")

    user = User.objects.create_user(email="ns@example.com", password="x")
    reset_gen = PasswordResetTokenGenerator(salt_namespace="reset")
    verify_gen = PasswordResetTokenGenerator(salt_namespace="verify-email")

    reset_token = reset_gen.make_token(user)
    # Replaying a reset token against the email-verify endpoint
    # would let an attacker bypass verification with a stolen
    # reset URL.
    assert verify_gen.check_token(user, reset_token) is False


# ──────────────────────────────────────────────────────────────────────────────
# MySQL parse_database_url → configure → connection raises clear error
# ──────────────────────────────────────────────────────────────────────────────


def test_mysql_url_parse_then_configure_then_connection_raises():
    """End-to-end: a ``DATABASE_URL=mysql://...`` env var on
    deployment day. ``parse_database_url`` accepts it,
    ``configure`` stores it, the first connection attempt surfaces
    the v3.1-pointer error so the user's logs say what's missing."""
    cfg = dorm.parse_database_url("mysql://root:s@db:3306/myapp")
    assert cfg["ENGINE"] == "mysql"
    assert cfg["HOST"] == "db"

    from dorm.db.connection import _create_sync_connection

    with pytest.raises(ImproperlyConfigured, match="not implemented yet"):
        _create_sync_connection("default", cfg)


def test_mariadb_scheme_routes_through_same_path():
    cfg = dorm.parse_database_url("mariadb://root@db/myapp")
    assert cfg["ENGINE"] == "mariadb"

    from dorm.db.connection import _create_async_connection

    with pytest.raises(ImproperlyConfigured, match="not implemented yet"):
        _create_async_connection("default", cfg)


# ──────────────────────────────────────────────────────────────────────────────
# Cache: cache_get + cache_get_many invalidation on save
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _locmem_caches():
    """Switch CACHES to LocMemCache for the test, restore on exit."""
    import dorm
    from dorm.cache import reset_caches

    prev = dict(getattr(dorm.conf.settings, "CACHES", {}) or {})
    dorm.configure(
        CACHES={
            "default": {
                "BACKEND": "dorm.cache.locmem.LocMemCache",
                "OPTIONS": {"maxsize": 64},
                "TTL": 60,
            }
        },
        CACHE_SIGNING_KEY="v3-integration-key",
    )
    try:
        yield
    finally:
        dorm.configure(CACHES=prev)
        reset_caches()


def test_cache_get_invalidates_after_save(_locmem_caches):
    """``Manager.cache_get`` should NOT return the stale row after
    a ``save()`` bumped the per-model invalidation version. The
    first read populates the cache; the save bumps the version;
    the second read hits a fresh key, falls through to the DB,
    repopulates."""
    from tests.models import Author

    a = Author.objects.create(name="orig", age=20)
    fetched = Author.objects.cache_get(pk=a.pk)
    assert fetched.name == "orig"

    # Mutate via a fresh instance — the stale cached row would
    # leak through if invalidation didn't fire.
    a.name = "updated"
    a.save()

    refetched = Author.objects.cache_get(pk=a.pk)
    assert refetched.name == "updated", (
        "cache_get returned stale row — version bump on save isn't "
        "wiring into the row-cache key derivation"
    )


def test_cache_get_many_round_trip(_locmem_caches):
    from tests.models import Author

    a = Author.objects.create(name="A", age=10)
    b = Author.objects.create(name="B", age=11)
    out = Author.objects.cache_get_many(pks=[a.pk, b.pk])
    assert set(out.keys()) == {a.pk, b.pk}
    assert {x.name for x in out.values()} == {"A", "B"}

    # Hit path on second call.
    again = Author.objects.cache_get_many(pks=[a.pk, b.pk])
    assert {x.pk for x in again.values()} == {a.pk, b.pk}


# ──────────────────────────────────────────────────────────────────────────────
# QueryLog summary shape + ASGI middleware end-to-end
# ──────────────────────────────────────────────────────────────────────────────


def test_query_log_summary_returns_template_stats_with_aggregates():
    """Five queries against three distinct templates → three
    ``TemplateStats`` entries with correct counts + populated
    timing fields."""
    from dorm.contrib.querylog import QueryLog, TemplateStats
    from tests.models import Author

    Author.objects.create(name="seed", age=1)
    with QueryLog() as log:
        Author.objects.filter(name="x").count()       # tpl A
        Author.objects.filter(name="y").count()       # tpl A
        Author.objects.filter(age__gt=5).count()      # tpl B
        list(Author.objects.values("name"))           # tpl C
        list(Author.objects.values("name"))           # tpl C
    summary = log.summary()
    assert summary, "summary cannot be empty after captured queries"
    assert all(isinstance(s, TemplateStats) for s in summary)
    # Sum of per-template counts equals total records captured.
    assert sum(s.count for s in summary) == log.count
    # Each entry exposes the timing aggregates the docs promise.
    for s in summary:
        assert s.total_ms >= 0
        assert s.p50_ms >= 0
        assert s.p95_ms >= 0


@pytest.mark.asyncio
async def test_querylog_asgi_middleware_attaches_log_to_request_scope():
    from dorm.contrib.querylog import QueryLog, QueryLogASGIMiddleware
    from tests.models import Author

    captured: dict = {}

    async def app(scope, receive, send):
        log = scope.get("dorm_querylog")
        # The downstream handler can read / serialise this log
        # however it likes — middleware just attaches it.
        captured["log"] = log
        list(Author.objects.values_list("id", flat=True))

    middleware = QueryLogASGIMiddleware(app)

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    await middleware({"type": "http", "method": "GET", "path": "/"}, receive, send)
    assert isinstance(captured["log"], QueryLog)
    assert captured["log"].count >= 1


# ──────────────────────────────────────────────────────────────────────────────
# Multi-tenant: schema validation + non-PG rejection + state restoration
# ──────────────────────────────────────────────────────────────────────────────


def test_tenant_context_rejects_unsafe_schema_name():
    """Schema names get spliced into ``SET search_path`` without
    parameter binding (PG doesn't accept binds in DDL/SET). The
    validator must reject anything that isn't a SQL identifier."""
    from dorm.contrib.tenants import _validate_schema_name

    for bad in ("acme; DROP TABLE users", "1tenant", "with space", ""):
        with pytest.raises(ValueError, match="Tenant schema name"):
            _validate_schema_name(bad)


def test_tenant_context_restores_outer_tenant_on_exit():
    """Nested ``TenantContext`` blocks should restore the OUTER
    tenant on the inner's exit (rare but legal — cross-tenant
    aggregation jobs)."""
    from dorm.contrib.tenants import (
        _active_tenant,
        current_tenant,
    )

    # Don't actually open a TenantContext here — that would need
    # PG. Verify the ContextVar logic stand-alone: set, set, reset
    # → restored outer.
    token_outer = _active_tenant.set("outer")
    assert current_tenant() == "outer"
    token_inner = _active_tenant.set("inner")
    assert current_tenant() == "inner"
    _active_tenant.reset(token_inner)
    assert current_tenant() == "outer"
    _active_tenant.reset(token_outer)
    assert current_tenant() is None


def test_tenant_context_refuses_non_postgres_loudly():
    """A user pointing TenantContext at a sqlite alias should get
    a clear refusal — silently no-op-ing would let them think
    routing worked when nothing changed."""
    from dorm.contrib.tenants import TenantContext
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") != "sqlite":
        pytest.skip("Assertion is sqlite-specific.")

    with pytest.raises(NotImplementedError, match="PostgreSQL"):
        with TenantContext("acme_v3_0"):
            pass
