"""Tests for ``dorm.contrib.auth``: password helpers + User/Group/Permission."""

from __future__ import annotations

import pytest

from dorm.contrib.auth.password import (
    PBKDF2_DEFAULT_ITERATIONS,
    check_password,
    is_password_usable,
    make_password,
)


# ──────────────────────────────────────────────────────────────────────────────
# Password helpers — pure function, no DB
# ──────────────────────────────────────────────────────────────────────────────


def test_make_password_returns_pbkdf2_format():
    h = make_password("hunter2")
    assert h.startswith("pbkdf2_sha256$")
    parts = h.split("$")
    assert len(parts) == 4
    assert int(parts[1]) == PBKDF2_DEFAULT_ITERATIONS


def test_check_password_round_trip():
    h = make_password("correct horse battery staple")
    assert check_password("correct horse battery staple", h)
    assert not check_password("wrong-password", h)


def test_make_password_uses_unique_salt():
    """Two encodings of the same password must produce different
    hashes — otherwise rainbow-table attacks become trivial."""
    a = make_password("same")
    b = make_password("same")
    assert a != b
    assert check_password("same", a)
    assert check_password("same", b)


def test_unusable_password_sentinel():
    h = make_password(None)
    assert h.startswith("!")
    assert not is_password_usable(h)
    assert not check_password("", h)
    assert not check_password("anything", h)


def test_check_password_rejects_malformed_hashes():
    assert not check_password("x", "")
    assert not check_password("x", "not-pbkdf2")
    assert not check_password("x", "pbkdf2_sha256$not-an-int$salt$hash")
    assert not check_password("x", "argon2$1$salt$hash")  # unknown algorithm


def test_check_password_constant_time_for_close_misses():
    """Smoke test — check_password should not crash on hashes that are
    one character off from a valid one."""
    h = make_password("hunter2")
    # Truncate the hash bytes — still well-formed structurally.
    parts = h.split("$")
    parts[3] = parts[3][:-1] + ("A" if parts[3][-1] != "A" else "B")
    assert not check_password("hunter2", "$".join(parts))


# ──────────────────────────────────────────────────────────────────────────────
# Model-level tests — need a configured DB.
#
# The conftest's ``configure_dorm`` autouse fixture sets up
# DATABASES + INSTALLED_APPS = ["tests"]. We don't add the auth app
# to INSTALLED_APPS to avoid needing migrations to run; instead we
# create the auth tables by hand inside the test fixture so the
# password-hashing path is exercised end-to-end.
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _auth_tables():
    from dorm.db.connection import get_connection
    from dorm.contrib.auth.models import Group, Permission, User
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    # Drop in dependency order then recreate.
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


def test_create_user_hashes_password(_auth_tables):
    from dorm.contrib.auth.models import User

    user = User.objects.create_user(email="alice@example.com", password="hunter2")
    assert user.email == "alice@example.com"
    # Password is stored hashed, NOT in plain text.
    assert user.password != "hunter2"
    assert user.password.startswith("pbkdf2_sha256$")
    assert user.check_password("hunter2")
    assert not user.check_password("nope")


def test_create_user_normalises_email(_auth_tables):
    from dorm.contrib.auth.models import User

    user = User.objects.create_user(email="  Alice@Example.COM ", password="x")
    assert user.email == "alice@example.com"
    assert user.username == "alice@example.com"


def test_set_password_changes_hash_and_verification(_auth_tables):
    from dorm.contrib.auth.models import User

    user = User.objects.create_user(email="b@c.com", password="old")
    old_hash = user.password
    user.set_password("new")
    user.save()
    assert user.password != old_hash
    assert user.check_password("new")
    assert not user.check_password("old")


def test_create_superuser_sets_flags(_auth_tables):
    from dorm.contrib.auth.models import User

    su = User.objects.create_superuser(email="root@root.com", password="x")
    assert su.is_staff
    assert su.is_superuser
    assert su.is_active


def test_create_superuser_rejects_demoted_flags(_auth_tables):
    from dorm.contrib.auth.models import User

    with pytest.raises(ValueError, match="is_staff"):
        User.objects.create_superuser(
            email="x@x.com", password="x", is_staff=False
        )


def test_unusable_password_blocks_login(_auth_tables):
    from dorm.contrib.auth.models import User

    user = User.objects.create_user(email="sso@x.com")
    assert not user.has_usable_password()
    assert not user.check_password("anything")


def test_has_perm_superuser_short_circuits(_auth_tables):
    from dorm.contrib.auth.models import User

    su = User.objects.create_superuser(email="r@r.com", password="x")
    # Superusers say yes to any permission, even one that doesn't exist.
    assert su.has_perm("articles.publish")
    assert su.has_perm("anything")


def test_has_perm_inactive_user_returns_false(_auth_tables):
    from dorm.contrib.auth.models import Permission, User

    p = Permission.objects.create(name="Publish", codename="articles.publish")
    user = User.objects.create_user(email="x@x.com", password="x", is_active=False)
    user.user_permissions.add(p)
    assert not user.has_perm("articles.publish")


def test_has_perm_direct_permission(_auth_tables):
    from dorm.contrib.auth.models import Permission, User

    p = Permission.objects.create(name="Publish", codename="articles.publish")
    user = User.objects.create_user(email="x@x.com", password="x")
    assert not user.has_perm("articles.publish")
    user.user_permissions.add(p)
    assert user.has_perm("articles.publish")


def test_has_perm_via_group(_auth_tables):
    from dorm.contrib.auth.models import Group, Permission, User

    p = Permission.objects.create(name="Publish", codename="articles.publish")
    g = Group.objects.create(name="editors")
    g.permissions.add(p)

    user = User.objects.create_user(email="x@x.com", password="x")
    user.groups.add(g)
    assert user.has_perm("articles.publish")
