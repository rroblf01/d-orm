"""Tests for the PII helpers and field flag.

The conftest's session-scoped ``configure_dorm`` fixture handles
``dorm.configure(...)`` for the entire suite — we deliberately do not
re-configure at module-import time here. Tests that need a private
SQLite (so they can ``CREATE TABLE`` for the per-file model) wrap the
re-configure in a fixture that restores the prior settings on
teardown; otherwise the next PG test in the suite would silently see
a SQLite alias and fail with ``NotImplementedError`` from
``_ensure_postgres``.
"""
from __future__ import annotations

import pytest

import dorm
from dorm.contrib.pii import (
    anonymize_row,
    has_pii_fields,
    mask_dict,
    mask_instance,
    pii_fields,
    reset_cache,
)


@pytest.fixture
def restore_settings():
    """Snapshot DATABASES + INSTALLED_APPS so a test that reconfigures
    dorm doesn't leak the change into the next test in the suite."""
    from dorm.conf import settings

    saved_db = {alias: dict(cfg) for alias, cfg in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    from dorm.db.connection import _async_connections, _sync_connections

    _sync_connections.clear()
    _async_connections.clear()
    reset_cache()


class PIIPerson(dorm.Model):
    email = dorm.EmailField(pii=True)
    full_name = dorm.CharField(max_length=120, pii=True)
    username = dorm.CharField(max_length=40)
    age = dorm.IntegerField(null=True, pii=True)

    class Meta:
        app_label = "tests"


class NoPIIModel(dorm.Model):
    name = dorm.CharField(max_length=40)

    class Meta:
        app_label = "tests"


class TestPIIFields:
    def test_pii_fields_lists_flagged_columns(self):
        names = {f.name for f in pii_fields(PIIPerson)}
        assert names == {"email", "full_name", "age"}

    def test_pii_fields_excludes_non_pii(self):
        names = {f.name for f in pii_fields(PIIPerson)}
        assert "username" not in names

    def test_has_pii_fields(self):
        assert has_pii_fields(PIIPerson)
        assert not has_pii_fields(NoPIIModel)

    def test_pii_fields_handles_non_models(self):
        assert pii_fields(int) == []


class TestMaskInstance:
    def test_string_fields_become_redacted_sentinel(self):
        person = PIIPerson(
            email="user@example.com",
            full_name="Jane Doe",
            username="jane",
            age=30,
        )
        mask_instance(person)
        assert person.email == "[REDACTED]"
        assert person.full_name == "[REDACTED]"
        # Non-PII column untouched.
        assert person.username == "jane"

    def test_integer_pii_becomes_none(self):
        person = PIIPerson(
            email="user@example.com", full_name="X", username="x", age=30
        )
        mask_instance(person)
        assert person.age is None

    def test_null_value_not_masked(self):
        # Avoid clobbering a legitimately-null column with the sentinel,
        # which would break NOT NULL checks downstream.
        person = PIIPerson(
            email="user@example.com",
            full_name="X",
            username="x",
            age=None,
        )
        mask_instance(person)
        assert person.age is None


class TestMaskDict:
    def test_pii_columns_masked(self):
        row = {
            "email": "a@b.com",
            "full_name": "X",
            "username": "u",
            "age": 30,
        }
        masked = mask_dict(PIIPerson, row)
        assert masked["email"] == "[REDACTED]"
        assert masked["full_name"] == "[REDACTED]"
        assert masked["age"] is None
        assert masked["username"] == "u"

    def test_empty_dict_returns_empty(self):
        assert mask_dict(PIIPerson, {}) == {}

    def test_unknown_keys_preserved(self):
        masked = mask_dict(PIIPerson, {"unknown_col": 42, "email": "x@y"})
        assert masked["unknown_col"] == 42
        assert masked["email"] == "[REDACTED]"


class TestAnonymizeRow:
    def test_anonymize_persists(self, tmp_path, restore_settings):
        # Spin up a real sqlite to verify the save side-effect — the
        # ``restore_settings`` fixture undoes the reconfigure on
        # teardown so the next test sees the conftest backend again.
        db = tmp_path / "pii.db"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )
        from dorm.migrations.schema import SchemaEditor
        from dorm.db.connection import get_connection

        # Reset connection registry so the new DB takes effect.
        from dorm.db.connection import _sync_connections
        _sync_connections.clear()

        with SchemaEditor(get_connection()) as se:
            se.create_model(PIIPerson)

        person = PIIPerson.objects.create(
            email="user@example.com",
            full_name="Jane Doe",
            username="jane",
            age=30,
        )
        anonymize_row(person)

        reloaded = PIIPerson.objects.get(pk=person.pk)
        assert reloaded.email == "[REDACTED]"
        assert reloaded.full_name == "[REDACTED]"
        assert reloaded.age is None
        # Non-PII column untouched.
        assert reloaded.username == "jane"


class TestFieldFlag:
    def test_pii_flag_persists_on_deconstruct(self):
        f = dorm.CharField(max_length=10, pii=True)
        _, _, _, kwargs = f.deconstruct()
        assert kwargs["pii"] is True

    def test_pii_default_omitted_from_deconstruct(self):
        f = dorm.CharField(max_length=10)
        _, _, _, kwargs = f.deconstruct()
        assert "pii" not in kwargs
