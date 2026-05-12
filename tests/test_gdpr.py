"""Tests for ``dorm.contrib.gdpr.erase_subject``."""
from __future__ import annotations

import pytest


@pytest.fixture
def fresh_db(tmp_path):
    import dorm
    from dorm.conf import settings
    from dorm.db.connection import _async_connections, _sync_connections

    saved_db = {a: dict(c) for a, c in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _sync_connections.clear()
    _async_connections.clear()
    db_path = tmp_path / "gdpr.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db_path)}},
        INSTALLED_APPS=["tests"],
    )
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()


def _materialise(*models):
    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor

    conn = get_connection()
    with SchemaEditor(conn) as se:
        for m in models:
            se.create_model(m)


class TestEraseSubjectBasics:
    def test_default_rules_redact_string_fields(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            email = dorm.EmailField()
            name = dorm.CharField(max_length=20)
            score = dorm.IntegerField(default=0)

            class Meta:
                app_label = "tests"

        _materialise(_U)
        u = _U.objects.create(email="a@x.com", name="Alice", score=10)
        summary = erase_subject(_U, u.pk)
        assert summary == {"tests._U": 1}
        u.refresh_from_db()
        # String fields redacted; numeric left alone.
        assert u.email == "[REDACTED]"
        assert u.name == "[REDACTED]"
        assert u.score == 10

    def test_explicit_rules_override_defaults(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            email = dorm.EmailField()
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _materialise(_U)
        u = _U.objects.create(email="a@x.com", name="Alice")
        erase_subject(_U, u.pk, rules={"email": "random_email"})
        u.refresh_from_db()
        # Only ``email`` rewritten because explicit rules win.
        assert u.email.startswith("anon-") and u.email.endswith("@example.test")
        assert u.name == "Alice"

    def test_unknown_field_in_rules_raises(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _materialise(_U)
        u = _U.objects.create(name="x")
        with pytest.raises(ValueError, match="unknown field"):
            erase_subject(_U, u.pk, rules={"nope": "redact"})

    def test_missing_subject_returns_zero(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _materialise(_U)
        summary = erase_subject(_U, 99999)
        assert summary == {"tests._U": 0}

    def test_idempotent_on_redact(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _materialise(_U)
        u = _U.objects.create(name="Alice")
        erase_subject(_U, u.pk)
        erase_subject(_U, u.pk)
        u.refresh_from_db()
        assert u.name == "[REDACTED]"


class TestCascade:
    def test_cascade_rewrites_referencing_rows(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        class _Note(dorm.Model):
            owner = dorm.ForeignKey(_U, on_delete=dorm.CASCADE)
            body = dorm.TextField()

            class Meta:
                app_label = "tests"

        _materialise(_U, _Note)
        u = _U.objects.create(name="Alice")
        _Note.objects.create(owner=u, body="secret 1")
        _Note.objects.create(owner=u, body="secret 2")
        # Unrelated row.
        other = _U.objects.create(name="Bob")
        _Note.objects.create(owner=other, body="not erased")

        summary = erase_subject(_U, u.pk, cascade=[_Note])
        assert summary == {"tests._U": 1, "tests._Note": 2}
        # Only the referencing rows got redacted.
        notes = list(_Note.objects.filter(owner=u))
        assert all(n.body == "[REDACTED]" for n in notes)
        other_notes = list(_Note.objects.filter(owner=other))
        assert other_notes[0].body == "not erased"

    def test_cascade_without_fk_match_reports_zero(self, fresh_db):
        import dorm
        from dorm.contrib.gdpr import erase_subject

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        class _Unrelated(dorm.Model):
            label = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _materialise(_U, _Unrelated)
        u = _U.objects.create(name="x")
        _Unrelated.objects.create(label="kept")
        summary = erase_subject(_U, u.pk, cascade=[_Unrelated])
        assert summary["tests._Unrelated"] == 0
        row = _Unrelated.objects.first()
        assert row is not None
        assert row.label == "kept"
