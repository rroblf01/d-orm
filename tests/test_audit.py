"""End-to-end tests for ``dorm.contrib.audit``.

Each test isolates settings to a per-test SQLite DB so the dynamic
audit + source models don't collide with the session-wide schema.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def fresh_db(tmp_path):
    """Reconfigure dorm to point at a fresh SQLite DB inside *tmp_path*,
    restoring the original config + connection caches on teardown."""
    import dorm
    from dorm.conf import settings
    from dorm.db.connection import _async_connections, _sync_connections

    saved_db = {a: dict(c) for a, c in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _sync_connections.clear()
    _async_connections.clear()
    db_path = tmp_path / "audit.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db_path)}},
        INSTALLED_APPS=["tests"],
    )
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()


def _create_tables(source_cls):
    """Materialise both the source model and its audit sibling."""
    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor

    conn = get_connection()
    with SchemaEditor(conn) as se:
        se.create_model(source_cls)
        se.create_model(source_cls._audit_model)


class TestAuditDecoratorBasics:
    def test_decorator_builds_audit_sibling(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audited

        @audited(fields=["email"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)
            email = dorm.EmailField()

            class Meta:
                app_label = "tests"

        # Sibling exists + has the documented columns.
        audit_cls = _U._audit_model  # ty: ignore[unresolved-attribute]
        col_names = {f.name for f in audit_cls._meta.fields}
        assert {"audit_id", "target_id", "field", "old_value", "new_value", "actor", "at", "operation"} <= col_names

    def test_decorator_is_idempotent(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audited

        @audited()
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        first = _U._audit_model  # ty: ignore[unresolved-attribute]
        # Re-apply the decorator — must be a no-op.
        _U2 = audited()(_U)
        assert _U2 is _U
        assert _U._audit_model is first  # ty: ignore[unresolved-attribute]

    def test_unknown_field_raises(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audited

        with pytest.raises(ValueError, match="unknown field"):
            @audited(fields=["nope"])
            class _U(dorm.Model):
                name = dorm.CharField(max_length=20)

                class Meta:
                    app_label = "tests"


class TestAuditWritesTrail:
    def test_insert_emits_one_row_per_watched_field(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        @audited(fields=["name", "email"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)
            email = dorm.EmailField(default="x@y.com")

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="Alice", email="a@x.com")
        rows = list(audit_history(u).order_by("field"))
        assert len(rows) == 2
        assert {r.field for r in rows} == {"name", "email"}
        assert all(r.operation == "+" for r in rows)
        assert all(r.old_value is None for r in rows)
        names = {r.field: r.new_value for r in rows}
        assert names["name"] == "Alice"
        assert names["email"] == "a@x.com"

    def test_update_emits_only_changed_field(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        @audited(fields=["name", "score"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)
            score = dorm.IntegerField(default=0)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="Bob", score=5)
        # Wipe the two insert-time rows so the update assertion is precise.
        _U._audit_model.objects.all().delete()  # ty: ignore[unresolved-attribute]
        u.score = 10
        u.save()
        rows = list(audit_history(u))
        assert len(rows) == 1
        assert rows[0].field == "score"
        assert rows[0].old_value == "5"
        assert rows[0].new_value == "10"
        assert rows[0].operation == "~"

    def test_update_with_no_changes_emits_nothing(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        @audited(fields=["name"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="same")
        _U._audit_model.objects.all().delete()  # ty: ignore[unresolved-attribute]
        # No-op save.
        u.save()
        assert audit_history(u).count() == 0

    def test_delete_emits_minus_rows(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audited

        @audited(fields=["name"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="bye")
        pk_val = u.pk
        _U._audit_model.objects.all().delete()  # ty: ignore[unresolved-attribute]
        u.delete()
        # ``audit_history`` requires an instance; query the sibling
        # directly using the recorded target_id.
        rows = list(_U._audit_model.objects.filter(target_id=pk_val))  # ty: ignore[unresolved-attribute]
        assert len(rows) == 1
        assert rows[0].operation == "-"
        assert rows[0].old_value == "bye"
        assert rows[0].new_value is None


class TestActorResolution:
    def test_actor_getter_invoked(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        actor_state = {"user": "carol"}

        @audited(fields=["name"], actor_getter=lambda: actor_state["user"])
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="x")
        actor_state["user"] = "dave"
        u.name = "y"
        u.save()
        rows = list(audit_history(u))
        assert {r.actor for r in rows} == {"carol", "dave"}

    def test_actor_getter_exception_swallowed(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        def _broken():
            raise RuntimeError("boom")

        @audited(fields=["name"], actor_getter=_broken)
        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(name="x")
        rows = list(audit_history(u))
        # Insert succeeded + actor recorded as NULL.
        assert len(rows) == 1
        assert rows[0].actor is None


class TestAuditHistoryHelper:
    def test_raises_on_undecorated(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history

        class _Plain(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        with pytest.raises(LookupError, match="not @audited"):
            audit_history(_Plain(name="x"))

    def test_history_ordering_newest_first(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        @audited(fields=["n"])
        class _U(dorm.Model):
            n = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(n=1)
        u.n = 2
        u.save()
        u.n = 3
        u.save()
        rows = list(audit_history(u))
        # default ordering = ``-at, -audit_id`` → newest first.
        assert rows[0].new_value == "3"


class TestEmptyWatchedList:
    def test_default_watches_all_concrete_fields(self, fresh_db):
        import dorm
        from dorm.contrib.audit import audit_history, audited

        @audited()
        class _U(dorm.Model):
            a = dorm.CharField(max_length=10, default="A")
            b = dorm.IntegerField(default=0)

            class Meta:
                app_label = "tests"

        _create_tables(_U)
        u = _U.objects.create(a="x", b=1)
        rows = {r.field for r in audit_history(u)}
        assert rows == {"a", "b"}
