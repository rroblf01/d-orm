"""Coverage for v3.2 ``dorm.contrib.history`` audit-trail mixin.

The decorator builds a sibling ``<Name>Historical`` model and wires
``post_save`` / ``post_delete`` to record every change. Tests cover
insert / update / delete tagging, user attribution via contextvars,
manual recording for code paths that bypass ``save()``, and async
parity.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.contrib.history import (
    arecord_history_for,
    current_history_user,
    record_history_for,
    reset_history_user,
    set_history_user,
    track_history,
)


def _ddl_for(model_cls):
    """Drop + recreate *model_cls*'s table using the same DDL emitter
    that migrations use. Returns ``(connection, table_name, cascade_str)``
    so the caller can DROP at teardown."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    table = model_cls._meta.db_table
    conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in model_cls._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        f'CREATE TABLE "{table}" (\n  ' + ",\n  ".join(filter(None, cols)) + "\n)"
    )
    return conn, table, cascade


def _setup_pair(src_cls):
    """Create both the source AND its history table, return cleanup."""
    conn, src_table, cascade = _ddl_for(src_cls)
    hist_cls = src_cls._history_model
    _ddl_for(hist_cls)

    def _cleanup():
        conn.execute_script(
            f'DROP TABLE IF EXISTS "{hist_cls._meta.db_table}"{cascade}'
        )
        conn.execute_script(f'DROP TABLE IF EXISTS "{src_table}"{cascade}')

    return _cleanup


# ─────────────────────────────────────────────────────────────────────────────
# Decorator builds the sibling model with the right shape
# ─────────────────────────────────────────────────────────────────────────────


def test_track_history_builds_sibling_model():
    @track_history
    class _Article(dorm.Model):
        title = dorm.CharField(max_length=200)
        body = dorm.TextField()

        class Meta:
            db_table = "v3_2_hist_build"
            app_label = "v3_2_hist"

    hist = _Article._history_model  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    field_names = {f.name for f in hist._meta.fields}
    # Source columns mirrored
    assert "title" in field_names and "body" in field_names
    # Audit columns added
    for col in ("history_id", "history_date", "history_type", "history_user_id"):
        assert col in field_names
    assert hist._meta.db_table == "v3_2_hist_build_history"


def test_track_history_is_idempotent():
    @track_history
    class _Once(dorm.Model):
        x = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_once"
            app_label = "v3_2_hist"

    h1 = _Once._history_model  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    track_history(_Once)
    h2 = _Once._history_model  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    assert h1 is h2


def test_track_history_demotes_pk_on_history_table():
    """Source PK must NOT be unique on the history table — same row id
    appears once per change."""

    @track_history
    class _Demote(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_hist_demote"
            app_label = "v3_2_hist"

    hist = _Demote._history_model  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    src_pk_attname = _Demote._meta.pk.attname
    f = hist._meta.get_field(src_pk_attname)
    assert f.primary_key is False
    assert f.unique is False


# ─────────────────────────────────────────────────────────────────────────────
# Sync save / delete record history
# ─────────────────────────────────────────────────────────────────────────────


def test_save_records_insert_then_update():
    @track_history
    class _Post(dorm.Model):
        title = dorm.CharField(max_length=80)

        class Meta:
            db_table = "v3_2_hist_post"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Post)
    try:
        p = _Post.objects.create(title="hello")
        rows = list(_Post.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert len(rows) == 1
        assert rows[0].history_type == "+"
        assert rows[0].title == "hello"

        p.title = "hi"
        p.save()
        kinds = sorted(r.history_type for r in _Post.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert kinds == ["+", "~"]
    finally:
        cleanup()


def test_delete_records_minus_row():
    @track_history
    class _Doomed(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_hist_doomed"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Doomed)
    try:
        d = _Doomed.objects.create(name="goodbye")
        d.delete()
        kinds = sorted(r.history_type for r in _Doomed.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert kinds == ["+", "-"]
        # The '-' row preserves the field values from before deletion.
        gone = _Doomed.history.filter(history_type="-").first()  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert gone is not None
        assert gone.name == "goodbye"
    finally:
        cleanup()


# ─────────────────────────────────────────────────────────────────────────────
# User attribution via contextvar
# ─────────────────────────────────────────────────────────────────────────────


def test_history_records_user_id_from_contextvar():
    @track_history
    class _Tracked(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_user"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Tracked)
    try:
        token = set_history_user(42)
        try:
            _Tracked.objects.create(v=1)
        finally:
            reset_history_user(token)
        row = _Tracked.history.first()  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert row is not None
        assert row.history_user_id == 42
    finally:
        cleanup()


def test_current_history_user_default_none():
    assert current_history_user() is None


# ─────────────────────────────────────────────────────────────────────────────
# Manual recording for paths that bypass save()
# ─────────────────────────────────────────────────────────────────────────────


def test_record_history_for_writes_arbitrary_kind():
    @track_history
    class _Manual(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_manual"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Manual)
    try:
        obj = _Manual.objects.create(v=99)
        # Wipe the auto-created '+' row so the test's assertion is
        # specific to record_history_for's effect.
        _Manual.history.all().delete()  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        record_history_for(obj, "~", user_id=7)
        rows = list(_Manual.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert len(rows) == 1
        assert rows[0].history_type == "~"
        assert rows[0].history_user_id == 7
    finally:
        cleanup()


def test_record_history_for_rejects_invalid_kind():
    @track_history
    class _Bad(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_bad"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Bad)
    try:
        obj = _Bad.objects.create(v=1)
        with pytest.raises(ValueError, match=r"kind must"):
            record_history_for(obj, "X")
    finally:
        cleanup()


def test_record_history_for_rejects_untracked_model():
    class _Plain(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_plain"
            app_label = "v3_2_hist"

    obj = _Plain(v=0)
    with pytest.raises(TypeError, match="not history-tracked"):
        record_history_for(obj, "+")


# ─────────────────────────────────────────────────────────────────────────────
# Async parity
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_asave_records_history():
    @track_history
    class _Async(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_async"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_Async)
    try:
        obj = _Async(v=10)
        await obj.asave()
        rows = list(_Async.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert len(rows) == 1
        assert rows[0].history_type == "+"
        assert rows[0].v == 10
    finally:
        cleanup()


@pytest.mark.asyncio
async def test_arecord_history_for_writes_row():
    @track_history
    class _AsyncManual(dorm.Model):
        v = dorm.IntegerField()

        class Meta:
            db_table = "v3_2_hist_async_manual"
            app_label = "v3_2_hist"

    cleanup = _setup_pair(_AsyncManual)
    try:
        obj = _AsyncManual.objects.create(v=5)
        _AsyncManual.history.all().delete()  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        await arecord_history_for(obj, "-", user_id=11)
        rows = list(_AsyncManual.history.all())  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert len(rows) == 1
        assert rows[0].history_type == "-"
        assert rows[0].history_user_id == 11
    finally:
        cleanup()
