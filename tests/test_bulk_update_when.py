"""Tests for QuerySet.bulk_update_when / abulk_update_when."""
from __future__ import annotations

import pytest

import dorm
from dorm.expressions import Q
from dorm.migrations.schema import SchemaEditor


class _Score(dorm.Model):
    name = dorm.CharField(max_length=32)
    score = dorm.IntegerField()
    label = dorm.CharField(max_length=8, default="")
    featured = dorm.BooleanField(default=False)

    class Meta:
        app_label = "tests"


@pytest.fixture(autouse=True)
def fresh_schema(tmp_path):
    """Use a fresh SQLite for the bulk_update_when round-trip checks
    (need a real DB so the CASE/WHEN SQL actually runs). Snapshot the
    conftest settings on entry and restore them on teardown so the
    next PG test in the suite isn't blindsided by a stale SQLite
    alias."""
    from dorm.conf import settings
    from dorm.db.connection import (
        _async_connections,
        _sync_connections,
        get_connection,
    )

    saved_db = {alias: dict(cfg) for alias, cfg in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)

    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "buw.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    with SchemaEditor(get_connection()) as se:
        se.create_model(_Score)
    yield
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()


class TestBulkUpdateWhen:
    def test_two_branches_with_default(self):
        _Score.objects.create(name="a", score=95)
        _Score.objects.create(name="b", score=80)
        _Score.objects.create(name="c", score=50)

        n = _Score.objects.bulk_update_when(
            [
                (Q(score__gte=90), {"label": "A", "featured": True}),
                (Q(score__gte=70), {"label": "B"}),
            ],
            default={"label": "C", "featured": False},
        )
        assert n == 3
        rows = {r.name: (r.label, r.featured) for r in _Score.objects.all()}
        assert rows["a"] == ("A", True)
        assert rows["b"] == ("B", False)
        # Default fires when no condition matched.
        assert rows["c"] == ("C", False)

    def test_default_preserves_unchanged_column_when_omitted(self):
        _Score.objects.create(name="a", score=95, label="OLD")
        _Score.objects.create(name="b", score=10, label="OLD")
        _Score.objects.bulk_update_when(
            [(Q(score__gte=90), {"label": "NEW"})]
        )
        assert _Score.objects.get(name="a").label == "NEW"
        # No default → ELSE branch keeps the previous column value.
        assert _Score.objects.get(name="b").label == "OLD"

    def test_dict_condition_accepted(self):
        _Score.objects.create(name="a", score=95)
        _Score.objects.bulk_update_when(
            [({"score__gte": 90}, {"label": "A"})]
        )
        assert _Score.objects.get(name="a").label == "A"

    def test_invalid_condition_type_rejected(self):
        with pytest.raises(TypeError, match="must be a Q"):
            _Score.objects.bulk_update_when(
                [(123, {"label": "X"})]  # type: ignore[list-item]
            )

    def test_empty_cases_is_noop(self):
        _Score.objects.create(name="a", score=95)
        n = _Score.objects.bulk_update_when([])
        assert n == 0

    def test_empty_values_dict_skipped(self):
        _Score.objects.create(name="a", score=95)
        n = _Score.objects.bulk_update_when(
            [(Q(score__gte=90), {})]
        )
        assert n == 0


class TestABulkUpdateWhen:
    async def test_async_two_branches(self):
        await _Score.objects.acreate(name="a", score=95)
        await _Score.objects.acreate(name="b", score=10)
        n = await _Score.objects.abulk_update_when(
            [
                (Q(score__gte=90), {"label": "HIGH"}),
            ],
            default={"label": "LOW"},
        )
        assert n == 2
        rows = {r.name: r.label async for r in _Score.objects.all()}
        assert rows == {"a": "HIGH", "b": "LOW"}

    async def test_async_empty_is_noop(self):
        await _Score.objects.acreate(name="a", score=95)
        n = await _Score.objects.abulk_update_when([])
        assert n == 0
