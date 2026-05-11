"""Tests for the per-template query stats collector."""
from __future__ import annotations

import json

import pytest

import dorm
from dorm.contrib import querystats
from dorm.migrations.schema import SchemaEditor


class _Q(dorm.Model):
    name = dorm.CharField(max_length=32)

    class Meta:
        app_label = "tests"


@pytest.fixture(autouse=True)
def fresh(tmp_path):
    from dorm.conf import settings
    from dorm.db.connection import _async_connections, _sync_connections, get_connection

    saved = {a: dict(c) for a, c in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "qs.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    with SchemaEditor(get_connection()) as se:
        se.create_model(_Q)
    querystats.reset()
    querystats.collector().enable()
    yield
    querystats.collector().disable()
    querystats.reset()
    dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
    _sync_connections.clear()
    _async_connections.clear()


class TestQueryStatsCollector:
    def test_aggregates_by_template(self):
        for i in range(5):
            _Q.objects.create(name=f"n{i}")
        list(_Q.objects.filter(name="n0"))
        list(_Q.objects.filter(name="n1"))
        snapshot = querystats.collector().snapshot()
        # INSERT template + SELECT template appear at minimum.
        templates = [s.template for s in snapshot]
        assert any("INSERT INTO" in t for t in templates)
        assert any("SELECT" in t for t in templates)

    def test_count_increments(self):
        list(_Q.objects.filter(name="x"))
        list(_Q.objects.filter(name="y"))
        # Two ``SELECT`` calls with different literals → same template
        select_rows = [
            s for s in querystats.collector().snapshot()
            if s.template.startswith("SELECT")
        ]
        assert select_rows and select_rows[0].count >= 2

    def test_render_text_emits_prometheus_lines(self):
        list(_Q.objects.filter(name="a"))
        text = querystats.render_text()
        assert "# TYPE dorm_template_count counter" in text
        assert "dorm_template_count{" in text
        assert "dorm_template_p50_ms" in text

    def test_render_json_returns_list_of_dicts(self):
        list(_Q.objects.filter(name="a"))
        payload = querystats.render_json()
        assert isinstance(payload, list)
        assert payload and "template" in payload[0]
        assert all(set(d) >= {"template", "count", "total_ms", "p50_ms", "p95_ms", "p99_ms"} for d in payload)
        # Roundtrip through json.dumps to ensure all values are serialisable.
        json.dumps(payload)

    def test_reset_clears(self):
        list(_Q.objects.filter(name="a"))
        assert querystats.collector().snapshot()
        querystats.reset()
        assert querystats.collector().snapshot() == []

    def test_disable_stops_recording(self):
        querystats.collector().disable()
        querystats.reset()
        list(_Q.objects.filter(name="a"))
        assert querystats.collector().snapshot() == []

    def test_enable_idempotent(self):
        querystats.collector().enable()
        querystats.collector().enable()  # should not duplicate
        list(_Q.objects.filter(name="a"))
        select_rows = [
            s for s in querystats.collector().snapshot()
            if s.template.startswith("SELECT")
        ]
        # Without de-dup, each query would be recorded twice.
        assert select_rows and select_rows[0].count == 1

    def test_reservoir_bounded(self):
        querystats.reset()
        querystats.collector().enable(reservoir_max=10)
        for _ in range(50):
            list(_Q.objects.filter(name="a"))
        select_rows = [
            s for s in querystats.collector().snapshot()
            if s.template.startswith("SELECT")
        ]
        # Reservoir caps the samples list, but the count keeps climbing.
        assert select_rows and len(select_rows[0].samples) <= 10
        assert select_rows[0].count >= 50

    def test_percentile_estimate(self):
        from dorm.contrib.querystats import TemplateStats

        s = TemplateStats(template="x")
        for i in range(1, 101):  # 1..100
            s.add(float(i), reservoir_max=1000)
        assert 49.0 <= s.percentile(0.5) <= 51.0
        assert 94.0 <= s.percentile(0.95) <= 96.0
