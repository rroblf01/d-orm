"""Tests for ``dorm.contrib.prometheus``.

The exporter listens on ``post_query`` and renders Prometheus
text-exposition output. We exercise install / record / render /
uninstall — no real Prometheus scraper involved.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_prom():
    from dorm.contrib.prometheus import uninstall
    uninstall()
    yield
    uninstall()


def test_install_idempotent():
    from dorm.contrib.prometheus import install

    install()
    install()  # second call must not raise / double-attach.


def test_metrics_render_after_query():
    from dorm.contrib.prometheus import install, metrics_response
    from tests.models import Author

    install()
    Author.objects.filter(name="z").count()

    out = metrics_response()
    assert "dorm_queries_total" in out
    assert "dorm_query_duration_seconds_bucket" in out
    assert "dorm_query_duration_seconds_sum" in out
    assert "dorm_query_duration_seconds_count" in out
    # Counter format includes label set + integer value.
    assert "outcome=" in out


def test_metrics_response_is_text_exposition_shape():
    from dorm.contrib.prometheus import install, metrics_response

    install()
    out = metrics_response()
    # Empty state — no scenario emitted any HELP / TYPE pairs yet.
    # Output should still be a valid (empty-ish) string with a
    # trailing newline (Prometheus text format requires it).
    assert isinstance(out, str)
    assert out.endswith("\n")


def test_cache_hit_miss_counters():
    from dorm.contrib.prometheus import (
        metrics_response,
        record_cache_hit,
        record_cache_miss,
    )

    record_cache_hit("default")
    record_cache_hit("default")
    record_cache_miss("default")

    out = metrics_response()
    assert 'dorm_cache_hits_total{alias="default"} 2' in out
    assert 'dorm_cache_misses_total{alias="default"} 1' in out


def test_uninstall_resets_state():
    from dorm.contrib.prometheus import (
        install,
        metrics_response,
        record_cache_hit,
        uninstall,
    )

    install()
    record_cache_hit("default")
    uninstall()

    out = metrics_response()
    assert "dorm_cache_hits_total" not in out
    assert "dorm_queries_total" not in out


def test_label_escaping_handles_special_chars():
    from dorm.contrib.prometheus import _label_pairs

    rendered = _label_pairs(alias='a"b\\c\nd')
    # Quotes, backslashes and newlines must be escaped per the text
    # exposition spec — otherwise downstream scrapers reject the
    # whole sample.
    assert '\\"' in rendered
    assert "\\\\" in rendered
    assert "\\n" in rendered
