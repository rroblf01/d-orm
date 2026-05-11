"""Tests for the OTel log-correlation filter."""
from __future__ import annotations

import logging

import pytest


otel = pytest.importorskip("opentelemetry")
from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)

from dorm.contrib.otel import TraceContextLogFilter, install_log_correlation  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_tracer():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    yield
    exporter.shutdown()


class TestTraceContextLogFilter:
    def _make_record(self) -> logging.LogRecord:
        return logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )

    def test_placeholders_without_active_span(self):
        record = self._make_record()
        TraceContextLogFilter().filter(record)
        assert getattr(record, "otel_trace_id") == "-"
        assert getattr(record, "otel_span_id") == "-"

    def test_attaches_active_trace_ids(self):
        tracer = trace.get_tracer("test")
        record = self._make_record()
        with tracer.start_as_current_span("span-1"):
            TraceContextLogFilter().filter(record)
        trace_id = getattr(record, "otel_trace_id")
        span_id = getattr(record, "otel_span_id")
        assert trace_id != "-"
        assert len(trace_id) == 32
        assert span_id != "-"
        assert len(span_id) == 16

    def test_filter_returns_true_always(self):
        record = self._make_record()
        assert TraceContextLogFilter().filter(record) is True


class TestInstallLogCorrelation:
    def test_attaches_to_root_logger(self):
        root = logging.getLogger()
        before = len(root.filters)
        install_log_correlation()
        after = len(root.filters)
        assert after == before + 1
        # Cleanup so subsequent tests aren't polluted.
        for f in list(root.filters):
            if isinstance(f, TraceContextLogFilter):
                root.removeFilter(f)

    def test_idempotent(self):
        install_log_correlation()
        before = sum(
            isinstance(f, TraceContextLogFilter)
            for f in logging.getLogger().filters
        )
        install_log_correlation()
        install_log_correlation()
        after = sum(
            isinstance(f, TraceContextLogFilter)
            for f in logging.getLogger().filters
        )
        assert after == before
        for f in list(logging.getLogger().filters):
            if isinstance(f, TraceContextLogFilter):
                logging.getLogger().removeFilter(f)

    def test_targets_named_loggers(self):
        install_log_correlation(["dorm.test.logcorr"])
        target = logging.getLogger("dorm.test.logcorr")
        assert any(isinstance(f, TraceContextLogFilter) for f in target.filters)
        for f in list(target.filters):
            if isinstance(f, TraceContextLogFilter):
                target.removeFilter(f)
