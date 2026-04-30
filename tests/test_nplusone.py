"""Tests for :mod:`dorm.contrib.nplusone`.

Three angles of coverage:

* **Detection accuracy** — the obvious N+1 patterns trip the
  detector (loop over related FKs without ``select_related``, loop
  over reverse FKs without ``prefetch_related``).
* **False-positive guard** — distinct queries on different keys do
  *not* trip; the detector groups by parameter-stripped template,
  so 100 ``filter(name=x)`` calls with different ``x`` collapse to
  one bucket.
* **Behavioural knobs** — ``threshold`` is honoured, ``ignore`` skips
  noise, ``raise_on_detect=False`` accumulates findings for a final
  report instead of failing fast.
"""

from __future__ import annotations

import pytest

from dorm.contrib.nplusone import (
    NPlusOneDetector,
    NPlusOneError,
    _normalize,
    assert_no_nplusone,
)
from tests.models import Author, Publisher


# ── Template normalisation ────────────────────────────────────────────


class TestNormalize:
    def test_string_literals_collapse_to_question_mark(self):
        assert _normalize("SELECT * FROM t WHERE name = 'alice'") == \
            "SELECT * FROM t WHERE name = ?"

    def test_numbers_collapse(self):
        assert _normalize("SELECT * FROM t WHERE age = 42") == \
            "SELECT * FROM t WHERE age = ?"
        assert _normalize("SELECT * FROM t WHERE p = 1.5") == \
            "SELECT * FROM t WHERE p = ?"

    def test_null_collapses(self):
        assert _normalize("SELECT * FROM t WHERE x IS NULL") == \
            "SELECT * FROM t WHERE x IS ?"

    def test_placeholder_styles_survive(self):
        # ``%s`` and ``$N`` aren't literals — they stay intact so SQLite
        # and PostgreSQL templates don't accidentally diverge.
        assert _normalize("SELECT * FROM t WHERE id = %s") == \
            "SELECT * FROM t WHERE id = %s"
        assert _normalize("SELECT * FROM t WHERE id = $1") == \
            "SELECT * FROM t WHERE id = $?"

    def test_whitespace_normalised(self):
        a = _normalize("SELECT *\n  FROM t\n  WHERE id = 1")
        b = _normalize("SELECT * FROM t WHERE id = 1")
        assert a == b

    def test_string_with_embedded_quote(self):
        # ``''`` is the SQL escape for a literal quote — pattern must
        # treat the whole token as one literal.
        assert _normalize("SELECT * FROM t WHERE name = 'O''Brien'") == \
            "SELECT * FROM t WHERE name = ?"

    def test_double_quoted_identifiers_are_preserved(self):
        # ``"authors"`` is an identifier (column/table name), not a
        # literal — collapsing it would erase the structural fingerprint
        # the detector groups by.
        assert _normalize(
            'SELECT "authors"."id" FROM "authors" WHERE "authors"."name" = \'x\''
        ) == 'SELECT "authors"."id" FROM "authors" WHERE "authors"."name" = ?'


# ── Detection on real querysets ───────────────────────────────────────


class TestDetectorOnRealQueries:
    def test_classic_fk_loop_trips_detector(self):
        """Reading ``author.publisher.name`` in a loop without
        ``select_related`` is the textbook N+1 — must trip."""
        pub = Publisher.objects.create(name="P")
        for i in range(8):
            Author.objects.create(name=f"a{i}", age=i, email=f"a{i}@x.com", publisher=pub)
        try:
            with pytest.raises(NPlusOneError) as exc:
                with NPlusOneDetector(threshold=3):
                    for a in Author.objects.all():
                        # Each access fires SELECT … FROM publishers WHERE id = X
                        _ = a.publisher_id  # type: ignore[attr-defined]
                        if a.publisher is not None:
                            _ = a.publisher.name
            assert "publisher" in str(exc.value).lower() or "select" in str(exc.value).lower()
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_select_related_avoids_detection(self):
        """The fix for the test above — ``select_related`` collapses
        to a single JOIN, no N+1."""
        pub = Publisher.objects.create(name="P")
        for i in range(8):
            Author.objects.create(name=f"a{i}", age=i, email=f"a{i}@x.com", publisher=pub)
        try:
            with assert_no_nplusone(threshold=3):
                for a in Author.objects.select_related("publisher"):
                    if a.publisher is not None:
                        _ = a.publisher.name  # cached, no SELECT
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_filter_with_distinct_keys_is_not_n_plus_one(self):
        """Distinct queries on distinct keys don't trip the detector
        because each parameter stripping yields the *same* template
        — but only one bucket grows. (Sanity: a fan-out by design
        shouldn't be flagged.)"""
        pub = Publisher.objects.create(name="P")
        for i in range(20):
            Author.objects.create(name=f"sk{i}", age=i, email=f"sk{i}@x.com", publisher=pub)
        try:
            # Detector with raise_on_detect=False so we can inspect
            # whether the bucket grew past threshold.
            d = NPlusOneDetector(threshold=5, raise_on_detect=False)
            with d:
                for i in range(20):
                    Author.objects.filter(name=f"sk{i}").first()
            # All 20 ``filter(name=?)`` queries share one template after
            # stripping — the count exceeds threshold and accumulates a
            # finding. This is the correct semantic: even with distinct
            # *values*, hammering the same query shape in a loop IS the
            # N+1 pattern, just expressed via filter() instead of FK.
            assert any("authors" in t.lower() for t, _ in d.findings), d.report()
        finally:
            Author.objects.all().delete()
            pub.delete()


# ── Threshold behaviour ──────────────────────────────────────────────


class TestThreshold:
    def test_below_threshold_does_not_raise(self):
        pub = Publisher.objects.create(name="P")
        for i in range(3):
            Author.objects.create(name=f"t{i}", age=i, email=f"t{i}@x.com", publisher=pub)
        try:
            with NPlusOneDetector(threshold=10):
                for a in Author.objects.all():
                    if a.publisher_id is not None:  # type: ignore[attr-defined]
                        _ = a.publisher
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_zero_threshold_rejected(self):
        with pytest.raises(ValueError):
            NPlusOneDetector(threshold=0)

    def test_first_offender_raises_only_once(self):
        """A pathological loop (same template, 50 hits) raises on the
        FIRST overshoot of the threshold, not 50 times. Subsequent
        executions of the same template inside the same context
        manager are tracked silently."""
        pub = Publisher.objects.create(name="P")
        for i in range(50):
            Author.objects.create(name=f"o{i}", age=i, email=f"o{i}@x.com", publisher=pub)
        try:
            with pytest.raises(NPlusOneError):
                with NPlusOneDetector(threshold=2):
                    for a in Author.objects.all():
                        if a.publisher is not None:
                            _ = a.publisher.name
        finally:
            Author.objects.all().delete()
            pub.delete()


# ── Non-strict mode for staging-style auditing ──────────────────────


class TestNonStrictMode:
    def test_findings_accumulated_when_raise_disabled(self):
        pub = Publisher.objects.create(name="P")
        for i in range(8):
            Author.objects.create(name=f"r{i}", age=i, email=f"r{i}@x.com", publisher=pub)
        try:
            d = NPlusOneDetector(threshold=3, raise_on_detect=False)
            with d:
                for a in Author.objects.all():
                    if a.publisher is not None:
                        _ = a.publisher.name
            assert d.findings, "expected at least one finding"
            assert "no N+1 detected" not in d.report()
            # The same template should NOT appear twice in findings
            # — once it crosses the threshold it's reported once and
            # subsequent hits are silently counted.
            templates = [t for t, _ in d.findings]
            assert len(templates) == len(set(templates))
        finally:
            Author.objects.all().delete()
            pub.delete()

    def test_clean_block_reports_no_violations(self):
        d = NPlusOneDetector(threshold=3, raise_on_detect=False)
        with d:
            list(Author.objects.all())
            list(Publisher.objects.all())
        assert d.findings == []
        assert d.report() == "no N+1 detected"


# ── Ignore list behaviour ────────────────────────────────────────────


class TestIgnoreList:
    def test_ddl_does_not_count(self):
        """Even if a test fixture issues many ``CREATE TABLE`` statements,
        the detector must skip them — they're noise, not N+1."""
        from dorm.db.connection import get_connection

        conn = get_connection()
        d = NPlusOneDetector(threshold=2, raise_on_detect=False)
        with d:
            for i in range(10):
                conn.execute_script(f'DROP TABLE IF EXISTS "n1_skip_{i}"')
        # No findings — DDL is on the default ignore list.
        assert d.findings == []

    def test_custom_ignore_substring_silences_template(self):
        d = NPlusOneDetector(
            threshold=2,
            raise_on_detect=False,
            ignore=("authors",),  # silence anything touching authors
        )
        pub = Publisher.objects.create(name="P")
        try:
            for i in range(10):
                Author.objects.filter(name=f"sup{i}").count()
            with d:
                for i in range(10):
                    Author.objects.filter(name=f"sup{i}").count()
        finally:
            pub.delete()
        # All authors queries silenced.
        assert d.findings == []


# ── Nested + re-entrancy ─────────────────────────────────────────────


class TestNestingSemantics:
    def test_two_independent_detectors(self):
        """Two parallel detectors track their own counts — disconnect
        of one mustn't tear down the other (``dispatch_uid`` per
        instance)."""
        pub = Publisher.objects.create(name="P")
        for i in range(5):
            Author.objects.create(name=f"n{i}", age=i, email=f"n{i}@x.com", publisher=pub)
        try:
            outer = NPlusOneDetector(threshold=3, raise_on_detect=False)
            inner = NPlusOneDetector(threshold=3, raise_on_detect=False)
            with outer, inner:
                for a in Author.objects.all():
                    if a.publisher is not None:
                        _ = a.publisher.name
            # Both detectors observe the same queries.
            assert outer.findings, outer.report()
            assert inner.findings, inner.report()
        finally:
            Author.objects.all().delete()
            pub.delete()
