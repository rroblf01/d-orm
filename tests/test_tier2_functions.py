"""Tests for Tier-2.1: Case/When expressions and DB functions."""
from __future__ import annotations

import dorm
from tests.models import Author


def _ann(obj: object, attr: str):
    """Read a dynamically-injected annotation attribute."""
    return getattr(obj, attr)


# ── Coalesce ─────────────────────────────────────────────────────────────────

def test_coalesce_returns_first_non_null():
    Author.objects.create(name="Alice", age=30, email=None)
    Author.objects.create(name="Bob", age=25, email="bob@example.com")

    results = list(
        Author.objects.annotate(
            contact=dorm.Coalesce(dorm.F("email"), dorm.Value("no-email"))
        ).order_by("name")
    )
    alice = next(r for r in results if r.name == "Alice")
    bob = next(r for r in results if r.name == "Bob")
    assert _ann(alice, "contact") == "no-email"
    assert _ann(bob, "contact") == "bob@example.com"


# ── Upper / Lower ─────────────────────────────────────────────────────────────

def test_upper_annotation():
    Author.objects.create(name="alice", age=20)
    result = Author.objects.filter(name="alice").annotate(
        upper_name=dorm.Upper(dorm.F("name"))
    ).first()
    assert result is not None
    assert _ann(result, "upper_name") == "ALICE"


def test_lower_annotation():
    Author.objects.create(name="ALICE", age=20)
    result = Author.objects.filter(name="ALICE").annotate(
        lower_name=dorm.Lower(dorm.F("name"))
    ).first()
    assert result is not None
    assert _ann(result, "lower_name") == "alice"


# ── Length ────────────────────────────────────────────────────────────────────

def test_length_annotation():
    Author.objects.create(name="Bob", age=30)
    result = Author.objects.filter(name="Bob").annotate(
        name_len=dorm.Length(dorm.F("name"))
    ).first()
    assert result is not None
    assert _ann(result, "name_len") == 3


# ── Concat ────────────────────────────────────────────────────────────────────

def test_concat_annotation():
    Author.objects.create(name="Alice", age=30)
    result = Author.objects.filter(name="Alice").annotate(
        greeting=dorm.Concat(dorm.Value("Hello, "), dorm.F("name"))
    ).first()
    assert result is not None
    assert _ann(result, "greeting") == "Hello, Alice"


# ── Now ───────────────────────────────────────────────────────────────────────

def test_now_annotation_returns_value():
    Author.objects.create(name="Alice", age=30)
    result = Author.objects.filter(name="Alice").annotate(ts=dorm.Now()).first()
    assert result is not None
    assert _ann(result, "ts") is not None


# ── Cast ─────────────────────────────────────────────────────────────────────

def test_cast_integer_to_text():
    Author.objects.create(name="Alice", age=42)
    result = Author.objects.filter(name="Alice").annotate(
        age_str=dorm.Cast(dorm.F("age"), output_field="TEXT")
    ).first()
    assert result is not None
    assert str(_ann(result, "age_str")) == "42"


# ── Abs ───────────────────────────────────────────────────────────────────────

def test_abs_annotation():
    Author.objects.create(name="Alice", age=30)
    result = Author.objects.filter(name="Alice").annotate(
        pos_age=dorm.Abs(dorm.F("age"))
    ).first()
    assert result is not None
    assert _ann(result, "pos_age") == 30


# ── Case / When ───────────────────────────────────────────────────────────────

def test_case_when_basic():
    Author.objects.create(name="Teen", age=16)
    Author.objects.create(name="Adult", age=25)

    results = {
        r.name: _ann(r, "category")
        for r in Author.objects.annotate(
            category=dorm.Case(
                dorm.When(age__lt=18, then=dorm.Value("minor")),
                default=dorm.Value("adult"),
            )
        )
    }
    assert results["Teen"] == "minor"
    assert results["Adult"] == "adult"


def test_case_when_multiple_branches():
    Author.objects.create(name="A", age=10)
    Author.objects.create(name="B", age=20)
    Author.objects.create(name="C", age=60)

    results = {
        r.name: _ann(r, "group")
        for r in Author.objects.annotate(
            group=dorm.Case(
                dorm.When(age__lt=18, then=dorm.Value("child")),
                dorm.When(age__lt=50, then=dorm.Value("adult")),
                default=dorm.Value("senior"),
            )
        )
    }
    assert results["A"] == "child"
    assert results["B"] == "adult"
    assert results["C"] == "senior"


def test_case_when_with_q_object():
    Author.objects.create(name="Active", age=30, is_active=True)
    Author.objects.create(name="Inactive", age=30, is_active=False)

    results = {
        r.name: _ann(r, "status")
        for r in Author.objects.annotate(
            status=dorm.Case(
                dorm.When(dorm.Q(is_active=True), then=dorm.Value("active")),
                default=dorm.Value("inactive"),
            )
        )
    }
    assert results["Active"] == "active"
    assert results["Inactive"] == "inactive"


def test_case_when_no_default_returns_null():
    Author.objects.create(name="NoMatch", age=50)

    result = Author.objects.filter(name="NoMatch").annotate(
        label=dorm.Case(
            dorm.When(age__lt=18, then=dorm.Value("minor")),
        )
    ).first()
    assert result is not None
    assert _ann(result, "label") is None


# ── Combined: Case + Coalesce ─────────────────────────────────────────────────

def test_coalesce_with_case():
    Author.objects.create(name="Alice", age=15, email=None)

    result = Author.objects.filter(name="Alice").annotate(
        label=dorm.Coalesce(
            dorm.Case(
                dorm.When(age__lt=18, then=dorm.Value("minor")),
            ),
            dorm.Value("unknown"),
        )
    ).first()
    assert result is not None
    assert _ann(result, "label") == "minor"
