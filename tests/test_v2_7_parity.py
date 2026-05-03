"""Tests pinning Django parity items verified in v2.7.

- ``Manager.in_bulk`` (already present — round-trip)
- ``Manager.from_queryset`` (already present — smoke test)
- ``Prefetch(to_attr=...)`` (already present — round-trip via Book.author)
- ``Meta.managed = False`` skips migration emission (NEW)
"""

from __future__ import annotations


import dorm


# ──────────────────────────────────────────────────────────────────────────────
# Manager.in_bulk
# ──────────────────────────────────────────────────────────────────────────────


def test_in_bulk_returns_dict_keyed_by_pk():
    from tests.models import Author

    a = Author.objects.create(name="A", age=10)
    b = Author.objects.create(name="B", age=20)
    out = Author.objects.in_bulk([a.pk, b.pk])
    assert set(out.keys()) == {a.pk, b.pk}
    assert out[a.pk].name == "A"


# ──────────────────────────────────────────────────────────────────────────────
# Manager.from_queryset
# ──────────────────────────────────────────────────────────────────────────────


def test_from_queryset_attaches_custom_methods():
    """Custom QuerySet methods must surface on the Manager built by
    ``from_queryset``."""
    from dorm import Manager, QuerySet

    class _AuthorQuerySet(QuerySet):
        def adults(self):
            return self.filter(age__gte=18)

    NewManager = Manager.from_queryset(_AuthorQuerySet)
    assert callable(getattr(NewManager, "adults", None))


# ──────────────────────────────────────────────────────────────────────────────
# Prefetch(to_attr=...) — round-trip via Book.author reverse relation
# ──────────────────────────────────────────────────────────────────────────────


def test_prefetch_to_attr_lands_on_named_attribute():
    from dorm import Prefetch
    from tests.models import Author, Book

    a1 = Author.objects.create(name="A", age=30)
    Book.objects.create(title="T1", author=a1, pages=10)
    Book.objects.create(title="T2", author=a1, pages=20)

    qs = Author.objects.prefetch_related(
        Prefetch("book_set", to_attr="cached_books")
    )
    fetched = list(qs.filter(pk=a1.pk))[0]
    # ``to_attr`` lands the prefetched list on the named attribute,
    # leaving the original related descriptor untouched.
    assert hasattr(fetched, "cached_books")
    assert {b.title for b in fetched.cached_books} == {"T1", "T2"}  # ty:ignore[not-iterable]


# ──────────────────────────────────────────────────────────────────────────────
# Meta.managed = False — autodetector skips it
# ──────────────────────────────────────────────────────────────────────────────


def test_meta_managed_false_is_skipped_by_state():
    """A model with ``managed = False`` must not appear in the
    project state used to generate migrations — the user is
    declaring 'this table lives outside dorm's purview'."""
    from dorm.migrations.state import ProjectState

    class _ExternalTable(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "tests_managed_audit"
            db_table = "_external_legacy"
            managed = False

    state = ProjectState.from_apps(app_label="tests_managed_audit")
    assert "tests_managed_audit._externaltable" not in state.models, (
        "managed=False model leaked into migration state — "
        "makemigrations would emit a CreateModel for an externally "
        "managed table."
    )


def test_meta_managed_default_true_appears_in_state():
    """Sanity: ``managed=True`` (the default) DOES land in the state."""
    from dorm.migrations.state import ProjectState

    class _ManagedTable(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "tests_managed_default"
            db_table = "_managed_dummy"

    state = ProjectState.from_apps(app_label="tests_managed_default")
    assert "tests_managed_default._managedtable" in state.models


# ``QuerySet.alias`` exists and is exercised broadly elsewhere in
# the suite (see ``test_django_parity_features.py``). v2.7 didn't
# touch its semantics — no extra coverage added here.
