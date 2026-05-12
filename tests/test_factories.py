"""Tests for ``dorm.factories``."""
from __future__ import annotations

import pytest

from dorm.factories import Factory, LazyFunction, Sequence, SubFactory
from tests.models import Author, Publisher


class AuthorFactory(Factory):
    class Meta:
        model = Author

    name = Sequence(lambda n: f"author{n}")
    age = LazyFunction(lambda: 30)


class PublisherFactory(Factory):
    class Meta:
        model = Publisher

    name = Sequence(lambda n: f"pub{n}")


class TestBuild:
    def test_build_returns_unsaved_instance(self):
        AuthorFactory.reset_sequence()
        a = AuthorFactory.build()
        assert isinstance(a, Author)
        assert a.pk is None
        assert a.name == "author1"
        assert a.age == 30

    def test_overrides_win(self):
        AuthorFactory.reset_sequence()
        a = AuthorFactory.build(name="custom", age=99)
        assert a.name == "custom"
        assert a.age == 99


class TestCreate:
    def test_create_persists(self):
        AuthorFactory.reset_sequence()
        a = AuthorFactory.create()
        assert a.pk is not None
        # Round-trip via the manager.
        assert Author.objects.get(pk=a.pk).name == "author1"

    def test_create_with_overrides(self):
        AuthorFactory.reset_sequence()
        a = AuthorFactory.create(name="Alice", age=42)
        a.refresh_from_db()
        assert a.name == "Alice"
        assert a.age == 42


class TestSequenceIsolation:
    def test_two_factories_get_independent_counters(self):
        AuthorFactory.reset_sequence()
        PublisherFactory.reset_sequence()
        a = AuthorFactory.build()
        p = PublisherFactory.build()
        assert a.name == "author1"
        assert p.name == "pub1"

    def test_sequence_advances_each_call(self):
        AuthorFactory.reset_sequence()
        names = [AuthorFactory.build().name for _ in range(3)]
        assert names == ["author1", "author2", "author3"]


class TestBatches:
    def test_build_batch(self):
        AuthorFactory.reset_sequence()
        batch = AuthorFactory.build_batch(4)
        assert len(batch) == 4
        assert [a.name for a in batch] == [f"author{i}" for i in (1, 2, 3, 4)]

    def test_create_batch_persists(self):
        AuthorFactory.reset_sequence()
        batch = AuthorFactory.create_batch(3)
        assert len(batch) == 3
        assert Author.objects.filter(name__startswith="author").count() == 3

    def test_batch_size_negative_rejected(self):
        with pytest.raises(ValueError):
            AuthorFactory.build_batch(-1)
        with pytest.raises(ValueError):
            AuthorFactory.create_batch(-2)


class TestSubFactory:
    def test_subfactory_creates_related_row(self):
        PublisherFactory.reset_sequence()

        class _A(Factory):
            class Meta:
                model = Author

            name = Sequence(lambda n: f"sub-author{n}")
            age = 21
            publisher = SubFactory(PublisherFactory)

        _A.reset_sequence()
        a = _A.create()
        assert a.publisher is not None
        assert a.publisher.pk is not None
        # The sub-row is in the DB.
        assert Publisher.objects.filter(pk=a.publisher.pk).exists()

    def test_subfactory_build_strategy_skips_save(self):
        class _A(Factory):
            class Meta:
                model = Author

            name = "Bob"
            age = 21
            publisher = SubFactory(PublisherFactory, strategy="build")

        a = _A.build()
        assert a.publisher.pk is None  # not persisted

    def test_invalid_strategy_rejected(self):
        with pytest.raises(ValueError, match="strategy must be"):
            SubFactory(PublisherFactory, strategy="bogus")


class TestMisuse:
    def test_missing_meta_model_raises(self):
        class _Broken(Factory):
            pass

        with pytest.raises(RuntimeError, match="no Meta.model"):
            _Broken.build()


class TestPlainCallableNotPickedAsField:
    """Methods on a factory subclass must not be mistaken for field
    declarations — they're ordinary helpers."""

    def test_method_not_resolved(self):
        class _A(Factory):
            class Meta:
                model = Author

            name = "x"
            age = 1

            def helper(self):
                return "x"

        a = _A.build()
        assert isinstance(a, Author)
        assert a.name == "x"


# Smoke: tested factory + a temporal/audit-style flow should compose.

def test_dorm_imports_factory_exports():
    import dorm.factories as mod

    assert hasattr(mod, "Factory")
    assert hasattr(mod, "Sequence")
    assert hasattr(mod, "LazyFunction")
    assert hasattr(mod, "SubFactory")
