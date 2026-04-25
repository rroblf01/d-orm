"""Runtime sanity checks for the descriptor changes that primarily improve
*static* typing:

  - ``Field[T]`` generic on every concrete subclass — type checkers see
    ``user.name`` as ``str`` (etc.) instead of ``Any``.
  - ``_ForeignKeyIdDescriptor`` — exposes ``obj.author_id`` as ``int | None``
    on the type level and writes through to the same dict slot the FK
    descriptor reads.

These tests don't probe the type checker (that's covered by ty in CI) but
guard the *runtime* contract so a refactor that breaks the descriptor
protocol surfaces here too.
"""

from __future__ import annotations

import dorm
from tests.models import Author, Publisher


def test_field_attribute_returns_python_value_at_runtime():
    """``user.name`` must yield the stored str, not the descriptor object."""
    a = Author(name="Sam", age=42, email="s@x.com")
    assert isinstance(a.name, str)
    assert isinstance(a.age, int)
    assert a.name == "Sam"
    assert a.age == 42


def test_field_meta_access_returns_field_instance():
    """The Field instance lives on ``_meta``; the metaclass deletes the
    class-body attribute so plain instance dict access works directly."""
    name_field = Author._meta.get_field("name")
    assert isinstance(name_field, dorm.fields.Field)
    assert name_field.attname == "name"


def test_fk_descriptor_returns_related_instance():
    p = Publisher.objects.create(name="P-fk")
    a = Author.objects.create(name="FKa", age=1, email="fka@x.com", publisher=p)
    try:
        # Force a fresh fetch (no cache).
        a2 = Author.objects.get(pk=a.pk)
        related = a2.publisher
        assert related is not None
        assert related.pk == p.pk
        assert related.name == "P-fk"
    finally:
        a.delete()
        p.delete()


def test_fk_id_descriptor_reads_pk_value():
    """``obj.author_id`` (the FK column) returns the int PK, not the model."""
    p = Publisher.objects.create(name="P-id")
    a = Author.objects.create(name="FKid", age=1, email="fid@x.com", publisher=p)
    try:
        fetched = Author.objects.get(pk=a.pk)
        # Direct read of the FK column slot.
        pid = fetched.publisher_id
        assert isinstance(pid, int)
        assert pid == p.pk
    finally:
        a.delete()
        p.delete()


def test_fk_id_descriptor_writes_through_to_same_slot():
    """Writing to ``obj.author_id`` updates the dict slot the FK descriptor
    reads, and clears its cached related-instance lookup."""
    p1 = Publisher.objects.create(name="P1")
    p2 = Publisher.objects.create(name="P2")
    a = Author.objects.create(name="FKswitch", age=1, email="sw@x.com", publisher=p1)
    try:
        # Trigger the FK descriptor's cache once.
        _ = a.publisher
        cached = a.__dict__.get("_cache_publisher")
        assert cached is not None
        assert cached.pk == p1.pk

        # Now flip the FK via the typed _id descriptor.
        a.publisher_id = p2.pk
        # Cache invalidated → next read fetches afresh.
        assert "_cache_publisher" not in a.__dict__
        assert a.publisher_id == p2.pk
        a.save()

        refetched = Author.objects.get(pk=a.pk)
        assert refetched.publisher_id == p2.pk
        assert refetched.publisher.pk == p2.pk
    finally:
        a.delete()
        p1.delete()
        p2.delete()


def test_fk_id_descriptor_accepts_none():
    p = Publisher.objects.create(name="P-none")
    a = Author.objects.create(name="N", age=1, email="n@x.com", publisher=p)
    try:
        a.publisher_id = None
        assert a.publisher_id is None
        a.save()
        refetched = Author.objects.get(pk=a.pk)
        assert refetched.publisher_id is None
    finally:
        a.delete()
        p.delete()
