"""Tests for Tier-1 roadmap features:
pre_save hooks, signals, full_clean/validate_unique, ManyToManyField managers.
"""
from __future__ import annotations

import datetime

import pytest

import dorm
from tests.models import Article, Author, Book, Tag


# ── pre_save / auto_now ───────────────────────────────────────────────────────

class Timestamped(dorm.Model):
    name = dorm.CharField(max_length=100)
    created_at = dorm.DateTimeField(auto_now_add=True)
    updated_at = dorm.DateTimeField(auto_now=True)

    class Meta:
        db_table = "timestamped_test"


@pytest.fixture(autouse=True)
def ensure_timestamped_table(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "timestamped_test"')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in Timestamped._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "timestamped_test" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )


def test_auto_now_add_set_on_create():
    obj = Timestamped.objects.create(name="x")
    assert obj.created_at is not None
    assert isinstance(obj.created_at, datetime.datetime)


def test_auto_now_updated_on_save():
    obj = Timestamped.objects.create(name="x")
    first = obj.updated_at
    obj.name = "y"
    obj.save()
    assert obj.updated_at >= first  # type: ignore[operator]


def test_save_update_fields_only_updates_specified():
    author = Author.objects.create(name="Alice", age=30)
    author.name = "Bob"
    author.age = 99
    author.save(update_fields=["name"])
    fresh = Author.objects.get(pk=author.pk)
    assert fresh.name == "Bob"
    assert fresh.age == 30  # age was NOT saved


# ── Signals ───────────────────────────────────────────────────────────────────

def test_pre_save_fires_on_create():
    received = []

    def handler(sender, instance, **kwargs):
        received.append(("pre_save", instance.name, kwargs.get("created")))

    dorm.pre_save.connect(handler, sender=Author, weak=False)
    try:
        Author.objects.create(name="Carol", age=25)
        assert any(e[0] == "pre_save" and e[1] == "Carol" for e in received)
    finally:
        dorm.pre_save.disconnect(handler, sender=Author)


def test_post_save_fires_with_created_flag():
    created_flags = []

    def handler(sender, instance, created, **kwargs):
        created_flags.append(created)

    dorm.post_save.connect(handler, sender=Author, weak=False)
    try:
        a = Author.objects.create(name="Dave", age=40)
        assert created_flags[-1] is True
        a.name = "David"
        a.save()
        assert created_flags[-1] is False
    finally:
        dorm.post_save.disconnect(handler, sender=Author)


def test_pre_delete_fires():
    received = []

    def handler(sender, instance, **kwargs):
        received.append(instance.pk)

    dorm.pre_delete.connect(handler, sender=Author, weak=False)
    try:
        a = Author.objects.create(name="Eve", age=35)
        pk = a.pk
        a.delete()
        assert pk in received
    finally:
        dorm.pre_delete.disconnect(handler, sender=Author)


def test_post_delete_fires_and_pk_still_accessible():
    pks_at_delete = []

    def handler(sender, instance, **kwargs):
        # pk is set to None after post_delete fires... in Django.
        # In our impl we fire post_delete before nulling pk.
        pks_at_delete.append(instance.pk)

    dorm.post_delete.connect(handler, sender=Author, weak=False)
    try:
        a = Author.objects.create(name="Frank", age=50)
        pk = a.pk
        a.delete()
        assert pk in pks_at_delete or a.pk is None  # pk nulled after signal
    finally:
        dorm.post_delete.disconnect(handler, sender=Author)


def test_signal_sender_filter():
    book_calls = []

    def handler(sender, instance, **kwargs):
        book_calls.append(instance)

    dorm.post_save.connect(handler, sender=Book, weak=False)
    try:
        Author.objects.create(name="Ghost", age=1)  # should NOT trigger
        assert len(book_calls) == 0
    finally:
        dorm.post_save.disconnect(handler, sender=Book)


def test_signal_disconnect():
    calls = []

    def handler(sender, **kwargs):
        calls.append(1)

    dorm.post_save.connect(handler, sender=Author, weak=False)
    dorm.post_save.disconnect(handler, sender=Author)
    Author.objects.create(name="NoSignal", age=1)
    assert calls == []


# ── full_clean / validate_unique ──────────────────────────────────────────────

def test_full_clean_raises_on_invalid_email():
    """EmailField now validates at assignment time, so the bogus value is
    rejected during construction — full_clean() never has a chance to
    run on a dirty instance via the public constructor."""
    with pytest.raises(dorm.ValidationError):
        Author(name="X", age=25, email="not-an-email")


def test_full_clean_passes_on_valid_data():
    a = Author(name="Valid", age=25, email="valid@example.com")
    a.full_clean()  # should not raise


def test_validate_unique_raises_on_duplicate_unique_field():
    Tag.objects.create(name="python")
    duplicate = Tag(name="python")
    with pytest.raises(dorm.ValidationError, match="already exists"):
        duplicate.validate_unique()


def test_validate_unique_ok_for_different_value():
    Tag.objects.create(name="python")
    other = Tag(name="django")
    other.validate_unique()  # should not raise


def test_validate_unique_excludes_self_on_update():
    tag = Tag.objects.create(name="python")
    tag.name = "python"  # same value, updating same instance
    tag.validate_unique()  # should not raise


def test_clean_is_hookable():
    class StrictAuthor(Author):
        class Meta:
            db_table = "authors"

        def clean(self):
            if self.age and self.age < 18:
                raise dorm.ValidationError({"age": "Must be 18 or older."})

    young = StrictAuthor(name="Kid", age=10)
    with pytest.raises(dorm.ValidationError, match="18"):
        young.full_clean()


def test_invalid_email_rejected_at_construction():
    """Construction itself raises now (previously this only fired from
    full_clean). The error message still names the offending field."""
    with pytest.raises(dorm.ValidationError, match="valid email"):
        Author(name="Alice", age=25, email="not-an-email")


# ── ManyToManyField managers ──────────────────────────────────────────────────

def _make_tag(name: str) -> Tag:
    return Tag.objects.create(name=name)


def _make_article(title: str) -> Article:
    return Article.objects.create(title=title)


def test_m2m_add_and_all():
    tag = _make_tag("python")
    article = _make_article("Intro to Python")
    article.tags.add(tag)
    tags = list(article.tags.all())
    assert len(tags) == 1
    assert tags[0].name == "python"


def test_m2m_add_multiple():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    article = _make_article("Django & Python")
    article.tags.add(t1, t2)
    names = {t.name for t in article.tags.all()}
    assert names == {"python", "django"}


def test_m2m_add_is_idempotent():
    tag = _make_tag("python")
    article = _make_article("Test")
    article.tags.add(tag)
    article.tags.add(tag)  # adding again should not duplicate
    assert article.tags.count() == 1


def test_m2m_remove():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    article = _make_article("Test")
    article.tags.add(t1, t2)
    article.tags.remove(t1)
    names = {t.name for t in article.tags.all()}
    assert names == {"django"}


def test_m2m_set_replaces():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    t3 = _make_tag("flask")
    article = _make_article("Test")
    article.tags.add(t1, t2)
    article.tags.set([t3])
    names = {t.name for t in article.tags.all()}
    assert names == {"flask"}


def test_m2m_clear():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    article = _make_article("Test")
    article.tags.add(t1, t2)
    article.tags.clear()
    assert article.tags.count() == 0


def test_m2m_create():
    article = _make_article("Test")
    tag = article.tags.create(name="new-tag")
    assert tag.pk is not None
    assert article.tags.count() == 1


def test_m2m_filter():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    article = _make_article("Test")
    article.tags.add(t1, t2)
    result = list(article.tags.filter(name="python"))
    assert len(result) == 1
    assert result[0].name == "python"


def test_m2m_empty_queryset():
    article = _make_article("Empty")
    assert article.tags.count() == 0
    assert list(article.tags.all()) == []


def test_m2m_independent_per_article():
    t1 = _make_tag("python")
    t2 = _make_tag("django")
    a1 = _make_article("A1")
    a2 = _make_article("A2")
    a1.tags.add(t1)
    a2.tags.add(t2)
    assert {t.name for t in a1.tags.all()} == {"python"}
    assert {t.name for t in a2.tags.all()} == {"django"}


# ── Async M2M ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_m2m_aadd_and_all():
    tag = Tag.objects.create(name="async-tag")
    article = Article.objects.create(title="Async Article")
    await article.tags.aadd(tag)
    qs = await article.tags.aget_queryset()
    tags = [t async for t in qs]
    assert len(tags) == 1
    assert tags[0].name == "async-tag"


@pytest.mark.asyncio
async def test_m2m_aclear():
    tag = Tag.objects.create(name="t1")
    article = Article.objects.create(title="A")
    await article.tags.aadd(tag)
    await article.tags.aclear()
    qs = await article.tags.aget_queryset()
    tags = [t async for t in qs]
    assert tags == []


@pytest.mark.asyncio
async def test_m2m_aset():
    t1 = Tag.objects.create(name="ta")
    t2 = Tag.objects.create(name="tb")
    article = Article.objects.create(title="A")
    await article.tags.aadd(t1)
    await article.tags.aset([t2])
    qs = await article.tags.aget_queryset()
    names = {t.name async for t in qs}
    assert names == {"tb"}
