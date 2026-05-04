"""Coverage for v3.3.0 Django-ORM parity additions.

- :meth:`QuerySet.values_list(named=True)` returns namedtuples.
- :func:`prefetch_related_objects` retrofits prefetch on a list of
  already-loaded instances.
- :class:`FilteredRelation` annotates a JOIN with a ``Q`` condition
  baked into the ``ON`` clause.
- ``dorm makemigrations --check`` exits non-zero when a diff is
  pending.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from dorm import Q, prefetch_related_objects
from dorm.expressions import FilteredRelation
from tests.models import Author, Book, Publisher


# ─────────────────────────────────────────────────────────────────────────────
# values_list(named=True)
# ─────────────────────────────────────────────────────────────────────────────


def test_values_list_named_returns_namedtuples():
    Author.objects.create(name="alpha", age=20, email="a@x.com")
    Author.objects.create(name="beta", age=30, email="b@x.com")
    rows = list(Author.objects.values_list("name", "age", named=True))
    assert all(hasattr(r, "name") and hasattr(r, "age") for r in rows)
    by_name = {r.name: r.age for r in rows}
    assert by_name == {"alpha": 20, "beta": 30}


def test_values_list_named_class_is_named_row():
    Author.objects.create(name="x", age=1, email="x@x.com")
    rows = list(Author.objects.values_list("name", named=True))
    assert type(rows[0]).__name__ == "Row"


def test_values_list_named_rejects_combination_with_flat():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Author.objects.values_list("name", flat=True, named=True)


@pytest.mark.asyncio
async def test_avalues_list_named_returns_namedtuples():
    Author.objects.create(name="async-a", age=11, email="a@y.com")
    rows = await Author.objects.avalues_list("name", "age", named=True)
    assert all(hasattr(r, "name") for r in rows)
    assert any(r.name == "async-a" and r.age == 11 for r in rows)


# ─────────────────────────────────────────────────────────────────────────────
# prefetch_related_objects()
# ─────────────────────────────────────────────────────────────────────────────


def test_prefetch_related_objects_retrofits_fk():
    pub = Publisher.objects.create(name="OReilly")
    a1 = Author.objects.create(
        name="a1", age=20, email="a1@x.com", publisher=pub
    )
    a2 = Author.objects.create(
        name="a2", age=30, email="a2@x.com", publisher=pub
    )

    # Imitate a hand-built list — no prefetch.
    bare: list[Author] = [
        Author.objects.get(pk=a1.pk),
        Author.objects.get(pk=a2.pk),
    ]
    prefetch_related_objects(bare, "publisher")
    # After the call, accessing .publisher must NOT issue a fresh query.
    # Verify by reading the descriptor's cache slot directly.
    cached_a1 = bare[0].__dict__.get("_cache_publisher")
    cached_a2 = bare[1].__dict__.get("_cache_publisher")
    assert cached_a1 is not None and cached_a1.pk == pub.pk
    assert cached_a2 is not None and cached_a2.pk == pub.pk


def test_prefetch_related_objects_empty_list_is_noop():
    # Must not raise even with nothing to prefetch.
    prefetch_related_objects([], "publisher")


def test_prefetch_related_objects_rejects_mixed_models():
    pub = Publisher.objects.create(name="MixedCo")
    a = Author.objects.create(name="a", age=1, email="a@x.com")
    with pytest.raises(ValueError, match="same model class"):
        prefetch_related_objects([pub, a], "publisher")


# ─────────────────────────────────────────────────────────────────────────────
# FilteredRelation
# ─────────────────────────────────────────────────────────────────────────────


def test_filtered_relation_validates_relation_name():
    with pytest.raises(ValueError, match="non-empty string"):
        FilteredRelation("", condition=Q(x=1))


def test_filtered_relation_validates_condition_is_q():
    with pytest.raises(TypeError, match="must be a Q"):
        FilteredRelation("books", condition="not a q")  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]


def test_filtered_relation_repr_round_trip():
    fr = FilteredRelation("books", condition=Q(published=True))
    assert "FilteredRelation" in repr(fr)
    assert "books" in repr(fr)


def test_filtered_relation_emits_left_join_with_condition():
    """Forward FK: filter authors whose 'official' publisher (with
    name='OfficialPub') has a specific name. The FilteredRelation
    re-uses the same publisher table but anchors filtering through
    the FR alias."""
    pub_official = Publisher.objects.create(name="OfficialPub")
    pub_other = Publisher.objects.create(name="OtherPub")

    a1 = Author.objects.create(
        name="off-author", age=22, email="off@x.com", publisher=pub_official
    )
    a2 = Author.objects.create(
        name="other-author", age=22, email="ot@x.com", publisher=pub_other
    )

    qs = (
        Author.objects
        .annotate(
            official=FilteredRelation(
                "publisher", condition=Q(name="OfficialPub")
            )
        )
        .filter(official__name="OfficialPub")
    )
    pks = sorted(o.pk for o in qs)
    assert pks == [a1.pk]
    assert a2.pk not in pks


def test_filtered_relation_reverse_fk_join():
    """Reverse FK: load only published books per author via FR."""
    a = Author.objects.create(name="auth-fr", age=33, email="x@x.com")
    Book.objects.create(title="published-1", author=a, published=True)
    Book.objects.create(title="published-2", author=a, published=True)
    Book.objects.create(title="draft-1", author=a, published=False)

    # Filter authors that have AT LEAST ONE published book via FR.
    qs = (
        Author.objects
        .annotate(
            pub_books=FilteredRelation(
                "book_set", condition=Q(published=True)
            )
        )
        .filter(pub_books__title="published-1")
    )
    # ``pk`` alias must resolve to the real PK column even with the
    # FilteredRelation JOIN in flight — regression: previously this
    # emitted bare ``"pk"`` which PG rejected as ``column "pk" does
    # not exist`` once any JOIN qualified the SELECT list.
    pks = list(qs.values_list("pk", flat=True))
    assert a.pk in pks


def test_filtered_relation_empty_condition_acts_as_left_join():
    """``condition=Q()`` with no kwargs is the unconditional tautology
    (matches Django). The FR alias becomes a plain LEFT JOIN that
    always-matches — used as a "give me every related row" alias."""
    a1 = Author.objects.create(name="empty-cond-1", age=20, email="ec1@x.com")
    a2 = Author.objects.create(name="empty-cond-2", age=21, email="ec2@x.com")
    Book.objects.create(title="ec-book", author=a1, published=False)

    qs = (
        Author.objects
        .filter(name__startswith="empty-cond")
        .annotate(any_book=FilteredRelation("book_set", condition=Q()))
        .filter(any_book__title="ec-book")
    )
    pks = sorted(o.pk for o in qs)
    assert pks == [a1.pk]
    assert a2.pk not in pks


def test_filtered_relation_works_with_order_by_through_alias():
    """Order through the FR alias — the JOIN keeps its rows ordered
    by a column on the joined model."""
    a1 = Author.objects.create(name="ord-a", age=99, email="o1@x.com")
    a2 = Author.objects.create(name="ord-b", age=99, email="o2@x.com")
    a3 = Author.objects.create(name="ord-c", age=99, email="o3@x.com")
    Book.objects.create(title="zeta", author=a1, published=True)
    Book.objects.create(title="alpha", author=a2, published=True)
    Book.objects.create(title="mu", author=a3, published=True)

    qs = (
        Author.objects
        .filter(age=99)
        .annotate(pub=FilteredRelation("book_set", condition=Q(published=True)))
        .filter(pub__title__in=["alpha", "mu", "zeta"])
        .order_by("pub__title")
    )
    titles = [o.name for o in qs]
    # The JOIN brings each author's published-book title alongside;
    # ordering by ``pub__title`` should sort alpha → mu → zeta.
    assert titles == ["ord-b", "ord-c", "ord-a"]


def test_filtered_relation_two_aliases_same_relation():
    """Two FRs over the same relation with different conditions —
    each gets its own JOIN alias."""
    a = Author.objects.create(name="dual-fr", age=50, email="d@x.com")
    Book.objects.create(title="published", author=a, published=True)
    Book.objects.create(title="draft", author=a, published=False)

    qs = (
        Author.objects
        .filter(pk=a.pk)
        .annotate(
            pub=FilteredRelation("book_set", condition=Q(published=True)),
            drafts=FilteredRelation("book_set", condition=Q(published=False)),
        )
        .filter(pub__title="published", drafts__title="draft")
    )
    pks = list(qs.values_list("pk", flat=True))
    assert pks == [a.pk]


def test_filtered_relation_idempotent_on_repeat_compile():
    """Compile the same FR queryset twice — params must not duplicate
    on the second :meth:`as_select` call (the per-compile reset
    in ``as_select`` prevents the build-up)."""
    Author.objects.create(name="idem", age=10, email="i@x.com")
    qs = (
        Author.objects
        .annotate(here=FilteredRelation("publisher", condition=Q(name="X")))
        .filter(here__name="X")
    )
    sql1, params1 = qs._query.as_select(None)
    sql2, params2 = qs._query.as_select(None)
    assert sql1 == sql2
    assert params1 == params2 == ["X", "X"]


# ─────────────────────────────────────────────────────────────────────────────
# dorm makemigrations --check
# ─────────────────────────────────────────────────────────────────────────────


def test_makemigrations_check_exits_zero_when_no_changes(tmp_path: Path, capsys, monkeypatch):
    from dorm import cli
    from dorm.conf import settings as dorm_settings

    saved_apps = list(dorm_settings.INSTALLED_APPS)
    saved_databases = dict(dorm_settings.DATABASES)
    saved_configured = dorm_settings._configured
    saved_path = list(sys.path)
    saved_mods = set(sys.modules)
    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    try:
        (tmp_path / "settings.py").write_text(
            f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": {str(tmp_path / "db.sqlite3")!r}}}}}\n'
            'INSTALLED_APPS = ["shop"]\n'
        )
        app = tmp_path / "shop"
        app.mkdir()
        (app / "__init__.py").touch()
        (app / "models.py").write_text(
            "import dorm\n"
            "class Product(dorm.Model):\n"
            "    name = dorm.CharField(max_length=80)\n"
        )

        # First makemigrations writes 0001_initial.py.
        cli.cmd_makemigrations(
            argparse.Namespace(
                apps=["shop"],
                empty=False,
                name=None,
                settings="settings",
                merge=False,
                enable_pgvector=False,
                check=False,
            )
        )
        capsys.readouterr()

        # Second run with --check sees no diff → returns without sys.exit.
        cli.cmd_makemigrations(
            argparse.Namespace(
                apps=["shop"],
                empty=False,
                name=None,
                settings="settings",
                merge=False,
                enable_pgvector=False,
                check=True,
            )
        )
        out = capsys.readouterr().out
        assert "No changes detected" in out
    finally:
        sys.path[:] = saved_path
        for mod in list(sys.modules):
            if mod in saved_mods:
                continue
            file = getattr(sys.modules.get(mod), "__file__", None)
            if file and str(tmp_path) in str(file):
                del sys.modules[mod]
            elif mod in {"settings", "shop", "shop.models", "shop.migrations"}:
                del sys.modules[mod]
        dorm_settings.INSTALLED_APPS = saved_apps
        dorm_settings.DATABASES = saved_databases
        dorm_settings._configured = saved_configured


def test_makemigrations_check_exits_one_when_pending(tmp_path: Path, capsys, monkeypatch):
    from dorm import cli
    from dorm.conf import settings as dorm_settings

    saved_apps = list(dorm_settings.INSTALLED_APPS)
    saved_databases = dict(dorm_settings.DATABASES)
    saved_configured = dorm_settings._configured
    saved_path = list(sys.path)
    saved_mods = set(sys.modules)
    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    try:
        (tmp_path / "settings.py").write_text(
            f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": {str(tmp_path / "db.sqlite3")!r}}}}}\n'
            'INSTALLED_APPS = ["shop"]\n'
        )
        app = tmp_path / "shop"
        app.mkdir()
        (app / "__init__.py").touch()
        (app / "models.py").write_text(
            "import dorm\n"
            "class Product(dorm.Model):\n"
            "    name = dorm.CharField(max_length=80)\n"
        )

        # No previous migration on disk → diff is pending.
        with pytest.raises(SystemExit) as exc_info:
            cli.cmd_makemigrations(
                argparse.Namespace(
                    apps=["shop"],
                    empty=False,
                    name=None,
                    settings="settings",
                    merge=False,
                    enable_pgvector=False,
                    check=True,
                )
            )
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Pending changes" in out

        # No file got written under --check.
        assert not list((tmp_path / "shop" / "migrations").glob("*_*.py"))
    finally:
        sys.path[:] = saved_path
        for mod in list(sys.modules):
            if mod in saved_mods:
                continue
            file = getattr(sys.modules.get(mod), "__file__", None)
            if file and str(tmp_path) in str(file):
                del sys.modules[mod]
            elif mod in {"settings", "shop", "shop.models", "shop.migrations"}:
                del sys.modules[mod]
        dorm_settings.INSTALLED_APPS = saved_apps
        dorm_settings.DATABASES = saved_databases
        dorm_settings._configured = saved_configured
