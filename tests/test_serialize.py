"""Tests for the JSON fixtures pipeline (dorm.serialize) and the
``dumpdata`` / ``loaddata`` CLI subcommands."""
from __future__ import annotations

import json

import pytest

import dorm  # noqa: F401  — establishes registry side effects for resolve()
from dorm.serialize import deserialize, dumps, load, serialize
from tests.models import Article, Author, Book, Publisher, Tag


@pytest.fixture(autouse=True)
def _pin_test_models_in_registry():
    """Pin ``_model_registry`` entries to ``tests.models`` for the
    duration of each test.

    Other test files (notably ``test_orm.py``) declare model classes
    with the same names — ``Author`` / ``Book`` — under the same
    auto-derived ``app_label="tests"`` (the metaclass uses just the
    first dotted component of ``__module__``). Whichever module gets
    imported last wins the registry slot, which silently breaks
    ``serialize.load`` because ``_resolve_model("tests.Author")``
    returns the wrong class. We restore the canonical mapping here so
    the serialize round-trip tests stay deterministic regardless of
    test ordering.
    """
    from dorm.models import _model_registry

    saved = {
        key: _model_registry.get(key)
        for key in (
            "Author", "Book", "Publisher", "Article", "Tag",
            "tests.Author", "tests.Book", "tests.Publisher",
            "tests.Article", "tests.Tag",
        )
    }
    pinned = {
        "Author": Author, "Book": Book, "Publisher": Publisher,
        "Article": Article, "Tag": Tag,
        "tests.Author": Author, "tests.Book": Book,
        "tests.Publisher": Publisher, "tests.Article": Article,
        "tests.Tag": Tag,
    }
    _model_registry.update(pinned)
    yield
    for key, val in saved.items():
        if val is None:
            _model_registry.pop(key, None)
        else:
            _model_registry[key] = val


# ── serialize / deserialize ───────────────────────────────────────────────────


class TestSerialize:
    def test_serialize_empty_model(self):
        out = serialize([Author])
        assert out == []

    def test_serialize_single_row(self):
        Author.objects.create(name="Alice", age=30, is_active=True)
        out = serialize([Author])
        assert len(out) == 1
        rec = out[0]
        assert rec["model"] == "tests.Author"
        assert rec["pk"] is not None
        assert rec["fields"]["name"] == "Alice"
        assert rec["fields"]["age"] == 30
        assert rec["fields"]["is_active"] is True
        # FK with no related object serialised as None.
        assert rec["fields"]["publisher"] is None

    def test_serialize_foreign_key_uses_pk(self):
        pub = Publisher.objects.create(name="Pub-A")
        Author.objects.create(name="Bob", age=22, publisher=pub)
        out = serialize([Author])
        assert out[0]["fields"]["publisher"] == pub.pk

    def test_serialize_m2m_includes_related_pks(self):
        t1 = Tag.objects.create(name="alpha")
        t2 = Tag.objects.create(name="beta")
        article = Article.objects.create(title="Hello")
        article.tags.add(t1, t2)

        out = serialize([Article])
        rec = next(r for r in out if r["pk"] == article.pk)
        assert sorted(rec["fields"]["tags"]) == sorted([t1.pk, t2.pk])

    def test_dumps_is_pretty_when_indented(self):
        Author.objects.create(name="x", age=1)
        text = dumps([Author], indent=2)
        assert "\n  " in text  # indentation present
        # Sanity: it is parseable.
        json.loads(text)

    def test_deserialize_round_trip(self):
        a1 = Author.objects.create(name="DeRoundtrip", age=42)
        text = dumps([Author])
        rebuilt = list(deserialize(text))
        # ``deserialize`` resolves the target model through the global
        # registry, which other test modules can rebind by declaring a
        # class of the same name. Match on attributes rather than class
        # identity so the test stays correct under that pre-existing
        # collision (see the registry note in ``dorm.models``).
        assert any(
            type(o).__name__ == "Author"
            and o.pk == a1.pk
            and o.name == "DeRoundtrip"
            for o in rebuilt
        )


# ── load() ────────────────────────────────────────────────────────────────────


class TestLoad:
    def test_load_inserts_rows_with_explicit_pk(self):
        # Pre-allocate publishers since Author has a non-null FK.
        text = json.dumps(
            [
                {"model": "tests.Publisher", "pk": 100, "fields": {"name": "PubLoad"}},
                {
                    "model": "tests.Author",
                    "pk": 101,
                    "fields": {
                        "name": "Loaded",
                        "age": 50,
                        "is_active": True,
                        "email": None,
                        "publisher": 100,
                    },
                },
            ]
        )
        loaded = load(text)
        assert loaded == 2

        author = Author.objects.get(pk=101)
        assert author.name == "Loaded"
        assert author.publisher_id == 100
        # Lazy-load via FK descriptor still works.
        assert author.publisher.name == "PubLoad"

    def test_load_round_trip_signals_not_fired(self):
        from dorm.signals import post_save

        fired: list[str] = []

        def receiver(sender, instance, **kwargs):
            fired.append(instance.name)

        post_save.connect(receiver, sender=Author, weak=False)
        try:
            text = json.dumps(
                [
                    {
                        "model": "tests.Author",
                        "pk": 200,
                        "fields": {
                            "name": "QuietLoad",
                            "age": 21,
                            "is_active": True,
                            "email": None,
                            "publisher": None,
                        },
                    }
                ]
            )
            load(text)
        finally:
            post_save.disconnect(receiver)

        # ``loaddata`` bypasses ``save()`` for speed; signals stay quiet.
        assert fired == []
        assert Author.objects.filter(pk=200).exists()

    def test_load_m2m_rows(self):
        t1 = Tag.objects.create(name="L1")
        t2 = Tag.objects.create(name="L2")
        text = json.dumps(
            [
                {
                    "model": "tests.Article",
                    "pk": 300,
                    "fields": {"title": "ArtM2M", "tags": [t1.pk, t2.pk]},
                }
            ]
        )
        load(text)
        article = Article.objects.get(pk=300)
        related = sorted(t.name for t in article.tags.all())
        assert related == ["L1", "L2"]

    def test_load_unknown_field_ignored(self):
        text = json.dumps(
            [
                {
                    "model": "tests.Author",
                    "pk": 400,
                    "fields": {
                        "name": "Tolerant",
                        "age": 22,
                        "is_active": True,
                        "email": None,
                        "publisher": None,
                        "future_field_not_in_schema": "ignored",
                    },
                }
            ]
        )
        load(text)
        assert Author.objects.get(pk=400).name == "Tolerant"

    def test_load_rolls_back_on_bad_record(self):
        # First record OK, second references a missing model so the
        # whole batch should roll back atomically.
        text = json.dumps(
            [
                {
                    "model": "tests.Author",
                    "pk": 500,
                    "fields": {
                        "name": "ShouldNotPersist",
                        "age": 22,
                        "is_active": True,
                        "email": None,
                        "publisher": None,
                    },
                },
                {"model": "tests.NoSuchModel", "pk": 501, "fields": {}},
            ]
        )
        with pytest.raises(LookupError):
            load(text)
        assert not Author.objects.filter(pk=500).exists()


# ── CLI: dumpdata / loaddata (in-process) ─────────────────────────────────────


def _no_op_load_settings(*_a, **_kw):
    """Stand-in for ``dorm.cli._load_settings`` used by these tests.

    The real loader re-imports a settings module and reconfigures the
    DATABASES dict — which would clobber the session-wide test config.
    The tests that exercise the full settings reload live in
    ``test_cli_inprocess.py``; here we just want to verify that the
    new ``cmd_dumpdata`` / ``cmd_loaddata`` argument plumbing reaches
    the serializer with the right targets.
    """
    return None


def _no_op_load_apps(*_a, **_kw):
    return None


class TestDumpdataCLI:
    def test_cmd_dumpdata_writes_to_output(self, monkeypatch, tmp_path):
        from dorm import cli

        monkeypatch.setattr(cli, "_load_settings", _no_op_load_settings)
        monkeypatch.setattr(cli, "_load_apps", _no_op_load_apps)

        Publisher.objects.create(name="CLIPub")

        out = tmp_path / "dump.json"
        ns = argparse_namespace(
            targets=["tests.Publisher"],
            indent=2,
            output=str(out),
            settings=None,
        )
        cli.cmd_dumpdata(ns)

        data = json.loads(out.read_text())
        assert any(rec["fields"]["name"] == "CLIPub" for rec in data)
        # Indent honoured.
        assert "\n  " in out.read_text()

    def test_cmd_dumpdata_unknown_target_exits_nonzero(self, monkeypatch, capsys):
        from dorm import cli

        monkeypatch.setattr(cli, "_load_settings", _no_op_load_settings)
        monkeypatch.setattr(cli, "_load_apps", _no_op_load_apps)

        ns = argparse_namespace(
            targets=["not_a_real.thing"],
            indent=None,
            output=None,
            settings=None,
        )
        with pytest.raises(SystemExit) as exc_info:
            cli.cmd_dumpdata(ns)
        assert exc_info.value.code != 0
        captured = capsys.readouterr()
        assert "matched no" in captured.err or "not found" in captured.err


class TestLoaddataCLI:
    def test_cmd_loaddata_inserts(self, monkeypatch, tmp_path):
        from dorm import cli

        monkeypatch.setattr(cli, "_load_settings", _no_op_load_settings)
        monkeypatch.setattr(cli, "_load_apps", _no_op_load_apps)

        fixture = tmp_path / "fixture.json"
        fixture.write_text(
            json.dumps(
                [
                    {
                        "model": "tests.Publisher",
                        "pk": 8500,
                        "fields": {"name": "FixtureLoaded"},
                    }
                ]
            )
        )

        ns = argparse_namespace(
            fixtures=[str(fixture)],
            database="default",
            settings=None,
        )
        cli.cmd_loaddata(ns)

        assert Publisher.objects.filter(pk=8500, name="FixtureLoaded").exists()

    def test_cmd_loaddata_missing_file_errors(self, monkeypatch, capsys, tmp_path):
        from dorm import cli

        monkeypatch.setattr(cli, "_load_settings", _no_op_load_settings)
        monkeypatch.setattr(cli, "_load_apps", _no_op_load_apps)

        ns = argparse_namespace(
            fixtures=[str(tmp_path / "nope.json")],
            database="default",
            settings=None,
        )
        with pytest.raises(SystemExit):
            cli.cmd_loaddata(ns)
        captured = capsys.readouterr()
        assert "not found" in captured.err


def argparse_namespace(**kwargs):
    """Tiny stand-in for ``argparse.Namespace`` to avoid the import noise."""
    import argparse

    return argparse.Namespace(**kwargs)


# ── Round-trip serializer ↔ load() preserves data ────────────────────────────


class TestRoundTrip:
    def test_publishers_and_books_round_trip(self):
        pub = Publisher.objects.create(name="Round")
        author = Author.objects.create(name="RTAuthor", age=30, publisher=pub)
        Book.objects.create(title="RTBook", author=author, pages=120, published=True)

        text = dumps([Publisher, Author, Book], indent=2)

        # Wipe and reload from the dump.
        Book.objects.all().delete()
        Author.objects.all().delete()
        Publisher.objects.all().delete()

        load(text)

        assert Publisher.objects.filter(name="Round").exists()
        a = Author.objects.get(name="RTAuthor")
        assert a.publisher.name == "Round"
        b = Book.objects.get(title="RTBook")
        assert b.author_id == a.pk
        assert b.published is True
