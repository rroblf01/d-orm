"""Coverage-tightening tests for ``dorm.serialize``.

The base ``test_serialize.py`` covers the JSON round-trip for the
common types (str, int, FK, M2M). This file pokes the branches it
doesn't reach: the type-specific arms of ``_serialize_value``
(timedelta → microseconds, Decimal → str, UUID → str, bytes → b64
envelope, ``dorm.Range`` → ``__range__`` envelope, lists / dicts of
the above), the deserialize-side recognition of those envelopes,
the ``serialize()`` polymorphism (model class vs queryset vs bare
iterable), and the malformed-input rejection paths in ``load()``.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import enum
import json
import uuid
from typing import Any

import pytest

import dorm
from dorm.serialize import (
    _deserialize_value,
    _resolve_model,
    _serialize_value,
    deserialize,
    dumps,
    load,
    serialize,
)
from tests.models import Article, Author, Publisher, Tag


# ── _serialize_value: per-type branches ──────────────────────────────────────


class _Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class TestSerializeValueScalars:
    def test_none(self):
        assert _serialize_value(None) is None

    def test_enum_member_serialises_to_value(self):
        assert _serialize_value(_Status.ACTIVE) == "active"

    def test_datetime_serialises_to_isoformat(self):
        dt = datetime.datetime(2026, 4, 28, 12, 30, 45)
        assert _serialize_value(dt) == "2026-04-28T12:30:45"

    def test_date_serialises_to_isoformat(self):
        d = datetime.date(2026, 4, 28)
        assert _serialize_value(d) == "2026-04-28"

    def test_time_serialises_to_isoformat(self):
        t = datetime.time(9, 0, 1)
        assert _serialize_value(t) == "09:00:01"

    def test_timedelta_serialises_to_microseconds_int(self):
        td = datetime.timedelta(minutes=5, microseconds=42)
        encoded = _serialize_value(td)
        assert encoded == 5 * 60 * 10 ** 6 + 42

    def test_negative_timedelta_round_trips_microseconds(self):
        td = -datetime.timedelta(seconds=2)
        assert _serialize_value(td) == -2 * 10 ** 6

    def test_decimal_serialises_to_str(self):
        assert _serialize_value(decimal.Decimal("1.50")) == "1.50"

    def test_uuid_serialises_to_str(self):
        u = uuid.UUID("00000000-0000-0000-0000-0000000000ff")
        assert _serialize_value(u) == "00000000-0000-0000-0000-0000000000ff"

    def test_bytes_emits_b64_envelope(self):
        encoded = _serialize_value(b"\x00\x01\x02hello")
        assert isinstance(encoded, dict)
        assert "__bytes__" in encoded
        # Round-trips losslessly.
        assert base64.b64decode(encoded["__bytes__"]) == b"\x00\x01\x02hello"

    def test_bytearray_and_memoryview_use_same_envelope(self):
        for kind in (bytearray(b"abc"), memoryview(b"def")):
            encoded = _serialize_value(kind)
            assert isinstance(encoded, dict) and "__bytes__" in encoded

    def test_range_emits_range_envelope(self):
        r = dorm.Range(1, 10, bounds="[)")
        encoded = _serialize_value(r)
        assert encoded == {
            "__range__": True,
            "lower": 1,
            "upper": 10,
            "bounds": "[)",
        }

    def test_range_with_decimal_endpoints_recurses_through_serializer(self):
        r = dorm.Range(decimal.Decimal("1.5"), decimal.Decimal("9.99"))
        encoded = _serialize_value(r)
        # Inner endpoints are themselves serialised — Decimals → str.
        assert encoded["lower"] == "1.5"
        assert encoded["upper"] == "9.99"

    def test_list_recurses_through_serializer(self):
        encoded = _serialize_value(
            [decimal.Decimal("1"), datetime.date(2026, 1, 1)]
        )
        assert encoded == ["1", "2026-01-01"]

    def test_tuple_serialises_as_list(self):
        encoded = _serialize_value((1, 2, 3))
        assert encoded == [1, 2, 3]

    def test_dict_recurses_through_serializer(self):
        encoded = _serialize_value(
            {"k": decimal.Decimal("3"), "n": _Status.ARCHIVED}
        )
        assert encoded == {"k": "3", "n": "archived"}

    def test_native_json_types_pass_through_unchanged(self):
        for v in [True, 42, 3.14, "hello"]:
            assert _serialize_value(v) is v


# ── _deserialize_value: envelope recognition ────────────────────────────────


class TestDeserializeValueEnvelopes:
    def test_none_returns_none_without_calling_field(self):
        # ``field`` is never touched on the None path; pass a sentinel
        # to prove it.
        sentinel = object()
        assert _deserialize_value(sentinel, None) is None

    def test_bytes_envelope_reverses_b64(self):
        encoded = {"__bytes__": base64.b64encode(b"hi-bytes").decode()}
        assert _deserialize_value(None, encoded) == b"hi-bytes"

    def test_range_envelope_reconstructs_dorm_range(self):
        encoded = {
            "__range__": True,
            "lower": 1,
            "upper": 10,
            "bounds": "(]",
        }
        out = _deserialize_value(None, encoded)
        assert isinstance(out, dorm.Range)
        assert out.lower == 1 and out.upper == 10 and out.bounds == "(]"

    def test_range_envelope_with_default_bounds(self):
        # ``bounds`` missing → default ``"[)"``.
        out = _deserialize_value(None, {"__range__": True, "lower": 1, "upper": 5})
        assert out.bounds == "[)"

    def test_unknown_dict_falls_through_to_field_to_python(self):
        """A plain dict that isn't an envelope is forwarded to
        ``field.to_python`` — the typical JSONField case."""
        f = dorm.JSONField()
        out = _deserialize_value(f, {"a": 1})
        assert out == {"a": 1}


# ── serialize(): polymorphic input handling ─────────────────────────────────


@pytest.fixture(autouse=True)
def _pin_test_models_in_registry():
    """Other test files (notably ``test_orm.py``) declare model
    classes with the same names under the same auto-derived
    ``app_label="tests"``, so the global registry can briefly point
    ``"tests.Author"`` at the wrong class. Pin to ``tests.models``
    for the duration of this file so the round-trip tests stay
    deterministic.
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
        "Author": Author, "Publisher": Publisher,
        "Article": Article, "Tag": Tag,
        "tests.Author": Author, "tests.Publisher": Publisher,
        "tests.Article": Article, "tests.Tag": Tag,
    }
    _model_registry.update(pinned)
    yield
    for key, val in saved.items():
        if val is None:
            _model_registry.pop(key, None)
        else:
            _model_registry[key] = val


class TestSerializePolymorphism:
    def test_accepts_model_class_directly(self):
        Author.objects.create(name="P-class", age=10)
        out = serialize([Author])
        assert any(r["fields"]["name"] == "P-class" for r in out)

    def test_accepts_queryset(self):
        Author.objects.create(name="P-qs", age=10)
        qs = Author.objects.filter(name="P-qs")
        out = serialize([qs])
        assert len(out) == 1
        assert out[0]["fields"]["name"] == "P-qs"

    def test_accepts_bare_list_of_instances(self):
        a = Author.objects.create(name="P-list", age=10)
        b = Author.objects.create(name="P-list-2", age=20)
        out = serialize([[a, b]])
        names = sorted(r["fields"]["name"] for r in out)
        assert names == ["P-list", "P-list-2"]

    def test_concatenates_multiple_sources_in_order(self):
        Author.objects.create(name="src-a", age=1)
        Publisher.objects.create(name="src-p")
        out = serialize([Author, Publisher])
        labels = [r["model"] for r in out]
        # All authors come before all publishers — order matches the
        # input concatenation, not the DB id.
        first_pub = next(i for i, label in enumerate(labels) if "Publisher" in label)
        last_author = max(
            (i for i, label in enumerate(labels) if "Author" in label), default=-1
        )
        assert last_author < first_pub


# ── _resolve_model: bare-name fallback + missing → LookupError ──────────────


class TestResolveModel:
    def test_resolves_app_qualified_name(self):
        assert _resolve_model("tests.Author") is Author

    def test_falls_back_to_bare_name_when_no_app_match(self):
        # Strip the app prefix to a label that doesn't exist;
        # resolver should still find it via the bare ``Author`` key.
        from dorm.models import _model_registry

        # Sanity: bare key still points to our class (pinned by fixture).
        assert _model_registry.get("Author") is Author
        # Use an app prefix that has no entry but a bare name that does.
        assert _resolve_model("ghostapp.Author") is Author

    def test_unknown_label_raises_lookup_error(self):
        with pytest.raises(LookupError, match="not found in INSTALLED_APPS"):
            _resolve_model("ghostapp.GhostModel")

    def test_unknown_bare_label_raises_lookup_error(self):
        with pytest.raises(LookupError):
            _resolve_model("DoesNotExistAnywhere")


# ── load(): malformed root rejected ─────────────────────────────────────────


class TestLoadMalformedInput:
    def test_root_must_be_a_list(self):
        bad = json.dumps({"model": "tests.Publisher", "pk": 1, "fields": {}})
        with pytest.raises(ValueError, match="must be a JSON array"):
            load(bad)

    def test_record_with_no_model_label_skipped(self):
        # Each record needs a "model" key; ones without should be
        # silently skipped (forward-compat: tools may emit empty
        # records).
        text = json.dumps([{"pk": 1, "fields": {"name": "x"}}, {}])
        # No exception; nothing inserted.
        loaded = load(text)
        assert loaded == 0


# ── deserialize(): per-record shape ─────────────────────────────────────────


class TestDeserializeShape:
    def test_skips_records_without_model_label(self):
        text = json.dumps(
            [
                {"pk": 1, "fields": {"name": "ghost"}},
                {
                    "model": "tests.Publisher",
                    "pk": 100,
                    "fields": {"name": "real"},
                },
            ]
        )
        rebuilt = list(deserialize(text))
        # The first entry is skipped silently; only the labelled one
        # makes it through.
        assert len(rebuilt) == 1
        assert type(rebuilt[0]).__name__ == "Publisher"
        assert rebuilt[0].pk == 100
        assert rebuilt[0].name == "real"

    def test_unknown_field_is_ignored(self):
        """Forward-compat path: a fixture written against a future
        schema can carry columns this version doesn't know — they
        get dropped, not raised."""
        text = json.dumps(
            [
                {
                    "model": "tests.Publisher",
                    "pk": 200,
                    "fields": {"name": "fwd", "future_column": 42},
                }
            ]
        )
        rebuilt = list(deserialize(text))
        assert rebuilt[0].name == "fwd"
        assert not hasattr(rebuilt[0], "future_column")

    def test_record_without_pk_skips_pk_assignment(self):
        text = json.dumps(
            [
                {
                    "model": "tests.Publisher",
                    "pk": None,
                    "fields": {"name": "pk-less"},
                }
            ]
        )
        rebuilt = list(deserialize(text))
        # ``pk`` stays at the default (None); the row name still landed.
        assert rebuilt[0].name == "pk-less"
        assert rebuilt[0].pk is None


# ── dumps(): indented vs compact ────────────────────────────────────────────


class TestDumpsFormatting:
    def test_compact_default(self):
        Author.objects.create(name="dump-c", age=1)
        text = dumps([Author])
        # No newlines in the compact form.
        assert "\n" not in text
        # Still parseable.
        json.loads(text)

    def test_indent_pretty_prints(self):
        Author.objects.create(name="dump-p", age=1)
        text = dumps([Author], indent=2)
        # Pretty-printed → at least one indented line.
        assert "\n  " in text
        json.loads(text)


# ── M2M fallback when relation manager raises ──────────────────────────────


class TestM2MSerializationFallback:
    def test_m2m_serialises_empty_list_when_manager_raises(
        self, monkeypatch
    ):
        """``_row_to_dict`` wraps the M2M relation read in a bare
        try/except so a misconfigured row still produces a record
        (with an empty M2M list) rather than crashing the whole dump.

        Drive the fallback by monkey-patching the descriptor to
        raise on ``.all()``.
        """
        article = Article.objects.create(title="dump-fallback")

        # Replace ``Article.tags`` access with one that raises.
        from dorm.related_managers import ManyToManyDescriptor

        descriptor = type(article).__dict__.get("tags")
        # Sanity — the test only matters if the relation actually has
        # a descriptor in scope.
        assert isinstance(descriptor, ManyToManyDescriptor)

        original_get = ManyToManyDescriptor.__get__

        def boom(self, instance, owner=None):
            if instance is article:
                raise RuntimeError("simulated relation failure")
            return original_get(self, instance, owner)

        monkeypatch.setattr(ManyToManyDescriptor, "__get__", boom)

        out: list[dict[str, Any]] = serialize([Article])
        record = next(r for r in out if r["pk"] == article.pk)
        # The fallback yields ``[]`` for ``tags`` — the row still
        # serialises despite the relation read failing.
        assert record["fields"]["tags"] == []
