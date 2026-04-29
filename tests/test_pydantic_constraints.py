"""Tests for the dorm → Pydantic constraint translation layer.

Each dorm Field constraint that the ORM enforces at assignment /
``full_clean`` time should also surface in the generated Pydantic
schema, so that:

* FastAPI rejects bad input at the request boundary (HTTP 422) instead
  of letting it reach ``Model(...)`` only to raise later.
* The OpenAPI document (``Schema.model_json_schema()``) advertises the
  same shape that the database actually accepts.

These tests pin both ends — Pydantic-side validation behaviour AND the
JSON Schema output — for ``max_length``, ``max_digits`` /
``decimal_places``, ``choices``, positive-int ``ge=0``, ``EmailField``
/ ``URLField`` format hints, default propagation, and the
``MinValueValidator`` / ``MaxValueValidator`` / ``RegexValidator``
adapter path.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

pytest.importorskip("pydantic")

from pydantic import BaseModel, ValidationError

import dorm
from dorm.contrib.pydantic import DormSchema, schema_for
from dorm.validators import (
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    RegexValidator,
)


def _json_schema_property(model_cls: type[BaseModel], name: str) -> dict[str, Any]:
    """Return the OpenAPI/JSON-Schema fragment describing field *name*.

    Nullable fields render as ``{"anyOf": [{...}, {"type": "null"}]}``;
    we look inside the first non-null arm so callers can assert against
    the same shape regardless of optionality.
    """
    schema = model_cls.model_json_schema()
    prop = schema["properties"][name]
    if "anyOf" in prop:
        for arm in prop["anyOf"]:
            if arm.get("type") != "null":
                merged = dict(arm)
                # Surface the union default so callers can read it
                # alongside the non-null shape.
                if "default" in prop:
                    merged.setdefault("default", prop["default"])
                return merged
        return prop
    return prop


# ── max_length on Char-derived fields ─────────────────────────────────────────


def test_charfield_max_length_rejected_at_pydantic_boundary():
    class _M(dorm.Model):
        name = dorm.CharField(max_length=5)

        class Meta:
            db_table = "pyd_max_len_charfield"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"name": "12345"})  # boundary OK
    with pytest.raises(ValidationError) as exc:
        Schema.model_validate({"name": "123456"})
    assert "at most 5 characters" in str(exc.value)


def test_charfield_max_length_in_json_schema():
    class _M(dorm.Model):
        name = dorm.CharField(max_length=42)

        class Meta:
            db_table = "pyd_max_len_charfield_schema"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "name")["maxLength"] == 42


def test_email_and_url_max_length_propagate():
    """``EmailField`` defaults to 254, ``URLField`` to 200 — both
    should appear in the JSON Schema even though the Python annotation
    stays ``str``."""
    class _M(dorm.Model):
        email = dorm.EmailField()
        homepage = dorm.URLField()

        class Meta:
            db_table = "pyd_email_url_max"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "email")["maxLength"] == 254
    assert _json_schema_property(Schema, "homepage")["maxLength"] == 200


def test_slug_max_length_propagates():
    class _M(dorm.Model):
        slug = dorm.SlugField()

        class Meta:
            db_table = "pyd_slug_max"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "slug")["maxLength"] == 50


def test_max_length_survives_optional_wrap():
    """Nullable fields should still carry the constraint — without this
    fix, ``EmailField(null=True)`` accepted strings of any length."""
    class _M(dorm.Model):
        email = dorm.EmailField(null=True)

        class Meta:
            db_table = "pyd_max_len_optional"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"email": None})  # None is fine
    Schema.model_validate({"email": "a@b.co"})
    with pytest.raises(ValidationError):
        Schema.model_validate({"email": "x" * 255})


# ── DecimalField max_digits / decimal_places ──────────────────────────────────


def test_decimalfield_max_digits_and_places_enforced():
    class _M(dorm.Model):
        amount = dorm.DecimalField(max_digits=5, decimal_places=2)

        class Meta:
            db_table = "pyd_decimal_constraints"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"amount": Decimal("123.45")})  # exactly 5 digits
    with pytest.raises(ValidationError):
        Schema.model_validate({"amount": Decimal("1234.56")})  # 6 digits
    with pytest.raises(ValidationError):
        Schema.model_validate({"amount": Decimal("1.234")})  # 3 decimals


# ── choices → Literal ─────────────────────────────────────────────────────────


def test_charfield_choices_become_literal_in_json_schema():
    class _M(dorm.Model):
        status = dorm.CharField(
            max_length=10,
            choices=[("draft", "Draft"), ("published", "Published")],
        )

        class Meta:
            db_table = "pyd_choices_charfield"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    prop = _json_schema_property(Schema, "status")
    # Pydantic emits ``enum`` for Literal-of-strings, type stays ``string``.
    assert set(prop["enum"]) == {"draft", "published"}
    Schema.model_validate({"status": "draft"})
    with pytest.raises(ValidationError):
        Schema.model_validate({"status": "ghost"})


def test_choices_handle_flat_value_list():
    """Some users declare choices as a flat list of values rather than
    the canonical ``[(value, label), ...]`` shape — the translator
    must accept both."""
    class _M(dorm.Model):
        kind = dorm.CharField(max_length=4, choices=["a", "b", "c"])

        class Meta:
            db_table = "pyd_choices_flat"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"kind": "a"})
    with pytest.raises(ValidationError):
        Schema.model_validate({"kind": "z"})


# ── Positive integer fields ───────────────────────────────────────────────────


def test_positive_integer_fields_reject_negative_values():
    class _M(dorm.Model):
        age = dorm.PositiveIntegerField()
        rank = dorm.PositiveSmallIntegerField()

        class Meta:
            db_table = "pyd_positive_ints"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"age": 0, "rank": 0})
    Schema.model_validate({"age": 1000, "rank": 5})
    with pytest.raises(ValidationError):
        Schema.model_validate({"age": -1, "rank": 0})
    with pytest.raises(ValidationError):
        Schema.model_validate({"age": 0, "rank": -1})


def test_positive_integer_emits_minimum_zero_in_json_schema():
    class _M(dorm.Model):
        age = dorm.PositiveIntegerField()

        class Meta:
            db_table = "pyd_positive_int_schema"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "age")["minimum"] == 0


# ── Default propagation ───────────────────────────────────────────────────────


def test_default_value_replaces_none_placeholder():
    """Before the fix, any field with ``has_default()`` was forced to
    ``T | None`` with default ``None`` in the schema — meaning a
    payload that omitted the field arrived at the model as ``None``
    instead of the field's real default."""
    class _M(dorm.Model):
        active = dorm.BooleanField(default=False)
        retries = dorm.IntegerField(default=3)
        label = dorm.CharField(max_length=20, default="anon")

        class Meta:
            db_table = "pyd_default_propagation"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    inst = Schema.model_validate({})
    assert inst.active is False  # type: ignore
    assert inst.retries == 3  # type: ignore
    assert inst.label == "anon"  # type: ignore
    # Annotation stays the bare type (no ``| None``) — the field is
    # optional in Pydantic terms but not nullable.
    assert Schema.model_fields["active"].annotation is bool
    assert Schema.model_fields["retries"].annotation is int


def test_callable_default_uses_default_factory():
    counter = {"n": 0}

    def _next() -> int:
        counter["n"] += 1
        return counter["n"]

    class _M(dorm.Model):
        seq = dorm.IntegerField(default=_next)

        class Meta:
            db_table = "pyd_callable_default"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    a = Schema.model_validate({})
    b = Schema.model_validate({})
    assert (a.seq, b.seq) == (1, 2), "default_factory should re-run per-instance"  # type: ignore


def test_nullable_field_without_default_keeps_none_default():
    class _M(dorm.Model):
        nickname = dorm.CharField(max_length=20, null=True)

        class Meta:
            db_table = "pyd_nullable_no_default"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    f = Schema.model_fields["nickname"]
    # Annotation includes ``| None`` so JSON ``null`` is accepted.
    assert f.annotation == (str | None)
    assert f.default is None
    assert f.is_required() is False


# ── EmailField / URLField OpenAPI format hint ─────────────────────────────────


def test_emailfield_advertises_email_format():
    class _M(dorm.Model):
        email = dorm.EmailField()

        class Meta:
            db_table = "pyd_email_format"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "email")["format"] == "email"


def test_urlfield_advertises_uri_format():
    class _M(dorm.Model):
        homepage = dorm.URLField()

        class Meta:
            db_table = "pyd_url_format"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "homepage")["format"] == "uri"


# ── Validator translation (Min/Max/Regex) ─────────────────────────────────────


def test_min_value_validator_translates_to_ge():
    class _M(dorm.Model):
        score = dorm.IntegerField(validators=[MinValueValidator(10)])

        class Meta:
            db_table = "pyd_min_value"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"score": 10})
    with pytest.raises(ValidationError):
        Schema.model_validate({"score": 9})
    assert _json_schema_property(Schema, "score")["minimum"] == 10


def test_max_value_validator_translates_to_le():
    class _M(dorm.Model):
        score = dorm.IntegerField(validators=[MaxValueValidator(99)])

        class Meta:
            db_table = "pyd_max_value"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"score": 99})
    with pytest.raises(ValidationError):
        Schema.model_validate({"score": 100})
    assert _json_schema_property(Schema, "score")["maximum"] == 99


def test_regex_validator_translates_to_pattern():
    class _M(dorm.Model):
        code = dorm.CharField(
            max_length=10, validators=[RegexValidator(r"^[A-Z]{3}$")]
        )

        class Meta:
            db_table = "pyd_regex"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"code": "ABC"})
    with pytest.raises(ValidationError):
        Schema.model_validate({"code": "ab"})
    assert _json_schema_property(Schema, "code")["pattern"] == r"^[A-Z]{3}$"


def test_min_length_validator_translates_to_min_length():
    class _M(dorm.Model):
        slug = dorm.CharField(max_length=20, validators=[MinLengthValidator(3)])

        class Meta:
            db_table = "pyd_min_length"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    Schema.model_validate({"slug": "abc"})
    with pytest.raises(ValidationError):
        Schema.model_validate({"slug": "ab"})
    assert _json_schema_property(Schema, "slug")["minLength"] == 3


def test_explicit_max_length_validator_only_tightens_field_max_length():
    """A ``MaxLengthValidator(N)`` applied on top of an existing
    ``CharField(max_length=M)`` must never *loosen* the constraint —
    take the smaller of the two so the field's stored shape can't
    be exceeded."""
    from dorm.validators import MaxLengthValidator

    class _M(dorm.Model):
        # Field max_length=20, validator max_length=5 → effective 5.
        short = dorm.CharField(max_length=20, validators=[MaxLengthValidator(5)])

        class Meta:
            db_table = "pyd_validator_tighten"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "short")["maxLength"] == 5


def test_validator_combined_with_native_constraint_takes_strictest():
    """Combining a ``MinValueValidator`` with a positive int field
    (which already implies ``ge=0``) should keep the higher floor."""
    class _M(dorm.Model):
        age = dorm.PositiveIntegerField(validators=[MinValueValidator(18)])

        class Meta:
            db_table = "pyd_validator_combined"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "age")["minimum"] == 18
    with pytest.raises(ValidationError):
        Schema.model_validate({"age": 17})
    Schema.model_validate({"age": 18})


# ── DormSchema (Meta-driven) parity ───────────────────────────────────────────


def test_dormschema_meta_propagates_constraints():
    """``DormSchema``'s metaclass path must apply the same constraint
    translation as :func:`schema_for` — not a separate code path."""
    class _M(dorm.Model):
        name = dorm.CharField(max_length=8)
        age = dorm.PositiveIntegerField()
        status = dorm.CharField(
            max_length=4, choices=[("on", "On"), ("off", "Off")]
        )

        class Meta:
            db_table = "pyd_dormschema_constraints"
            app_label = "tests"

    class _S(DormSchema):
        class Meta:
            model = _M

    _S.model_validate({"name": "ok", "age": 5, "status": "on"})
    with pytest.raises(ValidationError):
        _S.model_validate({"name": "x" * 9, "age": 5, "status": "on"})
    with pytest.raises(ValidationError):
        _S.model_validate({"name": "ok", "age": -1, "status": "on"})
    with pytest.raises(ValidationError):
        _S.model_validate({"name": "ok", "age": 5, "status": "maybe"})


def test_dormschema_meta_propagates_default():
    class _M(dorm.Model):
        retries = dorm.IntegerField(default=3)

        class Meta:
            db_table = "pyd_dormschema_default"
            app_label = "tests"

    class _S(DormSchema):
        class Meta:
            model = _M

    obj = _S.model_validate({})
    assert obj.retries == 3  # type: ignore
    assert _S.model_fields["retries"].annotation is int


def test_dormschema_meta_with_callable_default_uses_factory():
    """Mirror :func:`test_callable_default_uses_default_factory` but
    via the metaclass path — both must wire ``default_factory`` so
    each validation runs the callable fresh."""
    counter = {"n": 0}

    def _next() -> int:
        counter["n"] += 1
        return counter["n"]

    class _M(dorm.Model):
        seq = dorm.IntegerField(default=_next)

        class Meta:
            db_table = "pyd_dormschema_factory"
            app_label = "tests"

    class _S(DormSchema):
        class Meta:
            model = _M

    a = _S.model_validate({})
    b = _S.model_validate({})
    assert (a.seq, b.seq) == (1, 2)  # type: ignore


# ── Validator kwargs merging coverage ─────────────────────────────────────────


def test_min_length_validator_combines_with_charfield():
    """The ``min_length`` merge branch is only hit when the field
    already declares a min_length and a validator adds another. Use
    two MinLengthValidators to cover that branch deterministically."""
    class _M(dorm.Model):
        slug = dorm.CharField(
            max_length=20,
            validators=[MinLengthValidator(2), MinLengthValidator(5)],
        )

        class Meta:
            db_table = "pyd_min_length_merge"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    # Strictest min_length wins — value must be at least 5 chars.
    assert _json_schema_property(Schema, "slug")["minLength"] == 5
    with pytest.raises(ValidationError):
        Schema.model_validate({"slug": "abc"})  # 3 chars
    Schema.model_validate({"slug": "abcde"})


def test_max_value_validators_merge_takes_lowest_ceiling():
    class _M(dorm.Model):
        # Two MaxValueValidators with different ceilings — strictest wins.
        score = dorm.IntegerField(
            validators=[MaxValueValidator(100), MaxValueValidator(50)]
        )

        class Meta:
            db_table = "pyd_max_value_merge"
            app_label = "tests"

    Schema = schema_for(_M, exclude=("id",))
    assert _json_schema_property(Schema, "score")["maximum"] == 50
    Schema.model_validate({"score": 50})
    with pytest.raises(ValidationError):
        Schema.model_validate({"score": 51})


# ── _field_to_type recursive cases ────────────────────────────────────────────


def test_array_field_passes_through_inner_type():
    """Array of ints surfaces as ``list[int]`` and validates each
    element. The inner mapping path is shared with ``_field_to_type``,
    so this also exercises the ArrayField branch."""
    from dorm.contrib.pydantic import _field_to_type
    from typing import get_origin, get_args

    annot = _field_to_type(dorm.ArrayField(dorm.IntegerField()))
    assert get_origin(annot) is list
    assert get_args(annot) == (int,)


def test_generated_field_inherits_output_field_type():
    """``GeneratedField(output_field=DecimalField(...))`` must surface
    as ``Decimal`` in the schema — the generation expression is
    irrelevant to Pydantic, only the column type."""
    from dorm.contrib.pydantic import _field_to_type

    gen = dorm.GeneratedField(
        expression="amount * 1.21",
        output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
    )
    assert _field_to_type(gen) is Decimal


def test_filefield_coerce_falls_back_to_str_for_unknown_objects():
    """Edge case in ``_coerce_field_file_to_str`` — an object without
    ``.name`` is rendered via ``str(value)``. Without coverage here a
    refactor that broke this branch would only surface when a user
    posts a custom File subclass."""
    from dorm.contrib.pydantic import _coerce_field_file_to_str

    class _Weird:
        # No ``.name`` attribute on purpose.
        def __str__(self) -> str:
            return "weird/path.bin"

    assert _coerce_field_file_to_str(_Weird()) == "weird/path.bin"
    # An object whose ``.name`` isn't a string also falls through to
    # ``str(value)``.
    class _BadName:
        name = 12345  # not a string

        def __str__(self) -> str:
            return "bad-name"

    assert _coerce_field_file_to_str(_BadName()) == "bad-name"


# ── Meta.nested wiring ────────────────────────────────────────────────────────


def test_meta_nested_swaps_fk_for_subschema():
    """A FK declared in ``Meta.nested`` must be replaced by the
    sub-schema (not surface as a bare ``int``)."""
    class _Pub(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "pyd_pub_nested"
            app_label = "tests"

    class _Auth(dorm.Model):
        name = dorm.CharField(max_length=20)
        publisher = dorm.ForeignKey(_Pub, on_delete=dorm.CASCADE, null=True)

        class Meta:
            db_table = "pyd_auth_nested"
            app_label = "tests"

    class PubSchema(DormSchema):
        class Meta:
            model = _Pub

    class AuthSchema(DormSchema):
        class Meta:
            model = _Auth
            nested = {"publisher": PubSchema}

    # Nullable FK → ``PubSchema | None`` per the implementation.
    assert AuthSchema.model_fields["publisher"].annotation == (PubSchema | None)


def test_meta_nested_with_unknown_field_raises_type_error():
    """Typos in ``Meta.nested`` must fail at class-construction time.
    The lazy alternative would surface as a confusing AttributeError
    deep inside Pydantic; this raise gives the user the field name
    they meant to type."""
    class _M(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "pyd_nested_typo"
            app_label = "tests"

    class _Sub(DormSchema):
        class Meta:
            model = _M

    with pytest.raises(TypeError, match="ghost_field"):
        class _Bad(DormSchema):  # noqa: F841
            class Meta:
                model = _M
                nested = {"ghost_field": _Sub}


def test_meta_nested_for_m2m_emits_list_of_subschema():
    """M2M fields are skipped by default (no ``column``); listing them
    in ``Meta.nested`` opts them in as ``list[SubSchema]``."""
    from tests.models import Article

    class TagOut(DormSchema):
        id: int
        name: str

    class ArticleOut(DormSchema):
        class Meta:
            model = Article
            nested = {"tags": TagOut}

    field = ArticleOut.model_fields["tags"]
    # Annotation is ``list[TagOut]`` (per ``annotations[f.name] =
    # list[sub_schema]`` in the metaclass).
    assert field.annotation == list[TagOut]


def test_meta_fields_and_exclude_are_mutually_exclusive():
    """``Meta.fields`` and ``Meta.exclude`` together are a config
    smell — explicit error rather than silent precedence."""
    from tests.models import Author

    with pytest.raises(TypeError, match="not both"):
        class _Bad(DormSchema):  # noqa: F841
            class Meta:
                model = Author
                fields = ("name",)
                exclude = ("id",)


def test_meta_without_model_attribute_raises_type_error():
    """A ``Meta`` block missing ``model = ...`` is the most common user
    mistake — fail loudly."""
    with pytest.raises(TypeError, match="Meta.model is required"):
        class _NoModel(DormSchema):  # noqa: F841
            class Meta:
                pass


def test_user_annotation_overrides_meta_field():
    """When the user types a field explicitly in the class body, the
    metaclass must keep the explicit annotation instead of overwriting
    it with the auto-derived one."""
    from tests.models import Author

    class _Override(DormSchema):
        # Make ``age`` explicitly a string in this view — silly, but
        # the assertion is that the user's intent wins.
        age: str

        class Meta:
            model = Author

    assert _Override.model_fields["age"].annotation is str
