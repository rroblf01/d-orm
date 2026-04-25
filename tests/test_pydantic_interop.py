"""Tests for dorm.contrib.pydantic — FastAPI-friendly schema generation.

Most attribute access on the validated instances goes through ``getattr``
because ``schema_for`` produces classes with fields determined at runtime;
ty can't see those, hence the per-line ``# type: ignore`` comments below.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest

pytest.importorskip("pydantic")

from pydantic import BaseModel, ConfigDict, ValidationError

import dorm
from dorm.contrib.pydantic import DormSchema, schema_for
from tests.models import Article, Author


# ── Generation ────────────────────────────────────────────────────────────────


def test_schema_for_returns_basemodel_subclass():
    Schema = schema_for(Author)
    assert issubclass(Schema, BaseModel)
    assert Schema.__name__ == "AuthorSchema"


def test_schema_for_custom_name():
    Schema = schema_for(Author, name="AuthorOut")
    assert Schema.__name__ == "AuthorOut"


def test_schema_includes_all_db_columns_by_default():
    Schema = schema_for(Author)
    fields = Schema.model_fields
    # tests/models.py Author has: id (auto), name, age, email, is_active, publisher
    for expected in ("id", "name", "age", "email", "is_active"):
        assert expected in fields, f"missing {expected!r}; got {list(fields)}"


def test_schema_excludes_m2m():
    """M2M fields don't have a column; they should be excluded from the schema."""
    Schema = schema_for(Article)
    assert "tags" not in Schema.model_fields


def test_schema_fk_is_int():
    """A ForeignKey serializes as int (its FK column type)."""
    Schema = schema_for(Author)
    # Author.publisher is a nullable FK → int | None
    assert Schema.model_fields["publisher"].annotation == (int | None)


# ── Field-type mapping ────────────────────────────────────────────────────────


def test_field_type_mapping_covers_common_types():
    """Define a model that exercises every supported field type and verify
    each one maps to the correct Python type."""

    class _Wide(dorm.Model):
        s = dorm.CharField(max_length=10)
        t = dorm.TextField()
        i = dorm.IntegerField()
        bi = dorm.BigIntegerField()
        f = dorm.FloatField()
        d = dorm.DecimalField()
        b = dorm.BooleanField(default=False)
        dt = dorm.DateTimeField(null=True)
        da = dorm.DateField(null=True)
        ti = dorm.TimeField(null=True)
        u = dorm.UUIDField(null=True)
        e = dorm.EmailField(null=True)
        url = dorm.URLField(null=True)
        sl = dorm.SlugField(null=True)
        bn = dorm.BinaryField(null=True)
        ip = dorm.IPAddressField(null=True)

        class Meta:
            db_table = "test_wide"
            app_label = "tests"

    Schema = schema_for(_Wide)
    fields = Schema.model_fields

    # Required (non-null, non-default) fields stay as the bare type.
    assert fields["s"].annotation is str
    assert fields["t"].annotation is str
    assert fields["i"].annotation is int
    assert fields["bi"].annotation is int
    assert fields["f"].annotation is float
    assert fields["d"].annotation is Decimal

    # Nullable ones are Type | None.
    assert fields["dt"].annotation == (datetime | None)
    assert fields["da"].annotation == (date | None)
    assert fields["ti"].annotation == (time | None)
    assert fields["u"].annotation == (UUID | None)
    assert fields["bn"].annotation == (bytes | None)
    # EmailField / URLField / IPAddressField map to plain ``str`` in the
    # Pydantic schema; dorm enforces the format itself at assignment time.
    assert fields["e"].annotation == (str | None)
    assert fields["url"].annotation == (str | None)
    assert fields["ip"].annotation == (str | None)


# ── exclude / only / optional knobs ───────────────────────────────────────────


def test_exclude_drops_named_fields():
    Schema = schema_for(Author, exclude=("id", "publisher"))
    assert "id" not in Schema.model_fields
    assert "publisher" not in Schema.model_fields
    assert "name" in Schema.model_fields


def test_only_keeps_listed_fields():
    Schema = schema_for(Author, only=("name", "email"))
    assert set(Schema.model_fields) == {"name", "email"}


def test_optional_marks_required_field_as_nullable():
    """`optional` lets PATCH-style schemas accept partial input."""
    Schema = schema_for(Author, only=("name", "age"), optional=("name",))
    fields = Schema.model_fields
    # `name` is non-null on Author; here it's optional → annotation includes None.
    assert fields["name"].annotation == (str | None)
    # `age` stays required.
    assert fields["age"].annotation is int


def test_auto_pk_is_optional():
    """Auto-incrementing PKs are optional in input schemas."""
    Schema = schema_for(Author)
    # id is BigAutoField → int | None with default None.
    assert Schema.model_fields["id"].annotation == (int | None)


# ── from_attributes — the FastAPI use case ────────────────────────────────────


def test_validate_from_dorm_instance():
    """FastAPI relies on Pydantic reading attrs from the response object;
    schema_for() enables that via from_attributes=True."""
    Schema = schema_for(Author)
    a = Author.objects.create(name="Alice", age=30, email="alice@example.com")
    try:
        validated = Schema.model_validate(a)
        assert validated.name == "Alice"  # type: ignore
        assert validated.age == 30  # type: ignore
        assert validated.email == "alice@example.com"  # type: ignore
        # round-trip dict
        d = validated.model_dump()
        assert d["name"] == "Alice"
        assert d["age"] == 30
    finally:
        a.delete()


def test_validate_from_dict():
    """Schema also accepts dict input (e.g. POST body via FastAPI)."""
    Schema = schema_for(Author, exclude=("id",))
    obj = Schema.model_validate({"name": "Bob", "age": 25, "email": "b@x.com"})
    assert obj.name == "Bob"  # type: ignore
    assert obj.age == 25  # type: ignore


def test_validation_rejects_wrong_type():
    Schema = schema_for(Author, exclude=("id",))
    with pytest.raises(ValidationError):
        Schema.model_validate({"name": "X", "age": "not-a-number"})


def test_create_dorm_from_pydantic_payload():
    """Round-trip: parse a request body via Pydantic → create via dorm."""
    UserCreate = schema_for(Author, exclude=("id",))
    payload = UserCreate.model_validate(
        {"name": "RT", "age": 33, "email": "rt@x.com"}
    )
    a = Author.objects.create(**payload.model_dump(exclude_none=True))
    try:
        fetched = Author.objects.get(pk=a.pk)
        assert fetched.name == "RT"
    finally:
        a.delete()


# ── Custom base / ConfigDict propagation ──────────────────────────────────────


def test_custom_base_class_is_honored():
    class _MyBase(BaseModel):
        model_config = ConfigDict(str_strip_whitespace=True)

    Schema = schema_for(Author, base=_MyBase, only=("name", "age"))
    assert issubclass(Schema, _MyBase)
    # The from_attributes flag set by schema_for is preserved (overrides base).
    assert Schema.model_config.get("from_attributes") is True


# ── DormSchema: typed pattern ─────────────────────────────────────────────────


def test_dorm_schema_is_basemodel_with_from_attributes():
    """DormSchema is a thin BaseModel subclass with from_attributes=True."""
    assert issubclass(DormSchema, BaseModel)
    assert DormSchema.model_config["from_attributes"] is True
    assert DormSchema.model_config["arbitrary_types_allowed"] is True


def test_dorm_schema_typed_subclass_validates_from_dorm():
    """Explicit-only subclass: type checkers see every field on UserOut."""

    class UserOut(DormSchema):
        id: int
        name: str
        age: int
        email: str | None = None

    a = Author.objects.create(name="DSAlice", age=27, email="dsa@x.com")
    try:
        out = UserOut.model_validate(a)
        assert out.name == "DSAlice"
        assert out.age == 27
        assert out.email == "dsa@x.com"
        assert out.id == a.pk
    finally:
        a.delete()


def test_dorm_schema_subclass_can_override_config():
    """Subclasses can extend model_config without losing from_attributes."""

    class StrictUserOut(DormSchema):
        model_config = ConfigDict(
            from_attributes=True,
            arbitrary_types_allowed=True,
            str_strip_whitespace=True,
            extra="forbid",
        )
        name: str
        age: int

    a = Author.objects.create(name="  trimmed  ", age=1, email="t@x.com")
    try:
        out = StrictUserOut.model_validate(a)
        assert out.name == "trimmed"
    finally:
        a.delete()


def test_dorm_schema_rejects_unknown_field_when_strict():
    class StrictOut(DormSchema):
        model_config = ConfigDict(from_attributes=True, extra="forbid")
        name: str

    with pytest.raises(ValidationError):
        StrictOut.model_validate({"name": "ok", "unexpected": 1})


# ── DormSchema with class Meta: Django-REST-style auto-fill ───────────────────


def test_meta_model_auto_fills_all_fields():
    """class Meta: model = X (default fields='__all__') populates fields
    matching the dorm Model's columns."""

    class UserOut(DormSchema):
        class Meta:
            model = Author

    declared = set(UserOut.model_fields)
    # Should include every non-M2M dorm column (id, name, age, email, is_active, publisher).
    assert {"id", "name", "age", "email", "is_active", "publisher"} <= declared


def test_meta_fields_whitelist():
    class UserSlim(DormSchema):
        class Meta:
            model = Author
            fields = ("id", "name")

    assert set(UserSlim.model_fields) == {"id", "name"}


def test_meta_exclude_blacklist():
    class UserPublic(DormSchema):
        class Meta:
            model = Author
            exclude = ("id", "publisher")

    declared = set(UserPublic.model_fields)
    assert "id" not in declared
    assert "publisher" not in declared
    assert "name" in declared


def test_meta_optional_makes_required_field_nullable():
    """`optional` lets PATCH-style schemas accept partial input even when
    the underlying dorm field is non-null."""

    class UserPatch(DormSchema):
        class Meta:
            model = Author
            fields = ("name", "age")
            optional = ("name",)

    fields = UserPatch.model_fields
    # `name` is non-null on Author; in this schema it's optional.
    assert fields["name"].annotation == (str | None)
    assert fields["age"].annotation is int


def test_meta_fields_and_exclude_are_mutually_exclusive():
    with pytest.raises(TypeError, match="fields.*exclude"):

        class _Bad(DormSchema):
            class Meta:
                model = Author
                fields = ("name",)
                exclude = ("id",)


def test_meta_requires_model():
    with pytest.raises(TypeError, match="Meta.model is required"):

        class _Bad(DormSchema):
            class Meta:
                exclude = ("id",)


def test_meta_validates_from_dorm_instance():
    """End-to-end: declare via Meta, validate from a dorm instance."""

    class UserOut(DormSchema):
        class Meta:
            model = Author
            fields = ("id", "name", "age")

    a = Author.objects.create(name="MetaAlice", age=33, email="mc@x.com")
    try:
        out = UserOut.model_validate(a)
        assert out.name == "MetaAlice"  # type: ignore
        assert out.age == 33  # type: ignore
        assert out.id == a.pk  # type: ignore
    finally:
        a.delete()


def test_explicit_field_overrides_meta_autofill():
    """An explicit annotation wins over the Meta-derived one."""

    class UserOut(DormSchema):
        # Author.age is IntegerField → would auto-map to `int`.
        # Force it nullable here.
        age: int | None = None

        class Meta:
            model = Author
            fields = ("name", "age")

    fields = UserOut.model_fields
    assert fields["age"].annotation == (int | None)
    assert fields["name"].annotation is str  # auto-generated
    # Default for explicit override is honored.
    instance = UserOut.model_validate({"name": "x"})
    assert instance.age is None


def test_extra_field_alongside_meta_autofill():
    """Adding fields not on the dorm model — typical input schema use."""

    class UserCreate(DormSchema):
        confirm_password: str

        class Meta:
            model = Author
            exclude = ("id",)

    assert "confirm_password" in UserCreate.model_fields
    # Author fields auto-filled too.
    assert "name" in UserCreate.model_fields
    obj = UserCreate.model_validate(
        {"name": "Z", "age": 22, "email": "z@x.com", "confirm_password": "secret"}
    )
    assert obj.confirm_password == "secret"


def test_field_validator_works_with_meta():
    """Pydantic field validators apply to auto-generated fields too."""
    from pydantic import field_validator

    class UserCreate(DormSchema):
        @field_validator("email")
        @classmethod
        def lower(cls, v: str) -> str:
            return v.lower()

        class Meta:
            model = Author
            exclude = ("id",)

    obj = UserCreate.model_validate(
        {"name": "Z", "age": 1, "email": "Mixed@CASE.com"}
    )
    assert obj.email == "mixed@case.com"  # type: ignore


def test_meta_excludes_m2m_automatically():
    class ArticleOut(DormSchema):
        class Meta:
            model = Article

    # Article has Article.tags = ManyToManyField — must not appear here.
    assert "tags" not in ArticleOut.model_fields
    assert "title" in ArticleOut.model_fields


def test_explicit_annotations_are_preserved_under_pep_649():
    """Regression: in Python 3.14, class-body annotations live in
    ``__annotate_func__`` (PEP 649), not in ``__annotations__``. The
    metaclass must read both, otherwise user-declared fields plus
    ``@field_validator`` referencing them raises PydanticUserError."""
    from pydantic import field_validator

    class CustomerCreate(DormSchema):
        # Both fields here are user-declared, not on the dorm Author model.
        email_check: str
        confirm: str

        @field_validator("email_check", "confirm")
        @classmethod
        def lower(cls, v: str) -> str:
            return v.lower()

        class Meta:
            model = Author
            fields = ("name",)  # plus the two user-declared ones above

    fields = set(CustomerCreate.model_fields)
    # User annotations survived (both must be present).
    assert "email_check" in fields
    assert "confirm" in fields
    # Auto-fill from Meta.fields also worked.
    assert "name" in fields

    # Validator actually runs on the explicit fields.
    obj = CustomerCreate.model_validate(
        {"name": "X", "email_check": "MIXED@x.com", "confirm": "MIXED@x.com"}
    )
    assert obj.email_check == "mixed@x.com"
    assert obj.confirm == "mixed@x.com"


def test_email_field_rejects_invalid_at_dorm_assignment():
    """Regression: dorm's ``EmailField.to_python`` now validates at
    assignment time, so ``Customer(email="example")`` (and therefore
    ``Customer.objects.create(email="example")``) raises before any row
    is written. The Pydantic schema treats it as plain ``str`` — dorm
    enforces the format itself, no email-validator dependency."""
    from dorm.exceptions import ValidationError as DormValidationError
    import dorm

    class _C(dorm.Model):
        name = dorm.CharField(max_length=100)
        email = dorm.EmailField()

        class Meta:
            db_table = "test_email_validate"
            app_label = "tests"

    # Bogus value rejected on construction — same path Customer.objects.create() uses.
    with pytest.raises(DormValidationError):
        _C(name="x", email="example")
    with pytest.raises(DormValidationError):
        _C(name="x", email="string")

    # A real address constructs fine.
    obj = _C(name="x", email="user@example.com")
    assert obj.email == "user@example.com"


def test_email_field_blank_or_null_allowed_when_configured():
    """Empty string / None must still be accepted when the field allows it
    (otherwise blank=True / null=True would be useless)."""
    import dorm

    class _C2(dorm.Model):
        name = dorm.CharField(max_length=100)
        email = dorm.EmailField(null=True, blank=True)

        class Meta:
            db_table = "test_email_validate_optional"
            app_label = "tests"

    # None and "" pass through; only non-empty bogus strings raise.
    _C2(name="x", email=None)
    _C2(name="x", email="")


def test_meta_subclass_can_be_subclassed_further():
    """A schema declared via Meta is itself a regular class — usable as a
    base for further specialization."""

    class UserOut(DormSchema):
        class Meta:
            model = Author
            fields = ("id", "name")

    class AdminUserOut(UserOut):
        is_admin: bool = False

    assert "is_admin" in AdminUserOut.model_fields
    # Inherits the Meta-derived ones.
    assert "name" in AdminUserOut.model_fields
    assert "id" in AdminUserOut.model_fields


# ── Async path: response_model with async dorm queries ────────────────────────


async def test_async_create_and_validate():
    """Mirrors the typical FastAPI async route pattern."""
    UserOut = schema_for(Author)
    a = await Author.objects.acreate(
        name="AsyncFA", age=40, email="afa@x.com"
    )
    try:
        out = UserOut.model_validate(a)
        assert out.name == "AsyncFA"  # type: ignore
        assert out.age == 40  # type: ignore
    finally:
        await a.adelete()
