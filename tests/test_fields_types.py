"""Pure unit tests for individual field types — to_python, get_db_prep_value,
from_db_value, validate, db_type. These don't need a live database; they
just exercise the conversion / validation surface that every backend
relies on. Big coverage win on dorm/fields.py without per-test DB setup."""

from __future__ import annotations

import datetime
import decimal
import json
import uuid

import pytest

from dorm.exceptions import ValidationError
from dorm.fields import (
    ArrayField,
    BigIntegerField,
    BinaryField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    EmailField,
    FloatField,
    GenericIPAddressField,
    IntegerField,
    IPAddressField,
    JSONField,
    PositiveIntegerField,
    PositiveSmallIntegerField,
    SlugField,
    SmallIntegerField,
    TextField,
    TimeField,
    URLField,
    UUIDField,
)


class _SQLite:
    vendor = "sqlite"


class _Postgres:
    vendor = "postgresql"


SQLITE = _SQLite()
PG = _Postgres()


# ── IntegerField family ───────────────────────────────────────────────────────


def test_integer_field_to_python_coerces_strings():
    f = IntegerField()
    assert f.to_python("42") == 42
    assert f.to_python(7.0) == 7
    assert f.to_python(None) is None


def test_small_integer_field_db_type_per_vendor():
    assert SmallIntegerField().db_type(PG) == "SMALLINT"
    assert SmallIntegerField().db_type(SQLITE) == "INTEGER"


def test_big_integer_field_db_type_per_vendor():
    assert BigIntegerField().db_type(PG) == "BIGINT"
    assert BigIntegerField().db_type(SQLITE) == "INTEGER"


def test_positive_integer_field_rejects_negative():
    f = PositiveIntegerField()
    f.name = "qty"
    with pytest.raises(ValidationError):
        f.validate(-1, model_instance=None)
    f.validate(0, model_instance=None)
    f.validate(99, model_instance=None)


def test_positive_small_integer_field_db_type():
    assert PositiveSmallIntegerField().db_type(PG) == "SMALLINT"
    assert PositiveSmallIntegerField().db_type(SQLITE) == "INTEGER"


# ── FloatField / DecimalField ─────────────────────────────────────────────────


def test_float_field_coerces_int_and_string():
    f = FloatField()
    assert f.to_python("3.14") == pytest.approx(3.14)
    assert f.to_python(2) == 2.0
    assert f.to_python(None) is None


def test_decimal_field_round_trip_uses_strings():
    f = DecimalField(max_digits=8, decimal_places=2)
    val = f.to_python("12.34")
    assert isinstance(val, decimal.Decimal)
    assert val == decimal.Decimal("12.34")
    # Round-tripping through get_db_prep_value should preserve value
    db_val = f.get_db_prep_value(val)
    assert isinstance(db_val, str)
    assert decimal.Decimal(db_val) == val


def test_decimal_field_db_type_format():
    f = DecimalField(max_digits=12, decimal_places=4)
    assert f.db_type(SQLITE) == "NUMERIC(12, 4)"


def test_decimal_field_get_db_prep_value_handles_none():
    f = DecimalField()
    assert f.get_db_prep_value(None) is None


# ── CharField / TextField ─────────────────────────────────────────────────────


def test_char_field_validate_rejects_too_long():
    f = CharField(max_length=5)
    f.name = "code"
    with pytest.raises(ValidationError):
        f.validate("toolong", model_instance=None)
    f.validate("ok", model_instance=None)


def test_char_field_db_type_includes_max_length():
    assert CharField(max_length=42).db_type(SQLITE) == "VARCHAR(42)"


def test_text_field_to_python_stringifies():
    f = TextField()
    assert f.to_python(123) == "123"
    assert f.to_python(None) is None
    assert f.db_type(SQLITE) == "TEXT"


# ── BooleanField ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("true", True),
        ("YES", True),
        ("1", True),
        ("no", False),
        ("0", False),
        (None, None),
    ],
)
def test_boolean_field_to_python(value, expected):
    f = BooleanField()
    assert f.to_python(value) == expected


def test_boolean_field_db_type_per_vendor():
    assert BooleanField().db_type(PG) == "BOOLEAN"
    assert BooleanField().db_type(SQLITE) == "INTEGER"


def test_boolean_field_from_db_value_normalises_int():
    f = BooleanField()
    assert f.from_db_value(1) is True
    assert f.from_db_value(0) is False
    assert f.from_db_value(None) is None


# ── DateField / TimeField / DateTimeField ─────────────────────────────────────


def test_date_field_parses_iso_string():
    f = DateField()
    assert f.to_python("2026-04-26") == datetime.date(2026, 4, 26)


def test_date_field_extracts_from_datetime():
    f = DateField()
    dt = datetime.datetime(2026, 4, 26, 12, 30)
    assert f.to_python(dt) == datetime.date(2026, 4, 26)


def test_date_field_get_db_prep_value_returns_isoformat():
    f = DateField()
    assert f.get_db_prep_value(datetime.date(2026, 4, 26)) == "2026-04-26"


def test_date_field_from_db_value_parses_string():
    f = DateField()
    assert f.from_db_value("2026-04-26") == datetime.date(2026, 4, 26)
    assert f.from_db_value(None) is None


def test_time_field_parses_iso_string():
    f = TimeField()
    assert f.to_python("12:30:45") == datetime.time(12, 30, 45)


def test_time_field_round_trip():
    f = TimeField()
    t = datetime.time(8, 15)
    assert f.get_db_prep_value(t) == "08:15:00"
    assert f.from_db_value("08:15:00") == t


def test_datetime_field_parses_iso_string():
    f = DateTimeField()
    val = f.to_python("2026-04-26T12:30:45")
    assert val == datetime.datetime(2026, 4, 26, 12, 30, 45)


def test_datetime_field_db_type_per_vendor():
    assert DateTimeField().db_type(PG) == "TIMESTAMP"
    assert DateTimeField().db_type(SQLITE) == "DATETIME"


def test_datetime_field_auto_now_add_default_set():
    f = DateTimeField(auto_now_add=True)
    assert callable(f.default)
    assert f.editable is False


# ── EmailField / URLField / SlugField ─────────────────────────────────────────


def test_email_field_default_max_length():
    f = EmailField()
    assert f.max_length == 254


def test_email_field_rejects_invalid_on_assignment():
    f = EmailField()
    f.name = "email"
    with pytest.raises(ValidationError):
        f.to_python("not-an-email")


def test_email_field_accepts_valid():
    f = EmailField()
    assert f.to_python("a@b.co") == "a@b.co"
    assert f.to_python(None) is None
    assert f.to_python("") == ""


def test_email_field_validate_rejects_invalid():
    f = EmailField()
    f.name = "email"
    with pytest.raises(ValidationError):
        f.validate("nope", model_instance=None)


def test_url_field_default_max_length():
    assert URLField().max_length == 200


def test_slug_field_validate_rejects_spaces():
    f = SlugField()
    f.name = "slug"
    with pytest.raises(ValidationError):
        f.validate("not a slug", model_instance=None)
    f.validate("hello-world_42", model_instance=None)


def test_slug_field_indexed_by_default():
    f = SlugField()
    assert f.db_index is True
    assert f.max_length == 50


# ── UUIDField ─────────────────────────────────────────────────────────────────


def test_uuid_field_to_python_accepts_string_and_uuid():
    f = UUIDField()
    u = uuid.uuid4()
    assert f.to_python(u) is u
    assert f.to_python(str(u)) == u
    assert f.to_python(None) is None


def test_uuid_field_get_db_prep_value_stringifies():
    f = UUIDField()
    u = uuid.uuid4()
    assert f.get_db_prep_value(u) == str(u)
    assert f.get_db_prep_value(None) is None


def test_uuid_field_from_db_value_parses_string():
    f = UUIDField()
    u = uuid.uuid4()
    assert f.from_db_value(str(u)) == u
    assert f.from_db_value(u) == u
    assert f.from_db_value(None) is None


def test_uuid_field_db_type_per_vendor():
    assert UUIDField().db_type(PG) == "UUID"
    assert UUIDField().db_type(SQLITE) == "VARCHAR(36)"


# ── IPAddressField / GenericIPAddressField ────────────────────────────────────


def test_ip_address_field_accepts_v4():
    f = IPAddressField()
    assert f.to_python("10.0.0.1") == "10.0.0.1"


def test_ip_address_field_rejects_v6():
    f = IPAddressField()
    with pytest.raises(ValidationError):
        f.to_python("::1")


def test_ip_address_field_rejects_garbage():
    f = IPAddressField()
    with pytest.raises(ValidationError):
        f.to_python("not.an.address")


def test_ip_address_field_db_type_per_vendor():
    assert IPAddressField().db_type(PG) == "INET"
    assert IPAddressField().db_type(SQLITE) == "VARCHAR(39)"


def test_generic_ip_field_accepts_v4_and_v6():
    f = GenericIPAddressField()
    assert f.to_python("10.0.0.1") == "10.0.0.1"
    assert f.to_python("::1") == "::1"


def test_generic_ip_field_rejects_garbage():
    f = GenericIPAddressField()
    with pytest.raises(ValidationError):
        f.to_python("nonsense")


# ── JSONField ─────────────────────────────────────────────────────────────────


def test_json_field_to_python_parses_string():
    f = JSONField()
    assert f.to_python('{"a": 1}') == {"a": 1}
    assert f.to_python(None) is None
    assert f.to_python([1, 2, 3]) == [1, 2, 3]   # passthrough for already-decoded


def test_json_field_get_db_prep_value_serialises():
    f = JSONField()
    assert json.loads(f.get_db_prep_value({"a": 1})) == {"a": 1}
    assert f.get_db_prep_value(None) is None


def test_json_field_from_db_value_parses_string():
    f = JSONField()
    assert f.from_db_value('[1, 2]') == [1, 2]
    assert f.from_db_value(None) is None
    assert f.from_db_value({"already": "decoded"}) == {"already": "decoded"}


def test_json_field_db_type_per_vendor():
    assert JSONField().db_type(PG) == "JSONB"
    assert JSONField().db_type(SQLITE) == "TEXT"


# ── ArrayField ────────────────────────────────────────────────────────────────


def test_array_field_db_type_postgres():
    f = ArrayField(IntegerField())
    assert f.db_type(PG) == "INTEGER[]"


def test_array_field_raises_on_sqlite():
    f = ArrayField(CharField(max_length=10))
    with pytest.raises(NotImplementedError):
        f.db_type(SQLITE)


def test_array_field_to_python_coerces_inner_values():
    f = ArrayField(IntegerField())
    assert f.to_python(["1", "2", "3"]) == [1, 2, 3]
    assert f.to_python(None) is None


def test_array_field_to_python_accepts_iterables():
    f = ArrayField(IntegerField())
    assert f.to_python((10, 20, 30)) == [10, 20, 30]
    assert f.to_python(iter([5])) == [5]


def test_array_field_get_db_prep_value_delegates_to_inner():
    f = ArrayField(IntegerField())
    assert f.get_db_prep_value([1, 2]) == [1, 2]
    assert f.get_db_prep_value(None) is None


# ── BinaryField ───────────────────────────────────────────────────────────────


def test_binary_field_to_python_normalises_bytes():
    f = BinaryField()
    assert f.to_python(b"abc") == b"abc"
    assert f.to_python(bytearray(b"abc")) == b"abc"
    assert f.to_python(memoryview(b"abc")) == b"abc"
    assert f.to_python(None) is None


def test_binary_field_db_type_per_vendor():
    assert BinaryField().db_type(PG) == "BYTEA"
    assert BinaryField().db_type(SQLITE) == "BLOB"


# ── Validators ────────────────────────────────────────────────────────────────


def test_min_max_value_validators():
    from dorm.validators import MaxValueValidator, MinValueValidator

    MinValueValidator(0)(0)
    with pytest.raises(ValidationError):
        MinValueValidator(0)(-1)
    MaxValueValidator(100)(100)
    with pytest.raises(ValidationError):
        MaxValueValidator(100)(101)
    # repr just exists for debug printing
    assert "MinValueValidator" in repr(MinValueValidator(0))
    assert "MaxValueValidator" in repr(MaxValueValidator(100))


def test_min_max_length_validators():
    from dorm.validators import MaxLengthValidator, MinLengthValidator

    MinLengthValidator(3)("abcd")
    with pytest.raises(ValidationError):
        MinLengthValidator(3)("ab")
    MaxLengthValidator(5)("ab")
    with pytest.raises(ValidationError):
        MaxLengthValidator(5)("toolong")
    assert "MinLengthValidator" in repr(MinLengthValidator(3))
    assert "MaxLengthValidator" in repr(MaxLengthValidator(5))


def test_regex_validator():
    from dorm.validators import RegexValidator

    rv = RegexValidator(r"^[a-z]+$")
    rv("abc")
    with pytest.raises(ValidationError):
        rv("ABC")
    assert "RegexValidator" in repr(rv)


def test_email_validator_via_call_class():
    from dorm.validators import EmailValidator

    EmailValidator()("a@b.co")
    with pytest.raises(ValidationError):
        EmailValidator()("not-an-email")
    assert "EmailValidator" in repr(EmailValidator())


# ── Field options ─────────────────────────────────────────────────────────────


def test_field_choices_validation():
    f = CharField(max_length=10, choices=[("a", "A"), ("b", "B")])
    f.name = "letter"
    f.validate("a", model_instance=None)
    with pytest.raises(ValidationError):
        f.validate("z", model_instance=None)


def test_field_validators_run_during_validate():
    from dorm.validators import MinValueValidator

    f = IntegerField(validators=[MinValueValidator(10)])
    f.name = "score"
    f.validate(20, model_instance=None)
    with pytest.raises(ValidationError):
        f.validate(5, model_instance=None)


def test_field_null_skips_validation_of_value():
    """null=True + value=None should be fine — neither validators nor
    type-specific checks should fire."""
    f = IntegerField(null=True)
    f.name = "maybe"
    f.validate(None, model_instance=None)


def test_field_null_false_rejects_none():
    """A non-null field must reject None at validate() time."""
    f = CharField(max_length=10, null=False)
    f.name = "name"
    with pytest.raises(ValidationError):
        f.validate(None, model_instance=None)


def test_field_has_default_detects_callable_and_value():
    assert IntegerField(default=0).has_default() is True
    assert IntegerField(default=lambda: 0).has_default() is True
    assert IntegerField().has_default() is False


def test_field_get_default_calls_callable():
    f = IntegerField(default=lambda: 42)
    assert f.get_default() == 42
    assert IntegerField(default=7).get_default() == 7
    assert IntegerField().get_default() is None
