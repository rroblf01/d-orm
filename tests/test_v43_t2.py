"""Tier-2 extra field types for v4.3."""
from __future__ import annotations

import decimal

import pytest


# ── MoneyField ──────────────────────────────────────────────────────────────


class TestMoneyField:
    def test_money_value_object(self):
        from dorm.contrib.extra_fields import Money

        m = Money(amount=decimal.Decimal("10.00"), currency="EUR")
        assert str(m) == "10.00 EUR"

    def test_field_accepts_decimal(self):
        from dorm.contrib.extra_fields import MoneyField

        f = MoneyField(currency="EUR")
        v = f.to_python(decimal.Decimal("1.23"))
        assert v.amount == decimal.Decimal("1.23")
        assert v.currency == "EUR"

    def test_field_accepts_int_str_float(self):
        from dorm.contrib.extra_fields import MoneyField

        f = MoneyField(currency="USD")
        assert f.to_python(1).amount == decimal.Decimal("1")
        assert f.to_python("2.50").amount == decimal.Decimal("2.50")
        assert f.to_python(3.14).amount == decimal.Decimal("3.14")

    def test_field_rejects_unknown_type(self):
        from dorm.contrib.extra_fields import MoneyField
        from dorm.exceptions import ValidationError

        f = MoneyField()
        with pytest.raises(ValidationError):
            f.to_python([1, 2])

    def test_currency_mismatch_rejected(self):
        from dorm.contrib.extra_fields import Money, MoneyField
        from dorm.exceptions import ValidationError

        f = MoneyField(currency="EUR")
        with pytest.raises(ValidationError, match="currency mismatch"):
            f.to_python(Money(amount=decimal.Decimal("1"), currency="USD"))

    def test_invalid_currency_rejected(self):
        from dorm.contrib.extra_fields import MoneyField

        with pytest.raises(ValueError):
            MoneyField(currency="XX")  # not 3 letters

    def test_db_prep_strips_currency(self):
        from dorm.contrib.extra_fields import Money, MoneyField

        f = MoneyField(currency="USD")
        prepped = f.get_db_prep_value(Money(decimal.Decimal("1.50"), "USD"))
        assert prepped == decimal.Decimal("1.50")

    def test_from_db_value(self):
        from dorm.contrib.extra_fields import MoneyField

        f = MoneyField(currency="USD")
        v = f.from_db_value(decimal.Decimal("9.99"))
        assert v.amount == decimal.Decimal("9.99")
        assert v.currency == "USD"

    def test_deconstruct_preserves_currency(self):
        from dorm.contrib.extra_fields import MoneyField

        f = MoneyField(currency="JPY")
        _, _, _, kwargs = f.deconstruct()
        assert kwargs["currency"] == "JPY"


# ── SemverField ─────────────────────────────────────────────────────────────


class TestSemverField:
    @pytest.mark.parametrize(
        "v",
        ["1.0.0", "0.0.0", "10.20.30", "1.0.0-alpha", "1.0.0-rc.1", "1.0.0+build"],
    )
    def test_accepts_valid(self, v):
        from dorm.contrib.extra_fields import SemverField

        f = SemverField()
        assert f.to_python(v) == v

    @pytest.mark.parametrize("v", ["1", "1.0", "01.0.0", "1.0.0.0", "v1.0.0", ""])
    def test_rejects_invalid(self, v):
        from dorm.contrib.extra_fields import SemverField
        from dorm.exceptions import ValidationError

        f = SemverField()
        with pytest.raises(ValidationError):
            f.to_python(v)


# ── PhoneField ──────────────────────────────────────────────────────────────


class TestPhoneField:
    def test_accepts_e164(self):
        from dorm.contrib.extra_fields import PhoneField

        f = PhoneField()
        assert f.to_python("+34123456789") == "+34123456789"

    def test_strips_punctuation(self):
        from dorm.contrib.extra_fields import PhoneField

        f = PhoneField()
        assert f.to_python("+34 (123) 456-789") == "+34123456789"

    def test_rejects_short(self):
        from dorm.contrib.extra_fields import PhoneField
        from dorm.exceptions import ValidationError

        f = PhoneField()
        with pytest.raises(ValidationError):
            f.to_python("123")

    def test_rejects_letters(self):
        from dorm.contrib.extra_fields import PhoneField
        from dorm.exceptions import ValidationError

        f = PhoneField()
        with pytest.raises(ValidationError):
            f.to_python("+34abc12345")


# ── ColorField ──────────────────────────────────────────────────────────────


class TestColorField:
    def test_rrggbb_accepted(self):
        from dorm.contrib.extra_fields import ColorField

        assert ColorField().to_python("#aabbcc") == "#AABBCC"

    def test_rrggbbaa_accepted(self):
        from dorm.contrib.extra_fields import ColorField

        assert ColorField().to_python("#aabbcc80") == "#AABBCC80"

    def test_rejects_short_form(self):
        from dorm.contrib.extra_fields import ColorField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            ColorField().to_python("#fff")

    def test_rejects_missing_hash(self):
        from dorm.contrib.extra_fields import ColorField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            ColorField().to_python("aabbcc")


# ── JSONSchemaField ─────────────────────────────────────────────────────────


class TestJSONSchemaField:
    def test_requires_jsonschema(self):
        pytest.importorskip("jsonschema")
        from dorm.contrib.extra_fields import JSONSchemaField

        f = JSONSchemaField(
            schema={"type": "object", "properties": {"x": {"type": "integer"}}}
        )
        assert f.to_python({"x": 1}) == {"x": 1}

    def test_rejects_violating_payload(self):
        pytest.importorskip("jsonschema")
        from dorm.contrib.extra_fields import JSONSchemaField
        from dorm.exceptions import ValidationError

        f = JSONSchemaField(
            schema={"type": "object", "properties": {"x": {"type": "integer"}}}
        )
        with pytest.raises(ValidationError):
            f.to_python({"x": "not-an-int"})

    def test_bad_schema_rejected_at_construction(self):
        pytest.importorskip("jsonschema")
        from dorm.contrib.extra_fields import JSONSchemaField

        with pytest.raises(Exception):  # noqa: B017 - schema-validator dependent
            JSONSchemaField(schema={"type": "not-a-real-type"})

    def test_non_dict_schema_rejected(self):
        pytest.importorskip("jsonschema")
        from dorm.contrib.extra_fields import JSONSchemaField

        with pytest.raises(TypeError):
            JSONSchemaField(schema="not a dict")  # type: ignore[arg-type]  # ty:ignore[invalid-argument-type]
