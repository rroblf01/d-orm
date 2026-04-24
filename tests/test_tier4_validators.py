"""Tests for Tier-4.3: validators=[] on Field."""
from __future__ import annotations

import pytest

import dorm
from dorm.exceptions import ValidationError
from dorm.validators import (
    EmailValidator,
    MaxLengthValidator,
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    RegexValidator,
    validate_email,
)


# ── Model definitions ─────────────────────────────────────────────────────────

class ValidatedProduct(dorm.Model):
    name = dorm.CharField(
        max_length=200,
        validators=[MinLengthValidator(3), MaxLengthValidator(50)],
    )
    price = dorm.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(9999.99)],
    )
    sku = dorm.CharField(
        max_length=20,
        validators=[RegexValidator(r"^[A-Z]{2}-\d{4}$", "SKU must be XX-0000 format.")],
    )
    email = dorm.CharField(
        max_length=100,
        null=True,
        blank=True,
        validators=[validate_email],
    )

    class Meta:
        db_table = "val_products"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "val_products"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in ValidatedProduct._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "val_products" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )


# ── MinValueValidator / MaxValueValidator ─────────────────────────────────────

def test_min_value_passes():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234")
    p.full_clean()  # Should not raise


def test_min_value_fails():
    p = ValidatedProduct(name="Widget", price=-0.01, sku="AB-1234")
    with pytest.raises(ValidationError):
        p.full_clean()


def test_max_value_passes():
    p = ValidatedProduct(name="Widget", price=9999.99, sku="AB-1234")
    p.full_clean()


def test_max_value_fails():
    p = ValidatedProduct(name="Widget", price=10000.0, sku="AB-1234")
    with pytest.raises(ValidationError):
        p.full_clean()


def test_min_value_validator_directly():
    v = MinValueValidator(5)
    with pytest.raises(ValidationError):
        v(3)
    v(5)  # exact limit passes
    v(10)  # above limit passes


def test_max_value_validator_directly():
    v = MaxValueValidator(100)
    with pytest.raises(ValidationError):
        v(101)
    v(100)  # exact limit passes
    v(0)


# ── MinLengthValidator / MaxLengthValidator ───────────────────────────────────

def test_min_length_passes():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234")
    p.full_clean()


def test_min_length_fails():
    p = ValidatedProduct(name="Wi", price=1.0, sku="AB-1234")  # "Wi" has 2 chars < 3
    with pytest.raises(ValidationError):
        p.full_clean()


def test_max_length_fails():
    p = ValidatedProduct(name="W" * 51, price=1.0, sku="AB-1234")
    with pytest.raises(ValidationError):
        p.full_clean()


def test_min_length_validator_directly():
    v = MinLengthValidator(3)
    with pytest.raises(ValidationError):
        v("ab")
    v("abc")


def test_max_length_validator_directly():
    v = MaxLengthValidator(5)
    with pytest.raises(ValidationError):
        v("toolong")
    v("short")


# ── RegexValidator ────────────────────────────────────────────────────────────

def test_regex_passes():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234")
    p.full_clean()


def test_regex_fails():
    p = ValidatedProduct(name="Widget", price=1.0, sku="ab-1234")  # lowercase
    with pytest.raises(ValidationError):
        p.full_clean()


def test_regex_validator_directly():
    v = RegexValidator(r"^\d+$", "Only digits allowed.")
    v("12345")
    with pytest.raises(ValidationError) as exc_info:
        v("123abc")
    assert "Only digits allowed" in str(exc_info.value)


# ── EmailValidator ────────────────────────────────────────────────────────────

def test_email_validator_passes():
    v = EmailValidator()
    v("user@example.com")
    v("first.last+tag@sub.domain.org")


def test_email_validator_fails():
    v = EmailValidator()
    with pytest.raises(ValidationError):
        v("not-an-email")
    with pytest.raises(ValidationError):
        v("missing@tld")


def test_validate_email_shortcut():
    validate_email("hello@world.io")
    with pytest.raises(ValidationError):
        validate_email("bad-email")


def test_email_field_in_model_passes():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234", email="a@b.com")
    p.full_clean()


def test_email_field_in_model_fails():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234", email="notvalid")
    with pytest.raises(ValidationError):
        p.full_clean()


def test_email_field_null_skips_validation():
    p = ValidatedProduct(name="Widget", price=1.0, sku="AB-1234", email=None)
    p.full_clean()  # null value should not trigger email validator


# ── Custom callable validator ─────────────────────────────────────────────────

def test_custom_callable_validator():
    def no_spaces(value):
        if " " in value:
            raise ValidationError("Value must not contain spaces.")

    class SpacelessModel(dorm.Model):
        tag = dorm.CharField(max_length=50, validators=[no_spaces])

        class Meta:
            db_table = "val_spaceless"

    inst = SpacelessModel(tag="hello world")
    with pytest.raises(ValidationError):
        inst.full_clean()

    inst2 = SpacelessModel(tag="helloworld")
    inst2.full_clean()  # should not raise


# ── Validators preserved through deepcopy (abstract inheritance) ──────────────

def test_validators_preserved_after_deepcopy():
    class AbstractBase(dorm.Model):
        value = dorm.IntegerField(validators=[MinValueValidator(0)])

        class Meta:
            abstract = True

    class ConcreteModel(AbstractBase):
        class Meta:
            db_table = "val_concrete"

    field = ConcreteModel._meta.get_field("value")
    assert len(field.validators) == 1
    assert isinstance(field.validators[0], MinValueValidator)

    # The validator must still work
    inst = ConcreteModel(value=-1)
    with pytest.raises(ValidationError):
        inst.full_clean()
