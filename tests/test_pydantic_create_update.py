"""Tests for :func:`dorm.contrib.pydantic.create_schema_for` and
:func:`update_schema_for`.

Both helpers thin-wrap :func:`schema_for` with sensible Create / PATCH
defaults: auto-incrementing PKs and ``GeneratedField`` columns are
stripped (server-controlled), and the Update variant additionally
flips every remaining field to optional with default ``None`` so a
caller can send a partial body.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic")

from pydantic import BaseModel, ValidationError

import dorm
from dorm.contrib.pydantic import create_schema_for, update_schema_for
from tests.models import Author


# ── create_schema_for ────────────────────────────────────────────────


class TestCreateSchemaFor:
    def test_drops_auto_pk(self):
        Schema = create_schema_for(Author)
        assert "id" not in Schema.model_fields
        assert "name" in Schema.model_fields

    def test_default_name_is_create(self):
        Schema = create_schema_for(Author)
        assert Schema.__name__ == "AuthorCreate"

    def test_custom_name_honoured(self):
        Schema = create_schema_for(Author, name="AuthorIn")
        assert Schema.__name__ == "AuthorIn"

    def test_required_fields_stay_required(self):
        Schema = create_schema_for(Author)
        # Author.name is non-null without a default → required in Create.
        f = Schema.model_fields["name"]
        assert f.is_required()
        with pytest.raises(ValidationError):
            Schema.model_validate({"age": 30, "email": "a@b.co"})

    def test_drops_generated_field(self):
        class _M(dorm.Model):
            price = dorm.DecimalField(max_digits=10, decimal_places=2)
            tax = dorm.GeneratedField(
                expression="price * 0.21",
                output_field=dorm.DecimalField(max_digits=10, decimal_places=2),
            )

            class Meta:
                db_table = "pyd_create_gen"
                app_label = "tests"

        Schema = create_schema_for(_M)
        assert "tax" not in Schema.model_fields

    def test_user_exclude_extends_default(self):
        Schema = create_schema_for(Author, exclude=("publisher",))
        assert "publisher" not in Schema.model_fields
        assert "id" not in Schema.model_fields  # auto-PK still dropped
        assert "name" in Schema.model_fields

    def test_validates_payload_round_trip(self):
        Schema = create_schema_for(Author)
        obj = Schema.model_validate({"name": "Bob", "age": 40, "email": "b@x.com"})
        assert obj.name == "Bob"  # type: ignore
        assert obj.age == 40  # type: ignore

    def test_create_schema_is_basemodel(self):
        Schema = create_schema_for(Author)
        assert issubclass(Schema, BaseModel)


# ── update_schema_for ────────────────────────────────────────────────


class TestUpdateSchemaFor:
    def test_default_name_is_update(self):
        Schema = update_schema_for(Author)
        assert Schema.__name__ == "AuthorUpdate"

    def test_drops_auto_pk_and_generated(self):
        Schema = update_schema_for(Author)
        assert "id" not in Schema.model_fields

    def test_every_remaining_field_is_optional(self):
        Schema = update_schema_for(Author)
        for fname, finfo in Schema.model_fields.items():
            assert not finfo.is_required(), (
                f"{fname} should be optional in Update schema, got required"
            )
            assert finfo.default is None, (
                f"{fname} default should be None for PATCH, got {finfo.default!r}"
            )

    def test_empty_body_validates(self):
        """PATCH semantics: an empty body is legal — the caller is
        signalling 'no fields to change'."""
        Schema = update_schema_for(Author)
        Schema.model_validate({})

    def test_partial_body_validates(self):
        Schema = update_schema_for(Author)
        obj = Schema.model_validate({"name": "Alice"})
        # ``model_dump(exclude_unset=True)`` is the public contract for
        # turning "field omitted" → not in dict.
        dumped = obj.model_dump(exclude_unset=True)
        assert dumped == {"name": "Alice"}

    def test_constraint_translation_still_applies_on_update(self):
        """``max_length`` on ``CharField`` must still be enforced —
        PATCH bodies aren't a free pass on validation."""
        Schema = update_schema_for(Author)
        # Author.name is CharField(max_length=100) — try 200 chars.
        with pytest.raises(ValidationError):
            Schema.model_validate({"name": "x" * 200})

    def test_user_exclude_combines_with_pk_drop(self):
        Schema = update_schema_for(Author, exclude=("email",))
        assert "id" not in Schema.model_fields
        assert "email" not in Schema.model_fields
        assert "name" in Schema.model_fields

    def test_full_round_trip_via_orm(self):
        """Sanity: Update schema → ``model_dump(exclude_unset=True)``
        → ``setattr`` → ``save`` actually persists the change."""
        from dorm.db.connection import get_connection
        conn = get_connection()
        if getattr(conn, "vendor", "sqlite") not in ("sqlite", "postgresql"):
            pytest.skip("backend not supported in test")

        a = Author.objects.create(name="orig", age=1, email="orig@x.com")
        try:
            UpdateSchema = update_schema_for(Author)
            payload = UpdateSchema.model_validate({"age": 99})
            for k, v in payload.model_dump(exclude_unset=True).items():
                setattr(a, k, v)
            a.save()
            reloaded = Author.objects.get(pk=a.pk)
            assert reloaded.age == 99
            assert reloaded.name == "orig"  # untouched
        finally:
            a.delete()
