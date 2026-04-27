"""Pydantic interop coverage for the 2.1 field types.

The original ``test_pydantic_interop.py`` covers the v2.0 type map.
This file exercises every field added in 2.1: DurationField, EnumField,
CITextField, ArrayField, GeneratedField, RangeField subclasses and
FileField — making sure each one wires the right Python type into
``DormSchema`` / ``schema_for``.

Schema constructors / attributes go through ``model_validate`` /
``model_dump`` / ``getattr`` here, mirroring
``test_pydantic_interop.py``: dorm's schemas are built dynamically by
a metaclass, and ty can't see fields that don't exist at
class-definition time.
"""

from __future__ import annotations

import datetime
import enum
from decimal import Decimal
from typing import Any, get_args, get_origin

import pytest

pytest.importorskip("pydantic")

from pydantic import ValidationError

import dorm
from dorm.contrib.pydantic import (
    _FIELD_TYPE_MAP,
    DormSchema,
    _field_to_type,
    schema_for,
)
from dorm.storage import ContentFile, FieldFile


# ── Per-field-type unit checks ───────────────────────────────────────────────


class Priority(enum.Enum):
    LOW = "low"
    HIGH = "high"


class JobLevel(enum.IntEnum):
    JUNIOR = 1
    SENIOR = 3


class TestFieldToTypeMappings:
    """Every new field type produces the right annotation from
    ``_field_to_type``. Anything that returns Any is intentional —
    documented as 'override on your DormSchema if you need a typed
    surface'."""

    def test_duration_maps_to_timedelta(self):
        assert _field_to_type(dorm.DurationField()) is datetime.timedelta

    def test_enum_returns_the_enum_class(self):
        assert _field_to_type(dorm.EnumField(Priority)) is Priority
        assert _field_to_type(dorm.EnumField(JobLevel)) is JobLevel

    def test_citext_falls_through_to_textfield_str(self):
        # CITextField inherits from TextField, so the generic str
        # mapping wins via inheritance — no extra entry needed.
        assert _field_to_type(dorm.CITextField()) is str

    def test_array_field_yields_parameterised_list(self):
        annotated = _field_to_type(dorm.ArrayField(dorm.CharField(max_length=20)))
        assert get_origin(annotated) is list
        assert get_args(annotated) == (str,)

    def test_array_of_enum(self):
        # Inner mapping recurses, so an array of enums surfaces as
        # ``list[Priority]`` — Pydantic validates each element.
        annotated = _field_to_type(dorm.ArrayField(dorm.EnumField(Priority)))
        assert get_origin(annotated) is list
        assert get_args(annotated) == (Priority,)

    def test_generated_recurses_into_output_field(self):
        f = dorm.GeneratedField(
            expression="quantity * price",
            output_field=dorm.DecimalField(max_digits=10, decimal_places=2),
        )
        assert _field_to_type(f) is Decimal

    def test_range_field_family_is_any(self):
        # Documented "users override" — make sure it doesn't fall
        # through to the default Any-by-omission branch silently.
        for cls in (
            dorm.IntegerRangeField,
            dorm.BigIntegerRangeField,
            dorm.DecimalRangeField,
            dorm.DateRangeField,
            dorm.DateTimeRangeField,
        ):
            assert _field_to_type(cls()) is Any, f"{cls.__name__} should map to Any"

    def test_file_field_is_annotated_str_with_coercer(self):
        # The annotation is ``Annotated[str, BeforeValidator(...)]``;
        # what matters is that ``str`` is the underlying type so the
        # JSON schema looks like a normal string field.
        annotated = _field_to_type(dorm.FileField())
        # ``get_args`` on ``Annotated[X, ...]`` returns ``(X, *metadata)``.
        args = get_args(annotated)
        assert args[0] is str
        assert any("BeforeValidator" in repr(m) for m in args[1:])


# ── Coverage audit: every concrete field has a mapping ──────────────────────


def test_every_concrete_field_resolves_to_a_specific_type():
    """A regression guard: when someone adds a new field type to dorm,
    they should also wire it into ``_field_to_type`` (or accept the
    explicit ``Any`` fallback). This iterates the public field
    surface and complains about anything that quietly drops to
    ``Any`` without being on the documented allow-list."""
    # Fields we *intentionally* leave at Any (range types — see
    # docstring on ``_FIELD_TYPE_MAP``). JSONField is explicitly Any.
    intentional_any = {
        dorm.JSONField,
        dorm.RangeField,
        dorm.IntegerRangeField,
        dorm.BigIntegerRangeField,
        dorm.DecimalRangeField,
        dorm.DateRangeField,
        dorm.DateTimeRangeField,
    }
    # Constructor kwargs to instantiate each field for the probe.
    factories: dict[type, Any] = {
        dorm.AutoField: lambda: dorm.AutoField(),
        dorm.BigAutoField: lambda: dorm.BigAutoField(),
        dorm.SmallAutoField: lambda: dorm.SmallAutoField(),
        dorm.IntegerField: lambda: dorm.IntegerField(),
        dorm.SmallIntegerField: lambda: dorm.SmallIntegerField(),
        dorm.BigIntegerField: lambda: dorm.BigIntegerField(),
        dorm.PositiveIntegerField: lambda: dorm.PositiveIntegerField(),
        dorm.PositiveSmallIntegerField: lambda: dorm.PositiveSmallIntegerField(),
        dorm.FloatField: lambda: dorm.FloatField(),
        dorm.DecimalField: lambda: dorm.DecimalField(),
        dorm.CharField: lambda: dorm.CharField(max_length=10),
        dorm.TextField: lambda: dorm.TextField(),
        dorm.BooleanField: lambda: dorm.BooleanField(),
        dorm.NullBooleanField: lambda: dorm.NullBooleanField(),
        dorm.DateField: lambda: dorm.DateField(),
        dorm.TimeField: lambda: dorm.TimeField(),
        dorm.DateTimeField: lambda: dorm.DateTimeField(),
        dorm.EmailField: lambda: dorm.EmailField(),
        dorm.URLField: lambda: dorm.URLField(),
        dorm.SlugField: lambda: dorm.SlugField(),
        dorm.UUIDField: lambda: dorm.UUIDField(),
        dorm.IPAddressField: lambda: dorm.IPAddressField(),
        dorm.GenericIPAddressField: lambda: dorm.GenericIPAddressField(),
        dorm.JSONField: lambda: dorm.JSONField(),
        dorm.BinaryField: lambda: dorm.BinaryField(),
        dorm.ArrayField: lambda: dorm.ArrayField(dorm.IntegerField()),
        dorm.GeneratedField: lambda: dorm.GeneratedField(
            expression="1", output_field=dorm.IntegerField()
        ),
        dorm.DurationField: lambda: dorm.DurationField(),
        dorm.EnumField: lambda: dorm.EnumField(Priority),
        dorm.CITextField: lambda: dorm.CITextField(),
        dorm.FileField: lambda: dorm.FileField(),
        dorm.IntegerRangeField: lambda: dorm.IntegerRangeField(),
        dorm.BigIntegerRangeField: lambda: dorm.BigIntegerRangeField(),
        dorm.DecimalRangeField: lambda: dorm.DecimalRangeField(),
        dorm.DateRangeField: lambda: dorm.DateRangeField(),
        dorm.DateTimeRangeField: lambda: dorm.DateTimeRangeField(),
        dorm.RangeField: lambda: dorm.IntegerRangeField(),  # base is abstract
    }

    failures: list[str] = []
    for cls, factory in factories.items():
        try:
            field = factory()
        except Exception as exc:  # pragma: no cover — would mean a broken factory
            failures.append(f"{cls.__name__}: cannot construct → {exc}")
            continue
        annotation = _field_to_type(field)
        if annotation is Any and cls not in intentional_any:
            failures.append(
                f"{cls.__name__} silently fell through to Any. "
                "Add it to _FIELD_TYPE_MAP or _field_to_type."
            )

    assert not failures, "\n".join(failures)


def test_field_type_map_order_keeps_subclasses_first():
    """``isinstance`` short-circuits on the first match, so subclasses
    must appear before their parents in :data:`_FIELD_TYPE_MAP` or
    they get the parent mapping by mistake."""
    # The interesting cases: PositiveSmallIntegerField → SmallIntegerField
    # → BigIntegerField → IntegerField. The map orders them deepest first.
    order = [cls for cls, _ in _FIELD_TYPE_MAP]
    psi = order.index(dorm.PositiveSmallIntegerField)
    pi = order.index(dorm.PositiveIntegerField)
    si = order.index(dorm.SmallIntegerField)
    ii = order.index(dorm.IntegerField)
    assert psi < pi < ii
    assert si < ii


# ── End-to-end via DormSchema ───────────────────────────────────────────────


class TestDormSchemaWithNewFields:
    def test_duration_field_round_trips_via_dormschema(self):
        class _Job(dorm.Model):
            name = dorm.CharField(max_length=20)
            timeout = dorm.DurationField()

            class Meta:
                db_table = "pyd_jobs_2_1"

        class JobSchema(DormSchema):
            class Meta:
                model = _Job

        s = JobSchema.model_validate(
            {"name": "seed", "timeout": datetime.timedelta(minutes=5)}
        )
        assert s.model_dump()["timeout"] == datetime.timedelta(minutes=5)
        # Annotation check via ``model_fields`` — visible to type checkers.
        assert JobSchema.model_fields["timeout"].annotation is datetime.timedelta

    def test_enum_field_validates_membership(self):
        class _Task(dorm.Model):
            name = dorm.CharField(max_length=20)
            priority = dorm.EnumField(Priority, default=Priority.LOW)

            class Meta:
                db_table = "pyd_tasks_2_1"

        class TaskSchema(DormSchema):
            class Meta:
                model = _Task

        # Member instance accepted.
        TaskSchema.model_validate({"name": "x", "priority": Priority.HIGH})
        # Bare value also coerced (Pydantic v2 enum mode).
        TaskSchema.model_validate({"name": "x", "priority": "low"})
        # Anything else → ValidationError, so a FastAPI request body
        # with an unknown priority gets the standard 422.
        with pytest.raises(ValidationError):
            TaskSchema.model_validate({"name": "x", "priority": "urgent"})
        # ``priority`` has a default in the model so the schema treats
        # it as optional → ``Priority | None``.
        assert TaskSchema.model_fields["priority"].annotation == (Priority | None)

    def test_array_field_validates_element_type(self):
        class _Article(dorm.Model):
            title = dorm.CharField(max_length=50)
            tags = dorm.ArrayField(dorm.CharField(max_length=20), null=True)

            class Meta:
                db_table = "pyd_articles_2_1"

        class ArticleSchema(DormSchema):
            class Meta:
                model = _Article

        s = ArticleSchema.model_validate(
            {"title": "x", "tags": ["python", "orm"]}
        )
        assert s.model_dump()["tags"] == ["python", "orm"]
        # Element type is enforced — int elements get rejected with a
        # clean ValidationError rather than silently coerced.
        with pytest.raises(ValidationError):
            ArticleSchema.model_validate({"title": "y", "tags": [1, 2]})
        assert ArticleSchema.model_fields["tags"].annotation == (list[str] | None)

    def test_generated_field_uses_output_field_type(self):
        class _Order(dorm.Model):
            qty = dorm.IntegerField()
            price = dorm.DecimalField(max_digits=10, decimal_places=2)
            total = dorm.GeneratedField(
                expression="qty * price",
                output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
            )

            class Meta:
                db_table = "pyd_orders_2_1"

        Schema = schema_for(_Order)
        # GeneratedField inherits ``editable=False, null=True``, so its
        # column is optional in the schema.
        assert Schema.model_fields["total"].annotation == (Decimal | None)

    def test_citext_field_is_str_in_schema(self):
        class _Mailbox(dorm.Model):
            email = dorm.CITextField(unique=True)

            class Meta:
                db_table = "pyd_mailboxes_2_1"

        Schema = schema_for(_Mailbox)
        assert Schema.model_fields["email"].annotation is str

    def test_range_field_serialises_as_any(self):
        # Mapped to Any so users can override with a typed schema
        # without dorm forcing a particular shape on them.
        class _Reservation(dorm.Model):
            during = dorm.DateTimeRangeField(null=True)

            class Meta:
                db_table = "pyd_reservations_2_1"

        Schema = schema_for(_Reservation)
        # The schema permits anything (intentional fallback). Assert
        # the surface looks like ``Any | None`` — i.e. nullable + no
        # element type constraint — without depending on whether the
        # runtime collapses the union.
        annotation = Schema.model_fields["during"].annotation
        # Either: raw Any, ``Any | None`` (Union[Any, None]), or
        # the parameterised optional. All acceptable.
        is_any = annotation is Any
        is_union_with_any = get_origin(annotation) is type(int | None) or any(
            arg is Any for arg in get_args(annotation)
        )
        assert is_any or is_union_with_any, (
            f"RangeField annotation expected to permit Any, got {annotation!r}"
        )

    def test_file_field_accepts_string_and_field_file(self):
        class _Doc(dorm.Model):
            name = dorm.CharField(max_length=20)
            attachment = dorm.FileField(upload_to="x/", null=True, blank=True)

            class Meta:
                db_table = "pyd_docs_2_1"

        class DocSchema(DormSchema):
            class Meta:
                model = _Doc

        # Plain string (the typical "I already know the storage name" case).
        s = DocSchema.model_validate({"name": "a", "attachment": "x/file.pdf"})
        assert s.model_dump()["attachment"] == "x/file.pdf"

        # FieldFile (what ``from_attributes`` reads from a dorm
        # instance). The BeforeValidator unwraps to ``.name``.
        instance_field = _Doc._meta.get_field("attachment")
        ff = FieldFile(_Doc(name="a"), instance_field, "x/file.pdf")
        s2 = DocSchema.model_validate({"name": "a", "attachment": ff})
        assert s2.model_dump()["attachment"] == "x/file.pdf"

        # ``None`` round-trips through the optional wrapper.
        s3 = DocSchema.model_validate({"name": "a", "attachment": None})
        assert s3.model_dump()["attachment"] is None

    def test_file_field_via_from_attributes_reads_storage_name(self, tmp_path):
        """End-to-end: a model with a saved FieldFile serialised by
        Pydantic via ``from_attributes`` exposes the storage name as
        a plain string (FastAPI ``response_model`` case)."""
        from dorm.db.connection import get_connection
        from dorm.migrations.operations import _field_to_column_sql
        from dorm.storage import reset_storages

        class _DocFA(dorm.Model):
            name = dorm.CharField(max_length=20)
            attachment = dorm.FileField(upload_to="from-attrs/", null=True, blank=True)

            class Meta:
                db_table = "pyd_docs_fa"

        # Wire a fresh FileSystemStorage at tmp_path so the test
        # doesn't pollute other tests' STORAGES.
        saved = getattr(dorm.settings, "STORAGES", {})
        reset_storages()
        try:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES={
                    "default": {
                        "BACKEND": "dorm.storage.FileSystemStorage",
                        "OPTIONS": {"location": str(tmp_path)},
                    }
                },
            )
            conn = get_connection()
            cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
            conn.execute_script(f'DROP TABLE IF EXISTS "pyd_docs_fa"{cascade}')
            cols = [
                _field_to_column_sql(f.name, f, conn)
                for f in _DocFA._meta.fields
                if f.db_type(conn)
            ]
            conn.execute_script(
                'CREATE TABLE "pyd_docs_fa" (\n  '
                + ",\n  ".join(filter(None, cols))
                + "\n)"
            )

            class DocSchemaFA(DormSchema):
                class Meta:
                    model = _DocFA

            doc = _DocFA(name="x")
            doc.attachment = ContentFile(b"hi", name="hi.txt")
            doc.save()

            # ``model_validate`` reads via ``from_attributes`` —
            # exactly the path FastAPI uses.
            schema = DocSchemaFA.model_validate(doc)
            dumped = schema.model_dump()
            assert isinstance(dumped["attachment"], str)
            assert dumped["attachment"] == "from-attrs/hi.txt"

            conn.execute_script(f'DROP TABLE IF EXISTS "pyd_docs_fa"{cascade}')
        finally:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES=saved,
            )
            reset_storages()
