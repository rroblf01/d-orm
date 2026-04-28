"""Round-trip tests for the migration writer's coverage of the 2.2 fields.

The writer's job is to produce a Python file that, when imported and
executed, reconstructs the original ``operations`` list. Each test
here drives a `CreateModel` op with one of the new field types,
calls :func:`write_migration`, then **executes** the generated file
in a clean namespace and verifies the rebuilt field has the same
shape as the input.

This is a regression guard for the gap that bit 2.1: the previous
writer hardcoded a small set of field types and silently produced
``EnumField()`` / ``FileField()`` calls without their real arguments
when fed the newer types.
"""

from __future__ import annotations

import ast
import enum
import importlib.util
import sys
import tempfile
from pathlib import Path
from typing import Any

import dorm
from dorm.migrations.operations import AddField, CreateModel
from dorm.migrations.writer import write_migration


# Module-scope enums so the writer's user-import collector can produce
# a valid ``from tests.test_migration_writer_new_fields import …`` line.
class Priority(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Level(enum.IntEnum):
    JUNIOR = 1
    SENIOR = 3


# ── Helpers ──────────────────────────────────────────────────────────────────


def _execute_migration_file(path: Path) -> Any:
    """Import the generated migration file as a module and return it.

    The migration must be syntactically valid Python and resolve every
    name it references — that's the contract callers later rely on
    when ``MigrationLoader`` imports it. Failing here means a real
    user running ``dorm migrate`` would crash with the same error.
    """
    # Sanity-parse first so a SyntaxError gives a clear message before
    # we hand the file to importlib.
    source = path.read_text()
    try:
        ast.parse(source)
    except SyntaxError as exc:
        raise AssertionError(
            f"Generated migration is not valid Python:\n{source}\n→ {exc}"
        ) from exc

    spec = importlib.util.spec_from_file_location(
        f"_test_writer_{path.stem}", path
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:
        raise AssertionError(
            f"Generated migration failed to import:\n{source}\n→ {exc}"
        ) from exc
    return mod


def _round_trip_field(field, *, app="myapp") -> Any:
    """Write a CreateModel migration for one field, re-import the file,
    and return the rebuilt field instance."""
    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / app / "migrations"
        ops = [
            CreateModel(
                name="X",
                fields=[("c", field)],
                options={"db_table": "x"},
            )
        ]
        path = write_migration(app, mig_dir, 1, ops)
        mod = _execute_migration_file(path)
    rebuilt_op = mod.operations[0]
    assert isinstance(rebuilt_op, CreateModel)
    rebuilt_fields = dict(rebuilt_op.fields)
    return rebuilt_fields["c"]


# ── DurationField ────────────────────────────────────────────────────────────


def test_duration_field_round_trips():
    rebuilt = _round_trip_field(dorm.DurationField())
    assert isinstance(rebuilt, dorm.DurationField)


def test_duration_field_with_null_blank():
    rebuilt = _round_trip_field(dorm.DurationField(null=True, blank=True))
    assert isinstance(rebuilt, dorm.DurationField)
    assert rebuilt.null is True
    assert rebuilt.blank is True


# ── CITextField ──────────────────────────────────────────────────────────────


def test_citext_field_round_trips():
    rebuilt = _round_trip_field(dorm.CITextField(unique=True))
    assert isinstance(rebuilt, dorm.CITextField)
    assert rebuilt.unique is True


# ── EnumField ────────────────────────────────────────────────────────────────


def test_enum_field_string_enum_round_trips():
    rebuilt = _round_trip_field(dorm.EnumField(Priority))
    assert isinstance(rebuilt, dorm.EnumField)
    assert rebuilt.enum_cls is Priority


def test_enum_field_int_enum_round_trips():
    rebuilt = _round_trip_field(dorm.EnumField(Level))
    assert isinstance(rebuilt, dorm.EnumField)
    assert rebuilt.enum_cls is Level


def test_enum_field_with_default_round_trips():
    rebuilt = _round_trip_field(dorm.EnumField(Priority, default=Priority.HIGH))
    assert isinstance(rebuilt, dorm.EnumField)
    assert rebuilt.enum_cls is Priority
    # The default is stored as the enum member.
    assert rebuilt.default == Priority.HIGH


def test_enum_field_writer_emits_user_import():
    """Make sure the migration file contains the bare-name import for
    the enum module, not just the EnumField import. Without this the
    file ``NameError``s on load."""
    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / "myapp" / "migrations"
        ops = [
            CreateModel(
                name="X",
                fields=[("priority", dorm.EnumField(Priority))],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", mig_dir, 1, ops)
        source = path.read_text()
    assert "from dorm.fields import EnumField" in source
    assert "import Priority" in source, source


def test_enum_field_rejects_nested_enum():
    """Nested enums can't be reliably round-tripped; the writer fails
    fast rather than emit a broken migration."""
    class Outer:
        class Status(enum.Enum):
            A = "a"

    import pytest

    field = dorm.EnumField(Outer.Status)
    with pytest.raises(ValueError, match="nested enum"):
        _round_trip_field(field)


# ── FileField ────────────────────────────────────────────────────────────────


def test_file_field_round_trips_with_upload_to():
    rebuilt = _round_trip_field(
        dorm.FileField(upload_to="docs/%Y/", null=True, blank=True)
    )
    assert isinstance(rebuilt, dorm.FileField)
    assert rebuilt.upload_to == "docs/%Y/"
    assert rebuilt.null is True
    assert rebuilt.blank is True


def test_file_field_omits_storage_arg():
    """``storage`` is intentionally not serialised — it resolves at
    runtime from settings.STORAGES, and freezing an alias into the
    migration file would break environment-driven config."""
    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / "myapp" / "migrations"
        ops = [
            CreateModel(
                name="X",
                fields=[
                    ("att", dorm.FileField(upload_to="x/", storage="custom"))
                ],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", mig_dir, 1, ops)
        source = path.read_text()
    assert "storage=" not in source


def test_file_field_callable_upload_to_emits_marker():
    """A callable upload_to can't survive a round-trip; the writer
    leaves a FIXME marker so the user notices."""
    def location(instance, filename):
        return f"by-name/{filename}"

    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / "myapp" / "migrations"
        ops = [
            CreateModel(
                name="X",
                fields=[("att", dorm.FileField(upload_to=location))],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", mig_dir, 1, ops)
        source = path.read_text()
    assert "FIXME" in source
    assert "callable" in source


# ── ArrayField / GeneratedField (recursive) ──────────────────────────────────


def test_array_field_recurses_into_base_field():
    rebuilt = _round_trip_field(
        dorm.ArrayField(dorm.CharField(max_length=20), null=True)
    )
    assert isinstance(rebuilt, dorm.ArrayField)
    assert isinstance(rebuilt.base_field, dorm.CharField)
    assert rebuilt.base_field.max_length == 20
    assert rebuilt.null is True


def test_array_of_enum_collects_user_import():
    """Recursion through ArrayField's base_field still picks up the
    enum's user-side import."""
    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / "myapp" / "migrations"
        ops = [
            CreateModel(
                name="X",
                fields=[("tags", dorm.ArrayField(dorm.EnumField(Priority)))],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", mig_dir, 1, ops)
        source = path.read_text()
    assert "import Priority" in source


def test_generated_field_recurses_into_output_field():
    rebuilt = _round_trip_field(
        dorm.GeneratedField(
            expression="quantity * price",
            output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
        )
    )
    assert isinstance(rebuilt, dorm.GeneratedField)
    assert rebuilt.expression == "quantity * price"
    assert isinstance(rebuilt.output_field, dorm.DecimalField)
    assert rebuilt.output_field.max_digits == 12
    assert rebuilt.output_field.decimal_places == 2


# ── Range fields ─────────────────────────────────────────────────────────────


def test_integer_range_field_round_trips():
    rebuilt = _round_trip_field(dorm.IntegerRangeField(null=True))
    assert isinstance(rebuilt, dorm.IntegerRangeField)
    assert rebuilt.null is True


def test_decimal_range_field_round_trips():
    rebuilt = _round_trip_field(dorm.DecimalRangeField())
    assert isinstance(rebuilt, dorm.DecimalRangeField)


def test_date_range_field_round_trips():
    rebuilt = _round_trip_field(dorm.DateRangeField())
    assert isinstance(rebuilt, dorm.DateRangeField)


def test_datetime_range_field_round_trips():
    rebuilt = _round_trip_field(dorm.DateTimeRangeField())
    assert isinstance(rebuilt, dorm.DateTimeRangeField)


def test_big_integer_range_field_round_trips():
    rebuilt = _round_trip_field(dorm.BigIntegerRangeField())
    assert isinstance(rebuilt, dorm.BigIntegerRangeField)


# ── PositiveSmallIntegerField (gap from 2.0) ─────────────────────────────────


def test_positive_small_integer_field_round_trips():
    """Was missing from ``_FIELD_IMPORTS`` despite living in dorm
    since 2.0 — the migration would emit ``PositiveSmallIntegerField()``
    without its import line and fail at load."""
    rebuilt = _round_trip_field(dorm.PositiveSmallIntegerField())
    assert isinstance(rebuilt, dorm.PositiveSmallIntegerField)


# ── AddField with the new types ──────────────────────────────────────────────


def test_add_field_op_round_trips_enum():
    """``AddField`` carries a single field; make sure user imports are
    collected for it too, not just from CreateModel.fields."""
    with tempfile.TemporaryDirectory() as tmp:
        mig_dir = Path(tmp) / "myapp" / "migrations"
        ops = [
            AddField(
                model_name="X",
                name="prio",
                field=dorm.EnumField(Priority),
            )
        ]
        path = write_migration("myapp", mig_dir, 2, ops)
        source = path.read_text()
        mod = _execute_migration_file(path)
    assert "import Priority" in source
    rebuilt = mod.operations[0]
    assert isinstance(rebuilt, AddField)
    assert isinstance(rebuilt.field, dorm.EnumField)
    assert rebuilt.field.enum_cls is Priority
