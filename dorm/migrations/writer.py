from __future__ import annotations

import datetime as _dt
from pathlib import Path


_FIELD_IMPORTS = {
    "AutoField": "from dorm.fields import AutoField",
    "BigAutoField": "from dorm.fields import BigAutoField",
    "SmallAutoField": "from dorm.fields import SmallAutoField",
    "CharField": "from dorm.fields import CharField",
    "TextField": "from dorm.fields import TextField",
    "IntegerField": "from dorm.fields import IntegerField",
    "BigIntegerField": "from dorm.fields import BigIntegerField",
    "SmallIntegerField": "from dorm.fields import SmallIntegerField",
    "PositiveIntegerField": "from dorm.fields import PositiveIntegerField",
    "FloatField": "from dorm.fields import FloatField",
    "DecimalField": "from dorm.fields import DecimalField",
    "BooleanField": "from dorm.fields import BooleanField",
    "NullBooleanField": "from dorm.fields import NullBooleanField",
    "DateField": "from dorm.fields import DateField",
    "TimeField": "from dorm.fields import TimeField",
    "DateTimeField": "from dorm.fields import DateTimeField",
    "EmailField": "from dorm.fields import EmailField",
    "URLField": "from dorm.fields import URLField",
    "SlugField": "from dorm.fields import SlugField",
    "UUIDField": "from dorm.fields import UUIDField",
    "IPAddressField": "from dorm.fields import IPAddressField",
    "GenericIPAddressField": "from dorm.fields import GenericIPAddressField",
    "JSONField": "from dorm.fields import JSONField",
    "BinaryField": "from dorm.fields import BinaryField",
    "ForeignKey": "from dorm.fields import ForeignKey",
    "OneToOneField": "from dorm.fields import OneToOneField",
    "ManyToManyField": "from dorm.fields import ManyToManyField",
}


def _serialize_field(field) -> str:
    """Return Python source for recreating this field."""
    cls_name = field.__class__.__name__
    kwargs_parts = []

    if cls_name in ("CharField", "EmailField", "URLField", "SlugField") and field.max_length:
        kwargs_parts.append(f"max_length={field.max_length!r}")

    if cls_name == "DecimalField":
        kwargs_parts.append(f"max_digits={field.max_digits!r}")
        kwargs_parts.append(f"decimal_places={field.decimal_places!r}")

    if field.null:
        kwargs_parts.append("null=True")
    if field.blank:
        kwargs_parts.append("blank=True")
    if field.primary_key:
        kwargs_parts.append("primary_key=True")
    if field.unique and not field.primary_key:
        kwargs_parts.append("unique=True")
    if field.db_index:
        kwargs_parts.append("db_index=True")

    from ..fields import NOT_PROVIDED
    if field.default is not NOT_PROVIDED and not callable(field.default):
        kwargs_parts.append(f"default={field.default!r}")

    if cls_name in ("ForeignKey", "OneToOneField"):
        rel = field.remote_field_to
        if isinstance(rel, str):
            kwargs_parts.insert(0, f"{rel!r}")
        else:
            kwargs_parts.insert(0, f"'{rel.__name__}'")
        on_delete = getattr(field, "on_delete", "CASCADE")
        kwargs_parts.append(f"on_delete={on_delete!r}")

    if cls_name == "ManyToManyField":
        rel = field.remote_field_to
        if isinstance(rel, str):
            kwargs_parts.insert(0, f"{rel!r}")
        else:
            kwargs_parts.insert(0, f"'{rel.__name__}'")

    return f"{cls_name}({', '.join(kwargs_parts)})"


def _collect_imports(operations) -> list[str]:
    imports = set()
    imports.add("from dorm.migrations.operations import (")
    for op in operations:
        if hasattr(op, "fields"):
            for fname, field in op.fields:
                cls_name = field.__class__.__name__
                if cls_name in _FIELD_IMPORTS:
                    imports.add(_FIELD_IMPORTS[cls_name])
        if hasattr(op, "field"):
            cls_name = op.field.__class__.__name__
            if cls_name in _FIELD_IMPORTS:
                imports.add(_FIELD_IMPORTS[cls_name])
    return sorted(imports)


def write_empty_migration(
    app_label: str,
    migrations_dir: Path,
    number: int,
    name: str = "custom",
    dependencies: list | None = None,
) -> Path:
    """Write a blank migration template ready to be filled with RunPython / RunSQL."""
    migrations_dir = Path(migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)

    init = migrations_dir / "__init__.py"
    if not init.exists():
        init.write_text("")

    filename = f"{number:04d}_{name}"
    filepath = migrations_dir / f"{filename}.py"
    dep_str = repr(dependencies or [])

    content = f'''"""
Empty migration — add your RunPython / RunSQL operations below.
Generated: {_dt.datetime.now(_dt.timezone.utc).isoformat()}
"""
from dorm.migrations.operations import RunPython, RunSQL

dependencies = {dep_str}

operations = [
    # Example — uncomment and adapt:
    #
    # def forward(app_label, registry):
    #     MyModel = registry["MyModel"]
    #     MyModel.objects.filter(...).update(...)
    #
    # def backward(app_label, registry):
    #     pass  # or undo the forward logic
    #
    # RunPython(code=forward, reverse_code=backward),
    # RunSQL(sql="UPDATE ...", reverse_sql="UPDATE ..."),
]
'''

    filepath.write_text(content)
    return filepath


def write_migration(
    app_label: str,
    migrations_dir: Path,
    number: int,
    operations: list,
    dependencies: list | None = None,
) -> Path:
    migrations_dir = Path(migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Create __init__.py if missing
    init = migrations_dir / "__init__.py"
    if not init.exists():
        init.write_text("")

    name = f"{number:04d}_{'initial' if number == 1 else 'auto'}"
    filepath = migrations_dir / f"{name}.py"

    _collect_imports(operations)
    dep_str = repr(dependencies or [])

    op_lines = []
    for op in operations:
        op_lines.append(_serialize_operation(op))

    ops_str = ",\n    ".join(op_lines)

    # Build operation import list
    op_classes = {type(op).__name__ for op in operations}
    op_import = "    " + ",\n    ".join(sorted(op_classes)) + ","

    content = f'''"""
Auto-generated migration.
Generated: {_dt.datetime.now(_dt.timezone.utc).isoformat()}
"""
from dorm.migrations.operations import (
{op_import}
)
'''

    # AddIndex / RemoveIndex emit ``Index(...)`` literals in their
    # serialised form; the generated migration is unimportable without
    # the Index symbol in scope. ``write_squashed_migration`` already
    # handled this; ``write_migration`` was missing the import — any
    # auto-generated migration containing a (Add|Remove)Index would
    # crash at load time with ``NameError: name 'Index' is not defined``.
    if any(c in op_classes for c in ("AddIndex", "RemoveIndex")):
        content += "from dorm.indexes import Index\n"

    # Add field imports
    field_imports = set()
    for op in operations:
        _gather_field_imports(op, field_imports)
    for imp in sorted(field_imports):
        content += imp + "\n"

    content += f"""
dependencies = {dep_str}

operations = [
    {ops_str},
]
"""

    filepath.write_text(content)
    return filepath


def _gather_field_imports(op, imports: set):
    if hasattr(op, "fields"):
        for fname, field in op.fields:
            cls_name = field.__class__.__name__
            if cls_name in _FIELD_IMPORTS:
                imports.add(_FIELD_IMPORTS[cls_name])
    if hasattr(op, "field") and op.field is not None:
        cls_name = op.field.__class__.__name__
        if cls_name in _FIELD_IMPORTS:
            imports.add(_FIELD_IMPORTS[cls_name])


def _serialize_operation(op) -> str:
    cls = type(op).__name__

    if cls == "CreateModel":
        fields_str = ", ".join(
            f"({fname!r}, {_serialize_field(field)})"
            for fname, field in op.fields
        )
        opts_str = repr(op.options) if op.options else "{}"
        return f"CreateModel(\n        name={op.name!r},\n        fields=[{fields_str}],\n        options={opts_str},\n    )"

    if cls == "DeleteModel":
        return f"DeleteModel(name={op.name!r})"

    if cls == "AddField":
        return f"AddField(\n        model_name={op.model_name!r},\n        name={op.name!r},\n        field={_serialize_field(op.field)},\n    )"

    if cls == "RemoveField":
        return f"RemoveField(model_name={op.model_name!r}, name={op.name!r})"

    if cls == "AlterField":
        return f"AlterField(\n        model_name={op.model_name!r},\n        name={op.name!r},\n        field={_serialize_field(op.field)},\n    )"

    if cls == "RenameField":
        return f"RenameField(model_name={op.model_name!r}, old_name={op.old_name!r}, new_name={op.new_name!r})"

    if cls == "RenameModel":
        return f"RenameModel(old_name={op.old_name!r}, new_name={op.new_name!r})"

    if cls == "RunSQL":
        return f"RunSQL(sql={op.sql!r})"

    if cls == "AddIndex":
        idx = op.index
        fields_repr = repr(idx.fields)
        unique_repr = repr(idx.unique)
        name_repr = repr(getattr(idx, "name", None))
        return (
            f"AddIndex(\n        model_name={op.model_name!r},\n"
            f"        index=Index(fields={fields_repr}, unique={unique_repr}, name={name_repr}),\n    )"
        )

    if cls == "RemoveIndex":
        idx = op.index
        fields_repr = repr(idx.fields)
        unique_repr = repr(idx.unique)
        name_repr = repr(getattr(idx, "name", None))
        return (
            f"RemoveIndex(\n        model_name={op.model_name!r},\n"
            f"        index=Index(fields={fields_repr}, unique={unique_repr}, name={name_repr}),\n    )"
        )

    return repr(op)


def write_squashed_migration(
    app_label: str,
    migrations_dir: Path,
    number: int,
    operations: list,
    replaces: list,
    name: str = "squashed",
) -> Path:
    """Write a squashed migration that replaces a range of existing migrations."""
    migrations_dir = Path(migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)

    init = migrations_dir / "__init__.py"
    if not init.exists():
        init.write_text("")

    filename = f"{number:04d}_{name}"
    filepath = migrations_dir / f"{filename}.py"

    op_classes = {type(op).__name__ for op in operations}

    has_index = any(c in op_classes for c in ("AddIndex", "RemoveIndex"))

    op_import = "    " + ",\n    ".join(sorted(op_classes)) + ","

    op_lines = [_serialize_operation(op) for op in operations]
    ops_str = ",\n    ".join(op_lines)

    replaces_str = repr(replaces)

    content = f'''"""
Squashed migration — replaces {replaces_str}.
Generated: {_dt.datetime.now(_dt.timezone.utc).isoformat()}
"""
from dorm.migrations.operations import (
{op_import}
)
'''

    if has_index:
        content += "from dorm.indexes import Index\n"

    field_imports: set[str] = set()
    for op in operations:
        _gather_field_imports(op, field_imports)
    for imp in sorted(field_imports):
        content += imp + "\n"

    content += f"""
replaces = {replaces_str}

dependencies = []

operations = [
    {ops_str},
]
"""

    filepath.write_text(content)
    return filepath
