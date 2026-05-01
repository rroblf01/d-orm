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
    "PositiveSmallIntegerField": "from dorm.fields import PositiveSmallIntegerField",
    "FloatField": "from dorm.fields import FloatField",
    "DecimalField": "from dorm.fields import DecimalField",
    "BooleanField": "from dorm.fields import BooleanField",
    "NullBooleanField": "from dorm.fields import NullBooleanField",
    "DateField": "from dorm.fields import DateField",
    "TimeField": "from dorm.fields import TimeField",
    "DateTimeField": "from dorm.fields import DateTimeField",
    "DurationField": "from dorm.fields import DurationField",
    "EmailField": "from dorm.fields import EmailField",
    "URLField": "from dorm.fields import URLField",
    "SlugField": "from dorm.fields import SlugField",
    "UUIDField": "from dorm.fields import UUIDField",
    "IPAddressField": "from dorm.fields import IPAddressField",
    "GenericIPAddressField": "from dorm.fields import GenericIPAddressField",
    "JSONField": "from dorm.fields import JSONField",
    "BinaryField": "from dorm.fields import BinaryField",
    "ArrayField": "from dorm.fields import ArrayField",
    "GeneratedField": "from dorm.fields import GeneratedField",
    "FileField": "from dorm.fields import FileField",
    "EnumField": "from dorm.fields import EnumField",
    "CITextField": "from dorm.fields import CITextField",
    "RangeField": "from dorm.fields import RangeField",
    "IntegerRangeField": "from dorm.fields import IntegerRangeField",
    "BigIntegerRangeField": "from dorm.fields import BigIntegerRangeField",
    "DecimalRangeField": "from dorm.fields import DecimalRangeField",
    "DateRangeField": "from dorm.fields import DateRangeField",
    "DateTimeRangeField": "from dorm.fields import DateTimeRangeField",
    "ForeignKey": "from dorm.fields import ForeignKey",
    "OneToOneField": "from dorm.fields import OneToOneField",
    "ManyToManyField": "from dorm.fields import ManyToManyField",
    # contrib.pgvector lives outside ``dorm.fields`` — different
    # import path. Same pattern as the rest: writer emits the bare
    # class name, ``_FIELD_IMPORTS`` adds the matching ``from …
    # import …`` line at the top of the migration file.
    "VectorField": "from dorm.contrib.pgvector import VectorField",
}


def _is_module_level_callable(fn) -> bool:
    """True if *fn* is a plain ``def`` at the top level of an
    importable module — i.e. ``from <fn.__module__> import
    <fn.__name__>`` will recover the same callable.

    Disqualifies:
    - lambdas (``__name__`` is ``"<lambda>"``);
    - nested functions and closures (``"<locals>"`` shows up in
      ``__qualname__``);
    - bound methods (``__qualname__`` contains a dot for the owning
      class — round-tripping that requires the class import too,
      which we don't model);
    - builtins / synthetic callables whose ``__module__`` is missing
      or unimportable.
    """
    name = getattr(fn, "__name__", "") or ""
    qualname = getattr(fn, "__qualname__", "") or ""
    module = getattr(fn, "__module__", None)
    if not name or not qualname or not module:
        return False
    if name == "<lambda>" or name.startswith("<"):
        return False
    if "<locals>" in qualname or "." in qualname:
        return False
    return True


def _serialize_field(field) -> str:
    """Return Python source that, when ``eval``'d in a namespace where
    every needed name is imported, reconstructs *field*.

    The serialization is intentionally *not* a generic ``deconstruct``
    pass — each field type that requires special handling (FK targets,
    nested fields, enum classes) gets a hand-written branch so we can
    keep migration files easy to diff and read by humans. Field types
    not listed here fall through to ``ClassName()`` plus the common
    null/blank/default kwargs, which is enough for every "no-arg"
    column type (``DurationField``, ``CITextField``, the range
    family, …).
    """
    cls_name = field.__class__.__name__
    args_parts: list[str] = []   # positional args (FK target, enum class, …)
    kwargs_parts: list[str] = []

    # ── Type-specific positional / kwargs ────────────────────────────────────

    if cls_name in ("CharField", "EmailField", "URLField", "SlugField") and field.max_length:
        kwargs_parts.append(f"max_length={field.max_length!r}")

    elif cls_name == "DecimalField":
        kwargs_parts.append(f"max_digits={field.max_digits!r}")
        kwargs_parts.append(f"decimal_places={field.decimal_places!r}")

    elif cls_name in ("ForeignKey", "OneToOneField"):
        rel = field.remote_field_to
        if isinstance(rel, str):
            args_parts.append(f"{rel!r}")
        else:
            args_parts.append(f"'{rel.__name__}'")
        on_delete = getattr(field, "on_delete", "CASCADE")
        kwargs_parts.append(f"on_delete={on_delete!r}")

    elif cls_name == "ManyToManyField":
        rel = field.remote_field_to
        if isinstance(rel, str):
            args_parts.append(f"{rel!r}")
        else:
            args_parts.append(f"'{rel.__name__}'")

    elif cls_name == "EnumField":
        # The enum class is positional. Refer to it by its bare name —
        # the user-side import is added by ``_collect_user_imports``.
        # Nested enums (``Foo.Status``) are rejected: round-tripping a
        # qualified attribute access would force the migration to know
        # the outer class's import path too, which is fragile.
        enum_cls = field.enum_cls
        if "." in enum_cls.__qualname__:
            raise ValueError(
                f"EnumField for {enum_cls.__qualname__!r}: nested enum "
                "classes are not supported in migrations. Move the enum "
                "to module top-level so makemigrations can import it."
            )
        args_parts.append(enum_cls.__name__)
        # ``EnumField.__init__`` derives ``max_length`` from the enum's
        # longest member by default; only persist it if the user
        # overrode the default. There's no clean way to detect that
        # post-construction, so emit it whenever ``max_length`` is set
        # — round-trips are still equivalent because the default
        # would compute the same value.
        if getattr(field, "_is_string", False) and field.max_length is not None:
            kwargs_parts.append(f"max_length={field.max_length!r}")

    elif cls_name == "FileField":
        upload_to = getattr(field, "upload_to", "")
        if callable(upload_to):
            # Module-level functions can be round-tripped as
            # ``upload_to=<bare_name>`` plus a matching import line in
            # the migration's header; the import is contributed by
            # ``_collect_user_imports``. Lambdas, nested functions and
            # closures don't have a stable importable name, so we fall
            # back to a FIXME marker the user has to fill in by hand.
            if _is_module_level_callable(upload_to):
                kwargs_parts.append(f"upload_to={upload_to.__name__}")
            else:
                kwargs_parts.append(
                    "upload_to=''  # FIXME: original upload_to was a "
                    "lambda / nested function and could not be "
                    "round-tripped. Re-declare it at module scope and "
                    "edit this migration to use it."
                )
        elif upload_to:
            kwargs_parts.append(f"upload_to={upload_to!r}")
        if field.max_length and field.max_length != 255:
            kwargs_parts.append(f"max_length={field.max_length!r}")
        # ``storage`` is intentionally omitted: it resolves at runtime
        # from ``settings.STORAGES``. Hardcoding a string alias here
        # would freeze a production setting into the migration file.

    elif cls_name == "ArrayField":
        # Recurse on the base field. The base field's import is also
        # collected by the import gatherer.
        kwargs_parts.append(f"base_field={_serialize_field(field.base_field)}")

    elif cls_name == "GeneratedField":
        kwargs_parts.append(f"expression={field.expression!r}")
        kwargs_parts.append(
            f"output_field={_serialize_field(field.output_field)}"
        )
        if not getattr(field, "stored", True):
            kwargs_parts.append("stored=False")

    elif cls_name == "VectorField":
        # ``dorm.contrib.pgvector.VectorField`` lives outside
        # ``dorm.fields`` but ships the same constructor pattern:
        # ``dimensions`` is mandatory and goes positional. Without
        # this branch the writer emitted ``VectorField()`` and the
        # generated migration crashed at import with
        # ``TypeError: __init__() missing 1 required positional
        # argument: 'dimensions'``.
        kwargs_parts.append(f"dimensions={field.dimensions!r}")

    # ── Type-agnostic kwargs (apply to every field) ──────────────────────────

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
        # Enum defaults need the enum's bare name reference, not its
        # repr (``<Status.LOW: 'low'>`` would not eval). Emit the
        # canonical attribute access.
        import enum as _enum
        if isinstance(field.default, _enum.Enum):
            cls = type(field.default)
            kwargs_parts.append(f"default={cls.__name__}.{field.default.name}")
        else:
            kwargs_parts.append(f"default={field.default!r}")

    return f"{cls_name}({', '.join(args_parts + kwargs_parts)})"


def _collect_user_imports(operations) -> list[str]:
    """Gather user-side imports a migration file needs to re-evaluate.

    Currently only ``EnumField`` produces these — the migration source
    references the enum class by its bare name, so the file has to
    ``from <module> import <ClassName>`` it. We also walk into
    ``ArrayField.base_field`` and ``GeneratedField.output_field`` so
    nested enums get their imports too.
    """
    extra: set[str] = set()

    def _walk(field) -> None:
        cls_name = field.__class__.__name__
        if cls_name == "EnumField":
            enum_cls = getattr(field, "enum_cls", None)
            if enum_cls is None:
                return
            module = getattr(enum_cls, "__module__", None) or "<unknown>"
            qualname = getattr(enum_cls, "__qualname__", enum_cls.__name__)
            # The serializer rejects nested enums up-front, so we know
            # __qualname__ has no dots here and matches __name__.
            extra.add(f"from {module} import {enum_cls.__name__}")
            del qualname  # silence ty's "assigned but unused" check
        elif cls_name == "FileField":
            # Module-level upload_to callables get an import line so
            # the generated migration can resolve the bare name the
            # serializer emitted. Anything else (string template,
            # lambda, nested fn) needs no extra import.
            upload_to = getattr(field, "upload_to", None)
            if callable(upload_to) and _is_module_level_callable(upload_to):
                module = upload_to.__module__
                name = upload_to.__name__
                extra.add(f"from {module} import {name}")
        elif cls_name == "ArrayField":
            base = getattr(field, "base_field", None)
            if base is not None:
                _walk(base)
        elif cls_name == "GeneratedField":
            output = getattr(field, "output_field", None)
            if output is not None:
                _walk(output)

    for op in operations:
        if hasattr(op, "fields"):
            for _, field in op.fields:
                _walk(field)
        if hasattr(op, "field"):
            _walk(op.field)
    return sorted(extra)


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


def write_pgvector_extension_migration(
    app_label: str,
    migrations_dir: Path,
    number: int,
    name: str = "enable_pgvector",
    dependencies: list | None = None,
) -> Path:
    """Write a migration that enables pgvector's ``CREATE EXTENSION``.

    Generated by ``dorm makemigrations --enable-pgvector``. The
    template is intentionally tiny — one ``VectorExtension()``
    operation — so the file's purpose is obvious from a glance and
    diff'ing it against future bumps stays readable.
    """
    migrations_dir = Path(migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)

    init = migrations_dir / "__init__.py"
    if not init.exists():
        init.write_text("")

    filename = f"{number:04d}_{name}"
    filepath = migrations_dir / f"{filename}.py"
    dep_str = repr(dependencies or [])

    content = f'''"""
Enable the pgvector PostgreSQL extension.
Generated: {_dt.datetime.now(_dt.timezone.utc).isoformat()}
"""
from dorm.contrib.pgvector import VectorExtension

dependencies = {dep_str}

operations = [
    VectorExtension(),
]
'''
    filepath.write_text(content)
    return filepath


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

    # User-side imports for symbols referenced by ``_serialize_field``
    # (right now only ``EnumField`` enum classes). Without these, the
    # migration file would ``NameError`` at load time.
    for imp in _collect_user_imports(operations):
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
    """Walk *op* and add ``from dorm.fields import …`` lines for every
    field type it references.

    Recurses into ``ArrayField.base_field`` and
    ``GeneratedField.output_field`` so a migration that says
    ``ArrayField(base_field=CharField(max_length=20))`` gets *both*
    ``ArrayField`` *and* ``CharField`` in scope.
    """

    def _walk(field) -> None:
        if field is None:
            return
        cls_name = field.__class__.__name__
        if cls_name in _FIELD_IMPORTS:
            imports.add(_FIELD_IMPORTS[cls_name])
        # Nested fields the writer recursively serialises.
        base_field = getattr(field, "base_field", None)
        if base_field is not None:
            _walk(base_field)
        output_field = getattr(field, "output_field", None)
        if output_field is not None:
            _walk(output_field)

    if hasattr(op, "fields"):
        for _, field in op.fields:
            _walk(field)
    if hasattr(op, "field") and op.field is not None:
        _walk(op.field)


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
    for imp in _collect_user_imports(operations):
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
