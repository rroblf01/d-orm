"""Pydantic v2 interop for dorm models — designed for FastAPI use.

The recommended entry point is :class:`DormSchema`, which mirrors Django
REST Framework's ``ModelSerializer.Meta`` style: declare an inner
``class Meta`` pointing at a dorm Model, and the schema picks up the
fields automatically. Override types, add extra fields, or attach
``@field_validator`` decorators in the class body — those win over the
auto-generated ones::

    from pydantic import field_validator
    from dorm.contrib.pydantic import DormSchema
    from .models import User

    class UserOut(DormSchema):
        class Meta:
            model = User                            # required
            fields = "__all__"                       # or e.g. ("id", "name")
            # exclude = ("password",)                # mutually exclusive with `fields`
            # optional = ("phone",)                  # mark required cols as optional

    class UserCreate(DormSchema):
        confirm_password: str                       # extra field not on the model

        @field_validator("email")
        @classmethod
        def lower(cls, v: str) -> str:
            return v.lower()

        class Meta:
            model = User
            exclude = ("id", "created_at")

Fields you declare in the class body always win over Meta — pin a
specific type, change a default, or add ``Annotated[...]`` validators.

For one-line auto-generation without a Meta, see :func:`schema_for`. The
trade-off there: type checkers see the result as ``type[BaseModel]``,
so attribute access on validated instances is untyped. With ``DormSchema``
you only "give up" types on the auto-filled fields; explicit ones stay
fully typed.

This module loads only when imported, keeping ``pydantic`` an optional
extra (``pip install 'djanorm[pydantic]'``).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated, Any, Type
from uuid import UUID

try:
    from pydantic import BaseModel, ConfigDict, create_model
    from pydantic.functional_validators import BeforeValidator
except ImportError as e:
    raise ImportError(
        "Pydantic is required for dorm.contrib.pydantic. "
        "Install with: pip install 'djanorm[pydantic]'"
    ) from e

from ..fields import (
    ArrayField,
    AutoField,
    BigIntegerField,
    BinaryField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    DurationField,
    EmailField,
    EnumField,
    FileField,
    FloatField,
    ForeignKey,
    GeneratedField,
    GenericIPAddressField,
    IPAddressField,
    IntegerField,
    JSONField,
    ManyToManyField,
    OneToOneField,
    PositiveIntegerField,
    PositiveSmallIntegerField,
    RangeField,
    SlugField,
    SmallIntegerField,
    TextField,
    TimeField,
    URLField,
    UUIDField,
)
from ..models import Model

def _coerce_field_file_to_str(value: Any) -> Any:
    """Pydantic input adapter for :class:`dorm.FileField` columns.

    ``from_attributes=True`` reads the descriptor's :class:`FieldFile`
    wrapper, but the schema declares the column as ``str``. Without
    this validator Pydantic would refuse a ``FieldFile`` argument
    even though its ``.name`` round-trips losslessly to a string. We
    accept ``str`` / ``None`` / anything with a ``.name`` attribute,
    and fall back to ``str(value)`` so user-defined ``File`` subclasses
    still work as long as their ``__str__`` returns the storage name.
    """
    if value is None or isinstance(value, str):
        return value
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    return str(value)


# Annotated[str] subtype that quietly unwraps a FieldFile / File on
# input. Used for FileField columns where the descriptor returns the
# wrapper but the API contract is "the storage name".
_FieldFileStr = Annotated[str, BeforeValidator(_coerce_field_file_to_str)]


# All string-like fields map to ``str`` here. Format validation
# (email / URL / IP) is enforced by dorm's own field code at assignment
# time, so a request body with ``{"email": "example"}`` is rejected as
# soon as ``Customer(email="example")`` runs — no Pydantic-side validator
# (and no email-validator dependency) needed.
#
# Order matters: more-specific subclasses come before their parents so
# ``isinstance(field, parent)`` doesn't shadow a richer mapping. Fields
# that need *per-instance* logic (``EnumField`` exposing its
# ``enum_cls``, ``ArrayField`` parameterising the inner type,
# ``GeneratedField`` recursing into its ``output_field``) live in
# :func:`_field_to_type` instead of this static table.
_FIELD_TYPE_MAP: list[tuple[type, Any]] = [
    (PositiveSmallIntegerField, int),
    (PositiveIntegerField, int),
    (SmallIntegerField, int),
    (BigIntegerField, int),
    (AutoField, int),
    (IntegerField, int),
    (FloatField, float),
    (DecimalField, Decimal),
    (BooleanField, bool),
    (EmailField, str),
    (URLField, str),
    (SlugField, str),
    (CharField, str),
    (TextField, str),  # also matches CITextField (subclass) → str
    (UUIDField, UUID),
    (DateTimeField, datetime),
    (DateField, date),
    (TimeField, time),
    (DurationField, timedelta),
    (JSONField, Any),
    (BinaryField, bytes),
    (GenericIPAddressField, str),
    (IPAddressField, str),
    (FileField, _FieldFileStr),
    # PG-only range types: catch the whole family with the abstract
    # base. The Python value is :class:`dorm.Range`; serialising it
    # as ``Any`` lets FastAPI hand it off without forcing a strict
    # JSON Schema. Users who want a typed surface can override the
    # field on their ``DormSchema`` subclass with a ``BaseModel``
    # tailored to ``{lower, upper, bounds}``.
    (RangeField, Any),
]


def _field_to_type(field: Any) -> Any:
    """Map a dorm field instance to its Python type for Pydantic.

    Per-instance shapes (``EnumField`` exposing its enum class,
    ``ArrayField`` parameterised by element type, ``GeneratedField``
    delegating to ``output_field``) are handled here before falling
    back to :data:`_FIELD_TYPE_MAP`.
    """
    # FK / O2O serialize as the underlying PK column value (int by default).
    if isinstance(field, (ForeignKey, OneToOneField)):
        return int
    if isinstance(field, EnumField):
        # Pydantic v2 accepts an ``enum.Enum`` subclass as a type
        # annotation and validates membership automatically — perfect
        # match for our value semantics.
        return field.enum_cls
    if isinstance(field, GeneratedField):
        # The DB computes the value; for Pydantic purposes its type is
        # whatever ``output_field`` declares (``DecimalField`` →
        # ``Decimal``, etc.).
        return _field_to_type(field.output_field)
    if isinstance(field, ArrayField):
        # ``list[T]`` where T is whatever the base field maps to. The
        # inner mapping recurses, so an ``ArrayField(EnumField(Status))``
        # surfaces as ``list[Status]``.
        inner = _field_to_type(field.base_field)
        # ``list[inner]`` is a runtime ``GenericAlias``, which is what
        # Pydantic's ``create_model`` expects. Hoisting the lookup out
        # of the subscript also keeps ty happy — it rejects function
        # calls inside type-expression positions otherwise.
        return list[inner]
    for field_cls, py_type in _FIELD_TYPE_MAP:
        if isinstance(field, field_cls):
            return py_type
    return Any


def schema_for(
    model_cls: Type[Model],
    *,
    name: str | None = None,
    exclude: tuple[str, ...] = (),
    only: tuple[str, ...] | None = None,
    optional: tuple[str, ...] = (),
    base: Type[BaseModel] = BaseModel,
) -> Type[BaseModel]:
    """Generate a Pydantic v2 ``BaseModel`` mirroring *model_cls*.

    The result has ``model_config = ConfigDict(from_attributes=True)`` so
    Pydantic can read values directly from dorm instances — i.e. you can
    pass a dorm model to ``Schema.model_validate(instance)`` or use it as
    a FastAPI ``response_model``.

    Args:
        model_cls: The dorm Model class.
        name: Class name for the generated Pydantic model. Defaults to
            ``f"{model_cls.__name__}Schema"``.
        exclude: Field names to omit. Common for input schemas (e.g.
            ``("id", "created_at")``).
        only: If given, include *only* these field names.
        optional: Field names that should be Optional with a default of
            ``None`` even if the underlying dorm field is non-null. Useful
            for partial-update (PATCH) request bodies.
        base: Custom ``BaseModel`` base — useful for sharing
            ``ConfigDict`` settings across schemas.

    ManyToManyField is always excluded (M2M lives in a junction table; the
    pks aren't on the row itself). Add an explicit ``tags: list[int] = []``
    in a wrapper schema if you need to model them.
    """
    fields: dict[str, tuple[Any, Any]] = {}
    for f in model_cls._meta.fields:
        if not f.column:  # M2M, computed, etc.
            continue
        if isinstance(f, ManyToManyField):
            continue
        if only is not None and f.name not in only:
            continue
        if f.name in exclude:
            continue

        py_type = _field_to_type(f)
        # A field is "optional" in Pydantic terms when it has a default
        # (auto-incrementing PK, server defaults, nullable, or explicitly
        # marked) — meaning callers can omit it from input.
        is_optional = (
            f.null or isinstance(f, AutoField) or f.name in optional or f.has_default()
        )

        if is_optional:
            fields[f.name] = (py_type | None, None)
        else:
            fields[f.name] = (py_type, ...)

    cls_name = name or f"{model_cls.__name__}Schema"
    # Setting model_config *after* create_model() doesn't take effect —
    # Pydantic v2 freezes the schema at class-creation time, so the config
    # must reach create_model. Merge any config the caller's *base* declared.
    base_config = dict(getattr(base, "model_config", {}))
    base_config.update(from_attributes=True, arbitrary_types_allowed=True)
    config: ConfigDict = ConfigDict(**base_config)

    if base is BaseModel:
        pyd_cls = create_model(cls_name, __config__=config, **fields)  # type: ignore
    else:
        # __config__ and __base__ are mutually exclusive in pydantic.create_model.
        # Mirror the config onto a thin subclass of `base` so we can pass
        # __base__ instead.
        attrs = {"model_config": config}
        configured_base = type(f"_Configured{base.__name__}", (base,), attrs)
        pyd_cls = create_model(cls_name, __base__=configured_base, **fields)  # type: ignore
    return pyd_cls


try:
    # Pydantic exposes its model metaclass under the internal module; both
    # SQLModel and ormar rely on this same import path. It's stable across
    # 2.x.
    from pydantic._internal._model_construction import ModelMetaclass
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "Pydantic v2 is required for dorm.contrib.pydantic. "
        "Install with: pip install 'djanorm[pydantic]'"
    ) from e


def _resolve_user_annotations(namespace: dict) -> dict:
    """Return the user-declared annotations from a class namespace, supporting
    both legacy (`__annotations__` dict) and PEP 649 (`__annotate_func__`)
    layouts. Python 3.14 stores annotations lazily via `__annotate_func__`
    by default, leaving `__annotations__` unset until first access — reading
    it from the raw namespace would lose the user's typed fields."""
    annotations: dict = dict(namespace.get("__annotations__") or {})
    annotate_func = namespace.get("__annotate_func__")
    if annotate_func is not None:
        try:
            # `annotationlib.Format.VALUE` (= 2) materializes annotations as
            # actual type objects. Available since Python 3.14.
            try:
                from annotationlib import Format  # type: ignore
                lazy = annotate_func(Format.VALUE)
            except ImportError:
                lazy = annotate_func(2)
            if isinstance(lazy, dict):
                # The lazy dict wins where both have an entry — PEP 649
                # places the user's class-body annotations there.
                annotations.update(lazy)
        except Exception:
            pass
    return annotations


def _meta_apply(cls_name: str, namespace: dict, meta_cls: type) -> None:
    """Read user's ``class Meta`` and merge auto-generated field annotations
    into ``namespace`` (without overriding fields the user declared explicitly)."""
    model_cls = getattr(meta_cls, "model", None)
    if model_cls is None:
        raise TypeError(
            f"{cls_name}.Meta.model is required when using DormSchema with a Meta block."
        )

    meta_fields = getattr(meta_cls, "fields", "__all__")
    meta_exclude = tuple(getattr(meta_cls, "exclude", ()))
    meta_optional = tuple(getattr(meta_cls, "optional", ()))
    meta_nested = dict(getattr(meta_cls, "nested", {}) or {})

    if meta_fields != "__all__" and meta_exclude:
        raise TypeError(
            f"{cls_name}.Meta: pass either 'fields' or 'exclude', not both."
        )

    user_annotations = _resolve_user_annotations(namespace)
    # Start with the user's annotations so they keep their original order
    # in __annotations__ — affects field ordering in generated JSON Schema.
    annotations = dict(user_annotations)

    fields_by_name = {f.name: f for f in model_cls._meta.fields}

    for f in model_cls._meta.fields:
        # Skip non-column fields (M2M, computed, etc.). M2M can still be
        # opted into nested serialization via Meta.nested.
        is_m2m = isinstance(f, ManyToManyField)
        if not f.column and not is_m2m:
            continue
        if meta_fields != "__all__" and f.name not in meta_fields:
            continue
        if f.name in meta_exclude:
            continue
        # User declared this field explicitly — respect it.
        if f.name in user_annotations:
            continue
        if is_m2m and f.name not in meta_nested:
            # M2M fields are skipped by default; user must opt in via nested.
            continue

        # Nested relation? Use the configured sub-schema instead of the bare
        # PK / int — useful for FastAPI response models with ``publisher: PublisherOut``.
        if f.name in meta_nested:
            sub_schema = meta_nested[f.name]
            from ..fields import ForeignKey, OneToOneField

            if isinstance(f, ManyToManyField):
                annotations[f.name] = list[sub_schema]
                namespace.setdefault(f.name, [])
                continue
            if isinstance(f, (ForeignKey, OneToOneField)):
                annotations[f.name] = sub_schema | None if f.null else sub_schema
                if f.null:
                    namespace.setdefault(f.name, None)
                continue
            # Non-relational field listed in nested — fall through to the
            # normal scalar handling.

        py_type = _field_to_type(f)
        is_optional = (
            f.null
            or isinstance(f, AutoField)
            or f.name in meta_optional
            or f.has_default()
        )
        if is_optional:
            annotations[f.name] = py_type | None
            namespace.setdefault(f.name, None)
        else:
            annotations[f.name] = py_type

    # Detect typos / extras in Meta.nested early so the user knows.
    for nested_name in meta_nested:
        if nested_name not in fields_by_name:
            raise TypeError(
                f"{cls_name}.Meta.nested references unknown field {nested_name!r}; "
                f"valid fields: {sorted(fields_by_name)}"
            )

    namespace["__annotations__"] = annotations
    # Drop the lazy annotate function so Pydantic uses our merged dict
    # instead of re-evaluating user-only annotations from PEP 649.
    namespace.pop("__annotate_func__", None)


def _ensure_from_attributes_config(namespace: dict) -> None:
    """Make sure the resulting class has ``from_attributes=True`` so it can
    be populated from a dorm instance (FastAPI's ``response_model`` path)."""
    existing = dict(namespace.get("model_config", {}))
    existing.setdefault("from_attributes", True)
    existing.setdefault("arbitrary_types_allowed", True)
    namespace["model_config"] = ConfigDict(**existing)


class DormSchemaMeta(ModelMetaclass):
    """Metaclass that turns ``class Meta: model = X`` into auto-generated
    Pydantic fields, then defers to Pydantic's ModelMetaclass for the
    rest of the class machinery (validators, serializers, etc.)."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        meta_cls = namespace.pop("Meta", None)
        # Don't process the DormSchema base class itself.
        is_root = name == "DormSchema" and not any(
            isinstance(b, DormSchemaMeta) for b in bases
        )
        if not is_root:
            if meta_cls is not None:
                _meta_apply(name, namespace, meta_cls)
            _ensure_from_attributes_config(namespace)
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class DormSchema(BaseModel, metaclass=DormSchemaMeta):
    """Pydantic ``BaseModel`` with two ergonomic boosts:

    1. ``from_attributes=True`` is on by default, so FastAPI can serialize
       a dorm instance directly via ``response_model=YourSchema``.
    2. Subclasses can declare ``class Meta: model = SomeDormModel`` (with
       optional ``fields``, ``exclude``, ``optional`` lists) and the
       metaclass auto-fills the matching Pydantic fields. Anything you
       declare explicitly on the class wins over the Meta-derived defaults.

    Without a Meta block, ``DormSchema`` is just a plain ``BaseModel``
    with the ``from_attributes`` config — useful when you want a fully
    explicit, type-safe schema.

    Example::

        from pydantic import field_validator
        from dorm.contrib.pydantic import DormSchema

        class UserOut(DormSchema):
            class Meta:
                model = User
                fields = "__all__"          # default; or list to whitelist

        class UserCreate(DormSchema):
            confirm_password: str           # extra field

            @field_validator("email")
            @classmethod
            def lower(cls, v: str) -> str:
                return v.lower()

            class Meta:
                model = User
                exclude = ("id", "created_at")
                optional = ("phone",)        # nullable in this schema only

    Nested relations: pass ``Meta.nested`` mapping a relation field name
    to the sub-schema you want serialized. FK / O2O become the sub-schema
    (``Type | None`` if nullable); M2M becomes ``list[SubSchema]``::

        class PublisherOut(DormSchema):
            class Meta:
                model = Publisher

        class AuthorOut(DormSchema):
            class Meta:
                model = Author
                nested = {"publisher": PublisherOut}    # author.publisher → PublisherOut | None
    """

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


__all__ = ["schema_for", "DormSchema", "DormSchemaMeta"]
