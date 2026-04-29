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
from typing import Annotated, Any, Literal, Type
from uuid import UUID

try:
    from pydantic import BaseModel, ConfigDict, Field as PydField, create_model
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
from ..validators import (
    MaxLengthValidator,
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    RegexValidator,
)

def _coerce_field_file_to_str(value: Any) -> Any:
    """Pydantic input adapter for :class:`dorm.FileField` columns.

    ``from_attributes=True`` reads the descriptor's :class:`FieldFile`
    wrapper, but the schema declares the column as ``str``. Without
    this validator Pydantic would refuse a ``FieldFile`` argument
    even though its ``.name`` round-trips losslessly to a string. We
    accept ``str`` / ``None`` / anything with a ``.name`` attribute,
    and fall back to ``str(value)`` so user-defined ``File`` subclasses
    still work as long as their ``__str__`` returns the storage name.

    A ``FieldFile`` with no associated file has ``.name == ""``; we
    pass the empty string through unchanged. We *don't* collapse it
    to ``None``: when the Pydantic schema says ``Annotated[str, …] |
    None``, the ``BeforeValidator`` runs on the str arm and a
    ``None`` return there fails union resolution. Rendering "no
    file" as an empty string keeps the round-trip working; callers
    who want the JSON to show ``null`` can wire ``model_dump(
    exclude_none=True)`` or convert ``""`` → ``None`` in the route
    handler.
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


def _base_type_for(field: Any) -> Any:
    """Look up the bare Python type for *field* in :data:`_FIELD_TYPE_MAP`,
    without applying constraints.

    Recursive cases (``EnumField`` exposing its enum class, ``ArrayField``
    parameterised by element type, ``GeneratedField`` delegating to
    ``output_field``, FK/O2O collapsing to the FK column type) are
    resolved here so callers don't have to special-case them."""
    if isinstance(field, (ForeignKey, OneToOneField)):
        return int
    if isinstance(field, EnumField):
        return field.enum_cls
    if isinstance(field, GeneratedField):
        return _base_type_for(field.output_field)
    if isinstance(field, ArrayField):
        inner = _base_type_for(field.base_field)
        return list[inner]
    for field_cls, py_type in _FIELD_TYPE_MAP:
        if isinstance(field, field_cls):
            return py_type
    return Any


def _validator_kwargs(validators: list) -> dict[str, Any]:
    """Translate dorm's built-in validator instances into Pydantic
    ``Field()`` kwargs. Unknown validators are skipped — the dorm-side
    ``validate()`` still runs at ``full_clean`` time, so dropping them
    here only loses the up-front rejection at the API boundary, never
    the underlying check."""
    kwargs: dict[str, Any] = {}
    for v in validators:
        if isinstance(v, MinValueValidator):
            kwargs["ge"] = v.limit_value
        elif isinstance(v, MaxValueValidator):
            kwargs["le"] = v.limit_value
        elif isinstance(v, MinLengthValidator):
            kwargs["min_length"] = v.min_length
        elif isinstance(v, MaxLengthValidator):
            existing = kwargs.get("max_length")
            kwargs["max_length"] = (
                v.max_length if existing is None else min(existing, v.max_length)
            )
        elif isinstance(v, RegexValidator):
            # Pydantic uses Rust regex syntax via pydantic-core. Most
            # Python patterns work; users with truly Python-only regex
            # (look-arounds, named groups beyond Rust's grammar) can
            # override the field annotation manually.
            kwargs["pattern"] = v.regex.pattern
    return kwargs


def _field_constraint_kwargs(field: Any, base_type: Any) -> dict[str, Any]:
    """Build the kwargs for ``pydantic.Field(...)`` reflecting every
    constraint dorm enforces at assignment / clean time. Returns an
    empty dict when the field has no translatable constraints — callers
    use that to skip the ``Annotated`` wrapper entirely."""
    kwargs: dict[str, Any] = {}

    # String length: every Char-derived field plus FileField. The
    # ``max_length`` attribute lives directly on the field. ``FileField``
    # maps to ``Annotated[str, BeforeValidator(...)]`` (not bare ``str``),
    # so peek through ``Annotated`` before deciding whether to apply.
    string_like = base_type is str or getattr(base_type, "__origin__", None) is str
    if string_like:
        max_length = getattr(field, "max_length", None)
        if max_length:
            kwargs["max_length"] = max_length

    # DecimalField: max_digits / decimal_places line up 1:1 with
    # Pydantic's Field options. Both are always populated on dorm's
    # DecimalField (defaults 10 / 2).
    if isinstance(field, DecimalField):
        kwargs["max_digits"] = field.max_digits
        kwargs["decimal_places"] = field.decimal_places

    # Positive integer fields enforce >=0 at assignment time; surface
    # that to the schema so OpenAPI shows ``minimum: 0`` and Pydantic
    # rejects negatives at the boundary.
    if isinstance(field, (PositiveIntegerField, PositiveSmallIntegerField)):
        kwargs["ge"] = 0

    # OpenAPI ``format`` hints: keep the type as ``str`` (avoids the
    # email-validator dep) but document the intent so generated client
    # code and the docs UI render the right input affordance.
    if isinstance(field, EmailField):
        extra = dict(kwargs.get("json_schema_extra") or {})
        extra.setdefault("format", "email")
        kwargs["json_schema_extra"] = extra
    elif isinstance(field, URLField):
        extra = dict(kwargs.get("json_schema_extra") or {})
        extra.setdefault("format", "uri")
        kwargs["json_schema_extra"] = extra

    # User-supplied validators. Merged after built-ins so an explicit
    # MaxLengthValidator(N) can tighten — never loosen — the field's
    # own ``max_length``.
    if getattr(field, "validators", None):
        for k, v in _validator_kwargs(field.validators).items():
            if k == "max_length" and "max_length" in kwargs:
                kwargs["max_length"] = min(kwargs["max_length"], v)
            elif k == "min_length" and "min_length" in kwargs:
                kwargs["min_length"] = max(kwargs["min_length"], v)
            elif k == "ge" and "ge" in kwargs:
                kwargs["ge"] = max(kwargs["ge"], v)
            elif k == "le" and "le" in kwargs:
                kwargs["le"] = min(kwargs["le"], v)
            else:
                kwargs[k] = v

    return kwargs


def _field_to_type(field: Any) -> Any:
    """Map a dorm field instance to a Pydantic-ready type annotation.

    Returns one of:

    * The bare base type (``str``, ``int``, …) when the field has no
      translatable constraints — keeps things simple for callers that
      compare annotations directly against the underlying type.
    * ``Annotated[base, Field(...)]`` carrying ``max_length``,
      ``max_digits`` / ``decimal_places``, ``ge=0`` for positive ints,
      OpenAPI ``format`` hint, and any user-supplied validators
      (``MinValueValidator``, ``RegexValidator``, …) translated into
      Pydantic kwargs.
    * ``Literal[*values]`` when the field declares ``choices`` —
      enumerated in the JSON Schema, not just enforced server-side.

    The annotation never includes ``| None``; optionality is composed
    by :func:`_finalize_annotation`."""
    # Enum fields bring their own membership semantics via the enum
    # class; never reduce them to a Literal even though they expose
    # auto-derived ``choices`` for the admin layer.
    if isinstance(field, EnumField):
        return field.enum_cls

    # Choices take precedence over the type table: a ``CharField(choices=...)``
    # should validate as ``Literal[...]``, not as a free-form string.
    choices = getattr(field, "choices", None)
    if choices:
        # dorm stores choices as a list of (value, label) pairs in the
        # canonical case; tolerate the simpler list-of-values form too.
        values = tuple(c[0] if isinstance(c, (tuple, list)) else c for c in choices)
        if values:
            return Literal[values]  # ty: ignore[invalid-type-form]

    base = _base_type_for(field)
    constraint_kwargs = _field_constraint_kwargs(field, base)
    if not constraint_kwargs:
        return base
    return Annotated[base, PydField(**constraint_kwargs)]


def _finalize_annotation(annotation: Any, *, optional: bool) -> Any:
    """Compose the field annotation with optionality.

    Two shapes — picked by whether the field needs a per-arm validator
    (the only case today is :class:`FileField`, which carries a
    ``BeforeValidator`` that returns ``None`` for ``None`` input):

    * No ``BeforeValidator``: ``Annotated[base | None, Field(...)]``.
      ``model_fields[name].annotation`` reads back as ``base | None``,
      so existing call sites that compare against ``(str | None)`` /
      ``(int | None)`` keep working while constraints survive in
      ``model_fields[name].metadata`` and in the JSON Schema.
    * ``BeforeValidator`` on the non-None arm: ``Annotated[base,
      Field(...)] | None``. Putting the constraint *inside* the union
      arm prevents ``max_length`` from being applied to ``None`` after
      the validator returns ``None`` (otherwise pydantic-core raises
      ``TypeError: Unable to apply constraint 'max_length' to supplied
      value None`` under the flattened layout).
    """
    if not optional:
        return annotation
    if hasattr(annotation, "__metadata__") and any(
        type(m).__name__ == "BeforeValidator" for m in annotation.__metadata__
    ):
        return annotation | None
    if hasattr(annotation, "__metadata__"):
        inner = annotation.__origin__
        return Annotated[inner | None, *annotation.__metadata__]
    return annotation | None


def _field_default(field: Any, *, optional: bool) -> Any:
    """Return the default value to pair with the schema annotation.

    Precedence (most specific first):
    1. The dorm Field's own ``default`` (callable → ``default_factory``).
    2. ``None`` for fields the schema has marked optional (``null=True``,
       ``AutoField``, or listed in ``optional=`` / ``Meta.optional``).
    3. ``...`` (required) otherwise.
    """
    if field.has_default():
        if callable(field.default):
            return PydField(default_factory=field.default)
        return field.default
    if optional:
        return None
    return ...


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
        # A field is "optional" in Pydantic terms when callers can omit
        # it from the input — auto-incrementing PK, nullable column,
        # or explicit per-call opt-in. ``has_default()`` does NOT make
        # the annotation nullable: the default value pairs with the
        # bare type, so omitted input gets the field's real default
        # (e.g. ``False`` for ``BooleanField(default=False)``) instead
        # of being silently coerced to ``None``.
        is_optional = (
            f.null or isinstance(f, AutoField) or f.name in optional
        )
        annotation = _finalize_annotation(py_type, optional=is_optional)
        default = _field_default(f, optional=is_optional)
        fields[f.name] = (annotation, default)

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
        )
        annotations[f.name] = _finalize_annotation(py_type, optional=is_optional)
        # Pair the annotation with the field's real default (or ``None``
        # for nullable / optional). Skipping ``setdefault`` for required
        # fields lets Pydantic surface the missing-field error.
        if f.has_default():
            if callable(f.default):
                namespace.setdefault(f.name, PydField(default_factory=f.default))
            else:
                namespace.setdefault(f.name, f.default)
        elif is_optional:
            namespace.setdefault(f.name, None)

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
