from __future__ import annotations

import datetime
import decimal
import enum
import ipaddress
import json
import re
import uuid
from typing import Any, Generic, TypeVar, overload

from .exceptions import ValidationError

# T_value: the Python type stored in the model instance for a given field.
# Subclassing Field[T_value] (e.g. ``CharField(Field[str])``) lets static
# type checkers infer ``user.name`` as ``str`` instead of ``Any`` — the
# same trick SQLAlchemy 2.0 uses with ``Mapped[T]``.
_T = TypeVar("_T")

class _NotProvided:
    """Sentinel for fields with no default. Survives deepcopy as a singleton."""
    _instance: "_NotProvided | None" = None

    def __new__(cls) -> "_NotProvided":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __deepcopy__(self, memo: dict) -> "_NotProvided":
        return self

    def __copy__(self) -> "_NotProvided":
        return self

    def __repr__(self) -> str:
        return "NOT_PROVIDED"


NOT_PROVIDED = _NotProvided()


def _inline_literal(sql: str, params: list) -> str:
    """Replace each ``%s`` placeholder in *sql* with its corresponding
    ``params`` value, formatted as a SQL literal. Used by constraint
    DDL where parameter binding isn't available (``ALTER TABLE ... ADD
    CONSTRAINT ... CHECK (...)`` with bound params is not portable).

    Only handles the value shapes constraints actually produce —
    integers, floats, booleans, strings, NULL. Strings are
    single-quoted with the SQL ``''`` escape; other types use ``repr``
    via ``str()``. Identifier-shaped values must already be quoted by
    the caller (e.g. ``"col"``).
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    pi = 0
    while i < n:
        if sql[i] == "%" and i + 1 < n and sql[i + 1] == "s":
            if pi >= len(params):
                raise ValueError(
                    "_inline_literal: more %s placeholders than params"
                )
            v = params[pi]
            pi += 1
            if v is None:
                out.append("NULL")
            elif isinstance(v, bool):
                out.append("TRUE" if v else "FALSE")
            elif isinstance(v, (int, float)):
                out.append(str(v))
            elif isinstance(v, str):
                # Escape ``'`` per SQL literal rules. Reject embedded
                # NULs which can't appear in a SQL literal anyway.
                if "\x00" in v:
                    raise ValueError(
                        "_inline_literal: NUL byte cannot appear in a "
                        "SQL string literal."
                    )
                out.append("'" + v.replace("'", "''") + "'")
            else:
                # bytes, decimals, dates → fall back to str() and quote.
                # The constraint-emit path doesn't really hit this; if
                # someone needs richer types we'll extend.
                out.append("'" + str(v).replace("'", "''") + "'")
            i += 2
            continue
        out.append(sql[i])
        i += 1
    if pi != len(params):
        raise ValueError(
            f"_inline_literal: {len(params) - pi} param(s) left unconsumed"
        )
    return "".join(out)


class CompositePrimaryKey:
    """Multi-column primary key declaration.

    Set as a model class attribute alongside the column-defining
    fields; the named fields together become the table's primary
    key. ``obj.pk`` returns a tuple of the underlying values, and
    ``Model.objects.get(pk=(a, b))`` decomposes the tuple into
    per-column lookups.

    Example::

        class OrderLine(dorm.Model):
            order = dorm.ForeignKey(Order, on_delete=dorm.CASCADE)
            line_no = dorm.IntegerField()
            quantity = dorm.IntegerField()
            pk = dorm.CompositePrimaryKey("order", "line_no")

    Limitations (intentional, to keep the implementation minimal):

    - **Cannot be the target of a `ForeignKey`.** Multi-column FKs
      would require a sweeping rewrite of the JOIN compiler. Use a
      surrogate auto-PK plus a ``UniqueConstraint`` over the
      composite if you need both styles.
    - **No auto-incrementing component.** All component fields must
      be supplied explicitly on insert; ``bulk_create`` honours
      pre-set values.

    The class is *not* a :class:`Field` — it carries no column of
    its own, just declares which existing fields the metaclass
    should bundle into the PK constraint.
    """

    primary_key: bool = True
    # Make the migration writer / introspection code treat this like
    # a "field with no column" (similar to ``ManyToManyField``).
    concrete: bool = False
    column: str | None = None
    attname: str = "pk"
    auto_created: bool = False

    def __init__(self, *field_names: str) -> None:
        if not field_names:
            raise ValueError(
                "CompositePrimaryKey requires at least one field name."
            )
        self.field_names = field_names
        # Match the Field protocol enough that ``_meta.pk`` consumers
        # don't blow up on missing attributes. ``creation_counter`` is
        # set by the metaclass at attach time.
        self.creation_counter = -1
        self.name: str | None = None
        self.model: Any = None
        self.unique = True
        self.null = False
        self.blank = False
        self.editable = True
        self.serialize = True
        self.choices = None
        self.help_text = ""
        self.verbose_name: str | None = None
        self.db_column: str | None = None
        self.db_tablespace: str | None = None
        self.validators: list = []
        self.default: Any = NOT_PROVIDED
        self.db_index = False
        self.many_to_many = False
        self.many_to_one = False
        self.one_to_many = False
        self.one_to_one = False
        self.related_model = None

    def contribute_to_class(self, cls: Any, name: str) -> None:
        # Wire onto Meta but NOT on the class — there's no column to
        # back, so attribute access goes through the ``Model.pk``
        # property (which the metaclass overrides for composite PK).
        from .conf import _validate_identifier

        self.name = name
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        for field_name in self.field_names:
            _validate_identifier(field_name, kind="CompositePrimaryKey field")
        cls._meta.add_field(self)

    def get_default(self) -> None:
        return None

    def has_default(self) -> bool:
        return False

    def to_python(self, value: Any) -> Any:
        # Accept tuples / lists straight through; let the underlying
        # field types do their own coercion when each component lands.
        if value is None:
            return None
        if isinstance(value, (tuple, list)):
            return tuple(value)
        return value

    def get_db_prep_value(self, value: Any) -> Any:
        return value

    def from_db_value(self, value: Any) -> Any:
        return value

    def db_type(self, connection: Any) -> str | None:
        # No own column — the migration writer emits a separate
        # ``PRIMARY KEY (col1, col2)`` constraint instead.
        return None

    def validate(self, value: Any, model_instance: Any) -> None:
        return None

    def pre_save(self, model_instance: Any, add: bool) -> Any:
        return None

    def __repr__(self) -> str:
        return f"CompositePrimaryKey{self.field_names!r}"


CASCADE = "CASCADE"
PROTECT = "PROTECT"
SET_NULL = "SET NULL"
SET_DEFAULT = "SET DEFAULT"
DO_NOTHING = "NO ACTION"
RESTRICT = "RESTRICT"


class Field(Generic[_T]):
    """Base class for all dorm fields.

    The generic parameter ``_T`` is the Python type the field stores. Static
    type checkers use it via the overloaded ``__get__`` so ``user.name``
    (where ``name = CharField(...)``) is inferred as ``str``, not ``Any``.
    Runtime behaviour is identical regardless of the parameter.
    """

    creation_counter = 0
    auto_created = False

    def __init__(
        self,
        verbose_name: str | None = None,
        name: str | None = None,
        primary_key: bool = False,
        max_length: int | None = None,
        unique: bool = False,
        blank: bool = False,
        null: bool = False,
        db_index: bool = False,
        default: Any = NOT_PROVIDED,
        db_default: Any = NOT_PROVIDED,
        editable: bool = True,
        serialize: bool = True,
        choices: Any = None,
        help_text: str = "",
        db_column: str | None = None,
        db_tablespace: str | None = None,
        db_comment: str | None = None,
        validators: list | None = None,
    ):
        self.verbose_name = verbose_name
        self.name = name
        self.primary_key = primary_key
        self.max_length = max_length
        self.unique = unique or primary_key
        self.blank = blank
        self.null = null
        self.db_index = db_index
        self.default: Any = default
        # ``db_default`` lands on the column DDL as ``DEFAULT <literal>``,
        # making the database itself produce the value when the column
        # is omitted from an INSERT (think: server-side ``now()``,
        # sequence-driven defaults, schema-level booleans). Distinct
        # from ``default=`` which only fires when the Python ``Model``
        # constructor doesn't see a value. Both can coexist —
        # ``default`` wins on Python writes, ``db_default`` covers
        # raw SQL inserts.
        self.db_default: Any = db_default
        self.editable = editable
        self.serialize = serialize
        self.choices = choices
        self.help_text = help_text
        self.db_column = db_column
        self.db_tablespace = db_tablespace
        # ``db_comment`` lands as a ``COMMENT ON COLUMN`` after the
        # column DDL on PostgreSQL / MySQL. SQLite ignores comments.
        # Useful for schema-archaeology work where DBAs read the
        # column definitions directly.
        self.db_comment = db_comment
        self.validators: list = list(validators) if validators else []
        self.model = None
        self.attname = None
        self.column = None
        self.concrete = True
        self.many_to_many = False
        self.many_to_one = False
        self.one_to_many = False
        self.one_to_one = False
        self.related_model = None

        self.creation_counter = Field.creation_counter
        Field.creation_counter += 1

    def contribute_to_class(self, cls, name: str):
        from .conf import _validate_identifier

        self.name = name
        self.attname = name
        self.column = self.db_column or name
        _validate_identifier(self.column, kind=f"{cls.__name__}.{name}.db_column")
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)

    def deconstruct(self) -> tuple[str | None, str, list, dict]:
        """Return a 4-tuple suitable for migration serialisation:
        ``(name, dotted_class_path, args, kwargs)``.

        Mirrors Django's :meth:`django.db.models.Field.deconstruct`
        so custom fields the user writes — and migration tools that
        consume them — can reconstruct a ``Field`` instance from
        the serialised form by:

        .. code-block:: python

            from importlib import import_module
            mod_path, _, cls_name = path.rpartition(".")
            cls = getattr(import_module(mod_path), cls_name)
            field = cls(*args, **kwargs)

        The default implementation walks the constructor's keyword
        arguments and emits whichever ones differ from the
        framework-shipped defaults. Subclasses with extra
        constructor parameters should override and call ``super()``
        first to extend the kwargs dict.
        """
        path = f"{type(self).__module__}.{type(self).__qualname__}"
        kwargs: dict[str, Any] = {}
        # Mirror Django's "include only non-default values" rule —
        # keeps generated migrations terse and re-readable.
        defaults = {
            "primary_key": False,
            "max_length": None,
            "unique": False,
            "blank": False,
            "null": False,
            "db_index": False,
            "default": NOT_PROVIDED,
            "editable": True,
            "serialize": True,
            "choices": None,
            "help_text": "",
            "db_column": None,
            "db_tablespace": None,
            "validators": [],
        }
        for attr, default in defaults.items():
            val = getattr(self, attr, default)
            if val != default:
                kwargs[attr] = val
        # ``unique=primary_key`` is a derived value — drop it when it
        # only mirrors ``primary_key=True`` (otherwise the
        # reconstruction emits ``unique=True`` redundantly).
        if kwargs.get("primary_key") and kwargs.get("unique") is True:
            kwargs.pop("unique", None)
        return self.name, path, [], kwargs

    @overload
    def __get__(self, instance: None, owner: type) -> "Field[_T]": ...
    @overload
    def __get__(self, instance: object, owner: type) -> _T: ...
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.attname)

    def __set__(self, instance: object, value: _T | Any) -> None:
        # attname is set by contribute_to_class before any instance can
        # exist; the descriptor is unreachable while it's still None.
        assert self.attname is not None
        instance.__dict__[self.attname] = self.to_python(value)

    def has_default(self) -> bool:
        return self.default is not NOT_PROVIDED

    def get_default(self):
        if self.default is NOT_PROVIDED:
            return None
        if callable(self.default):
            return self.default()
        return self.default

    def to_python(self, value):
        return value

    def get_db_prep_value(self, value):
        return value

    def from_db_value(self, value):
        return value

    def db_type(self, connection) -> str | None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement db_type()")

    def validate(self, value, model_instance):
        if not self.null and value is None:
            raise ValidationError(f"Field '{self.name}' cannot be null.")
        if self.choices and value is not None:
            valid = [c[0] for c in self.choices]
            if value not in valid:
                raise ValidationError(
                    f"Value '{value}' is not a valid choice for '{self.name}'."
                )
        if value is not None:
            for validator in self.validators:
                validator(value)

    def pre_save(self, model_instance: Any, add: bool) -> Any:
        return model_instance.__dict__.get(self.attname)

    def get_internal_type(self) -> str:
        return self.__class__.__name__

    def __repr__(self):
        path = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return f"<{path}: {self.name}>"


class AutoField(Field[int]):
    def __init__(self, **kwargs):
        kwargs.setdefault("primary_key", True)
        kwargs.setdefault("editable", False)
        super().__init__(**kwargs)

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "SERIAL"
        return "INTEGER"

    def rel_db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "INTEGER"
        return "INTEGER"

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class BigAutoField(AutoField):
    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BIGSERIAL"
        return "INTEGER"

    def rel_db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BIGINT"
        return "INTEGER"


class SmallAutoField(AutoField):
    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "SMALLSERIAL"
        return "INTEGER"

    def rel_db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "SMALLINT"
        return "INTEGER"


class IntegerField(Field[int]):
    def to_python(self, value):
        if value is None:
            return None
        return int(value)

    def db_type(self, connection) -> str:
        return "INTEGER"


class SmallIntegerField(IntegerField):
    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "SMALLINT"
        return "INTEGER"


class BigIntegerField(IntegerField):
    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BIGINT"
        return "INTEGER"


class PositiveIntegerField(IntegerField):
    def db_type(self, connection) -> str:
        return "INTEGER"

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        if value is not None and value < 0:
            raise ValidationError(f"Field '{self.name}' must be a positive integer.")


class PositiveSmallIntegerField(PositiveIntegerField):
    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "SMALLINT"
        if backend == "mysql":
            return "SMALLINT UNSIGNED"
        return "INTEGER"


class PositiveBigIntegerField(PositiveIntegerField):
    """64-bit unsigned integer counterpart of :class:`PositiveIntegerField`.

    Mirrors Django's ``PositiveBigIntegerField`` — useful for IDs from
    upstream services that exceed ``2**31`` (Twitter snowflakes,
    KSUID-derived ints, etc.).
    """

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BIGINT"
        if backend == "mysql":
            return "BIGINT UNSIGNED"
        return "INTEGER"


class FloatField(Field[float]):
    def to_python(self, value):
        if value is None:
            return None
        return float(value)

    def db_type(self, connection) -> str:
        return "REAL"


class DecimalField(Field[decimal.Decimal]):
    def __init__(self, max_digits: int = 10, decimal_places: int = 2, **kwargs):
        self.max_digits = max_digits
        self.decimal_places = decimal_places
        super().__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return None
        return decimal.Decimal(str(value))

    def from_db_value(self, value):
        # SQLite stores NUMERIC with REAL affinity, so the driver
        # returns a Python ``float`` — not a :class:`decimal.Decimal`
        # as the field annotation promises. Round-trip through ``str``
        # so the returned value preserves the column's declared
        # precision (``Decimal('1.0001')``, not ``1.00009999...``).
        # PostgreSQL's psycopg adapter already returns ``Decimal`` so
        # the conversion is a no-op there; explicit guard keeps that
        # path zero-cost.
        if value is None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(str(value))

    def get_db_prep_value(self, value):
        if value is None:
            return None
        return str(value)

    def db_type(self, connection) -> str:
        return f"NUMERIC({self.max_digits}, {self.decimal_places})"


class CharField(Field[str]):
    def __init__(self, max_length: int = 255, **kwargs):
        self.max_length = max_length
        super().__init__(max_length=max_length, **kwargs)

    def to_python(self, value):
        if value is None:
            return None
        return str(value)

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        if value is not None and self.max_length and len(value) > self.max_length:
            raise ValidationError(
                f"Field '{self.name}' value is too long (max {self.max_length} chars)."
            )

    def db_type(self, connection) -> str:
        return f"VARCHAR({self.max_length})"


class TextField(Field[str]):
    def to_python(self, value):
        if value is None:
            return None
        return str(value)

    def db_type(self, connection) -> str:
        return "TEXT"


class BooleanField(Field[bool]):
    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)

    def get_db_prep_value(self, value):
        if value is None:
            return None
        return bool(value)

    def from_db_value(self, value):
        if value is None:
            return None
        return bool(value)

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BOOLEAN"
        return "INTEGER"


class NullBooleanField(BooleanField):
    def __init__(self, **kwargs):
        kwargs["null"] = True
        super().__init__(**kwargs)


class DateField(Field[datetime.date]):
    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, str):
            return datetime.date.fromisoformat(value)
        return value

    def get_db_prep_value(self, value):
        if isinstance(value, datetime.date):
            return value.isoformat()
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return datetime.date.fromisoformat(value)
        return value

    def db_type(self, connection) -> str:
        return "DATE"


class TimeField(Field[datetime.time]):
    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.time):
            return value
        if isinstance(value, str):
            return datetime.time.fromisoformat(value)
        return value

    def get_db_prep_value(self, value):
        if isinstance(value, datetime.time):
            return value.isoformat()
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return datetime.time.fromisoformat(value)
        return value

    def db_type(self, connection) -> str:
        return "TIME"


def _settings_use_tz() -> bool:
    """Read ``settings.USE_TZ`` defensively — settings may be
    unconfigured (e.g. during model class construction at import
    time). Default ``False`` matches Django <4.0 to avoid breaking
    existing dorm projects on upgrade."""
    try:
        from .conf import settings

        return bool(getattr(settings, "USE_TZ", False))
    except Exception:
        return False


def _settings_default_tz() -> datetime.tzinfo:
    """Resolve ``settings.TIME_ZONE`` to a ``tzinfo``. Falls back to
    UTC when the value is missing or unrecognised; tests / CLI tools
    that don't configure dorm still get a sensible default."""
    try:
        from .conf import settings
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        name = getattr(settings, "TIME_ZONE", "UTC") or "UTC"
        try:
            return ZoneInfo(name)
        except ZoneInfoNotFoundError:
            return datetime.timezone.utc
    except Exception:
        return datetime.timezone.utc


class DateTimeField(Field[datetime.datetime]):
    """Datetime column.

    ``settings.USE_TZ`` controls timezone handling:

    - ``False`` (default, Django <4 behaviour): naive datetimes are
      stored as-is. Aware datetimes are stored verbatim.
    - ``True`` (Django ≥4 behaviour): every read returns a
      tz-aware datetime. Naive datetimes coming in from the user
      are interpreted in ``settings.TIME_ZONE`` and converted to
      UTC before storage. PostgreSQL columns become
      ``TIMESTAMP WITH TIME ZONE`` so the engine round-trips the
      offset.
    """

    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        if auto_now or auto_now_add:
            kwargs["editable"] = False
        if auto_now_add:
            kwargs.setdefault("default", lambda: datetime.datetime.now(datetime.timezone.utc))
        super().__init__(**kwargs)

    def _make_aware(self, value: datetime.datetime) -> datetime.datetime:
        """Attach ``settings.TIME_ZONE`` to *value* when it's naive
        and ``USE_TZ`` is on. Already-aware datetimes pass through."""
        if value.tzinfo is None:
            return value.replace(tzinfo=_settings_default_tz())
        return value

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            if _settings_use_tz():
                return self._make_aware(value)
            return value
        if isinstance(value, str):
            parsed = datetime.datetime.fromisoformat(value)
            if _settings_use_tz():
                return self._make_aware(parsed)
            return parsed
        return value

    def get_db_prep_value(self, value):
        if isinstance(value, datetime.datetime):
            if _settings_use_tz():
                # Normalise to UTC before serialising — backends
                # store either ``TIMESTAMPTZ`` (PG) which preserves
                # the offset, or text (SQLite) which collapses to
                # whatever we write. UTC is the only safe wire form.
                aware = self._make_aware(value)
                return aware.astimezone(datetime.timezone.utc).isoformat()
            return value.isoformat()
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            parsed = datetime.datetime.fromisoformat(value)
        elif isinstance(value, datetime.datetime):
            parsed = value
        else:
            return value
        if _settings_use_tz():
            # SQLite stores ISO text without a guaranteed offset;
            # PostgreSQL TIMESTAMPTZ rehydrates with offset attached.
            # Either way we want a UTC-aware datetime on the
            # Python side so callers can compare across rows safely.
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            else:
                parsed = parsed.astimezone(datetime.timezone.utc)
        return parsed

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            return (
                "TIMESTAMP WITH TIME ZONE"
                if _settings_use_tz()
                else "TIMESTAMP"
            )
        return "DATETIME"

    def pre_save(self, model_instance, add: bool):
        assert self.attname is not None
        if self.auto_now or (self.auto_now_add and add):
            value = datetime.datetime.now(datetime.timezone.utc)
            setattr(model_instance, self.attname, value)
            return value
        return (
            super().get_default()
            if not hasattr(model_instance, self.attname)
            else getattr(model_instance, self.attname)
        )


class EmailField(CharField):
    EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 254)
        super().__init__(**kwargs)

    def to_python(self, value):
        # Validate at assignment time (``__set__`` invokes ``to_python``).
        # That way a bogus value is rejected by ``Customer(email="x")`` /
        # ``Customer.objects.create(email="x")`` even when the caller
        # never invokes ``full_clean()``. Reads from the DB go through
        # ``from_db_value`` (direct dict write) and bypass this check —
        # so historical bad rows still load.
        if value is None or value == "":
            return value
        if not isinstance(value, str) or not self.EMAIL_RE.match(value):
            raise ValidationError(
                {self.name or "email": f"'{value}' is not a valid email address."}
            )
        return value

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        if value and not self.EMAIL_RE.match(value):
            raise ValidationError(f"'{value}' is not a valid email address.")


class URLField(CharField):
    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 200)
        super().__init__(**kwargs)


class SlugField(CharField):
    SLUG_RE = re.compile(r"^[-a-zA-Z0-9_]+$")

    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 50)
        kwargs.setdefault("db_index", True)
        super().__init__(**kwargs)

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        if value and not self.SLUG_RE.match(value):
            raise ValidationError(f"'{value}' is not a valid slug.")


class UUIDField(Field[uuid.UUID]):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))

    def get_db_prep_value(self, value):
        if value is None:
            return None
        return str(value)

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "UUID"
        return "VARCHAR(36)"


class IPAddressField(Field[str]):
    def to_python(self, value):
        if value is None:
            return None
        try:
            return str(ipaddress.IPv4Address(value))
        except ValueError:
            raise ValidationError(f"'{value}' is not a valid IPv4 address.")

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "INET"
        return "VARCHAR(39)"


class GenericIPAddressField(Field[str]):
    def to_python(self, value):
        if value is None:
            return None
        try:
            return str(ipaddress.ip_address(value))
        except ValueError:
            raise ValidationError(f"'{value}' is not a valid IP address.")

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "INET"
        return "VARCHAR(39)"


class JSONField(Field[Any]):
    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return value

    def get_db_prep_value(self, value):
        if value is None:
            return None
        return json.dumps(value)

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return value

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "JSONB"
        return "TEXT"


class ArrayField(Field[list]):
    """PostgreSQL native array column.

    Stores a homogeneous list of values whose element type is given by
    *base_field*. SQLite doesn't have arrays — this field raises
    ``NotImplementedError`` at ``db_type`` time on SQLite so you find
    out at migrate, not at query.

    Example::

        class Article(dorm.Model):
            tags = dorm.ArrayField(dorm.CharField(max_length=50), null=True)

    Usage::

        Article.objects.create(tags=["python", "orm"])
        # Filter for membership (PG ``ANY`` operator) — use ``__contains``:
        Article.objects.filter(tags__contains=["python"])
    """

    def __init__(self, base_field: "Field", **kwargs: Any) -> None:
        self.base_field = base_field
        super().__init__(**kwargs)

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                "ArrayField is only supported on PostgreSQL. Use a separate "
                "M2M / JSONField on SQLite."
            )
        inner = self.base_field.db_type(connection)
        return f"{inner}[]"

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, list):
            return [self.base_field.to_python(v) for v in value]
        # Allow tuples / generators on assignment for ergonomic use.
        return [self.base_field.to_python(v) for v in list(value)]

    def get_db_prep_value(self, value):
        if value is None:
            return None
        return [self.base_field.get_db_prep_value(v) for v in value]


class GeneratedField(Field[Any]):
    """A column whose value is computed from a SQL expression at write
    time. PostgreSQL ≥ 12 and SQLite ≥ 3.31 both support
    ``GENERATED ALWAYS AS (...) STORED``; that's the form we emit.

    Example::

        class Order(dorm.Model):
            quantity = dorm.IntegerField()
            price    = dorm.DecimalField(max_digits=10, decimal_places=2)
            total    = dorm.GeneratedField(
                expression="quantity * price",
                output_field=dorm.DecimalField(max_digits=12, decimal_places=2),
            )

    The *expression* is spliced verbatim into the column DDL — it is
    your responsibility to keep it side-effect-free and to reference
    only columns of the same table. Field assignment is rejected at
    runtime: a generated column is computed by the database and any
    Python-side write would conflict with the underlying engine.

    *output_field* is required and supplies ``db_type`` /
    ``from_db_value`` / ``to_python``. It is otherwise inert.
    """

    GENERATED_EXPR_ALLOWED_RE = re.compile(
        r"^[A-Za-z0-9_+\-*/(),. \"'%]+$"
    )

    def __init__(
        self,
        *,
        expression: str,
        output_field: "Field",
        stored: bool = True,
        **kwargs: Any,
    ) -> None:
        if not isinstance(expression, str) or not expression.strip():
            raise ValidationError(
                "GeneratedField(expression=...) must be a non-empty string."
            )
        if not self.GENERATED_EXPR_ALLOWED_RE.match(expression):
            # Whitelist instead of blacklist: we splice this into DDL
            # without a parameter binding, so any character outside the
            # documented grammar (alphanumerics, arithmetic, comma,
            # parens, quotes, percent for modulo / LIKE patterns) is a
            # potential injection sink. Users with exotic needs can
            # write a ``RunSQL`` migration instead.
            raise ValidationError(
                f"GeneratedField(expression={expression!r}) contains characters "
                "outside the documented grammar (letters, digits, _, arithmetic, "
                "(),. and quotes). Use a RunSQL migration for complex generated "
                "columns."
            )
        self.expression = expression
        self.output_field = output_field
        self.stored = bool(stored)
        kwargs.setdefault("editable", False)
        # Generated columns can't be NULL-checked at the Python layer in
        # the usual way — defer NOT NULL handling to the database.
        kwargs.setdefault("null", True)
        super().__init__(**kwargs)

    def db_type(self, connection) -> str:
        base_type = self.output_field.db_type(connection)
        kind = "STORED" if self.stored else "VIRTUAL"
        return f"{base_type} GENERATED ALWAYS AS ({self.expression}) {kind}"

    def to_python(self, value):
        return self.output_field.to_python(value)

    def from_db_value(self, value):
        if hasattr(self.output_field, "from_db_value"):
            return self.output_field.from_db_value(value)
        return value

    def get_db_prep_value(self, value):
        # Generated columns are read-only — never bound on INSERT/UPDATE.
        return None

    def __set__(self, instance: object, value: Any) -> None:
        # Reject Python writes outright. Without this, an assignment
        # like ``order.total = ...`` would silently succeed, then the
        # next refresh_from_db would overwrite it — confusing.
        raise AttributeError(
            f"GeneratedField {self.attname!r} is read-only; the database "
            "computes it from the expression."
        )

    def pre_save(self, model_instance: Any, add: bool) -> Any:
        return None


class FilePathField(CharField):
    """``CharField`` whose value is restricted to a file path under
    *path*. Validates on assignment that the filename matches *match*
    (regex) and lives at *recursive* depth from *path*.

    Mirrors Django's ``FilePathField`` — used for "let the user pick
    a file off disk" forms (admin uploads, config selectors). The
    column type is plain ``VARCHAR``; the validation runs Python-side
    when a value is set and during ``full_clean()``.
    """

    def __init__(
        self,
        path: str = "",
        *,
        match: str | None = None,
        recursive: bool = False,
        allow_files: bool = True,
        allow_folders: bool = False,
        max_length: int = 100,
        **kwargs: Any,
    ) -> None:
        self.path = path
        self.match = match
        self.recursive = recursive
        self.allow_files = allow_files
        self.allow_folders = allow_folders
        super().__init__(max_length=max_length, **kwargs)


class BinaryField(Field[bytes]):
    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        return value

    def from_db_value(self, value):
        # psycopg returns ``memoryview`` for BYTEA columns; sqlite3
        # already returns ``bytes``. Without coercion the field's
        # ``Field[bytes]`` annotation lied — ``obj.data.startswith
        # (b"\\x89")`` raised ``AttributeError`` because ``memoryview``
        # has no ``.startswith``.
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        return value

    def db_type(self, connection) -> str:
        backend = getattr(connection, "vendor", "sqlite")
        if backend == "postgresql":
            return "BYTEA"
        return "BLOB"


class FileField(Field[Any]):
    """Stores a file via a pluggable :class:`dorm.storage.Storage` backend.

    The column itself is a ``VARCHAR(max_length)`` holding the
    storage-side *name* (relative path / S3 key). The Python value
    exposed by the descriptor is a :class:`dorm.storage.FieldFile` —
    a thin wrapper that knows the bound model instance and routes
    ``.url`` / ``.size`` / ``.open()`` / ``.delete()`` through the
    configured storage.

    Configuration:

    - ``upload_to`` — directory template for newly uploaded files.
      Accepts a static string (``"docs/"``), a ``strftime`` template
      (``"docs/%Y/%m/"``, evaluated at save time), or a callable
      ``f(instance, filename) -> str`` for fully dynamic paths.
    - ``storage`` — the :class:`Storage` to use. Either a
      ``Storage`` instance, a string alias resolved against
      ``settings.STORAGES`` (``"default"`` is the fallback), or
      ``None`` (which means: defer the lookup until first use, so
      ``settings.STORAGES`` can change between import time and the
      first request).

    Example::

        class Document(dorm.Model):
            name = dorm.CharField(max_length=100)
            attachment = dorm.FileField(upload_to="docs/%Y/%m/")

        doc = Document(name="Q1 report")
        doc.attachment = dorm.ContentFile(b"PDF bytes here", name="q1.pdf")
        doc.save()

        doc.attachment.url      # storage.url(name)
        doc.attachment.size     # storage.size(name)
        with doc.attachment.open("rb") as fh:
            data = fh.read()
        doc.attachment.delete()  # removes file + clears the column
    """

    # Public marker that ``Model.__init__`` checks to route assignment
    # through ``__set__`` (the descriptor path) instead of writing to
    # ``__dict__`` directly. Set this to ``True`` on any custom Field
    # subclass that installs itself as a class-level descriptor and
    # needs ``__set__`` to fire on assignment — see the
    # :ref:`Custom fields with descriptors` note in
    # :doc:`docs/models.md`.
    uses_class_descriptor: bool = True

    def __init__(
        self,
        upload_to: str | Any = "",
        *,
        storage: "str | Any | None" = None,
        max_length: int = 255,
        **kwargs: Any,
    ) -> None:
        self.upload_to = upload_to
        self._storage_arg = storage
        self.max_length = max_length
        kwargs.setdefault("max_length", max_length)
        super().__init__(**kwargs)

    def contribute_to_class(self, cls: Any, name: str) -> None:
        # Reinstall the FileField as a class-level descriptor so
        # ``obj.attachment = ContentFile(...)`` actually triggers our
        # ``__set__`` (the metaclass strips field instances from class
        # attrs by default). This mirrors what ``RelatedField`` does
        # for its FK descriptor.
        super().contribute_to_class(cls, name)
        setattr(cls, name, self)

    # ── Storage resolution ───────────────────────────────────────────────────

    @property
    def storage(self) -> "Any":
        """Lazily resolve the storage backend.

        Resolves on every access (not at field construction) so a
        ``FileField`` declared at module import time still picks up
        whatever ``settings.STORAGES`` looks like once
        :func:`dorm.configure` has run — and reflects later
        reconfigurations (typical in tests). Caching lives at the
        registry level: :func:`dorm.storage.get_storage` memoises the
        instance per alias and :func:`dorm.storage.reset_storages`
        invalidates it. We don't keep a copy here too, because that
        would require knowing when the registry was reset.
        """
        from .storage import Storage, get_storage

        arg = self._storage_arg
        if isinstance(arg, Storage):
            return arg
        alias = arg if isinstance(arg, str) and arg else "default"
        return get_storage(alias)

    # ── Naming ───────────────────────────────────────────────────────────────

    def _render_target_name(self, instance: Any, filename: str) -> str:
        """Combine ``upload_to`` with the user-supplied basename.

        ``upload_to`` is one of:
          - a string with optional ``strftime`` placeholders (rendered
            against the *current* time, matching Django).
          - a callable ``f(instance, filename) -> str`` that returns
            the full storage name (``upload_to`` itself, no further
            joining).
        """
        upload_to = self.upload_to
        if not isinstance(upload_to, str) and callable(upload_to):
            return str(upload_to(instance, filename))
        directory = upload_to or ""
        if directory:
            import datetime as _dt
            directory = _dt.datetime.now().strftime(directory)
        # ``filename`` may carry user-controlled path segments — keep
        # only the basename so the upload can never escape *upload_to*.
        from .storage import Storage
        cleaned = Storage.get_valid_name(filename)
        if directory:
            return f"{directory.rstrip('/')}/{cleaned}"
        return cleaned

    # ── Field hooks ──────────────────────────────────────────────────────────

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self
        from .storage import FieldFile

        cache_key = f"_fieldfile_{self.attname}"
        cached = instance.__dict__.get(cache_key)
        raw = instance.__dict__.get(self.attname)
        # Re-create the wrapper if the underlying name changed (assignment
        # via ``obj.attachment = "other.pdf"`` etc.) so the FieldFile we
        # return is always in sync with the stored column.
        if cached is None or cached.name != (raw or ""):
            wrapper = FieldFile(instance, self, raw if isinstance(raw, str) else None)
            if isinstance(raw, FieldFile):
                wrapper = raw
                wrapper.instance = instance
                wrapper.field = self
            instance.__dict__[cache_key] = wrapper
            cached = wrapper
        return cached

    def __set__(self, instance: Any, value: Any) -> None:
        from .storage import File, FieldFile

        assert self.attname is not None
        if value is None or value == "":
            instance.__dict__[self.attname] = None
            instance.__dict__.pop(f"_fieldfile_{self.attname}", None)
            return
        if isinstance(value, str):
            instance.__dict__[self.attname] = value
            instance.__dict__.pop(f"_fieldfile_{self.attname}", None)
            return
        if isinstance(value, FieldFile):
            # Reassigning a FieldFile (e.g. from another instance) just
            # carries its name across — the file already lives on the
            # storage; we don't want to copy or re-upload.
            instance.__dict__[self.attname] = value.name or None
            instance.__dict__.pop(f"_fieldfile_{self.attname}", None)
            return
        if isinstance(value, File):
            # Pending upload — stash the File on the instance under a
            # private slot. ``pre_save`` writes it to storage on the
            # next ``Model.save()`` and replaces the slot with the
            # final storage name.
            instance.__dict__[f"_pending_file_{self.attname}"] = value
            # ``attname`` itself stays at whatever was there before
            # (None for new instances) so save() can tell adding from
            # updating; pre_save populates it.
            instance.__dict__.pop(f"_fieldfile_{self.attname}", None)
            return
        raise ValidationError(
            f"FileField {self.name!r}: cannot assign {type(value).__name__}; "
            "expected File / ContentFile / str / None."
        )

    def pre_save(self, model_instance: Any, add: bool) -> Any:
        """Persist any pending upload, then return the storage name.

        Called by :meth:`Model.save` (and ``asave`` via the same path)
        before binding the column. If a ``File`` was assigned via
        ``__set__``, this is the moment it actually hits the storage —
        keeping I/O at save time avoids surprising side effects from
        attribute assignment.

        When the surrounding code is inside an :func:`atomic` /
        :func:`aatomic` block, the freshly written file is queued for
        deletion via :func:`on_rollback` so a transaction that later
        rolls back doesn't leave an orphan on disk / in S3. Outside any
        active transaction, no cleanup is registered (there's nothing
        to undo).
        """
        assert self.attname is not None
        pending = model_instance.__dict__.pop(f"_pending_file_{self.attname}", None)
        if pending is not None:
            target = self._render_target_name(model_instance, pending.name or "upload")
            saved = self.storage.save(
                target, pending, max_length=self.max_length
            )
            model_instance.__dict__[self.attname] = saved
            # Schedule a rollback cleanup so an outer ``atomic()`` that
            # later raises doesn't leave the bytes on storage with no
            # row referencing them. ``on_rollback`` is a no-op when
            # there's no active transaction, so the non-atomic happy
            # path stays unchanged.
            from .transaction import on_rollback
            storage = self.storage
            on_rollback(lambda _name=saved, _s=storage: _s.delete(_name))
        return model_instance.__dict__.get(self.attname)

    def get_db_prep_value(self, value: Any) -> Any:
        from .storage import FieldFile

        if value is None:
            return None
        if isinstance(value, FieldFile):
            return value.name or None
        return value

    def to_python(self, value: Any) -> Any:
        from .storage import File, FieldFile

        if value is None or isinstance(value, (str, File, FieldFile)):
            return value
        return str(value)

    def from_db_value(self, value: Any) -> Any:
        # Reads from the database surface as plain strings; the
        # descriptor wraps them on access (see ``__get__``).
        return value

    def db_type(self, connection: Any) -> str:
        return f"VARCHAR({self.max_length})"


class ImageField(FileField):
    """A :class:`FileField` that validates uploads parse as images.

    Stores the same way as ``FileField`` (storage name in a
    ``VARCHAR``) but rejects content that ``Pillow`` (PIL) can't open.
    The validation runs at assignment time, before the bytes hit
    storage — so a malformed payload never lands.

    Requires the ``image`` extra::

        pip install 'djanorm[image]'

    Example::

        class Avatar(dorm.Model):
            owner = dorm.ForeignKey(User, on_delete=dorm.CASCADE)
            picture = dorm.ImageField(upload_to="avatars/%Y/")

        avatar = Avatar(owner=user)
        avatar.picture = dorm.File(open("photo.jpg", "rb"), name="photo.jpg")
        avatar.save()  # Pillow check ran in __set__; if it weren't an
                       # image, the assignment above would have raised.

    String / FieldFile assignments aren't re-validated (those rows
    are already on storage and the bytes are out of dorm's hands).
    """

    def __set__(self, instance: Any, value: Any) -> None:
        from .storage import File, FieldFile

        # Only freshly-assigned ``File`` instances are content the
        # caller is uploading right now. Strings and ``FieldFile``s
        # refer to bytes already on storage; re-reading them here just
        # to validate is wasteful and would force the validation to
        # depend on storage being reachable.
        if isinstance(value, File) and not isinstance(value, FieldFile):
            self._validate_is_image(value)
        super().__set__(instance, value)

    @staticmethod
    def _validate_is_image(file_obj: Any) -> None:
        """Open the file via Pillow and verify it's a recognisable
        image. Resets the stream after verification so the storage
        backend reads the same bytes from position zero."""
        try:
            from PIL import Image, UnidentifiedImageError
        except ImportError as exc:
            raise ImportError(
                "ImageField requires the 'Pillow' package. Install the "
                "optional extra: pip install 'djanorm[image]'."
            ) from exc

        underlying = getattr(file_obj, "file", None)
        if underlying is None or not hasattr(underlying, "read"):
            raise ValidationError(
                "ImageField: cannot read content for image validation."
            )

        # Snapshot the stream position so a subsequent ``storage.save``
        # writes the full payload, not the bytes after the verify
        # cursor. ``Image.open`` is lazy — call ``verify`` to force
        # parsing without decoding pixels (cheaper than full load).
        position = underlying.tell() if hasattr(underlying, "tell") else None
        try:
            try:
                im = Image.open(underlying)
                im.verify()
            except (UnidentifiedImageError, OSError) as exc:
                raise ValidationError(
                    f"ImageField: file is not a recognisable image: {exc}"
                ) from exc
        finally:
            if position is not None and hasattr(underlying, "seek"):
                underlying.seek(position)


class DurationField(Field[datetime.timedelta]):
    """Stores a :class:`datetime.timedelta`.

    On PostgreSQL it maps to native ``INTERVAL`` (psycopg returns and
    accepts :class:`datetime.timedelta` directly). On SQLite — which has
    no interval type — we store the duration as an integer number of
    microseconds in a ``BIGINT`` column, matching Django's strategy. The
    Python value is always a ``timedelta``; the SQLite encoding is an
    implementation detail callers don't see.

    Example::

        class Job(dorm.Model):
            timeout = dorm.DurationField()

        Job.objects.create(timeout=datetime.timedelta(minutes=5))
    """

    _MICROS_PER_SECOND = 10 ** 6

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.timedelta):
            return value
        if isinstance(value, (int, float)):
            # Treat raw numbers as microseconds — matches the SQLite
            # storage round-trip and what users get back from
            # ``from_db_value`` on that backend.
            return datetime.timedelta(microseconds=int(value))
        if isinstance(value, str):
            return self._parse_iso8601(value)
        raise ValidationError(
            f"Field '{self.name}': cannot convert {value!r} to timedelta."
        )

    @classmethod
    def _parse_iso8601(cls, value: str) -> datetime.timedelta:
        # Accept the shapes ``isoformat`` / ``str(timedelta)`` /
        # SQLite microsecond ints round-trip back from the database:
        #
        #   * ``HH:MM:SS[.ffffff]`` — PG INTERVAL coerced to text.
        #   * ``"<int> microseconds"`` / ``"<int>"`` — SQLite raw
        #     integer storage or older Django dumps.
        #   * ``"-N day(s), HH:MM:SS"`` — Python's ``str(timedelta)``
        #     for negative durations. Django's parser accepts this
        #     shape; without it any negative interval written via
        #     ``str(td)`` (or read back from a custom PG cast that
        #     emits the Python repr) raised ``ValidationError``.
        s = value.strip()
        if not s:
            raise ValidationError(
                f"Field '{getattr(cls, 'name', '?')}': empty duration string."
            )
        if s.lstrip("-").isdigit():
            return datetime.timedelta(microseconds=int(s))

        # Native ``str(timedelta)`` form: ``"-1 day, 22:30:00"`` or
        # ``"5 days, 0:00:00"``. Pull the day component off and
        # apply it to the parsed HH:MM:SS portion.
        days_offset = datetime.timedelta(0)
        import re as _re

        day_match = _re.match(r"^(-?\d+)\s+days?,\s*(.*)$", s)
        if day_match:
            days_offset = datetime.timedelta(days=int(day_match.group(1)))
            s = day_match.group(2)

        # ``HH:MM:SS[.ffffff]`` with optional leading sign.
        sign = 1
        if s and s[0] in "+-":
            if s[0] == "-":
                sign = -1
            s = s[1:]
        parts = s.split(":")
        if len(parts) != 3:
            raise ValidationError(
                f"DurationField: cannot parse {value!r}; "
                "expected 'HH:MM:SS[.ffffff]' or microseconds as int."
            )
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            secs = float(parts[2])
        except ValueError as exc:
            raise ValidationError(
                f"DurationField: cannot parse {value!r}: {exc}"
            ) from exc
        td = datetime.timedelta(hours=hours, minutes=minutes, seconds=secs)
        td = -td if sign < 0 else td
        return days_offset + td

    def get_db_prep_value(self, value):
        if value is None:
            return None
        if not isinstance(value, datetime.timedelta):
            value = self.to_python(value)
        # On the binding path we don't have *connection* — the queryset
        # rewriter calls ``get_db_prep_value`` before the SQL is dispatched
        # to a specific backend. We return the timedelta unchanged so PG
        # binds it natively as INTERVAL; SQLite's adapter chain (see
        # ``dorm.db.backends.sqlite``) converts it to int microseconds at
        # the cursor boundary. ``from_db_value`` reverses both encodings.
        assert isinstance(value, datetime.timedelta)
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.timedelta):
            return value
        if isinstance(value, (int, float)):
            return datetime.timedelta(microseconds=int(value))
        if isinstance(value, str):
            return self._parse_iso8601(value)
        return value

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            return "INTERVAL"
        return "BIGINT"


# ── Enum field ────────────────────────────────────────────────────────────────


_TEnum = TypeVar("_TEnum", bound=enum.Enum)


class EnumField(Field[Any], Generic[_TEnum]):
    """Stores a Python :class:`enum.Enum` member.

    The column type is derived from the enum's underlying value type:
    string-valued enums map to ``VARCHAR(max_length)``, integer-valued
    enums to ``INTEGER``. The Python instance always carries the enum
    *member* (e.g. ``Status.ACTIVE``); reading from the DB rehydrates
    by member ``.value``.

    ``choices`` is auto-derived from the enum so admin / form layers see
    every member without restating them in ``Meta``.

    Example::

        class Status(enum.Enum):
            ACTIVE = "active"
            ARCHIVED = "archived"

        class Article(dorm.Model):
            status = dorm.EnumField(Status, default=Status.ACTIVE)
    """

    _STRING_DEFAULT_MAX = 50

    def __init__(
        self,
        enum_cls: type[_TEnum],
        *,
        max_length: int | None = None,
        **kwargs: Any,
    ) -> None:
        if not (isinstance(enum_cls, type) and issubclass(enum_cls, enum.Enum)):
            raise ValidationError(
                "EnumField(enum_cls=...) must be a subclass of enum.Enum."
            )
        self.enum_cls = enum_cls
        sample = next(iter(enum_cls)).value
        self._is_string = isinstance(sample, str)
        if self._is_string:
            longest = max(len(m.value) for m in enum_cls)
            self.max_length = max_length if max_length is not None else max(
                longest, self._STRING_DEFAULT_MAX
            )
            kwargs.setdefault("max_length", self.max_length)
        else:
            self.max_length = None
        # Auto-derive choices for admin / forms unless the caller
        # passed an explicit set (rare but allowed for narrowing).
        kwargs.setdefault(
            "choices",
            [(member.value, member.name) for member in enum_cls],
        )
        super().__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, self.enum_cls):
            return value
        try:
            return self.enum_cls(value)
        except ValueError as exc:
            valid = ", ".join(repr(m.value) for m in self.enum_cls)
            raise ValidationError(
                f"Field '{self.name}': {value!r} is not a valid member of "
                f"{self.enum_cls.__name__} (expected one of {valid})."
            ) from exc

    def get_db_prep_value(self, value):
        if value is None:
            return None
        if isinstance(value, self.enum_cls):
            return value.value
        # Allow already-coerced raw values on the write path so callers
        # can pass either ``Status.ACTIVE`` or ``"active"``.
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        try:
            return self.enum_cls(value)
        except ValueError:
            # Historical rows with a value outside the current enum
            # definition shouldn't crash on read — surface the raw
            # value so callers can migrate them.
            return value

    def db_type(self, connection) -> str:
        if self._is_string:
            return f"VARCHAR({self.max_length})"
        # Integer enums; ``BIGINT``-sized values are fine in INTEGER on
        # both backends (SQLite stores them in 64 bits regardless).
        return "INTEGER"


# ── Case-insensitive text ─────────────────────────────────────────────────────


class CITextField(TextField):
    """Case-insensitive text column.

    Maps to PostgreSQL's ``CITEXT`` (requires the ``citext`` extension
    on the database — issue ``RunSQL("CREATE EXTENSION IF NOT EXISTS
    citext")`` from a migration before adding the column). On SQLite,
    falls back to ``TEXT COLLATE NOCASE`` so equality / ``LIKE`` queries
    behave the same way without the extension.

    Example::

        class User(dorm.Model):
            email = dorm.CITextField(unique=True)

        # both succeed and find the same row:
        User.objects.get(email="Alice@example.com")
        User.objects.get(email="alice@example.com")
    """

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            return "CITEXT"
        return "TEXT COLLATE NOCASE"


# ── Range field (PostgreSQL) ──────────────────────────────────────────────────


class Range:
    """Plain-Python value type for PostgreSQL range columns.

    Mirrors the shape of psycopg's :class:`psycopg.types.range.Range`
    without coupling our public API to it. *bounds* is a two-character
    string with the inclusivity of each endpoint: ``"[)"`` (default,
    inclusive lower / exclusive upper), ``"(]"``, ``"[]"`` or ``"()"``.

    Either endpoint may be ``None`` to denote an unbounded side.
    """

    __slots__ = ("lower", "upper", "bounds")

    def __init__(
        self,
        lower: Any = None,
        upper: Any = None,
        bounds: str = "[)",
    ) -> None:
        if bounds not in ("[)", "(]", "[]", "()"):
            raise ValidationError(
                f"Range.bounds must be one of '[)', '(]', '[]', '()' — got {bounds!r}."
            )
        self.lower = lower
        self.upper = upper
        self.bounds = bounds

    @property
    def lower_inc(self) -> bool:
        return self.bounds[0] == "["

    @property
    def upper_inc(self) -> bool:
        return self.bounds[1] == "]"

    def is_empty(self) -> bool:
        # PostgreSQL has a distinct "empty" sentinel; we treat
        # ``Range(None, None, "()")`` as empty for our pure-Python
        # representation. Callers who need the literal ``empty`` keyword
        # PG returns can pass it through ``RawSQL``.
        return self.lower is None and self.upper is None and self.bounds == "()"

    def __repr__(self) -> str:
        return f"Range({self.lower!r}, {self.upper!r}, bounds={self.bounds!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Range):
            return NotImplemented
        return (
            self.lower == other.lower
            and self.upper == other.upper
            and self.bounds == other.bounds
        )

    def __hash__(self) -> int:
        return hash((self.lower, self.upper, self.bounds))


def _format_range_endpoint(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


class RangeField(Field[Range]):
    """Abstract base for PostgreSQL range columns.

    Concrete subclasses set :attr:`range_type` to the SQL type name
    (``int4range``, ``int8range``, ``numrange``, ``daterange``,
    ``tstzrange``). PostgreSQL is the only supported backend; SQLite
    raises :class:`NotImplementedError` from :meth:`db_type` so the
    limitation surfaces at migrate time, not at first query.

    The Python value is :class:`Range`. Reading from the database
    accepts both psycopg's ``Range`` and our own; binding for write
    formats the value as a typed range literal (``[lower,upper)``).

    Example::

        class Reservation(dorm.Model):
            during = dorm.DateTimeRangeField()

        from datetime import datetime, timezone
        Reservation.objects.create(
            during=dorm.Range(
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 8, tzinfo=timezone.utc),
            ),
        )
    """

    range_type: str = ""

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, Range):
            return value
        if isinstance(value, (list, tuple)) and len(value) in (2, 3):
            lower, upper = value[0], value[1]
            bounds = value[2] if len(value) == 3 else "[)"
            return Range(lower, upper, bounds=bounds)
        # Duck-type psycopg's Range without importing it (so we don't
        # require psycopg to construct fields on SQLite-only deploys).
        if all(hasattr(value, attr) for attr in ("lower", "upper", "lower_inc", "upper_inc")):
            bounds = (
                ("[" if value.lower_inc else "(")
                + ("]" if value.upper_inc else ")")
            )
            return Range(value.lower, value.upper, bounds=bounds)
        raise ValidationError(
            f"Field '{self.name}': cannot convert {value!r} to a Range."
        )

    def get_db_prep_value(self, value):
        if value is None:
            return None
        r = value if isinstance(value, Range) else self.to_python(value)
        # PostgreSQL accepts a typed range literal in string form during
        # INSERT — ``'[1,10)'::int4range``. Letting psycopg cast based
        # on the column type lets us avoid registering custom adapters
        # while keeping the wire format compact.
        return (
            f"{r.bounds[0]}{_format_range_endpoint(r.lower)},"
            f"{_format_range_endpoint(r.upper)}{r.bounds[1]}"
        )

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, Range):
            return value
        if isinstance(value, str):
            return self._parse_literal(value)
        # psycopg's Range — convert via to_python so we only depend on
        # its public attributes.
        return self.to_python(value)

    @staticmethod
    def _parse_literal(text: str) -> "Range | None":
        s = text.strip()
        if s == "empty":
            return Range(None, None, bounds="()")
        if not s or s[0] not in "[(" or s[-1] not in "])":
            raise ValidationError(
                f"RangeField: cannot parse range literal {text!r}."
            )
        inner = s[1:-1]
        # The endpoint values can themselves be quoted strings — but for
        # the types we support (numbers / dates / timestamps) PG returns
        # them unquoted. A naive split on the first comma is enough.
        if "," not in inner:
            raise ValidationError(
                f"RangeField: malformed range literal {text!r}."
            )
        lower_text, upper_text = inner.split(",", 1)
        lower = lower_text.strip().strip('"') or None
        upper = upper_text.strip().strip('"') or None
        return Range(lower, upper, bounds=s[0] + s[-1])

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"{self.__class__.__name__} is only supported on PostgreSQL. "
                "SQLite has no native range type — use two columns and a "
                "CHECK constraint instead."
            )
        return self.range_type


class IntegerRangeField(RangeField):
    range_type = "int4range"

    def to_python(self, value):
        r = super().to_python(value)
        if r is None:
            return None
        return Range(
            int(r.lower) if r.lower not in (None, "") else None,
            int(r.upper) if r.upper not in (None, "") else None,
            bounds=r.bounds,
        )

    def from_db_value(self, value):
        return self.to_python(super().from_db_value(value))


class BigIntegerRangeField(IntegerRangeField):
    range_type = "int8range"


class DecimalRangeField(RangeField):
    range_type = "numrange"

    def to_python(self, value):
        r = super().to_python(value)
        if r is None:
            return None
        return Range(
            decimal.Decimal(str(r.lower)) if r.lower not in (None, "") else None,
            decimal.Decimal(str(r.upper)) if r.upper not in (None, "") else None,
            bounds=r.bounds,
        )

    def from_db_value(self, value):
        return self.to_python(super().from_db_value(value))


class DateRangeField(RangeField):
    range_type = "daterange"

    @staticmethod
    def _coerce_date(v: Any) -> datetime.date | None:
        if v in (None, ""):
            return None
        if isinstance(v, datetime.datetime):
            return v.date()
        if isinstance(v, datetime.date):
            return v
        return datetime.date.fromisoformat(str(v))

    def to_python(self, value):
        r = super().to_python(value)
        if r is None:
            return None
        return Range(
            self._coerce_date(r.lower),
            self._coerce_date(r.upper),
            bounds=r.bounds,
        )

    def from_db_value(self, value):
        return self.to_python(super().from_db_value(value))


class DateTimeRangeField(RangeField):
    range_type = "tstzrange"

    @staticmethod
    def _coerce_dt(v: Any) -> datetime.datetime | None:
        if v in (None, ""):
            return None
        if isinstance(v, datetime.datetime):
            return v
        return datetime.datetime.fromisoformat(str(v))

    def to_python(self, value):
        r = super().to_python(value)
        if r is None:
            return None
        return Range(
            self._coerce_dt(r.lower),
            self._coerce_dt(r.upper),
            bounds=r.bounds,
        )

    def from_db_value(self, value):
        return self.to_python(super().from_db_value(value))


# ── Relational fields ──────────────────────────────────────────────────────────


class _ForeignKeyIdDescriptor:
    """Typed read/write descriptor for the underlying ``<fk>_id`` slot.

    The FK descriptor itself returns the related model instance; this one
    exposes the raw PK so ``obj.author_id`` is statically typed as
    ``int | None`` instead of ``Any``. Both descriptors back the same dict
    key, so writes via either path stay in sync.
    """

    def __init__(self, attname: str) -> None:
        self.attname = attname

    @overload
    def __get__(self, instance: None, owner: type) -> "_ForeignKeyIdDescriptor": ...
    @overload
    def __get__(self, instance: object, owner: type) -> int | None: ...
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.attname)

    def __set__(self, instance: object, value: int | None) -> None:
        instance.__dict__[self.attname] = value
        # Invalidate the cached related instance so the next attribute
        # access on the FK descriptor re-fetches with the new pk.
        # FK descriptor stores its cache under ``_cache_<name>`` where
        # ``<name> = attname[:-3]`` (we strip the trailing ``_id``).
        if self.attname.endswith("_id"):
            instance.__dict__.pop(f"_cache_{self.attname[:-3]}", None)


class RelatedField(Field[Any]):
    def __init__(self, to, on_delete=CASCADE, related_name=None, **kwargs):
        self.remote_field_to = to
        self.on_delete = on_delete
        self.related_name = related_name
        self.many_to_one = True
        super().__init__(**kwargs)

    def contribute_to_class(self, cls, name: str):
        from .conf import _validate_identifier

        self.name = name
        self.attname = f"{name}_id"
        self.column = self.db_column or f"{name}_id"
        _validate_identifier(self.column, kind=f"{cls.__name__}.{name}.db_column")
        if self.related_name:
            _validate_identifier(
                self.related_name, kind=f"{cls.__name__}.{name}.related_name"
            )
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)
        setattr(cls, name, self)  # install FK descriptor
        # Install a typed descriptor for the underlying ``<name>_id`` PK so
        # static type checkers see ``obj.author_id`` as ``int | None`` rather
        # than ``Any``. At runtime it just reads the same dict slot the FK
        # descriptor writes to.
        setattr(cls, self.attname, _ForeignKeyIdDescriptor(self.attname))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        cache_name = f"_cache_{self.name}"
        if cache_name in instance.__dict__:
            return instance.__dict__[cache_name]
        pk_val = instance.__dict__.get(self.attname)
        if pk_val is None:
            return None
        related_model = self._resolve_related_model()
        obj = related_model.objects.get(pk=pk_val)
        instance.__dict__[cache_name] = obj
        return obj

    def __set__(self, instance, value):
        if value is None:
            instance.__dict__[self.attname] = None
            instance.__dict__.pop(f"_cache_{self.name}", None)
        elif hasattr(value, "pk"):
            instance.__dict__[self.attname] = value.pk
            instance.__dict__[f"_cache_{self.name}"] = value
        else:
            instance.__dict__[self.attname] = value
            instance.__dict__.pop(f"_cache_{self.name}", None)

    def _resolve_related_model(self):
        if isinstance(self.remote_field_to, str):
            from .models import _model_registry  # noqa: PLC0415

            return _model_registry[self.remote_field_to]
        return self.remote_field_to

    def get_db_prep_value(self, value):
        if hasattr(value, "pk"):
            return value.pk
        return value

    def db_type(self, connection) -> str:
        related = self._resolve_related_model()
        pk_field = related._meta.pk
        rel_type = getattr(pk_field, "rel_db_type", None)
        if rel_type is not None:
            return rel_type(connection)
        return pk_field.db_type(connection)


_pending_reverse_relations: list[tuple] = []


class ForeignKey(RelatedField):
    def contribute_to_class(self, cls, name: str):
        super().contribute_to_class(cls, name)
        rel_name = self.related_name or f"{cls.__name__.lower()}_set"
        if not isinstance(self.remote_field_to, str):
            from .related_managers import ReverseFKDescriptor
            setattr(self.remote_field_to, rel_name, ReverseFKDescriptor(cls, self))
        else:
            _pending_reverse_relations.append((cls, self, rel_name))


class OneToOneField(RelatedField):
    def __init__(self, to, on_delete=CASCADE, **kwargs):
        kwargs.setdefault("unique", True)
        super().__init__(to, on_delete, **kwargs)
        # ``RelatedField.__init__`` (and ``Field.__init__`` further up)
        # set ``many_to_one = True`` / ``one_to_one = False`` after
        # calling super(), so the flag flip MUST happen last —
        # otherwise an OneToOneField pretends to be a regular FK and
        # breaks every code path that branches on ``one_to_one``
        # (cascade rules, select_related single-row hydration, etc.).
        self.one_to_one = True
        self.many_to_one = False

    def contribute_to_class(self, cls, name: str):
        # ``RelatedField.contribute_to_class`` registers the FK
        # column descriptor on the source model. We then mirror
        # ``ForeignKey.contribute_to_class`` to install the reverse
        # accessor on the *target* model — without it,
        # ``target_instance.<related_name>`` raised
        # ``AttributeError`` because no descriptor was wired.
        super().contribute_to_class(cls, name)
        rel_name = self.related_name or cls.__name__.lower()
        if not isinstance(self.remote_field_to, str):
            from .related_managers import ReverseOneToOneDescriptor

            setattr(
                self.remote_field_to,
                rel_name,
                ReverseOneToOneDescriptor(cls, self),
            )
        else:
            _pending_reverse_relations.append((cls, self, rel_name))


class ManyToManyField(Field[Any]):
    many_to_many = True
    concrete = False

    def __init__(self, to, through=None, related_name=None, **kwargs):
        self.remote_field_to = to
        self.through = through
        self.related_name = related_name
        kwargs["null"] = True
        super().__init__(**kwargs)
        self.many_to_many = True
        self.concrete = False

    def contribute_to_class(self, cls, name: str):
        from .conf import _validate_identifier

        self.name = name
        self.attname = name
        if self.through:
            _validate_identifier(
                self.through, kind=f"{cls.__name__}.{name}.through"
            )
        if self.related_name:
            _validate_identifier(
                self.related_name, kind=f"{cls.__name__}.{name}.related_name"
            )
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)
        from .related_managers import ManyToManyDescriptor
        setattr(cls, name, ManyToManyDescriptor(self))

    def db_type(self, connection) -> str | None:
        return None  # no column; uses junction table

    def _resolve_related_model(self):
        if isinstance(self.remote_field_to, str):
            from .models import _model_registry  # noqa: PLC0415

            return _model_registry[self.remote_field_to]
        return self.remote_field_to

    def _get_through_table(self) -> str:
        assert self.model is not None
        if self.through:
            if isinstance(self.through, str):
                from .models import _model_registry
                tm: Any = _model_registry[self.through]
                return tm._meta.db_table
            tm2: Any = self.through
            return tm2._meta.db_table
        return f"{self.model._meta.db_table}_{self.name}"  # type: ignore[union-attr]

    def _get_through_columns(self) -> tuple[str, str]:
        """Return (source_col, target_col) in the junction table."""
        assert self.model is not None
        if self.through:
            if isinstance(self.through, str):
                from .models import _model_registry
                through_model: Any = _model_registry[self.through]
            else:
                through_model = self.through
            rel_model = self._resolve_related_model()
            src_col = tgt_col = None
            for f in through_model._meta.fields:
                if hasattr(f, "_resolve_related_model"):
                    try:
                        rm = f._resolve_related_model()
                        if rm is self.model:
                            src_col = f.column
                        elif rm is rel_model:
                            tgt_col = f.column
                    except Exception:
                        pass
            return (
                src_col or f"{self.model.__name__}_id".lower(),
                tgt_col or f"{rel_model.__name__}_id".lower(),
            )
        rel_model = self._resolve_related_model()
        return (
            f"{self.model.__name__}_id".lower(),
            f"{rel_model.__name__}_id".lower(),
        )
