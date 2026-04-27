from __future__ import annotations

import datetime
import decimal
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
        editable: bool = True,
        serialize: bool = True,
        choices: Any = None,
        help_text: str = "",
        db_column: str | None = None,
        db_tablespace: str | None = None,
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
        self.editable = editable
        self.serialize = serialize
        self.choices = choices
        self.help_text = help_text
        self.db_column = db_column
        self.db_tablespace = db_tablespace
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


class DateTimeField(Field[datetime.datetime]):
    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        if auto_now or auto_now_add:
            kwargs["editable"] = False
        if auto_now_add:
            kwargs.setdefault("default", lambda: datetime.datetime.now(datetime.timezone.utc))
        super().__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        return value

    def get_db_prep_value(self, value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value

    def from_db_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        return value

    def db_type(self, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        return "TIMESTAMP" if vendor == "postgresql" else "DATETIME"

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


class BinaryField(Field[bytes]):
    def to_python(self, value):
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
