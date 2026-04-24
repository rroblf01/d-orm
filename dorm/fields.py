from __future__ import annotations

import datetime
import decimal
import ipaddress
import json
import re
import uuid
from typing import Any

from .exceptions import ValidationError

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
CASCADE = "CASCADE"
PROTECT = "PROTECT"
SET_NULL = "SET NULL"
SET_DEFAULT = "SET DEFAULT"
DO_NOTHING = "NO ACTION"
RESTRICT = "RESTRICT"


class Field:
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
        self.name = name
        self.attname = name
        self.column = self.db_column or name
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.attname)

    def __set__(self, instance, value):
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


class AutoField(Field):
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


class IntegerField(Field):
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


class FloatField(Field):
    def to_python(self, value):
        if value is None:
            return None
        return float(value)

    def db_type(self, connection) -> str:
        return "REAL"


class DecimalField(Field):
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


class CharField(Field):
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


class TextField(Field):
    def to_python(self, value):
        if value is None:
            return None
        return str(value)

    def db_type(self, connection) -> str:
        return "TEXT"


class BooleanField(Field):
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


class DateField(Field):
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


class TimeField(Field):
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


class DateTimeField(Field):
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


class UUIDField(Field):
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


class IPAddressField(Field):
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


class GenericIPAddressField(Field):
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


class JSONField(Field):
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


class BinaryField(Field):
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


class RelatedField(Field):
    def __init__(self, to, on_delete=CASCADE, related_name=None, **kwargs):
        self.remote_field_to = to
        self.on_delete = on_delete
        self.related_name = related_name
        self.many_to_one = True
        super().__init__(**kwargs)

    def contribute_to_class(self, cls, name: str):
        self.name = name
        self.attname = f"{name}_id"
        self.column = self.db_column or f"{name}_id"
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)
        setattr(cls, name, self)  # install FK descriptor

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
        self.one_to_one = True
        self.many_to_one = False
        super().__init__(to, on_delete, **kwargs)


class ManyToManyField(Field):
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
        self.name = name
        self.attname = name
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
