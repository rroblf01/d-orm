"""Model factories — minimal in-house alternative to factory_boy.

The factories declared here drive the same workflow as
``factory_boy`` / ``model_bakery`` but stay inside the dorm wheel
so test suites don't take an external dependency for an obvious
feature.

Usage::

    import dorm
    from dorm.factories import Factory, Sequence, LazyFunction, SubFactory

    class UserFactory(Factory):
        class Meta:
            model = User

        username = Sequence(lambda n: f"u{n}")
        email = Sequence(lambda n: f"u{n}@example.com")
        is_active = True

    class PostFactory(Factory):
        class Meta:
            model = Post

        title = LazyFunction(lambda: "post")
        author = SubFactory(UserFactory)

    u = UserFactory.create()                  # → User row in DB
    batch = UserFactory.create_batch(5)        # → list[User]
    unsaved = UserFactory.build(username="x")  # → User instance, no save

Declarations (class attributes on a :class:`Factory` subclass):

- Plain values become the default attribute value.
- :class:`Sequence` calls the wrapped callable with an
  auto-incrementing integer — useful for unique columns.
- :class:`LazyFunction` calls the wrapped zero-arg callable each
  time, so reused factories produce fresh values.
- :class:`SubFactory` creates a related row first and threads the
  resulting instance into the kwargs.

The ``Meta.model`` slot binds the factory to a dorm Model. Subclass
:class:`Factory` and define ``Meta.model = MyModel`` — the rest is
discovered automatically.
"""
from __future__ import annotations

import itertools
from typing import Any, Callable, ClassVar


class _Declaration:
    """Marker base for declarative attributes resolved at build time."""

    def resolve(self, *, sequence: int) -> Any:
        raise NotImplementedError


class Sequence(_Declaration):
    """Render an attribute from an auto-incrementing integer.

    *fn* receives the sequence number and returns the value::

        username = Sequence(lambda n: f"user{n}")
    """

    def __init__(self, fn: Callable[[int], Any]) -> None:
        self._fn = fn

    def resolve(self, *, sequence: int) -> Any:
        return self._fn(sequence)


class LazyFunction(_Declaration):
    """Render an attribute by calling *fn* (zero args) each time.

    Useful when the column wants ``datetime.now()`` per row, a
    fresh UUID, etc."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        self._fn = fn

    def resolve(self, *, sequence: int) -> Any:
        return self._fn()


class SubFactory(_Declaration):
    """Build a related row via another :class:`Factory` subclass and
    use the resulting instance as the attribute value.

    The default mode is :meth:`Factory.create` — the related row
    lands in the database, mirroring factory_boy's behaviour. Pass
    ``strategy="build"`` to skip the save (useful when the field
    will be threaded into a parent's ``build`` call)."""

    def __init__(
        self,
        factory: "type[Factory]",
        *,
        strategy: str = "create",
        **overrides: Any,
    ) -> None:
        if strategy not in ("create", "build"):
            raise ValueError(
                "SubFactory.strategy must be 'create' or 'build'; got "
                f"{strategy!r}"
            )
        self._factory = factory
        self._strategy = strategy
        self._overrides = overrides

    def resolve(self, *, sequence: int) -> Any:
        if self._strategy == "create":
            return self._factory.create(**self._overrides)
        return self._factory.build(**self._overrides)


class _Meta:
    """Default ``Meta`` shape used when a subclass forgets to declare
    one. The build helpers raise on a missing ``model`` so the
    failure mode is loud, not silent."""

    model: ClassVar[Any] = None


def _collect_declarations(cls: type) -> dict[str, Any]:
    """Walk the MRO and gather attribute defaults declared on each
    factory. Child classes win over parents — ``Factory.create(...)``
    overrides win over both."""
    out: dict[str, Any] = {}
    for base in reversed(cls.__mro__):
        if not isinstance(base, type) or base is object:
            continue
        for name, value in base.__dict__.items():
            if name.startswith("_") or name == "Meta":
                continue
            if callable(value) and not isinstance(value, _Declaration):
                # Bare callables (e.g. user-defined helpers) stay
                # methods — factories only treat ``_Declaration``
                # instances + plain values as field defaults.
                continue
            out[name] = value
    return out


class Factory:
    """Base class for model factories.

    Subclasses declare attribute defaults (plain values or
    :class:`_Declaration` instances) and a ``Meta`` inner class
    pointing at the target dorm Model. ``Meta`` is resolved
    dynamically via :func:`getattr` so subclass declarations
    (which are instance variables on the class object) don't
    collide with a strict ``ClassVar`` annotation on the base.
    """

    _sequence: ClassVar[Any]  # initialised in __init_subclass__

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Per-subclass sequence counter so two factories don't
        # share a global integer — that would break the "unique
        # email per factory" idiom most callers expect.
        cls._sequence = itertools.count(start=1)

    @classmethod
    def _next_sequence(cls) -> int:
        return next(cls._sequence)

    @classmethod
    def _resolve_kwargs(cls, overrides: dict[str, Any]) -> dict[str, Any]:
        defaults = _collect_declarations(cls)
        # Caller overrides win over declared defaults.
        defaults.update(overrides)
        seq = cls._next_sequence()
        resolved: dict[str, Any] = {}
        for k, v in defaults.items():
            if isinstance(v, _Declaration):
                resolved[k] = v.resolve(sequence=seq)
            else:
                resolved[k] = v
        return resolved

    @classmethod
    def _model(cls) -> Any:
        meta: Any = getattr(cls, "Meta", _Meta)
        model = getattr(meta, "model", None)
        if model is None:
            raise RuntimeError(
                f"{cls.__name__}: factory has no Meta.model set."
            )
        return model

    @classmethod
    def build(cls, **overrides: Any) -> Any:
        """Construct a model instance in memory — no DB write.

        Useful for unit tests that don't want the schema to exist
        yet, or for threading an unsaved instance through a
        higher-level test as input data."""
        model = cls._model()
        kwargs = cls._resolve_kwargs(overrides)
        return model(**kwargs)

    @classmethod
    def create(cls, **overrides: Any) -> Any:
        """Insert one row through the model's manager.

        Equivalent to ``model.objects.create(**resolved_kwargs)`` —
        triggers ``post_save`` / temporal / audit signals so the
        side-effects of a real write apply."""
        model = cls._model()
        kwargs = cls._resolve_kwargs(overrides)
        return model.objects.create(**kwargs)

    @classmethod
    def create_batch(cls, size: int, **overrides: Any) -> list[Any]:
        """Convenience for ``[cls.create(**overrides) for _ in range(size)]``.

        Each iteration advances the sequence counter so columns
        marked unique via :class:`Sequence` stay non-conflicting.
        """
        if size < 0:
            raise ValueError("create_batch size must be >= 0")
        return [cls.create(**overrides) for _ in range(size)]

    @classmethod
    def build_batch(cls, size: int, **overrides: Any) -> list[Any]:
        if size < 0:
            raise ValueError("build_batch size must be >= 0")
        return [cls.build(**overrides) for _ in range(size)]

    @classmethod
    def reset_sequence(cls) -> None:
        """Reset the per-factory sequence counter. Useful between
        tests when fixture isolation isn't strict about row ids."""
        cls._sequence = itertools.count(start=1)


__all__ = [
    "Factory",
    "LazyFunction",
    "Sequence",
    "SubFactory",
]
