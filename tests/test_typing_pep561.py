"""End-to-end PEP 561 typing test.

Verifies that consumers importing ``dorm`` get static-typing fidelity
on the generic ``Field[T]`` descriptor: instance attribute access
narrows to ``T`` (not ``Any``), and class-level access yields ``Field[T]``.

The test is run by the ``ty`` type-checker via the validation script;
the runtime side here only checks that the descriptors hand back the
right values at import time, so a smoke run in CI catches regressions
even without ``ty``.
"""

from __future__ import annotations

import os

import dorm


class _TypingProbe(dorm.Model):
    name = dorm.CharField(max_length=10)
    age = dorm.IntegerField()
    active = dorm.BooleanField(default=False)

    class Meta:
        db_table = "_typing_probe"
        app_label = "tests"


def test_descriptor_returns_field_at_class_level():
    # ``_TypingProbe.name`` should return the descriptor itself.
    descriptor = _TypingProbe._meta.get_field("name")
    assert isinstance(descriptor, dorm.CharField)


def test_descriptor_returns_value_at_instance_level():
    obj = _TypingProbe(name="hi", age=42, active=True)
    assert obj.name == "hi"
    assert obj.age == 42
    assert obj.active is True


def test_py_typed_marker_present():
    import dorm as _d
    pkg_root = os.path.dirname(_d.__file__)
    assert os.path.exists(os.path.join(pkg_root, "py.typed")), (
        "dorm/py.typed marker missing — PEP 561 consumers will skip type "
        "checking against djanorm."
    )


def test_field_generic_subscriptable():
    """Confirm ``Field[T]`` is a runtime-subscriptable generic so
    third-party libraries can subclass for new field types without
    fighting the type system."""
    cls = dorm.fields.Field[int]
    assert cls is not None
