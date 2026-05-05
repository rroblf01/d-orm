"""Tests for ``HStoreField``, native ``EnumField(native=True)`` and
the ``CreatePGEnum`` / ``DropPGEnum`` / ``AddPGEnumValue`` operations.
"""

from __future__ import annotations

import enum

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.exceptions import ValidationError
from dorm.migrations.operations import (
    AddPGEnumValue,
    CreatePGEnum,
    DropPGEnum,
)


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


# ── HStoreField unit tests (no live DB needed) ─────────────────────────


def test_hstorefield_dict_round_trip():
    f = dorm.HStoreField()
    out = f.get_db_prep_value({"a": "1", "b": None})
    assert out == {"a": "1", "b": None}


def test_hstorefield_rejects_non_string_keys():
    f = dorm.HStoreField()
    f.name = "tags"
    with pytest.raises(ValidationError):
        f.get_db_prep_value({1: "v"})


def test_hstorefield_rejects_non_string_values():
    f = dorm.HStoreField()
    f.name = "tags"
    with pytest.raises(ValidationError):
        f.get_db_prep_value({"k": 1})


def test_hstorefield_rejects_non_dict():
    f = dorm.HStoreField()
    f.name = "tags"
    with pytest.raises(ValidationError):
        f.get_db_prep_value(["not", "a", "dict"])


def test_hstorefield_db_type_pg():
    class Conn:
        vendor = "postgresql"

    f = dorm.HStoreField()
    assert f.db_type(Conn()) == "hstore"


def test_hstorefield_db_type_sqlite_falls_back_to_text():
    class Conn:
        vendor = "sqlite"

    f = dorm.HStoreField()
    assert f.db_type(Conn()) == "TEXT"


def test_hstorefield_to_python_decodes_json_on_sqlite():
    f = dorm.HStoreField()
    out = f.to_python('{"a": "1"}')
    assert out == {"a": "1"}


# ── EnumField(native=True) unit tests ────────────────────────────────


class _Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


def test_native_enum_db_type_pg_uses_quoted_type_name():
    class Conn:
        vendor = "postgresql"

    f = dorm.EnumField(_Status, native=True, type_name="status_enum")
    assert f.db_type(Conn()) == '"status_enum"'


def test_native_enum_falls_back_on_non_pg():
    class Conn:
        vendor = "sqlite"

    f = dorm.EnumField(_Status, native=True)
    assert f.db_type(Conn()).startswith("VARCHAR")


def test_native_enum_int_disallowed():
    class _IntEnum(enum.Enum):
        A = 1
        B = 2

    with pytest.raises(ValidationError, match="string-valued"):
        dorm.EnumField(_IntEnum, native=True)


def test_native_enum_default_type_name():
    f = dorm.EnumField(_Status, native=True)
    assert f.type_name == "_status_enum"


# ── Migration ops unit tests ─────────────────────────────────────────


def test_create_pg_enum_validation():
    from typing import cast as _cast

    with pytest.raises(ValueError):
        CreatePGEnum("x", [])
    with pytest.raises(ValueError):
        CreatePGEnum("x", _cast(list, [1, 2]))


def test_create_pg_enum_no_op_on_sqlite(db_config):
    if _is_postgres(db_config):
        pytest.skip("non-PG path")
    op = CreatePGEnum("x", ["a", "b"])
    op.database_forwards("tests", get_connection(), None, None)


def test_create_drop_pg_enum_pg(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    conn.execute_script('DROP TYPE IF EXISTS my_status CASCADE')

    create = CreatePGEnum("my_status", ["active", "archived"])
    create.database_forwards("tests", conn, None, None)
    rows = conn.execute(
        "SELECT typname FROM pg_type WHERE typname = %s", ["my_status"]
    )
    assert rows

    drop = DropPGEnum("my_status", reverse_values=["active", "archived"])
    drop.database_forwards("tests", conn, None, None)
    rows = conn.execute(
        "SELECT typname FROM pg_type WHERE typname = %s", ["my_status"]
    )
    assert not rows


def test_drop_pg_enum_irreversible_raises(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    conn.execute_script('DROP TYPE IF EXISTS my_x CASCADE')
    CreatePGEnum("my_x", ["one"]).database_forwards("tests", conn, None, None)
    drop = DropPGEnum("my_x", reverse_values=[])
    drop.database_forwards("tests", conn, None, None)
    with pytest.raises(NotImplementedError):
        drop.database_backwards("tests", conn, None, None)


def test_add_pg_enum_value(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    conn.execute_script('DROP TYPE IF EXISTS my_state CASCADE')
    CreatePGEnum("my_state", ["a", "b"]).database_forwards("tests", conn, None, None)
    try:
        AddPGEnumValue("my_state", "c").database_forwards("tests", conn, None, None)
        rows = conn.execute(
            "SELECT enumlabel FROM pg_enum e "
            "JOIN pg_type t ON e.enumtypid=t.oid WHERE t.typname=%s ORDER BY e.enumsortorder",
            ["my_state"],
        )
        assert [r["enumlabel"] for r in rows] == ["a", "b", "c"]
    finally:
        conn.execute_script('DROP TYPE IF EXISTS my_state CASCADE')


def test_add_pg_enum_value_irreversible():
    op = AddPGEnumValue("x", "y")
    with pytest.raises(NotImplementedError):
        op.database_backwards("tests", None, None, None)
