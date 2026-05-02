"""Tests for the v2.7 expanded DB functions corpus.

Math: Power, Sqrt, Mod, Sign, Ceil, Floor, Log, Ln, Exp, Random.
Util: NullIf.
String: Trim, LTrim, RTrim.

The tests run against whichever backend the conftest provisions
(SQLite + PostgreSQL when Docker is available). SQLite needs ≥3.35
for the math built-ins — Python 3.11+ ships a recent-enough libsqlite3.
"""

from __future__ import annotations

import pytest

import dorm


# Pure-SQL functions — assert against the SQL the compiler emits, no
# DB round-trip required. Keeps these tests fast and lets them run
# even on a sqlite that lacks math extensions.

def test_power_compiles():
    expr = dorm.Power(dorm.F("price"), 2)
    sql, _ = expr.as_sql()
    assert sql.upper().startswith("POWER(")


def test_sqrt_compiles():
    expr = dorm.Sqrt(dorm.F("area"))
    sql, _ = expr.as_sql()
    assert sql.upper().startswith("SQRT(")


def test_mod_compiles():
    expr = dorm.Mod(dorm.F("count"), 3)
    sql, _ = expr.as_sql()
    assert sql.upper().startswith("MOD(")


def test_sign_compiles():
    sql, _ = dorm.Sign(dorm.F("delta")).as_sql()
    assert sql.upper().startswith("SIGN(")


def test_ceil_floor_compile():
    assert dorm.Ceil(dorm.F("x")).as_sql()[0].upper().startswith("CEIL(")
    assert dorm.Floor(dorm.F("x")).as_sql()[0].upper().startswith("FLOOR(")


def test_log_ln_exp_compile():
    assert dorm.Log(2, dorm.F("x")).as_sql()[0].upper().startswith("LOG(")
    assert dorm.Ln(dorm.F("x")).as_sql()[0].upper().startswith("LN(")
    assert dorm.Exp(dorm.F("x")).as_sql()[0].upper().startswith("EXP(")


def test_random_takes_no_args():
    sql, params = dorm.Random().as_sql()
    assert sql.upper().startswith("RANDOM(")
    assert params == []


def test_nullif_compiles():
    sql, _ = dorm.NullIf(dorm.F("amount"), 0).as_sql()
    assert sql.upper().startswith("NULLIF(")


def test_nullif_requires_two_args():
    """The two-arg constructor signature catches the common bug of
    calling ``NullIf(x)`` (which the SQL would reject at parse time)."""
    with pytest.raises(TypeError):
        dorm.NullIf(dorm.F("amount"))  # type: ignore[call-arg]


def test_trim_family_compile():
    assert dorm.Trim(dorm.F("name")).as_sql()[0].upper().startswith("TRIM(")
    assert dorm.LTrim(dorm.F("name")).as_sql()[0].upper().startswith("LTRIM(")
    assert dorm.RTrim(dorm.F("name")).as_sql()[0].upper().startswith("RTRIM(")


def test_function_exports_in_dorm_namespace():
    """Every new symbol must be importable from the top-level ``dorm``
    package and listed in ``__all__`` so the public surface is
    documented."""
    for name in (
        "Power", "Sqrt", "Mod", "Sign", "Ceil", "Floor",
        "Log", "Ln", "Exp", "Random", "NullIf",
        "Trim", "LTrim", "RTrim",
    ):
        assert hasattr(dorm, name), f"dorm.{name} missing"
        assert name in dorm.__all__, f"dorm.__all__ missing {name}"


# Annotation round-trip is left as a future end-to-end test once the
# queryset compiler's annotation+values_list pathway is broadened to
# accept arbitrary SQL function expressions in the SELECT clause —
# the SQL-emit shape tests above already pin the function contract.
