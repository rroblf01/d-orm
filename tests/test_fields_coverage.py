"""Coverage-tightening tests for ``dorm.fields``.

Targets the parser / coercer branches the round-trip tests don't
naturally exercise: ``DurationField._parse_iso8601`` for every
input shape it claims to accept (and reject), ``RangeField._parse_literal``
for the PG ``empty`` keyword and malformed inputs, ``RangeField.to_python``
for the duck-typed psycopg-Range case, and the ``_format_range_endpoint``
branches that get hit only for date / datetime / unbounded values.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

import dorm
from dorm.exceptions import ValidationError
from dorm.fields import (
    DurationField,
    RangeField,
    _format_range_endpoint,
)


# ── DurationField._parse_iso8601: shape matrix ──────────────────────────────


class TestDurationFieldParse:
    """The parser accepts two shapes:
      - ``"<int>"`` / ``"-<int>"`` — raw microseconds.
      - ``"HH:MM:SS[.ffffff]"`` with optional leading sign.
    Anything else must raise :class:`ValidationError`."""

    def test_empty_string_rejected(self):
        with pytest.raises(ValidationError, match="empty duration"):
            DurationField._parse_iso8601("")

    def test_whitespace_only_rejected(self):
        # ``strip`` removes the spaces; the resulting empty string
        # falls through to the empty-rejection branch.
        with pytest.raises(ValidationError, match="empty duration"):
            DurationField._parse_iso8601("   ")

    def test_positive_microseconds_int_accepted(self):
        td = DurationField._parse_iso8601("1500")
        assert td == datetime.timedelta(microseconds=1500)

    def test_negative_microseconds_int_accepted(self):
        td = DurationField._parse_iso8601("-2000")
        assert td == datetime.timedelta(microseconds=-2000)

    def test_hms_format_basic(self):
        td = DurationField._parse_iso8601("01:02:03")
        assert td == datetime.timedelta(hours=1, minutes=2, seconds=3)

    def test_hms_format_with_microseconds(self):
        td = DurationField._parse_iso8601("00:00:01.5")
        assert td == datetime.timedelta(seconds=1, microseconds=500_000)

    def test_negative_hms_with_explicit_sign(self):
        td = DurationField._parse_iso8601("-01:00:00")
        assert td == datetime.timedelta(hours=-1)

    def test_positive_hms_with_explicit_plus_sign(self):
        td = DurationField._parse_iso8601("+01:00:00")
        assert td == datetime.timedelta(hours=1)

    def test_wrong_part_count_rejected(self):
        # Two segments, not three — not a valid HMS shape.
        with pytest.raises(ValidationError, match="HH:MM:SS"):
            DurationField._parse_iso8601("01:02")

    def test_too_many_parts_rejected(self):
        with pytest.raises(ValidationError, match="HH:MM:SS"):
            DurationField._parse_iso8601("01:02:03:04")

    def test_non_numeric_segment_rejected(self):
        # ``int("ab")`` raises; the ValueError is re-wrapped as a
        # ValidationError with the original message attached.
        with pytest.raises(ValidationError, match="cannot parse"):
            DurationField._parse_iso8601("ab:cd:ef")

    def test_float_seconds_segment_invalid_int_for_hours(self):
        # Hours must parse as int even when seconds may be float.
        with pytest.raises(ValidationError, match="cannot parse"):
            DurationField._parse_iso8601("1.5:00:00")


class TestDurationFieldToPython:
    """``to_python`` is the descriptor's coercer; it accepts more
    shapes than the raw parser (timedelta, int, float, str)."""

    def test_passes_through_existing_timedelta(self):
        td = datetime.timedelta(seconds=42)
        assert DurationField().to_python(td) is td

    def test_int_treated_as_microseconds(self):
        out = DurationField().to_python(1_000_000)
        assert out == datetime.timedelta(seconds=1)

    def test_float_treated_as_microseconds(self):
        out = DurationField().to_python(2_500_000.0)
        assert out == datetime.timedelta(microseconds=2_500_000)

    def test_string_routes_through_iso_parser(self):
        out = DurationField().to_python("00:00:05")
        assert out == datetime.timedelta(seconds=5)

    def test_none_passes_through(self):
        assert DurationField().to_python(None) is None

    def test_unsupported_type_raises_validation_error(self):
        f = DurationField()
        f.name = "td"
        with pytest.raises(ValidationError, match="cannot convert"):
            f.to_python(["not", "a", "timedelta"])


class TestDurationFieldFromDb:
    def test_string_round_trips_via_iso_parser(self):
        out = DurationField().from_db_value("00:00:01")
        assert out == datetime.timedelta(seconds=1)

    def test_int_round_trips_microseconds(self):
        out = DurationField().from_db_value(5_000_000)
        assert out == datetime.timedelta(seconds=5)

    def test_float_round_trips_microseconds(self):
        out = DurationField().from_db_value(2.5e6)
        assert out == datetime.timedelta(microseconds=2_500_000)

    def test_passes_through_existing_timedelta(self):
        td = datetime.timedelta(minutes=1)
        assert DurationField().from_db_value(td) is td

    def test_none_passes_through(self):
        assert DurationField().from_db_value(None) is None


# ── RangeField._parse_literal: PG range-literal grammar ─────────────────────


class TestRangeFieldParseLiteral:
    """The literal parser handles the on-the-wire form psycopg
    surfaces: ``[1,10)``, ``(,5]``, ``empty`` (PG's distinct-empty
    sentinel), and rejects anything else with a clear message."""

    def test_empty_keyword_returns_empty_range(self):
        r = RangeField._parse_literal("empty")
        assert r is not None
        assert r.is_empty()

    def test_basic_bounded_literal(self):
        r = RangeField._parse_literal("[1,10)")
        assert r is not None
        assert r.lower == "1"
        assert r.upper == "10"
        assert r.bounds == "[)"

    def test_unbounded_lower(self):
        r = RangeField._parse_literal("(,5]")
        assert r is not None
        assert r.lower is None
        assert r.upper == "5"
        assert r.bounds == "(]"

    def test_unbounded_both_sides(self):
        r = RangeField._parse_literal("[,)")
        assert r is not None
        assert r.lower is None
        assert r.upper is None

    def test_quoted_endpoint_strips_quotes(self):
        r = RangeField._parse_literal('["a","z"]')
        assert r is not None
        assert r.lower == "a"
        assert r.upper == "z"

    def test_whitespace_around_endpoints_stripped(self):
        r = RangeField._parse_literal("[ 1 , 10 )")
        assert r is not None
        assert r.lower == "1"
        assert r.upper == "10"

    def test_missing_opening_bracket_rejected(self):
        with pytest.raises(ValidationError, match="cannot parse"):
            RangeField._parse_literal("1,10)")

    def test_missing_closing_bracket_rejected(self):
        with pytest.raises(ValidationError, match="cannot parse"):
            RangeField._parse_literal("[1,10")

    def test_no_comma_rejected(self):
        with pytest.raises(ValidationError, match="malformed"):
            RangeField._parse_literal("[110)")

    def test_completely_empty_string_rejected(self):
        with pytest.raises(ValidationError, match="cannot parse"):
            RangeField._parse_literal("")


class TestRangeFieldToPython:
    def test_accepts_existing_range_unchanged(self):
        r = dorm.Range(1, 10)
        assert RangeField().to_python(r) is r

    def test_accepts_2_tuple(self):
        out = RangeField().to_python((1, 10))
        assert isinstance(out, dorm.Range)
        assert out.lower == 1 and out.upper == 10 and out.bounds == "[)"

    def test_accepts_3_tuple_with_bounds(self):
        out = RangeField().to_python((1, 10, "[]"))
        assert out.bounds == "[]"

    def test_accepts_2_list(self):
        out = RangeField().to_python([1, 10])
        assert isinstance(out, dorm.Range)

    def test_duck_types_psycopg_range(self):
        """psycopg returns a ``Range`` with ``.lower_inc`` /
        ``.upper_inc`` instead of a ``.bounds`` string. The coercer
        reads the inclusivity flags directly without importing
        psycopg."""

        class FakePsycopgRange:
            lower = 1
            upper = 10
            lower_inc = False
            upper_inc = True

        out = RangeField().to_python(FakePsycopgRange())
        assert out.bounds == "(]"
        assert out.lower == 1 and out.upper == 10

    def test_unsupported_input_raises(self):
        f = RangeField()
        f.name = "r"
        with pytest.raises(ValidationError, match="cannot convert"):
            f.to_python("not-a-range-shape")

    def test_none_passes_through(self):
        assert RangeField().to_python(None) is None


class TestRangeFieldGetDbPrepValue:
    """``get_db_prep_value`` formats a Range as the PG literal string
    psycopg can implicitly cast based on the column type."""

    def test_emits_canonical_literal_for_int_range(self):
        out = RangeField().get_db_prep_value(dorm.Range(1, 10))
        assert out == "[1,10)"

    def test_unbounded_endpoints_become_empty_strings(self):
        out = RangeField().get_db_prep_value(dorm.Range(None, 50, bounds="(]"))
        assert out == "(,50]"

    def test_dates_use_isoformat(self):
        r = dorm.Range(datetime.date(2026, 1, 1), datetime.date(2026, 2, 1))
        assert RangeField().get_db_prep_value(r) == "[2026-01-01,2026-02-01)"

    def test_datetime_uses_isoformat(self):
        r = dorm.Range(
            datetime.datetime(2026, 1, 1, 9, 0),
            datetime.datetime(2026, 1, 1, 17, 0),
        )
        assert RangeField().get_db_prep_value(r) == (
            "[2026-01-01T09:00:00,2026-01-01T17:00:00)"
        )

    def test_decimal_endpoints_round_trip_via_str(self):
        r = dorm.Range(decimal.Decimal("1.50"), decimal.Decimal("9.99"))
        assert RangeField().get_db_prep_value(r) == "[1.50,9.99)"

    def test_none_passes_through(self):
        assert RangeField().get_db_prep_value(None) is None

    def test_accepts_tuple_shorthand(self):
        # Calls ``to_python`` for the shape coercion, then formats.
        out = RangeField().get_db_prep_value((1, 10))
        assert out == "[1,10)"


# ── _format_range_endpoint scalar paths ─────────────────────────────────────


class TestFormatRangeEndpoint:
    def test_none_returns_empty_string(self):
        assert _format_range_endpoint(None) == ""

    def test_date_uses_isoformat(self):
        assert _format_range_endpoint(datetime.date(2026, 4, 28)) == "2026-04-28"

    def test_datetime_uses_isoformat(self):
        assert _format_range_endpoint(
            datetime.datetime(2026, 4, 28, 9, 0)
        ) == "2026-04-28T09:00:00"

    def test_int_falls_through_to_str(self):
        assert _format_range_endpoint(42) == "42"

    def test_decimal_falls_through_to_str(self):
        assert _format_range_endpoint(decimal.Decimal("3.14")) == "3.14"


# ── Range value type: equality / hash / is_empty ────────────────────────────


class TestRangeValueType:
    def test_eq_and_hash_match_for_equal_ranges(self):
        a = dorm.Range(1, 10, bounds="[)")
        b = dorm.Range(1, 10, bounds="[)")
        assert a == b
        assert hash(a) == hash(b)

    def test_eq_distinguishes_bounds(self):
        assert dorm.Range(1, 10, "[)") != dorm.Range(1, 10, "[]")

    def test_eq_returns_notimplemented_for_other_types(self):
        # Direct ``__eq__`` call returns NotImplemented; the ``==``
        # operator turns that into ``False`` per the data model.
        assert dorm.Range(1, 10).__eq__("not a range") is NotImplemented
        assert (dorm.Range(1, 10) == "not a range") is False

    def test_lower_inc_upper_inc_flags(self):
        assert dorm.Range(1, 10, "[)").lower_inc is True
        assert dorm.Range(1, 10, "[)").upper_inc is False
        assert dorm.Range(1, 10, "(]").lower_inc is False
        assert dorm.Range(1, 10, "(]").upper_inc is True

    def test_is_empty_only_for_canonical_empty(self):
        assert dorm.Range(None, None, bounds="()").is_empty() is True
        assert dorm.Range(None, 1, bounds="()").is_empty() is False
        assert dorm.Range(None, None, bounds="[)").is_empty() is False

    def test_repr_round_trips_through_eval_with_dorm_namespace(self):
        """``repr`` is human-readable; we don't promise eval-ability,
        but it should at least mention every component."""
        r = dorm.Range(1, 10, bounds="[)")
        text = repr(r)
        assert "1" in text and "10" in text and "[)" in text

    def test_invalid_bounds_rejected_at_construction(self):
        with pytest.raises(ValidationError, match="bounds"):
            dorm.Range(1, 10, bounds="??")


# ── RangeField.from_db_value branches ───────────────────────────────────────


class TestRangeFieldFromDbValue:
    def test_string_routes_through_parse_literal(self):
        r = RangeField().from_db_value("[1,10)")
        assert isinstance(r, dorm.Range)
        assert r.lower == "1" and r.upper == "10"

    def test_existing_range_passes_through(self):
        r = dorm.Range(1, 10)
        assert RangeField().from_db_value(r) is r

    def test_none_passes_through(self):
        assert RangeField().from_db_value(None) is None

    def test_psycopg_range_routed_through_to_python(self):
        class FakePG:
            lower = 1
            upper = 10
            lower_inc = True
            upper_inc = False

        out = RangeField().from_db_value(FakePG())
        assert isinstance(out, dorm.Range)
        assert out.bounds == "[)"


# ── EnumField error / coercion paths ────────────────────────────────────────


import enum  # noqa: E402  — used by the test classes below


class _Status(enum.Enum):
    LOW = "low"
    HIGH = "high"


class _Level(enum.IntEnum):
    JUNIOR = 1
    SENIOR = 3


class TestEnumFieldErrors:
    def test_to_python_with_unknown_string_raises(self):
        f = dorm.EnumField(_Status)
        f.name = "p"
        with pytest.raises(ValidationError, match="not a valid member"):
            f.to_python("unknown-priority")

    def test_to_python_with_unknown_int_for_int_enum_raises(self):
        f = dorm.EnumField(_Level)
        f.name = "lv"
        with pytest.raises(ValidationError, match="not a valid member"):
            f.to_python(99)

    def test_from_db_value_with_unknown_value_returns_raw(self):
        """Historical rows may carry a value the current enum no
        longer has — the read path returns the raw string instead of
        crashing, so callers can migrate forward."""
        f = dorm.EnumField(_Status)
        f.name = "p"
        out = f.from_db_value("legacy-value-no-longer-defined")
        assert out == "legacy-value-no-longer-defined"

    def test_from_db_value_none_passes_through(self):
        assert dorm.EnumField(_Status).from_db_value(None) is None

    def test_get_db_prep_value_passes_raw_string_through(self):
        """``get_db_prep_value`` already accepts ``Status.HIGH`` and
        returns ``"high"``. It should also leave a bare string
        untouched so callers can pre-coerce."""
        f = dorm.EnumField(_Status)
        assert f.get_db_prep_value("high") == "high"

    def test_get_db_prep_value_none_passes_through(self):
        assert dorm.EnumField(_Status).get_db_prep_value(None) is None

    def test_db_type_int_enum_is_integer(self):
        # SQLite connection stub.
        class _Conn:
            vendor = "sqlite"

        assert dorm.EnumField(_Level).db_type(_Conn()) == "INTEGER"

    def test_db_type_string_enum_is_varchar(self):
        class _Conn:
            vendor = "sqlite"

        col = dorm.EnumField(_Status).db_type(_Conn())
        assert col.startswith("VARCHAR(")
