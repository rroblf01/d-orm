"""Tests for new 2.1 field types: DurationField, EnumField, CITextField,
and the Range* family (PostgreSQL-only)."""
from __future__ import annotations

import datetime
import decimal
import enum
from typing import Any

import pytest

import dorm
from dorm.db.connection import get_connection


# ── Test models ───────────────────────────────────────────────────────────────


class Priority(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class JobLevel(enum.IntEnum):
    JUNIOR = 1
    MID = 2
    SENIOR = 3


class TimedJob(dorm.Model):
    name = dorm.CharField(max_length=100)
    timeout = dorm.DurationField()
    grace = dorm.DurationField(null=True, blank=True)

    class Meta:
        db_table = "nf_timed_jobs"


class TaggedTask(dorm.Model):
    title = dorm.CharField(max_length=100)
    priority = dorm.EnumField(Priority, default=Priority.LOW)
    level = dorm.EnumField(JobLevel, default=JobLevel.JUNIOR)

    class Meta:
        db_table = "nf_tagged_tasks"


class Mailbox(dorm.Model):
    address = dorm.CITextField(unique=True)

    class Meta:
        db_table = "nf_mailboxes"


class Reservation(dorm.Model):
    name = dorm.CharField(max_length=50)
    seats = dorm.IntegerRangeField(null=True, blank=True)
    price = dorm.DecimalRangeField(null=True, blank=True)
    when = dorm.DateRangeField(null=True, blank=True)
    during = dorm.DateTimeRangeField(null=True, blank=True)

    class Meta:
        db_table = "nf_reservations"


# ── Per-test table setup ──────────────────────────────────────────────────────


@pytest.fixture
def _create_simple_tables(clean_db):
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    from dorm.migrations.operations import _field_to_column_sql

    tables = [
        ("nf_timed_jobs", TimedJob),
        ("nf_tagged_tasks", TaggedTask),
        ("nf_mailboxes", Mailbox),
    ]
    for tbl, _ in tables:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    # CITEXT requires the extension on PG.
    if getattr(conn, "vendor", "sqlite") == "postgresql":
        conn.execute_script("CREATE EXTENSION IF NOT EXISTS citext")

    for tbl, model in tables:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{tbl}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )
    yield


@pytest.fixture
def _create_range_tables(clean_db):
    """Range fields are PG-only; this fixture skips on SQLite so the
    rest of the field tests still exercise on both backends."""
    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("Range fields are PostgreSQL-only.")

    from dorm.migrations.operations import _field_to_column_sql

    conn.execute_script('DROP TABLE IF EXISTS "nf_reservations" CASCADE')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in Reservation._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "nf_reservations" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield


# ── DurationField ─────────────────────────────────────────────────────────────


class TestDurationField:
    def test_round_trip_basic(self, _create_simple_tables):
        td = datetime.timedelta(minutes=5, seconds=30)
        job = TimedJob.objects.create(name="seed", timeout=td)
        loaded = TimedJob.objects.get(pk=job.pk)
        assert isinstance(loaded.timeout, datetime.timedelta)
        assert loaded.timeout == td

    def test_round_trip_microseconds(self, _create_simple_tables):
        td = datetime.timedelta(microseconds=123_456_789)
        job = TimedJob.objects.create(name="micro", timeout=td)
        loaded = TimedJob.objects.get(pk=job.pk)
        assert loaded.timeout == td

    def test_negative_duration(self, _create_simple_tables):
        td = -datetime.timedelta(hours=1, minutes=30)
        job = TimedJob.objects.create(name="neg", timeout=td)
        loaded = TimedJob.objects.get(pk=job.pk)
        assert loaded.timeout == td

    def test_null_allowed_when_configured(self, _create_simple_tables):
        job = TimedJob.objects.create(
            name="no-grace", timeout=datetime.timedelta(seconds=1)
        )
        assert job.grace is None
        loaded = TimedJob.objects.get(pk=job.pk)
        assert loaded.grace is None

    def test_invalid_input_rejected(self):
        f = dorm.DurationField()
        f.name = "timeout"
        with pytest.raises(dorm.ValidationError):
            f.to_python(["not", "a", "timedelta"])

    def test_db_type_per_vendor(self, _create_simple_tables):
        conn = get_connection()
        f = dorm.DurationField()
        if getattr(conn, "vendor", "sqlite") == "postgresql":
            assert f.db_type(conn) == "INTERVAL"
        else:
            assert f.db_type(conn) == "BIGINT"


# ── EnumField ─────────────────────────────────────────────────────────────────


class TestEnumField:
    def test_string_enum_round_trip(self, _create_simple_tables):
        task = TaggedTask.objects.create(title="t1", priority=Priority.HIGH)
        loaded = TaggedTask.objects.get(pk=task.pk)
        assert loaded.priority is Priority.HIGH
        assert isinstance(loaded.priority, Priority)

    def test_int_enum_round_trip(self, _create_simple_tables):
        task = TaggedTask.objects.create(title="t2", level=JobLevel.SENIOR)
        loaded = TaggedTask.objects.get(pk=task.pk)
        assert loaded.level is JobLevel.SENIOR
        assert isinstance(loaded.level, JobLevel)

    def test_default_applied(self, _create_simple_tables):
        task = TaggedTask.objects.create(title="defaults")
        # ``Priority.LOW`` is the declared default.
        assert task.priority is Priority.LOW
        assert task.level is JobLevel.JUNIOR

    def test_filter_by_enum_value(self, _create_simple_tables):
        TaggedTask.objects.create(title="hi", priority=Priority.HIGH)
        TaggedTask.objects.create(title="lo", priority=Priority.LOW)
        # Both ``Priority.HIGH`` and the bare ``"high"`` string are accepted
        # by the binding path.
        by_member = list(TaggedTask.objects.filter(priority=Priority.HIGH))
        by_value = list(TaggedTask.objects.filter(priority="high"))
        assert {t.title for t in by_member} == {"hi"}
        assert {t.title for t in by_value} == {"hi"}

    def test_invalid_value_raises(self, _create_simple_tables):
        with pytest.raises(dorm.ValidationError):
            TaggedTask(title="bad", priority="urgent")

    def test_choices_auto_populated(self):
        f = dorm.EnumField(Priority)
        assert f.choices == [
            ("low", "LOW"),
            ("medium", "MEDIUM"),
            ("high", "HIGH"),
        ]

    def test_rejects_non_enum_class(self):
        # Cast through ``Any`` so the static type-check passes; the
        # runtime guard is exactly what we're exercising here.
        with pytest.raises(dorm.ValidationError):
            dorm.EnumField(int)  # ty: ignore[invalid-argument-type]


# ── CITextField ───────────────────────────────────────────────────────────────


class TestCITextField:
    def test_db_type_per_vendor(self, _create_simple_tables):
        conn = get_connection()
        f = dorm.CITextField()
        if getattr(conn, "vendor", "sqlite") == "postgresql":
            assert f.db_type(conn) == "CITEXT"
        else:
            assert f.db_type(conn) == "TEXT COLLATE NOCASE"

    def test_case_insensitive_lookup(self, _create_simple_tables):
        Mailbox.objects.create(address="Alice@Example.com")
        # Canonical-cased lookup that succeeds via the column collation
        # (CITEXT on PG, NOCASE on SQLite).
        found = Mailbox.objects.filter(address="alice@example.com").first()
        assert found is not None
        assert found.address.lower() == "alice@example.com"

    def test_round_trip_value(self, _create_simple_tables):
        m = Mailbox.objects.create(address="MixedCase@Example.com")
        loaded = Mailbox.objects.get(pk=m.pk)
        assert loaded.address == "MixedCase@Example.com"

    def test_unique_is_case_insensitive(self, _create_simple_tables):
        Mailbox.objects.create(address="Bob@Example.com")
        with pytest.raises(dorm.IntegrityError):
            Mailbox.objects.create(address="bob@EXAMPLE.com")


# ── RangeField (PostgreSQL only) ──────────────────────────────────────────────


class TestRangeField:
    def test_integer_range_round_trip(self, _create_range_tables):
        Reservation.objects.create(name="ints", seats=dorm.Range(1, 10))
        loaded = Reservation.objects.get(name="ints")
        assert loaded.seats is not None
        assert loaded.seats.lower == 1
        assert loaded.seats.upper == 10
        assert loaded.seats.bounds == "[)"

    def test_decimal_range_round_trip(self, _create_range_tables):
        r = dorm.Range(decimal.Decimal("1.50"), decimal.Decimal("9.99"))
        Reservation.objects.create(name="prices", price=r)
        loaded = Reservation.objects.get(name="prices")
        assert loaded.price is not None
        assert loaded.price.lower == decimal.Decimal("1.50")
        assert loaded.price.upper == decimal.Decimal("9.99")

    def test_date_range_round_trip(self, _create_range_tables):
        r = dorm.Range(datetime.date(2026, 1, 1), datetime.date(2026, 2, 1))
        Reservation.objects.create(name="dates", when=r)
        loaded = Reservation.objects.get(name="dates")
        assert loaded.when == r

    def test_datetime_range_round_trip(self, _create_range_tables):
        r = dorm.Range(
            datetime.datetime(2026, 1, 1, 9, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 1, 1, 17, 0, tzinfo=datetime.timezone.utc),
            bounds="[]",
        )
        Reservation.objects.create(name="dt", during=r)
        loaded = Reservation.objects.get(name="dt")
        assert loaded.during is not None
        assert loaded.during.lower == r.lower
        assert loaded.during.upper == r.upper

    def test_unbounded_endpoints(self, _create_range_tables):
        # Use ``numrange`` (continuous) so PG doesn't canonicalise the
        # bounds — discrete range types like ``int4range`` rewrite all
        # outputs to ``[lower, upper)`` form, which is its own behaviour
        # to test in :meth:`test_integer_range_canonicalised`.
        Reservation.objects.create(
            name="open",
            price=dorm.Range(None, decimal.Decimal("99.50"), bounds="(]"),
        )
        loaded = Reservation.objects.get(name="open")
        assert loaded.price is not None
        assert loaded.price.lower is None
        assert loaded.price.upper == decimal.Decimal("99.50")
        assert loaded.price.bounds == "(]"

    def test_integer_range_canonicalised_by_pg(self, _create_range_tables):
        # PG normalises every discrete range to ``[lower, upper)`` form,
        # so ``(1, 5]`` round-trips as ``[2, 6)`` — checking we don't
        # silently lose data.
        Reservation.objects.create(
            name="canon", seats=dorm.Range(1, 5, bounds="(]")
        )
        loaded = Reservation.objects.get(name="canon")
        assert loaded.seats is not None
        assert loaded.seats.bounds == "[)"
        assert loaded.seats.lower == 2
        assert loaded.seats.upper == 6

    def test_db_type_unsupported_on_sqlite(self):
        conn = get_connection()
        if getattr(conn, "vendor", "sqlite") == "postgresql":
            pytest.skip("PG supports range fields; this checks the SQLite guard.")
        f = dorm.IntegerRangeField()
        with pytest.raises(NotImplementedError):
            f.db_type(conn)

    def test_range_value_equality(self):
        a = dorm.Range(1, 10, bounds="[)")
        b = dorm.Range(1, 10, bounds="[)")
        c = dorm.Range(1, 10, bounds="[]")
        assert a == b
        assert a != c
        assert hash(a) == hash(b)

    def test_range_invalid_bounds_rejected(self):
        with pytest.raises(dorm.ValidationError):
            dorm.Range(1, 10, bounds="<<")


# ── EnumField generic type parameter is honored at runtime ────────────────────


def test_enum_field_to_python_returns_member():
    f = dorm.EnumField(Priority)
    f.name = "priority"
    out: Any = f.to_python("medium")
    assert out is Priority.MEDIUM
