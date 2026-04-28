"""Coverage for the 2.2 additions to ``dorm.inspect`` — the ``inspectdb``
type table now recognises ``INTERVAL``, ``CITEXT`` and the range
family, so a project adopting dorm against an existing PostgreSQL
schema gets the right field classes instead of the
``TextField``-with-NOTE fallback.
"""

from __future__ import annotations

from dorm.inspect import _map_type, render_models


# ── _map_type unit checks ────────────────────────────────────────────────────


class TestMapTypeNewMappings:
    def test_interval_maps_to_duration_field(self):
        cls, _ = _map_type("interval", "postgresql")
        assert cls == "DurationField"

    def test_citext_maps_to_citext_field(self):
        cls, _ = _map_type("citext", "postgresql")
        assert cls == "CITextField"

    def test_int4range_maps_to_integer_range_field(self):
        cls, _ = _map_type("int4range", "postgresql")
        assert cls == "IntegerRangeField"

    def test_int8range_maps_to_big_integer_range_field(self):
        cls, _ = _map_type("int8range", "postgresql")
        assert cls == "BigIntegerRangeField"

    def test_numrange_maps_to_decimal_range_field(self):
        cls, _ = _map_type("numrange", "postgresql")
        assert cls == "DecimalRangeField"

    def test_daterange_maps_to_date_range_field(self):
        cls, _ = _map_type("daterange", "postgresql")
        assert cls == "DateRangeField"

    def test_tstzrange_maps_to_datetime_range_field(self):
        cls, _ = _map_type("tstzrange", "postgresql")
        assert cls == "DateTimeRangeField"

    def test_tsrange_also_maps_to_datetime_range_field(self):
        # Less common (no timezone), but PG returns ``tsrange`` as the
        # ``data_type``. Map to the same Python field as ``tstzrange``;
        # users who care about the timezone difference can edit by hand.
        cls, _ = _map_type("tsrange", "postgresql")
        assert cls == "DateTimeRangeField"

    def test_sqlite_does_not_get_pg_only_types(self):
        """``interval`` / ``citext`` / range names aren't valid SQLite
        column types. If someone declared one anyway, the inspector
        falls back through the sqlite map and lands on ``TextField``
        with a NOTE — which is the right outcome."""
        cls, kwargs = _map_type("interval", "sqlite")
        assert cls == "TextField"
        assert "_inspect_unknown" in kwargs


# ── render_models output for the new types ───────────────────────────────────


class TestRenderModelsNewTypes:
    def test_renders_duration_field_from_interval_column(self):
        rendered = render_models(
            [
                {
                    "name": "jobs",
                    "vendor": "postgresql",
                    "fks": {},
                    "columns": [
                        {"name": "id", "data_type": "integer"},
                        {"name": "timeout", "data_type": "interval"},
                    ],
                }
            ]
        )
        assert "dorm.DurationField()" in rendered

    def test_renders_citext_field(self):
        rendered = render_models(
            [
                {
                    "name": "users",
                    "vendor": "postgresql",
                    "fks": {},
                    "columns": [
                        {"name": "email", "data_type": "citext"},
                    ],
                }
            ]
        )
        assert "dorm.CITextField()" in rendered

    def test_renders_range_fields(self):
        rendered = render_models(
            [
                {
                    "name": "reservations",
                    "vendor": "postgresql",
                    "fks": {},
                    "columns": [
                        {"name": "seats", "data_type": "int4range"},
                        {"name": "price", "data_type": "numrange"},
                        {"name": "during", "data_type": "tstzrange"},
                        {"name": "stay", "data_type": "daterange"},
                    ],
                }
            ]
        )
        assert "dorm.IntegerRangeField()" in rendered
        assert "dorm.DecimalRangeField()" in rendered
        assert "dorm.DateTimeRangeField()" in rendered
        assert "dorm.DateRangeField()" in rendered
