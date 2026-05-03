"""Tests for ``dorm.contrib.pgvector``.

The module is layered so the *unit-level* pieces (field validation,
expression compilation, index SQL emission) are testable on plain
SQLite with no extension installed. The *integration-level* pieces
(round-tripping data through a real ``vector(N)`` column, kNN
ordering with the ``<->`` operator) require both PostgreSQL and a
loaded ``pgvector`` extension; they're guarded with
:func:`_pgvector_available` and skipped when either is missing so
the suite still runs green on a contributor's laptop.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import dorm
from dorm.contrib.pgvector import (
    CosineDistance,
    HnswIndex,
    IvfflatIndex,
    L2Distance,
    MaxInnerProduct,
    VectorExtension,
    VectorField,
)
from dorm.exceptions import ValidationError


# ── Helpers ──────────────────────────────────────────────────────────


def _is_postgresql() -> bool:
    from dorm.db.connection import get_connection

    return getattr(get_connection(), "vendor", "sqlite") == "postgresql"


def _pgvector_available() -> bool:
    """Return True iff the test backend is PostgreSQL AND the
    ``vector`` extension can be enabled."""
    if not _is_postgresql():
        return False
    from dorm.db.connection import get_connection

    conn = get_connection()
    try:
        conn.execute_script('CREATE EXTENSION IF NOT EXISTS "vector"')
        rows = conn.execute(
            "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
        )
        return bool(rows)
    except Exception:
        return False


# ── VectorField unit tests ───────────────────────────────────────────


class TestVectorFieldUnit:
    def test_dimensions_required_positive(self):
        with pytest.raises(ValueError):
            VectorField(dimensions=0)
        with pytest.raises(ValueError):
            VectorField(dimensions=-1)

    def test_dimensions_capped_at_pgvector_max(self):
        # pgvector's ``vector`` type maxes out at 16000.
        VectorField(dimensions=16000)  # boundary OK
        with pytest.raises(ValueError):
            VectorField(dimensions=16001)

    def test_db_type_is_blob_on_sqlite(self):
        # Was ``None`` (no-op) in 2.5.0 — 2.6 adds sqlite-vec
        # support, so the column is a BLOB carrying packed
        # float32 bytes.
        f = VectorField(dimensions=4)

        class _Conn:
            vendor = "sqlite"

        assert f.db_type(_Conn()) == "BLOB"

    def test_db_type_returns_none_on_unknown_backend(self):
        # Defensive: a backend that's neither pgvector nor
        # sqlite-vec / libsql / mysql gets None so the migration
        # writer skips the column entirely.
        f = VectorField(dimensions=4)

        class _Conn:
            vendor = "oracle"

        assert f.db_type(_Conn()) is None

    def test_db_type_returns_vector_n_on_mysql(self):
        # MariaDB 11.7+ / MySQL 9.0+ ship native VECTOR(N) — added
        # in 3.1 to round out the cross-backend vector story.
        f = VectorField(dimensions=384)

        class _MySQL:
            vendor = "mysql"

        class _Maria:
            vendor = "mariadb"

        assert f.db_type(_MySQL()) == "VECTOR(384)"
        assert f.db_type(_Maria()) == "VECTOR(384)"

    def test_db_type_returns_vector_n_on_pg(self):
        f = VectorField(dimensions=1536)

        class _Conn:
            vendor = "postgresql"

        assert f.db_type(_Conn()) == "vector(1536)"

    def test_get_db_prep_value_serialises_per_active_backend(self):
        f = VectorField(dimensions=3)
        out = f.get_db_prep_value([1.0, 2.5, -3.5])
        # The active default backend determines the wire format.
        # On the SQLite test path we get packed bytes; on PG, text.
        if _is_postgresql():
            assert out == "[1.0,2.5,-3.5]"
        else:
            import struct

            assert out == struct.pack("<3f", 1.0, 2.5, -3.5)

    def test_get_db_prep_value_rejects_wrong_length(self):
        f = VectorField(dimensions=3)
        f.name = "embedding"
        with pytest.raises(ValidationError):
            f.get_db_prep_value([1.0, 2.0])

    def test_get_db_prep_value_passes_none_through(self):
        f = VectorField(dimensions=3)
        assert f.get_db_prep_value(None) is None

    def test_get_db_prep_value_accepts_tuple(self):
        f = VectorField(dimensions=2)
        out = f.get_db_prep_value((1.5, 2.5))
        if _is_postgresql():
            assert out == "[1.5,2.5]"
        else:
            import struct

            assert out == struct.pack("<2f", 1.5, 2.5)

    def test_to_python_parses_pgvector_text_form(self):
        # Bare-driver path: psycopg returns the column text as-is
        # when pgvector-python isn't installed.
        f = VectorField(dimensions=3)
        assert f.to_python("[1,2.5,-3]") == [1.0, 2.5, -3.0]

    def test_to_python_handles_empty_string(self):
        f = VectorField(dimensions=3)
        assert f.to_python("[]") == []

    def test_to_python_passes_through_list(self):
        f = VectorField(dimensions=3)
        assert f.to_python([1, 2, 3]) == [1.0, 2.0, 3.0]

    def test_from_db_value_round_trips_through_to_python(self):
        f = VectorField(dimensions=3)
        # The two methods are aliases — pin so a refactor that
        # decouples them doesn't drop the conversion silently.
        for shape in ("[1,2,3]", [1, 2, 3], (1, 2, 3)):
            assert f.from_db_value(shape) == [1.0, 2.0, 3.0]


# ── Distance expressions ─────────────────────────────────────────────


class TestDistanceExpressions:
    def test_l2_distance_compiles_with_arrow_operator(self):
        sql, params = L2Distance("embedding", [1.0, 2.0]).as_sql()
        assert "<->" in sql
        assert sql.count("%s") == 1
        assert params == ["[1.0,2.0]"]

    def test_cosine_distance_compiles_with_diamond_operator(self):
        sql, _ = CosineDistance("embedding", [1.0, 2.0]).as_sql()
        assert "<=>" in sql

    def test_max_inner_product_compiles_with_hash_operator(self):
        sql, _ = MaxInnerProduct("embedding", [1.0, 2.0]).as_sql()
        assert "<#>" in sql

    def test_table_alias_qualifies_column(self):
        sql, _ = L2Distance("embedding", [1.0]).as_sql(table_alias="docs")
        assert '"docs"."embedding"' in sql

    def test_no_table_alias_emits_unqualified_column(self):
        sql, _ = L2Distance("embedding", [1.0]).as_sql()
        assert '"embedding"' in sql
        assert '".embedding"' not in sql

    def test_invalid_column_name_rejected_at_construction(self):
        # SQL-injection guard: column goes through identifier
        # validation before reaching the SQL.
        with pytest.raises(ValueError):
            L2Distance("evil; DROP TABLE x", [1.0])

    def test_compiled_sql_includes_vector_cast(self):
        """``%s::vector`` cast tells PostgreSQL to convert the
        bound text parameter to a vector before the operator runs.
        Without the cast pgvector ≥ 0.6 rejects the comparison
        with ``operator does not exist: vector <-> text``."""
        sql, _ = L2Distance("embedding", [1, 2]).as_sql()
        assert "::vector" in sql

    def test_numpy_array_serialises_through_tolist(self):
        # We don't import numpy at module top — the suite must run
        # without it. Skip when not installed.
        np = pytest.importorskip("numpy")

        sql, params = L2Distance("e", np.array([1.0, 2.5])).as_sql()
        # The serialised literal matches the list form.
        assert params == ["[1.0,2.5]"]
        assert "<->" in sql


# ── Index helpers ───────────────────────────────────────────────────


class TestVectorIndexes:
    def test_hnsw_create_sql_uses_method_and_with_clause(self):
        idx = HnswIndex(
            fields=["embedding"],
            name="doc_emb_hnsw",
            opclass="vector_l2_ops",
            m=16,
            ef_construction=64,
        )
        forward, reverse = idx.create_sql("documents", vendor="postgresql")
        assert "USING hnsw" in forward
        assert '"embedding" vector_l2_ops' in forward
        assert "WITH (ef_construction = 64, m = 16)" in forward
        assert reverse == 'DROP INDEX IF EXISTS "doc_emb_hnsw"'

    def test_ivfflat_create_sql_emits_lists_option(self):
        idx = IvfflatIndex(
            fields=["embedding"],
            name="doc_emb_ivf",
            opclass="vector_cosine_ops",
            lists=100,
        )
        forward, _ = idx.create_sql("documents", vendor="postgresql")
        assert "USING ivfflat" in forward
        assert '"embedding" vector_cosine_ops' in forward
        assert "WITH (lists = 100)" in forward

    def test_index_rejects_multiple_fields(self):
        with pytest.raises(ValueError):
            HnswIndex(fields=["a", "b"], opclass="vector_l2_ops")

    def test_index_rejects_unknown_opclass(self):
        with pytest.raises(ValueError):
            HnswIndex(fields=["embedding"], opclass="text_pattern_ops")

    def test_default_opclass_is_l2(self):
        idx = HnswIndex(fields=["embedding"])
        forward, _ = idx.create_sql("documents", vendor="postgresql")
        assert "vector_l2_ops" in forward

    def test_index_no_with_clause_when_no_storage_options(self):
        idx = HnswIndex(fields=["embedding"], opclass="vector_l2_ops")
        forward, _ = idx.create_sql("documents", vendor="postgresql")
        assert "WITH (" not in forward


# ── VectorExtension migration op ────────────────────────────────────


class TestVectorExtensionOp:
    def test_describe_includes_pgvector_label(self):
        op = VectorExtension()
        assert "pgvector" in op.describe().lower()

    def test_repr_round_trips_to_constructor(self):
        # The migration writer emits ``repr(op)`` into the file —
        # the result must be a valid Python expression that
        # reconstructs the operation.
        assert repr(VectorExtension()) == "VectorExtension()"

    def test_state_forwards_does_not_mutate_state(self):
        op = VectorExtension()
        state = object()  # any sentinel; the method should leave
        # it alone.
        op.state_forwards("myapp", state)

    def test_database_forwards_skips_on_unknown_backend(self):
        """A backend that's neither PG nor SQLite is silently
        skipped — the operation logs a warning but never raises."""
        called: list[str] = []

        class _Conn:
            vendor = "mysql"

            def execute_script(self, sql: str) -> None:
                called.append(sql)

        VectorExtension().database_forwards("myapp", _Conn(), None, None)
        assert called == []

    def test_database_forwards_emits_create_extension_on_pg(self):
        emitted: list[str] = []

        class _Conn:
            vendor = "postgresql"

            def execute_script(self, sql: str) -> None:
                emitted.append(sql)

        VectorExtension().database_forwards("myapp", _Conn(), None, None)
        assert len(emitted) == 1
        assert "CREATE EXTENSION" in emitted[0]
        assert "IF NOT EXISTS" in emitted[0]
        assert "vector" in emitted[0]

    def test_database_backwards_emits_drop_extension_on_pg(self):
        emitted: list[str] = []

        class _Conn:
            vendor = "postgresql"

            def execute_script(self, sql: str) -> None:
                emitted.append(sql)

        VectorExtension().database_backwards("myapp", _Conn(), None, None)
        assert len(emitted) == 1
        assert "DROP EXTENSION" in emitted[0]


# ── makemigrations writer ────────────────────────────────────────────


class TestPgvectorMigrationWriter:
    def test_writes_file_with_extension_op(self, tmp_path: Path):
        from dorm.migrations.writer import (
            write_pgvector_extension_migration,
        )

        path = write_pgvector_extension_migration(
            "myapp", tmp_path, number=1
        )
        text = path.read_text()
        assert "VectorExtension()" in text
        assert "from dorm.contrib.pgvector import VectorExtension" in text
        # __init__.py created so the file is importable.
        assert (tmp_path / "__init__.py").exists()

    def test_default_filename_is_0001_enable_pgvector(self, tmp_path: Path):
        from dorm.migrations.writer import (
            write_pgvector_extension_migration,
        )

        path = write_pgvector_extension_migration(
            "myapp", tmp_path, number=1
        )
        assert path.name == "0001_enable_pgvector.py"

    def test_custom_name_honoured(self, tmp_path: Path):
        from dorm.migrations.writer import (
            write_pgvector_extension_migration,
        )

        path = write_pgvector_extension_migration(
            "myapp", tmp_path, number=42, name="vector_setup"
        )
        assert path.name == "0042_vector_setup.py"

    def test_vector_field_serialises_with_dimensions(self):
        """Regression: the migration writer used to emit
        ``VectorField()`` without the mandatory ``dimensions=``
        keyword, producing migrations that crashed at import with
        ``TypeError: __init__() missing 1 required positional
        argument: 'dimensions'``."""
        from dorm.migrations.writer import _serialize_field

        out = _serialize_field(VectorField(dimensions=384))
        assert "dimensions=384" in out

    def test_vector_field_dimension_change_detected_on_sqlite(self):
        """Regression: ``VectorField(dimensions=384)`` →
        ``VectorField(dimensions=1536)`` produced no migration on
        SQLite because both columns map to ``BLOB`` and the
        autodetector compared only ``db_type``. The serialised
        form differs, so the autodetector now also diffs the
        writer's output and catches the change.
        """
        from dorm.migrations.autodetector import MigrationAutodetector
        from dorm.migrations.state import ProjectState

        old_field = VectorField(dimensions=384)
        new_field = VectorField(dimensions=1536)
        # Walk through state-by-hand to keep the test independent
        # of the dorm.Model autoload path.
        from_state = ProjectState()
        from_state.models["sales.document"] = {
            "name": "Document",
            "fields": {
                "id": dorm.AutoField(primary_key=True),
                "embedding": old_field,
            },
            "options": {},
        }
        to_state = ProjectState()
        to_state.models["sales.document"] = {
            "name": "Document",
            "fields": {
                "id": dorm.AutoField(primary_key=True),
                "embedding": new_field,
            },
            "options": {},
        }
        detector = MigrationAutodetector(from_state, to_state)
        changes = detector.changes(app_label="sales")
        ops = changes.get("sales", [])
        from dorm.migrations.operations import AlterField

        alter_ops = [o for o in ops if isinstance(o, AlterField)]
        assert alter_ops, (
            f"expected AlterField for dimension change, got {ops!r}"
        )
        assert alter_ops[0].name == "embedding"

    def test_vector_field_import_line_added(self):
        """The writer's ``_FIELD_IMPORTS`` table must include
        ``VectorField`` so generated migrations include the
        ``from dorm.contrib.pgvector import VectorField`` line.
        Without it the eval'd migration fails with NameError."""
        from dorm.migrations.writer import _FIELD_IMPORTS

        assert "VectorField" in _FIELD_IMPORTS
        assert (
            "from dorm.contrib.pgvector import VectorField"
            in _FIELD_IMPORTS["VectorField"]
        )

    def test_generated_file_imports_cleanly(self, tmp_path: Path):
        """Smoke: the file written is valid Python and the
        ``operations`` list has exactly one ``VectorExtension``."""
        from dorm.migrations.writer import (
            write_pgvector_extension_migration,
        )
        import importlib.util

        path = write_pgvector_extension_migration(
            "myapp", tmp_path, number=1
        )
        spec = importlib.util.spec_from_file_location("_genmig", path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        ops = getattr(mod, "operations", [])
        assert len(ops) == 1
        assert isinstance(ops[0], VectorExtension)


# ── Live PG + pgvector integration ──────────────────────────────────


@pytest.fixture
def pgvector_table(clean_db):
    """Skip + create a table with a real ``vector(3)`` column.

    Skipped automatically when the backend is SQLite or pgvector
    isn't installed in the PG service container."""
    if not _pgvector_available():
        pytest.skip("pgvector extension not available on this backend")

    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "pgvec_docs" CASCADE')
    conn.execute_script(
        'CREATE TABLE "pgvec_docs" ('
        '"id" SERIAL PRIMARY KEY, '
        '"title" VARCHAR(50), '
        '"embedding" vector(3))'
    )
    yield
    conn.execute_script('DROP TABLE IF EXISTS "pgvec_docs" CASCADE')


class TestVectorFieldRoundTrip:
    """End-to-end: define a model with a ``VectorField``, insert,
    fetch, verify the value round-trips."""

    def test_round_trip_through_real_pg_vector_column(self, pgvector_table):
        class _Doc(dorm.Model):
            title = dorm.CharField(max_length=50)
            embedding = VectorField(dimensions=3)

            class Meta:
                db_table = "pgvec_docs"
                app_label = "tests"

        obj = _Doc.objects.create(
            title="hello", embedding=[1.0, 2.0, 3.0]
        )
        reloaded = _Doc.objects.get(pk=obj.pk)
        assert reloaded.embedding == [1.0, 2.0, 3.0]

    def test_wrong_dimension_raises_validation_error(self, pgvector_table):
        class _Doc(dorm.Model):
            title = dorm.CharField(max_length=50)
            embedding = VectorField(dimensions=3)

            class Meta:
                db_table = "pgvec_docs"
                app_label = "tests"

        with pytest.raises(ValidationError):
            _Doc.objects.create(title="bad", embedding=[1.0, 2.0])


class TestKnnSearch:
    def test_l2_distance_orders_results(self, pgvector_table):
        """Three docs with known embeddings; querying for the
        nearest to [0,0,0] must return them in increasing-distance
        order."""
        class _Doc(dorm.Model):
            title = dorm.CharField(max_length=50)
            embedding = VectorField(dimensions=3)

            class Meta:
                db_table = "pgvec_docs"
                app_label = "tests"

        _Doc.objects.create(title="far", embedding=[10.0, 10.0, 10.0])
        _Doc.objects.create(title="mid", embedding=[1.0, 1.0, 1.0])
        _Doc.objects.create(title="near", embedding=[0.1, 0.0, 0.0])

        ranked = list(
            _Doc.objects.annotate(
                d=L2Distance("embedding", [0.0, 0.0, 0.0])
            ).order_by("d")
        )
        assert [d.title for d in ranked] == ["near", "mid", "far"]

    def test_cosine_distance_orders_results(self, pgvector_table):
        class _Doc(dorm.Model):
            title = dorm.CharField(max_length=50)
            embedding = VectorField(dimensions=3)

            class Meta:
                db_table = "pgvec_docs"
                app_label = "tests"

        _Doc.objects.create(title="anti", embedding=[-1.0, 0.0, 0.0])
        _Doc.objects.create(title="ortho", embedding=[0.0, 1.0, 0.0])
        _Doc.objects.create(title="same", embedding=[1.0, 0.0, 0.0])

        ranked = list(
            _Doc.objects.annotate(
                d=CosineDistance("embedding", [1.0, 0.0, 0.0])
            ).order_by("d")
        )
        # Same direction → 0, orthogonal → 1, opposite → 2.
        assert ranked[0].title == "same"
        assert ranked[-1].title == "anti"


class TestIndexCreatesOnRealTable:
    def test_hnsw_index_create_sql_executes(self, pgvector_table):
        from dorm.db.connection import get_connection

        idx = HnswIndex(
            fields=["embedding"],
            name="pgvec_docs_hnsw",
            opclass="vector_l2_ops",
            m=8,
            ef_construction=32,
        )
        forward, reverse = idx.create_sql(
            "pgvec_docs", vendor="postgresql"
        )
        conn = get_connection()
        try:
            conn.execute_script(forward)
            # Sanity: the index actually landed in pg_indexes.
            rows = conn.execute(
                "SELECT 1 FROM pg_indexes WHERE indexname = %s",
                ["pgvec_docs_hnsw"],
            )
            assert rows
        finally:
            conn.execute_script(reverse)


# ── SQLite path (sqlite-vec) ────────────────────────────────────────


def _sqlite_vec_available() -> bool:
    """Return True iff the test backend is SQLite AND the sqlite-vec
    Python package is importable AND ``enable_load_extension`` is
    supported by the active Python build."""
    if _is_postgresql():
        return False
    try:
        import sqlite_vec  # noqa: F401
    except ImportError:
        return False
    import sqlite3

    test_conn = sqlite3.connect(":memory:")
    try:
        test_conn.enable_load_extension(True)
        return True
    except (AttributeError, sqlite3.OperationalError):
        return False
    finally:
        test_conn.close()


class TestVectorFieldSqliteUnit:
    """Unit tests for the SQLite path that DON'T require the
    extension to be loaded — they just exercise the field's
    serialise / deserialise round-trip and the BLOB column type."""

    def test_db_type_is_blob_on_sqlite(self):
        f = VectorField(dimensions=4)

        class _Conn:
            vendor = "sqlite"

        assert f.db_type(_Conn()) == "BLOB"

    def test_packed_round_trip_via_to_python(self):
        import struct

        f = VectorField(dimensions=3)
        original = [1.0, 2.5, -3.5]
        packed = struct.pack("<3f", *original)
        # Float32 round-trip introduces tiny precision loss; allow
        # near-equality.
        recovered = f.to_python(packed)
        assert all(abs(a - b) < 1e-6 for a, b in zip(recovered, original))

    def test_corrupted_blob_length_raises(self):
        f = VectorField(dimensions=3)
        # 5 bytes — not a multiple of 4 → packed-float assumption
        # broken. Surface as ValidationError, not silent garbage.
        with pytest.raises(ValidationError):
            f.to_python(b"\x00\x00\x00\x00\x00")

    def test_memoryview_handled(self):
        import struct

        f = VectorField(dimensions=3)
        packed = struct.pack("<3f", 1.0, 2.0, 3.0)
        out = f.to_python(memoryview(packed))
        assert all(abs(a - b) < 1e-6 for a, b in zip(out, [1.0, 2.0, 3.0]))


class TestDistanceExpressionsSqliteCompile:
    """Compile-time vendor switching — these don't need the
    sqlite-vec extension loaded, just a connection-shaped object
    with ``vendor='sqlite'``."""

    def test_l2_compiles_to_vec_distance_l2_on_sqlite(self):
        class _Conn:
            vendor = "sqlite"

        sql, params = L2Distance("embedding", [1.0, 2.0]).as_sql(
            connection=_Conn()
        )
        assert sql.startswith("vec_distance_L2(")
        assert sql.count("%s") == 1
        # Param is packed bytes, not text.
        assert isinstance(params[0], bytes)

    def test_cosine_compiles_to_vec_distance_cosine_on_sqlite(self):
        class _Conn:
            vendor = "sqlite"

        sql, _ = CosineDistance("embedding", [1.0]).as_sql(connection=_Conn())
        assert sql.startswith("vec_distance_cosine(")

    def test_max_inner_product_unsupported_on_sqlite(self):
        class _Conn:
            vendor = "sqlite"

        with pytest.raises(NotImplementedError, match="L2-normalised"):
            MaxInnerProduct("embedding", [1.0]).as_sql(connection=_Conn())

    def test_pg_path_still_emits_operator_with_explicit_connection(self):
        class _Conn:
            vendor = "postgresql"

        sql, _ = L2Distance("embedding", [1.0]).as_sql(connection=_Conn())
        assert "<->" in sql

    def test_default_vendor_is_postgres_when_no_connection(self):
        # Backwards-compat: callers that compile without passing
        # connection (older code paths) still get the pgvector form.
        sql, _ = L2Distance("embedding", [1.0]).as_sql()
        assert "<->" in sql


class TestVectorExtensionSqlite:
    def test_database_forwards_loads_extension_when_available(
        self, tmp_path: Path
    ):
        if not _sqlite_vec_available():
            pytest.skip("sqlite-vec not installed or extension loading disabled")

        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "ve.db")})
        try:
            VectorExtension().database_forwards(
                "tests", wrapper, None, None
            )
            # Flag set so subsequent ``_new_connection`` re-loads.
            assert wrapper._vec_extension_enabled is True
            # Current conn already has vec_distance_L2 registered.
            row = wrapper.execute(
                "SELECT vec_distance_L2(?, ?) AS d",
                [b"\x00\x00\x80?\x00\x00\x00@", b"\x00\x00\x00@\x00\x00\x80?"],
            )
            # Two 2-d vectors: [1,2] and [2,1]. L2 = sqrt(2) ~= 1.4142
            d = float(dict(row[0])["d"])
            assert abs(d - 1.4142135) < 1e-4
        finally:
            wrapper.close()

    def test_backwards_clears_flag_on_sqlite(self, tmp_path: Path):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "vb.db")})
        wrapper._vec_extension_enabled = True
        VectorExtension().database_backwards(
            "tests", wrapper, None, None
        )
        assert wrapper._vec_extension_enabled is False

    def test_describe_mentions_both_backends(self):
        op = VectorExtension()
        # The describe text covers both vendors so a user reading
        # ``dorm migrate``'s output sees what's happening.
        text = op.describe().lower()
        assert "vector" in text


@pytest.fixture
def sqlite_vec_wrapper(tmp_path: Path):
    """Set up a SQLite wrapper with sqlite-vec auto-loaded.

    Skipped when sqlite-vec or ``enable_load_extension`` aren't
    available in the active Python build. The wrapper is fresh
    per-test (own DB file) so tests don't leak state."""
    if not _sqlite_vec_available():
        pytest.skip("sqlite-vec extension not available")

    from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

    wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "vec.db")})
    wrapper._vec_extension_enabled = True
    yield wrapper
    wrapper.close()


class TestSqliteVecRoundTrip:
    def test_blob_round_trip_preserves_floats(self, sqlite_vec_wrapper):
        wrapper = sqlite_vec_wrapper
        wrapper.execute_script(
            'CREATE TABLE "docs" ('
            '"id" INTEGER PRIMARY KEY, '
            '"name" TEXT, '
            '"embedding" BLOB)'
        )
        f = VectorField(dimensions=3)
        f.name = "embedding"
        prep = f.get_db_prep_value([1.0, 2.0, 3.0])
        assert isinstance(prep, bytes)
        wrapper.execute_write(
            'INSERT INTO "docs" ("name", "embedding") VALUES (%s, %s)',
            ["a", prep],
        )
        rows = wrapper.execute(
            'SELECT "embedding" FROM "docs" WHERE "name" = %s', ["a"]
        )
        # Read raw blob, run through ``to_python`` → list[float].
        raw = dict(rows[0])["embedding"]
        recovered = f.to_python(raw)
        assert all(abs(a - b) < 1e-6 for a, b in zip(recovered, [1.0, 2.0, 3.0]))

    def test_l2_distance_orders_results_under_sqlite_vec(
        self, sqlite_vec_wrapper
    ):
        wrapper = sqlite_vec_wrapper
        wrapper.execute_script(
            'CREATE TABLE "docs" ('
            '"id" INTEGER PRIMARY KEY, '
            '"name" TEXT, '
            '"embedding" BLOB)'
        )
        # Three docs at known distances from [0,0,0].
        f = VectorField(dimensions=3)
        f.name = "embedding"
        for label, vec in [
            ("far", [10.0, 10.0, 10.0]),
            ("mid", [1.0, 1.0, 1.0]),
            ("near", [0.1, 0.0, 0.0]),
        ]:
            wrapper.execute_write(
                'INSERT INTO "docs" ("name", "embedding") VALUES (%s, %s)',
                [label, f.get_db_prep_value(vec)],
            )

        # Compile a distance expression directly so we don't need a
        # full Model + queryset on top of the bare wrapper.
        class _Conn:
            vendor = "sqlite"

        expr_sql, expr_params = L2Distance(
            "embedding", [0.0, 0.0, 0.0]
        ).as_sql(table_alias="docs", connection=_Conn())
        rows = wrapper.execute(
            f'SELECT "name" FROM "docs" ORDER BY {expr_sql} ASC',
            expr_params,
        )
        order = [dict(r)["name"] for r in rows]
        assert order == ["near", "mid", "far"]

    def test_dimension_mismatch_raises_validation_error(
        self, sqlite_vec_wrapper
    ):
        f = VectorField(dimensions=3)
        f.name = "embedding"
        with pytest.raises(ValidationError):
            f.get_db_prep_value([1.0, 2.0])  # only 2 components
