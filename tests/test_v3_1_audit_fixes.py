"""Tests for v3.1 audit fixes + MySQL scaffold + MySQL vector support.

Pinned bugs:

1. ``Meta.proxy = True`` no longer mutates the parent's ``field.model``
   back-reference — the metaclass deep-copies parent fields before
   contribution.
2. ``parse_database_url`` recognises ``mysql://`` / ``mariadb://``
   and yields a config dict the connection router accepts.
3. ``_create_sync_connection`` / ``_create_async_connection`` route
   the MySQL engine name through the scaffold wrapper, which
   raises ``ImproperlyConfigured`` with a clear v3.2-pointer
   message.
4. ``VectorField.db_type`` returns ``VECTOR(N)`` on MySQL / MariaDB.
5. ``L2Distance`` / ``CosineDistance`` compile to MariaDB's
   ``VEC_DISTANCE_*`` family with ``VEC_FromBinary(?)`` binding.
   ``MaxInnerProduct`` raises ``NotImplementedError`` on MySQL —
   matches the libsql / sqlite-vec behaviour.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.exceptions import ImproperlyConfigured


# ──────────────────────────────────────────────────────────────────────────────
# Bug 1 — proxy doesn't mutate parent's field.model
# ──────────────────────────────────────────────────────────────────────────────


def test_proxy_does_not_steal_parent_field_model_reference():
    class _ProxyParent(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            app_label = "v3_1_proxy_audit"
            db_table = "v3_1_proxy_audit_parent"

    parent_field_model_before = _ProxyParent._meta.get_field("name").model

    class _ProxyChild(_ProxyParent):
        class Meta:
            app_label = "v3_1_proxy_audit"
            proxy = True

    parent_field_model_after = _ProxyParent._meta.get_field("name").model
    proxy_field_model = _ProxyChild._meta.get_field("name").model

    # The bug we fixed: subclass instantiation overwrote the
    # parent's ``field.model`` so subsequent parent queries hit the
    # subclass's table / descriptors. Deep-copy keeps both pointing
    # at the right class.
    assert parent_field_model_before is _ProxyParent
    assert parent_field_model_after is _ProxyParent
    assert proxy_field_model is _ProxyChild


# ──────────────────────────────────────────────────────────────────────────────
# Bug 2 — parse_database_url for mysql:// + mariadb://
# ──────────────────────────────────────────────────────────────────────────────


def test_parse_database_url_mysql_basic():
    cfg = dorm.parse_database_url("mysql://root:secret@localhost:3306/myapp")
    assert cfg["ENGINE"] == "mysql"
    assert cfg["USER"] == "root"
    assert cfg["PASSWORD"] == "secret"
    assert cfg["HOST"] == "localhost"
    assert cfg["PORT"] == 3306
    assert cfg["NAME"] == "myapp"


def test_parse_database_url_mariadb_basic():
    cfg = dorm.parse_database_url("mariadb://root@localhost/myapp")
    assert cfg["ENGINE"] == "mariadb"
    assert cfg["PORT"] == 3306  # default when scheme has no port


def test_parse_database_url_mysql_options_carried_to_options_dict():
    cfg = dorm.parse_database_url(
        "mysql://root:s@h/db?ssl_mode=REQUIRED&charset=utf8mb4"
    )
    assert cfg["OPTIONS"] == {"ssl_mode": "REQUIRED", "charset": "utf8mb4"}


def test_parse_database_url_unknown_scheme_message_lists_mysql():
    """Error message must mention mysql / mariadb so users with a
    typo'd scheme see them in the supported-list."""
    with pytest.raises(ImproperlyConfigured, match="mysql"):
        dorm.parse_database_url("oracle://x/y")


# ──────────────────────────────────────────────────────────────────────────────
# Bug 3 — MySQL connection routing
# ──────────────────────────────────────────────────────────────────────────────


def test_mysql_sync_connection_constructs_wrapper():
    """3.1 ships a real MySQL backend backed by ``pymysql``. The
    sync route should now return a usable wrapper (lazy connection
    on first ``execute``); only an actual network attempt would
    fail against a non-existent host."""
    from dorm.db.connection import _create_sync_connection

    conn = _create_sync_connection("default", {"ENGINE": "mysql", "NAME": "x"})
    assert conn.vendor == "mysql"


def test_mariadb_sync_connection_constructs_wrapper():
    from dorm.db.connection import _create_sync_connection

    conn = _create_sync_connection("default", {"ENGINE": "mariadb", "NAME": "x"})
    assert conn.vendor == "mysql"


def test_mysql_async_connection_constructs_wrapper():
    from dorm.db.connection import _create_async_connection

    conn = _create_async_connection("default", {"ENGINE": "mysql", "NAME": "x"})
    assert conn.vendor == "mysql"


# ──────────────────────────────────────────────────────────────────────────────
# Bug 4 — VectorField.db_type for MySQL / MariaDB
# ──────────────────────────────────────────────────────────────────────────────


def test_vectorfield_db_type_mysql_returns_vector_n():
    from dorm.contrib.pgvector import VectorField

    class _MySQLConn:
        vendor = "mysql"

    class _MariaConn:
        vendor = "mariadb"

    field = VectorField(dimensions=384)
    assert field.db_type(_MySQLConn()) == "VECTOR(384)"
    assert field.db_type(_MariaConn()) == "VECTOR(384)"


def test_vectorfield_db_type_unchanged_for_other_vendors():
    """Make sure the MySQL branch didn't perturb pgvector / libsql /
    sqlite types."""
    from dorm.contrib.pgvector import VectorField

    class _PG:
        vendor = "postgresql"

    class _Libsql:
        vendor = "libsql"

    class _Sqlite:
        vendor = "sqlite"

    f = VectorField(dimensions=128)
    assert f.db_type(_PG()) == "vector(128)"
    assert f.db_type(_Libsql()) == "F32_BLOB(128)"
    assert f.db_type(_Sqlite()) == "BLOB"


# ──────────────────────────────────────────────────────────────────────────────
# Bug 5 — distance expressions on MySQL / MariaDB
# ──────────────────────────────────────────────────────────────────────────────


def test_l2distance_mysql_emits_vec_distance_euclidean():
    from dorm.contrib.pgvector import L2Distance

    class _MySQL:
        vendor = "mysql"

    sql, params = L2Distance("embedding", [0.1, 0.2, 0.3]).as_sql(
        connection=_MySQL()
    )
    assert "VEC_DISTANCE_EUCLIDEAN" in sql
    assert "VEC_FromBinary(%s)" in sql
    assert len(params) == 1 and isinstance(params[0], bytes)


def test_cosine_distance_mariadb_emits_vec_distance_cosine():
    from dorm.contrib.pgvector import CosineDistance

    class _Maria:
        vendor = "mariadb"

    sql, _ = CosineDistance("emb", [0.5, 0.6]).as_sql(connection=_Maria())
    assert "VEC_DISTANCE_COSINE" in sql


def test_max_inner_product_mysql_raises_not_implemented():
    """MariaDB / MySQL ship Euclidean + Cosine only; inner-product
    isn't there. Match the libsql / sqlite-vec error path so users
    get a consistent message across "vector-light" backends."""
    from dorm.contrib.pgvector import MaxInnerProduct

    class _MySQL:
        vendor = "mysql"

    with pytest.raises(NotImplementedError, match="MariaDB"):
        MaxInnerProduct("emb", [0.1]).as_sql(connection=_MySQL())


# ──────────────────────────────────────────────────────────────────────────────
# VectorExtension migration is a no-op on MySQL / MariaDB
# ──────────────────────────────────────────────────────────────────────────────


def test_vector_extension_is_noop_on_mysql():
    """VECTOR functions are built into MariaDB 11.7+ / MySQL 9.0+ —
    no ``CREATE EXTENSION`` to run."""
    from dorm.contrib.pgvector.operations import VectorExtension

    class _DummyConn:
        vendor = "mysql"
        executed: list = []

        def execute_script(self, sql: str) -> None:
            self.__class__.executed.append(sql)

    _DummyConn.executed.clear()
    op = VectorExtension()
    op.database_forwards("app", _DummyConn(), None, None)
    assert _DummyConn.executed == [], (
        "VectorExtension must be a no-op on MariaDB / MySQL"
    )


def test_vector_extension_backwards_is_noop_on_mariadb():
    from dorm.contrib.pgvector.operations import VectorExtension

    class _DummyConn:
        vendor = "mariadb"
        executed: list = []

        def execute_script(self, sql: str) -> None:
            self.__class__.executed.append(sql)

    _DummyConn.executed.clear()
    op = VectorExtension()
    op.database_backwards("app", _DummyConn(), None, None)
    assert _DummyConn.executed == []


# ──────────────────────────────────────────────────────────────────────────────
# VectorField wire format on MySQL = packed float32 (same as libsql / sqlite)
# ──────────────────────────────────────────────────────────────────────────────


def test_vectorfield_get_db_prep_value_mysql_uses_packed_float32():
    """All "vector-light" backends share the packed-float32 wire
    format on insert. Verifies the binder doesn't accidentally hand
    pgvector text-form to MariaDB / MySQL (which would fail at
    parse time)."""
    import struct
    from dorm.contrib.pgvector.fields import VectorField, _pack_float32

    field = VectorField(dimensions=3)
    field.name = "emb"
    # Stub get_connection to simulate a MySQL connection without
    # actually opening one — the field reads ``connection.vendor``.
    import dorm.db.connection as conn_mod
    real_get = conn_mod.get_connection

    class _MySQLStub:
        vendor = "mysql"

    conn_mod.get_connection = lambda *a, **k: _MySQLStub()  # ty:ignore[invalid-assignment]
    try:
        out = field.get_db_prep_value([0.1, 0.2, 0.3])
    finally:
        conn_mod.get_connection = real_get

    assert isinstance(out, bytes)
    expected = _pack_float32([0.1, 0.2, 0.3])
    assert out == expected
    # Sanity — round-trip through unpack matches.
    reconstructed = list(struct.unpack(f"<{3}f", out))
    assert all(abs(a - b) < 1e-6 for a, b in zip(reconstructed, [0.1, 0.2, 0.3]))
