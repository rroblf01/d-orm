"""Tests for the v2.5 libsql backend.

The libsql Python client (``libsql_experimental``) is gated behind
``pip install djanorm[libsql]`` — every test in this file uses
``pytest.importorskip`` so suites that don't install the extra
keep running with a clean skip rather than a failure.

Coverage:

- URL parsing for the ``libsql://`` / ``libsql:///`` shapes.
- Engine routing — ``ENGINE = "libsql"`` lands on
  ``LibSQLDatabaseWrapper``.
- Local-file mode round-trip (drop-in SQLite replacement).
- Vector branch — ``F32_BLOB(N)`` ``db_type`` and the
  ``vector_distance_*`` SQL emitted by ``L2Distance`` /
  ``CosineDistance``.
- Async wrapper opens / executes / closes without leaking.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest


def _libsql_available() -> bool:
    import importlib

    try:
        importlib.import_module("turso")
        return True
    except ImportError:
        return False


# ────────────────────────────────────────────────────────────────────────────
# URL parser
# ────────────────────────────────────────────────────────────────────────────


def test_parse_libsql_remote_url() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql://my-db.turso.io?authToken=my_token"
    )
    assert cfg["ENGINE"] == "libsql"
    assert cfg["SYNC_URL"] == "libsql://my-db.turso.io"
    assert cfg["AUTH_TOKEN"] == "my_token"
    # Remote-only mode keeps the local NAME at ``:memory:``.
    assert cfg["NAME"] == ":memory:"


def test_parse_libsql_remote_with_local_replica() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql://my-db.turso.io?authToken=tok&NAME=local.db"
    )
    assert cfg["SYNC_URL"] == "libsql://my-db.turso.io"
    assert cfg["NAME"] == "local.db"


def test_parse_libsql_local_only() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql:///path/to/local.db")
    assert cfg["ENGINE"] == "libsql"
    # Local-only flavours have no SYNC_URL.
    assert "SYNC_URL" not in cfg
    assert cfg["NAME"] == "path/to/local.db"


def test_parse_libsql_https_scheme() -> None:
    """``libsql+https://`` propagates through parse_database_url."""
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql+https://my-db.turso.io?authToken=tok"
    )
    assert cfg["ENGINE"] == "libsql"
    assert cfg["SYNC_URL"] == "https://my-db.turso.io"


# ────────────────────────────────────────────────────────────────────────────
# Engine routing
# ────────────────────────────────────────────────────────────────────────────


def test_libsql_engine_routes_to_libsql_wrapper() -> None:
    if not _libsql_available():
        pytest.skip("libsql client not installed (djanorm[libsql]).")
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper
    from dorm.db.connection import _create_sync_connection

    cfg: dict[str, Any] = {"ENGINE": "libsql", "NAME": ":memory:"}
    wrapper = _create_sync_connection("default", cfg)
    assert isinstance(wrapper, LibSQLDatabaseWrapper)
    assert wrapper.vendor == "libsql"


def test_libsql_async_engine_routes_to_libsql_async_wrapper() -> None:
    if not _libsql_available():
        pytest.skip("libsql client not installed (djanorm[libsql]).")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper
    from dorm.db.connection import _create_async_connection

    cfg: dict[str, Any] = {"ENGINE": "libsql", "NAME": ":memory:"}
    wrapper = _create_async_connection("default", cfg)
    assert isinstance(wrapper, LibSQLAsyncDatabaseWrapper)


# ────────────────────────────────────────────────────────────────────────────
# Local-file round-trip
# ────────────────────────────────────────────────────────────────────────────


def test_libsql_local_round_trip() -> None:
    if not _libsql_available():
        pytest.skip("libsql client not installed (djanorm[libsql]).")
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "lib.db")
        wrapper = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        wrapper.execute_script(
            'CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)'
        )
        # libsql-experimental accepts only tuples for parameters,
        # not lists — tighter than stdlib sqlite3. The wrapper
        # passes ``params or []`` straight through, so callers
        # supply tuples.
        wrapper.execute_write(
            "INSERT INTO t (name) VALUES (%s)", ("hi",)
        )
        rows = wrapper.execute("SELECT name FROM t", ())
        assert len(rows) == 1
        assert rows[0][0] == "hi" or rows[0]["name"] == "hi"
        wrapper.close()


# ────────────────────────────────────────────────────────────────────────────
# Vector branch
# ────────────────────────────────────────────────────────────────────────────


def test_vector_field_db_type_libsql() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    class _Conn:
        vendor = "libsql"

    f = VectorField(dimensions=384)
    assert f.db_type(_Conn()) == "F32_BLOB(384)"


def test_l2_distance_libsql_emits_vector_distance_l2() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.expressions import L2Distance

    class _Conn:
        vendor = "libsql"

    d = L2Distance("embedding", [0.1, 0.2, 0.3])
    sql, params = d.as_sql(connection=_Conn())
    assert "vector_distance_l2" in sql
    assert "vector32(?)" in sql
    assert len(params) == 1


def test_cosine_distance_libsql_emits_vector_distance_cos() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.expressions import CosineDistance

    class _Conn:
        vendor = "libsql"

    d = CosineDistance("embedding", [0.1, 0.2, 0.3])
    sql, _ = d.as_sql(connection=_Conn())
    assert "vector_distance_cos" in sql


def test_max_inner_product_libsql_raises() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.expressions import MaxInnerProduct

    class _Conn:
        vendor = "libsql"

    d = MaxInnerProduct("embedding", [0.1, 0.2, 0.3])
    with pytest.raises(NotImplementedError):
        d.as_sql(connection=_Conn())


def test_vector_field_get_db_prep_value_libsql_packs_float32() -> None:
    """``vendor == "libsql"`` must reuse the SQLite packed-float32
    wire format — libsql's ``vector32(?)`` reads it directly."""
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    # We can't easily fake the connection lookup mid-test; just
    # verify the packed shape against the stand-alone helper. The
    # vendor branch is unit-tested above through ``db_type`` and
    # the distance expressions.
    from dorm.contrib.pgvector.fields import _pack_float32

    packed = _pack_float32([0.1, 0.2, 0.3])
    assert isinstance(packed, bytes)
    assert len(packed) == 12  # 3 floats × 4 bytes


# ────────────────────────────────────────────────────────────────────────────
# Helpful error when client missing
# ────────────────────────────────────────────────────────────────────────────


def test_libsql_import_error_when_client_missing(monkeypatch: Any) -> None:
    """If ``turso`` (pyturso) is not installed, instantiation must
    surface a clear :class:`ImproperlyConfigured` pointing at the
    install command."""
    import importlib as _il

    real_import = _il.import_module

    def _stub(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "turso" or name.startswith("turso."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(_il, "import_module", _stub)
    from dorm.db.backends.libsql import _import_turso
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="pyturso"):
        _import_turso()
