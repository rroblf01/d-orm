import faulthandler
import json
import os
import sys
import tempfile
import time
import psycopg
from dorm import configure

import pytest
import sqlite3

from dorm.db.connection import reset_connections


# Route faulthandler output to a per-process file under
# ``$DORM_FAULT_DIR`` (default ``/tmp/dorm-faults``) so C-level
# crashes surface a native stack trace in the CI log.
# Opt-out via ``DORM_DISABLE_FAULTHANDLER=1``.
if not os.environ.get("DORM_DISABLE_FAULTHANDLER"):
    _fault_dir = os.environ.get("DORM_FAULT_DIR", "/tmp/dorm-faults")
    try:
        os.makedirs(_fault_dir, exist_ok=True)
        _fault_path = os.path.join(_fault_dir, f"pid-{os.getpid()}.log")
        _fault_fp = open(_fault_path, "a", buffering=1)
        faulthandler.enable(file=_fault_fp, all_threads=True)
    except OSError:
        # Fall back to stderr if the fault dir isn't writable — better
        # than silently disabling the handler.
        faulthandler.enable(file=sys.stderr, all_threads=True)

# Re-export the transactional_db fixtures so test files can request them
# by name. They live in dorm.test for end users; here we make them
# part of the conftest so our own suite exercises them too.
from dorm.test import transactional_db, atransactional_db  # noqa: F401


_db_fd, _db_path = tempfile.mkstemp(suffix=".db")
os.close(_db_fd)


def _docker_available() -> bool:
    try:
        import docker  # installed as a testcontainers dependency

        docker.from_env().ping()
        return True
    except Exception:
        return False


def _minio_test_deps_available() -> bool:
    """Returns True iff every dep needed for the live-MinIO S3 tests is
    importable: testcontainers' MinIO module, boto3, and a reachable
    Docker daemon. Tests that need MinIO call this — when False they
    skip cleanly so users without Docker (or without the optional
    boto3 extra) still get a green run."""
    if not _docker_available():
        return False
    try:
        import boto3  # noqa: F401
        from testcontainers.minio import MinioContainer  # noqa: F401

        return True
    except ImportError:
        return False


def _ci_postgres_available() -> bool:
    """The CI workflow exposes a real Postgres service via DORM_TEST_POSTGRES_*.
    When set, prefer it over testcontainers (faster, no Docker daemon needed)."""
    return bool(os.environ.get("DORM_TEST_POSTGRES_HOST"))


def _ci_mysql_available() -> bool:
    return bool(os.environ.get("DORM_TEST_MYSQL_HOST"))


def _backends() -> list[str]:
    backends = ["sqlite"]
    if _ci_postgres_available() or _docker_available():
        backends.append("postgres")
    if _ci_mysql_available():
        backends.append("mysql")
    return backends


def _shared_admin_dsn(tmp_path_factory) -> dict:
    """Return *admin* DSN info for a Postgres instance.

    Sources, in preference order:
      1. CI env vars DORM_TEST_POSTGRES_HOST/_PORT/_USER/_PASSWORD/_DB.
      2. A testcontainers Postgres whose connection info is cached in
         the per-pytest-run tmp dir (so it doesn't leak across runs).
    """
    if _ci_postgres_available():
        return {
            "host": os.environ["DORM_TEST_POSTGRES_HOST"],
            "port": int(os.environ.get("DORM_TEST_POSTGRES_PORT", "5432")),
            "user": os.environ["DORM_TEST_POSTGRES_USER"],
            "password": os.environ["DORM_TEST_POSTGRES_PASSWORD"],
            "base_db": os.environ.get("DORM_TEST_POSTGRES_DB", "postgres"),
        }

    shared_root = tmp_path_factory.getbasetemp()
    info_file = shared_root / "shared_pg.json"

    if info_file.exists():
        return json.loads(info_file.read_text())

    from testcontainers.postgres import PostgresContainer

    pg = PostgresContainer("postgres:16-alpine")
    pg.start()
    pg._connect()

    info = {
        "host": pg.get_container_host_ip(),
        "port": int(pg.get_exposed_port(5432)),
        "user": pg.username,
        "password": pg.password,
        "base_db": pg.dbname,
    }
    info_file.write_text(json.dumps(info))
    return info


def _shared_minio_endpoint(tmp_path_factory) -> dict:
    """Return endpoint info for a MinIO instance.

    Starts a ``MinioContainer`` and caches its coordinates in the
    per-pytest-run tmp dir. testcontainers' ryuk sidecar tears the
    container down at session end.
    """
    shared_root = tmp_path_factory.getbasetemp()
    info_file = shared_root / "shared_minio.json"

    if info_file.exists():
        return json.loads(info_file.read_text())

    from testcontainers.minio import MinioContainer

    # Pin to a known-good tag so a future MinIO release doesn't
    # silently change the API surface tests rely on. Bump
    # deliberately when reviewing changelogs.
    minio = MinioContainer(image="minio/minio:RELEASE.2025-04-22T22-12-26Z")
    minio.start()
    host = minio.get_container_host_ip()
    api_port = int(minio.get_exposed_port(9000))
    info = {
        "endpoint_url": f"http://{host}:{api_port}",
        "access_key": minio.access_key,
        "secret_key": minio.secret_key,
        "region_name": "us-east-1",
    }
    info_file.write_text(json.dumps(info))
    return info


def _wait_for_minio(
    endpoint_url: str, access_key: str, secret_key: str, timeout: float = 30.0
) -> None:
    """Poll the MinIO endpoint until it answers a ``list_buckets`` call.

    ``MinioContainer.start()`` returns when Docker reports the
    container running, but MinIO itself takes another second or two to
    be reachable. Without a ready-probe, the first test occasionally
    fails with a connection-refused before the in-container daemon
    starts listening."""
    import boto3
    from botocore.exceptions import EndpointConnectionError
    from botocore.client import Config

    deadline = time.time() + timeout
    last_exc: Exception | None = None
    cfg = Config(signature_version="s3v4", retries={"max_attempts": 1})
    while time.time() < deadline:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=cfg,
        )
        try:
            client.list_buckets()
            return
        except EndpointConnectionError as exc:
            last_exc = exc
            time.sleep(0.3)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.3)
    raise RuntimeError(f"MinIO not ready after {timeout}s: {last_exc}")


@pytest.fixture(scope="session")
def minio_endpoint(tmp_path_factory):
    """Session-scoped MinIO endpoint.

    Skips the requesting test when Docker / boto3 / testcontainers'
    MinIO module aren't available — same gating philosophy as the
    Postgres backend tests.
    """
    if not _minio_test_deps_available():
        pytest.skip(
            "MinIO live-tests skipped: needs Docker + boto3 + "
            "testcontainers[minio]. Install with `pip install "
            "'djanorm[dev,s3]'` and start Docker."
        )
    info = _shared_minio_endpoint(tmp_path_factory)
    _wait_for_minio(info["endpoint_url"], info["access_key"], info["secret_key"])
    return info


def _wait_for_postgres(host, port, user, password, timeout: float = 30.0) -> None:
    import psycopg

    deadline = time.time() + timeout
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            c = psycopg.connect(
                f"host={host} port={port} user={user} password={password} dbname=postgres",
                connect_timeout=2,
            )
            c.close()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(0.3)
    raise RuntimeError(f"PostgreSQL not ready after {timeout}s: {last_exc}")


@pytest.fixture(scope="session", params=_backends(), ids=_backends())
def db_config(request, tmp_path_factory):
    """Yield a DATABASES dict for each available backend."""
    if request.param == "sqlite":
        yield {"ENGINE": "sqlite", "NAME": _db_path}
        return

    if request.param == "mysql":
        base_db = os.environ.get("DORM_TEST_MYSQL_DB", "mysql")
        worker_db = "dorm_test"

        import pymysql

        admin = pymysql.connect(
            host=os.environ["DORM_TEST_MYSQL_HOST"],
            port=int(os.environ.get("DORM_TEST_MYSQL_PORT", "3306")),
            user=os.environ["DORM_TEST_MYSQL_USER"],
            password=os.environ["DORM_TEST_MYSQL_PASSWORD"],
            db=base_db,
            autocommit=True,
        )
        try:
            with admin.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS `{worker_db}`")
                cur.execute(f"CREATE DATABASE `{worker_db}`")
        finally:
            admin.close()

        yield {
            "ENGINE": "mysql",
            "NAME": worker_db,
            "USER": os.environ["DORM_TEST_MYSQL_USER"],
            "PASSWORD": os.environ["DORM_TEST_MYSQL_PASSWORD"],
            "HOST": os.environ["DORM_TEST_MYSQL_HOST"],
            "PORT": int(os.environ.get("DORM_TEST_MYSQL_PORT", "3306")),
        }
        return

    admin = _shared_admin_dsn(tmp_path_factory)
    _wait_for_postgres(admin["host"], admin["port"], admin["user"], admin["password"])

    worker_db = "dorm_test"

    import psycopg
    from psycopg import sql

    admin_dsn = (
        f"host={admin['host']} port={admin['port']} "
        f"user={admin['user']} password={admin['password']} dbname=postgres"
    )
    admin_conn = psycopg.connect(admin_dsn, autocommit=True)
    try:
        with admin_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(worker_db))
            )
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(worker_db)))
    finally:
        admin_conn.close()

    yield {
        "ENGINE": "postgresql",
        "NAME": worker_db,
        "USER": admin["user"],
        "PASSWORD": admin["password"],
        "HOST": admin["host"],
        "PORT": admin["port"],
        # Cap pool size so leftover connections from failed tests don't
        # exhaust the server.
        "MIN_POOL_SIZE": 1,
        "MAX_POOL_SIZE": 3,
    }


@pytest.fixture(scope="session", autouse=True)
def configure_dorm(db_config):
    reset_connections()
    configure(
        DATABASES={"default": db_config},
        INSTALLED_APPS=["tests"],
    )
    yield
    # Force sync release of any async wrappers left behind by the final
    # test before close_all(): otherwise pytest's unraisableexception
    # plugin runs gc.collect() during teardown and surfaces warnings for
    # SQLite connections the wrappers were still holding.
    from dorm.db.connection import _async_connections, close_all

    for conn in _async_connections.values():
        force = getattr(conn, "force_close_sync", None)
        if force is not None:
            try:
                force()
            except Exception:
                pass
    _async_connections.clear()
    close_all()


@pytest.fixture(autouse=True)
def clean_db(configure_dorm):
    """Drop and recreate test tables before each test.

    Per-test teardown also force-closes any async PG / async SQLite pool
    that the test opened. Without this, async pools live until the
    *next* test calls ``reset_connections()`` — and on Python 3.14 +
    ``psycopg_pool``, the GC sometimes reaches the lingering pool
    BEFORE the next test starts, runs ``__del__`` against an already-
    closing session loop, and takes the process down with a SIGSEGV.

    The yield-based shape gives us the post-test hook we need: drain
    every async pool's libpq sockets synchronously so ``__del__`` is
    a no-op by the time GC reaches it.
    """
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    reset_connections()
    conn = get_connection()

    # Drop in dependency order (referencing tables first).
    # Use CASCADE on PostgreSQL to handle orphaned FK constraints from migration tests.
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    for tbl in ["articles_tags", "books", "articles", "tags", "authors", "publishers"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    from tests.models import Article, Author, Book, Publisher, Tag  # noqa: PLC0415

    for model, tbl in [
        (Publisher, "publishers"),
        (Author, "authors"),
        (Book, "books"),
        (Tag, "tags"),
        (Article, "articles"),
    ]:
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

    # Junction table for Article.tags (ManyToManyField)
    vendor = getattr(conn, "vendor", "sqlite")
    pk_type = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if vendor == "sqlite"
        else "SERIAL PRIMARY KEY"
    )
    conn.execute_script(
        f'CREATE TABLE IF NOT EXISTS "articles_tags" (\n'
        f'  "id" {pk_type},\n'
        f'  "article_id" BIGINT NOT NULL REFERENCES "articles"("id"),\n'
        f'  "tag_id" BIGINT NOT NULL REFERENCES "tags"("id")\n'
        f")"
    )

    yield

    # Post-test teardown: force-close every async wrapper opened by the
    # test. Without this, an async pool can survive into the next test;
    # on Python 3.14 the GC sometimes reaches it BEFORE
    # ``reset_connections`` does, finalises against a session loop in an
    # inconsistent state, and SIGSEGVs the process.
    from dorm.db.connection import _async_connections

    for async_conn in list(_async_connections.values()):
        force = getattr(async_conn, "force_close_sync", None)
        if force is not None:
            try:
                force()
            except Exception:
                pass
    _async_connections.clear()


@pytest.fixture
def db_connection():
    """Fixture para manejar la conexión a la base de datos SQLite."""
    conn = sqlite3.connect(":memory:")  # Base de datos en memoria
    yield conn
    conn.close()


@pytest.fixture
def direct_pg_connection(db_config):
    """Fixture para obtener una
    conexión directa a PostgreSQL usando db_config."""
    if db_config.get("ENGINE") != "postgresql":
        pytest.skip("Este test requiere PostgreSQL como backend.")

    conn = psycopg.connect(
        dbname=db_config.get("NAME"),
        user=db_config.get("USER"),
        password=db_config.get("PASSWORD"),
        host=db_config.get("HOST", "localhost"),
        port=db_config.get("PORT", 5432),
    )
    yield conn
    conn.close()
