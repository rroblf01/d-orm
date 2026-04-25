import json
import os
import tempfile
import time

import pytest

import dorm
from dorm.db.connection import reset_connections


_db_fd, _db_path = tempfile.mkstemp(suffix=".db")
os.close(_db_fd)


def _docker_available() -> bool:
    try:
        import docker  # installed as a testcontainers dependency
        docker.from_env().ping()
        return True
    except Exception:
        return False


def _ci_postgres_available() -> bool:
    """The CI workflow exposes a real Postgres service via DORM_TEST_POSTGRES_*.
    When set, prefer it over testcontainers (faster, no Docker daemon needed)."""
    return bool(os.environ.get("DORM_TEST_POSTGRES_HOST"))


def _backends() -> list[str]:
    backends = ["sqlite"]
    if _ci_postgres_available() or _docker_available():
        backends.append("postgres")
    return backends


def _shared_admin_dsn(tmp_path_factory, worker_id: str) -> dict:
    """Return *admin* DSN info for a Postgres instance shared across xdist workers.

    Sources, in preference order:
      1. CI env vars DORM_TEST_POSTGRES_HOST/_PORT/_USER/_PASSWORD/_DB.
      2. A single testcontainers Postgres started by whichever xdist worker
         arrives first; subsequent workers read its connection info from a
         file in the *per-pytest-run* tmp dir (so the file doesn't leak
         across pytest invocations and point at a dead container).

    The previous behaviour of one container per worker consistently raced
    docker into killing 3 of 4 containers under ``pytest -n 4`` — only one
    survived, leaving the other workers waiting forever.
    """
    if _ci_postgres_available():
        return {
            "host": os.environ["DORM_TEST_POSTGRES_HOST"],
            "port": int(os.environ.get("DORM_TEST_POSTGRES_PORT", "5432")),
            "user": os.environ["DORM_TEST_POSTGRES_USER"],
            "password": os.environ["DORM_TEST_POSTGRES_PASSWORD"],
            "base_db": os.environ.get("DORM_TEST_POSTGRES_DB", "postgres"),
        }

    # Resolve a tmp dir that is shared between xdist workers of the SAME
    # pytest run, but NOT across pytest invocations.
    #   - master mode: getbasetemp() is `pytest-N/`. Use it directly.
    #   - xdist worker: getbasetemp() is `pytest-N/popen-gwK/`. Its parent
    #     (`pytest-N/`) is shared with sibling workers.
    bt = tmp_path_factory.getbasetemp()
    shared_root = bt if worker_id == "master" else bt.parent
    info_file = shared_root / "shared_pg.json"
    lock_path = shared_root / "shared_pg.lock"

    import fcntl
    fh = open(str(lock_path), "a+")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX)

        if info_file.exists():
            return json.loads(info_file.read_text())

        # First worker: spawn the container. Don't wrap in `with` — we need
        # it alive past this fixture's exit. testcontainers' ryuk sidecar
        # cleans it up at session end. Bump max_connections so xdist
        # workers leaving stale conns across event-loop transitions don't
        # exhaust the server (default 100 is tight for 4 workers × pools).
        from testcontainers.postgres import PostgresContainer
        pg = PostgresContainer("postgres:16-alpine")
        pg.with_command(["postgres", "-c", "max_connections=500"])
        pg.start()
        # PostgresContainer.start() returns when the docker container is
        # running, but PG may still be initializing internally. _connect()
        # polls `psql -c 'select version();'` until it succeeds.
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
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


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
def db_config(request, tmp_path_factory, worker_id):
    """Yield a DATABASES dict for each available backend.

    For PostgreSQL, **one** Postgres instance is shared across all xdist
    workers; each worker gets its own database (named after PYTEST_XDIST_WORKER)
    so parallel suites don't trample each other.
    """
    if request.param == "sqlite":
        yield {"ENGINE": "sqlite", "NAME": _db_path}
        return

    admin = _shared_admin_dsn(tmp_path_factory, worker_id)
    _wait_for_postgres(admin["host"], admin["port"], admin["user"], admin["password"])

    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    base_db = admin["base_db"]
    worker_db = f"dorm_test_{worker}" if worker != "main" else base_db

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
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(worker_db))
            )
    finally:
        admin_conn.close()

    yield {
        "ENGINE": "postgresql",
        "NAME": worker_db,
        "USER": admin["user"],
        "PASSWORD": admin["password"],
        "HOST": admin["host"],
        "PORT": admin["port"],
        # 4 xdist workers × 10 (default) = 40 active conns; with both sync
        # and async pool plus the admin conn we used for CREATE DATABASE,
        # we get close to PG's default max_connections=100. Cap tightly so
        # cross-worker xdist runs don't exhaust the server when leftover
        # connections from failed tests leak.
        "MIN_POOL_SIZE": 1,
        "MAX_POOL_SIZE": 3,
    }


@pytest.fixture(scope="session", autouse=True)
def configure_dorm(db_config):
    reset_connections()
    dorm.configure(
        DATABASES={"default": db_config},
        INSTALLED_APPS=["tests"],
    )
    yield
    from dorm.db.connection import close_all
    close_all()


@pytest.fixture(autouse=True)
def clean_db(configure_dorm):
    """Drop and recreate test tables before each test."""
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
    pk_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if vendor == "sqlite" else "SERIAL PRIMARY KEY"
    conn.execute_script(
        f'CREATE TABLE IF NOT EXISTS "articles_tags" (\n'
        f'  "id" {pk_type},\n'
        f'  "article_id" BIGINT NOT NULL REFERENCES "articles"("id"),\n'
        f'  "tag_id" BIGINT NOT NULL REFERENCES "tags"("id")\n'
        f")"
    )
