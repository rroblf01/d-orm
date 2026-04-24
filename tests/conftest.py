import os
import tempfile

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


def _backends() -> list[str]:
    backends = ["sqlite"]
    if _docker_available():
        backends.append("postgres")
    return backends


@pytest.fixture(scope="session", params=_backends(), ids=_backends())
def db_config(request):
    """Yield a DATABASES dict for each available backend."""
    if request.param == "sqlite":
        yield {"ENGINE": "sqlite", "NAME": _db_path}
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield {
            "ENGINE": "postgresql",
            "NAME": pg.dbname,
            "USER": pg.username,
            "PASSWORD": pg.password,
            "HOST": pg.get_container_host_ip(),
            "PORT": int(pg.get_exposed_port(5432)),
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
