import os
import tempfile

import pytest

import dorm
from dorm.db.connection import reset_connections


# Shared temp DB file for the whole session
_db_fd, _db_path = tempfile.mkstemp(suffix=".db")
os.close(_db_fd)


@pytest.fixture(scope="session", autouse=True)
def configure_dorm():
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": _db_path}},
        INSTALLED_APPS=["tests"],
    )


@pytest.fixture(autouse=True)
def clean_db(configure_dorm):
    """Wipe all tables before each test and recreate them."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    reset_connections()
    conn = get_connection()

    # Drop all known test tables
    for tbl in ["books", "authors"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"')

    # Recreate Author table
    from tests.models import Author, Book  # noqa: PLC0415
    for model, tbl in [(Author, "authors"), (Book, "books")]:
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
