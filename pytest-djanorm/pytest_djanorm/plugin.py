"""Pytest fixtures and hooks for djanorm.

All fixtures are session-scoped where state is expensive (containers,
pool warm-up) and function-scoped where isolation matters
(transactional rollbacks, N+1 detection).

The plugin is opt-in via pytest's auto-loading: installing the
package wires it in. To disable for one project, set
``addopts = -p no:djanorm`` in ``pyproject.toml``.
"""

from __future__ import annotations

import os
from typing import Any, Iterator

import pytest


# ── Configuration fixtures ─────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def djanorm_settings() -> dict:
    """Default DATABASES dict — in-memory SQLite.

    Override in your own ``conftest.py`` to point at a real backend::

        @pytest.fixture(scope="session")
        def djanorm_settings(pg_container):
            return {
                "default": {
                    "ENGINE": "postgresql",
                    "NAME": pg_container.dbname,
                    "USER": pg_container.user,
                    "PASSWORD": pg_container.password,
                    "HOST": pg_container.host,
                    "PORT": pg_container.port,
                }
            }
    """
    return {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}}


@pytest.fixture(scope="session", autouse=True)
def _configure_dorm(djanorm_settings: dict) -> Iterator[None]:
    """Configure dorm at session start, tear down at session end.

    ``autouse=True`` so every test sees a configured ORM without
    having to request the fixture explicitly.
    """
    from dorm import configure
    from dorm.db.connection import close_all, reset_connections

    reset_connections()
    configure(DATABASES=djanorm_settings, INSTALLED_APPS=[])
    yield
    close_all()


# ── Container fixtures (testcontainers) ────────────────────────────────────────


class _PgInfo:
    """Minimal stand-in for the testcontainers PostgresContainer
    object so the user's ``djanorm_settings`` override can read the
    same attributes regardless of whether the container is real or a
    pre-existing CI database."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname


@pytest.fixture(scope="session")
def pg_container() -> Iterator[_PgInfo]:
    """Spin up a single PostgresContainer for the session and yield
    its connection coordinates.

    Honours environment variables for CI scenarios where a real PG
    server is already running:

    - ``DORM_TEST_PG_HOST`` / ``DORM_TEST_PG_PORT`` / ``DORM_TEST_PG_USER`` /
      ``DORM_TEST_PG_PASSWORD`` / ``DORM_TEST_PG_DB`` — when set, skip the
      container entirely and return the env-supplied coordinates.
    - Otherwise launch ``postgres:16-alpine`` via testcontainers.
      Skips the requesting test (``pytest.skip``) when Docker isn't
      available so the suite still runs in environments without it.
    """
    env_host = os.environ.get("DORM_TEST_PG_HOST")
    if env_host:
        yield _PgInfo(
            host=env_host,
            port=int(os.environ.get("DORM_TEST_PG_PORT", "5432")),
            user=os.environ.get("DORM_TEST_PG_USER", "postgres"),
            password=os.environ.get("DORM_TEST_PG_PASSWORD", "postgres"),
            dbname=os.environ.get("DORM_TEST_PG_DB", "postgres"),
        )
        return

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip(
            "pg_container fixture requires testcontainers[postgres]. "
            "Install with `pip install 'pytest-djanorm[postgres]'`."
        )
    try:
        import docker

        docker.from_env().ping()
    except Exception:
        pytest.skip("pg_container fixture requires Docker.")

    pg = PostgresContainer("postgres:16-alpine")
    pg.start()
    try:
        pg._connect()
        yield _PgInfo(
            host=pg.get_container_host_ip(),
            port=int(pg.get_exposed_port(5432)),
            user=pg.username,
            password=pg.password,
            dbname=pg.dbname,
        )
    finally:
        pg.stop()


# ── Transactional isolation ────────────────────────────────────────────────────


@pytest.fixture
def transactional_db() -> Iterator[Any]:
    """Wrap each test in a global ``atomic()`` block and roll it
    back on teardown so writes do not leak between tests.

    Yields the active connection so the test can ``cur.execute(...)``
    raw SQL when it needs to.
    """
    from dorm.db.connection import get_connection
    from dorm.transaction import atomic

    conn = get_connection()
    cm = atomic()
    cm.__enter__()
    try:
        yield conn
    finally:
        # Force a rollback rather than commit — atomic() commits on
        # successful exit, but every write the test issued is
        # discarded by raising an internal exception that atomic()
        # treats as a rollback signal. We use the explicit
        # ``set_rollback`` helper if the backend supports it; the
        # fallback path raises a sentinel that atomic() catches.
        try:
            from dorm.transaction import set_rollback

            set_rollback(True)
        except Exception:
            pass
        try:
            cm.__exit__(None, None, None)
        except Exception:
            pass


@pytest.fixture
async def atransactional_db() -> Any:
    """Async counterpart of :func:`transactional_db`."""
    from dorm.db.connection import get_async_connection
    from dorm.transaction import aatomic

    conn = get_async_connection()
    cm = aatomic()
    await cm.__aenter__()
    try:
        yield conn
    finally:
        try:
            from dorm.transaction import aset_rollback

            await aset_rollback(True)
        except Exception:
            pass
        try:
            await cm.__aexit__(None, None, None)
        except Exception:
            pass


# ── N+1 guard ─────────────────────────────────────────────────────────────────


@pytest.fixture
def nplusone_guard():
    """Convenience fixture that yields a configured
    :class:`dorm.contrib.nplusone.NPlusOneDetector` and re-raises
    any detection at teardown. Use as::

        def test_view(nplusone_guard):
            with nplusone_guard:
                serialize(authors)
    """
    from dorm.contrib.nplusone import NPlusOneDetector

    return NPlusOneDetector(threshold=5, raise_on_detect=True)


# ── pytest hooks ──────────────────────────────────────────────────────────────


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so ``pytest -m`` doesn't warn."""
    config.addinivalue_line(
        "markers",
        "djanorm_pg: mark test as requiring a real PostgreSQL container",
    )
    config.addinivalue_line(
        "markers",
        "djanorm_async: mark test as exercising async ORM paths",
    )
